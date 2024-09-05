package server

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/core/command"
	"github.com/Viet-ph/redis-go/core/info"
	"github.com/Viet-ph/redis-go/core/proto"

	"github.com/Viet-ph/redis-go/core/connection"
	"github.com/Viet-ph/redis-go/datastore"
	mul "github.com/Viet-ph/redis-go/internal/multiplexer"
	"golang.org/x/sys/unix"
)

type AsyncServer struct {
	// ConnectedClients  map[int]*connection.Conn    //   = make(map[int]*core.Conn)
	// ConnectedReplicas map[int]*connection.Replica // = make(map[int]*rep.Replica)

	iomultiplexer mul.Iomuliplexer
	fd            int
	maxClients    int
	store         *datastore.Datastore
	master        *connection.Conn
}

func NewAsyncServer(masterConn *connection.Conn) (*AsyncServer, error) {
	serverFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return &AsyncServer{}, err
	}

	if err := unix.SetsockoptInt(serverFD, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return &AsyncServer{}, err
	}

	// Set the Socket operate in a non-blocking mode
	if err := unix.SetNonblock(serverFD, true); err != nil {
		return &AsyncServer{}, err
	}

	// Bind the IP and the port
	ip4 := net.ParseIP(config.Host)

	if err := unix.Bind(serverFD, &unix.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return &AsyncServer{}, err
	}

	return &AsyncServer{
		fd:         serverFD,
		maxClients: 100,
		store:      datastore.NewDatastore(),
		master:     masterConn,
	}, nil
}

func (server *AsyncServer) Start() {
	defer server.close()

	// Start listening
	err := unix.Listen(server.fd, server.maxClients)
	if err != nil {
		fmt.Println("error while listening", err)
		os.Exit(1)
	}

	//Start of IO multiplexing
	fmt.Println("ready to accept connections")

	// Create an epoll instance
	server.iomultiplexer, err = mul.New(server.maxClients)
	if err != nil {
		fmt.Println("Error creating epoll instance", err)
		os.Exit(1)
	}

	// Add listener socket to epoll
	err = server.iomultiplexer.AddWatchFd(server.fd, mul.OpRead)
	if err != nil {
		fmt.Println("Error adding listener to epoll:", err)
		os.Exit(1)
	}

	// Add master socket to epoll if has any
	server.setupMaster()

	//Start event loop
	for {
		events, err := server.iomultiplexer.Poll(-1)
		fmt.Println("polled " + strconv.Itoa(len(events)) + " events")
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			fmt.Println("Error during epoll wait:" + err.Error())
			return
		}

		for _, event := range events {
			fd := mul.GetFdFromEvent(event)
			if fd == server.fd {
				err = server.acceptNewConnection()
				if err != nil {
					fmt.Println("Error connecting to client: ", err)
					continue
				}

			} else {
				if server.iomultiplexer.IsReadable(event) {
					if conn, exists := server.getConn(fd); exists {
						err := server.handleReadableEvent(conn)
						//Close connection on data copy from kernel space -> user space/command parsing errors
						if err != nil {
							fmt.Println("Error occured while serving client: " + err.Error())
							server.CloseConnecttion(conn)
							continue
						}
					} else {
						continue
					}
				}
				if server.iomultiplexer.IsWritable(event) {
					if client, exists := connection.ConnectedClients[fd]; exists {
						server.handleWritableEvent(client)
					} else {
						continue
					}
				}
			}
		}
	}
}

func (server *AsyncServer) acceptNewConnection() error {
	connFD, sa, err := unix.Accept(server.fd)
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return err
	}

	//Set new client socket fd as non-block so it wont block
	//the current thread while waiting for NIC doing its job
	err = unix.SetNonblock(connFD, true)
	if err != nil {
		fmt.Printf("Error setting file descriptor %d as non-block: %v\n", connFD, err)
		return err
	}

	//Add new client socket fd to epoll interesting list
	err = server.iomultiplexer.AddWatchFd(connFD, mul.OpRead)
	if err != nil {
		fmt.Println("Error subscribing client file descriptor to epoll:", err)
		return err
	}

	//Add new client into connected clients map
	conn, err := connection.NewConn(connFD, sa)
	if err != nil {
		fmt.Println("Error create new connection: ", err)
		return err
	}
	connection.ConnectedClients[int(connFD)] = conn

	//Print out client's ip and port
	ip, port := connection.ConnectedClients[int(connFD)].GetRemoteAddress()
	fmt.Printf("Client connected: IP = %s, Port = %d\n", ip.String(), port)

	return nil
}

func (server *AsyncServer) handleReadableEvent(conn *connection.Conn) error {
	//Read message sent from client
	underlyBuf := make([]byte, 0, config.DefaultMessageSize)
	buffer := bytes.NewBuffer(underlyBuf)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	//If it's a command, parse it into command object
	cmd, err := command.Parse(buffer)
	if err != nil {
		return fmt.Errorf("error parsing command")
	}

	//Propagate command to slaves if possible
	if info.Role == "master" && command.IsWriteCommand(cmd) {
		server.propagateCmd(underlyBuf[:bytesRead])
	}

	//Execute command
	result := command.ExecuteCmd(cmd, server.store)

	//Return early here if the executed comamnd is "Write" command,
	//current role is "slave" and the conn on received event is the "master"
	if info.Role == "slave" && connection.IsMaster(conn) && command.IsWriteCommand(cmd) {
		//Update replication offset
		connection.ReplicationOffset += bytesRead
		return nil
	}

	//Send result as response back to client and handle any possible errors
	server.respond(conn, cmd, result)

	return nil
}

func (server *AsyncServer) respond(conn *connection.Conn, cmd command.Command, result any) {
	encoder := proto.NewEncoder()
	err := encoder.Encode(result, true)
	if err != nil {
		encoder.Reset()
		result = errors.New("error encoding resp")
		encoder.Encode(result, false)
	}

	byteSliceResult := encoder.GetBufValue()

	//Queue datas to write and write immediately after
	if strings.Contains(cmd.Cmd, "PSYNC") {
		rdb, _ := core.RdbMarshall()
		err = conn.QueueDatas(byteSliceResult, rdb)
		server.promoteToSlave(conn)
	} else {
		err = conn.QueueDatas(byteSliceResult)
	}
	if err != nil {
		server.handleWritingError(err, conn)
	}
}

func (server *AsyncServer) handleWritableEvent(conn *connection.Conn) {
	fmt.Println("Seding response...")
	err := conn.DrainQueue()
	if err != nil {
		server.handleWritingError(err, conn)
		return
	}

	//Successfully drained and wrote all datas in queue,
	//modify fd to be polled on read event only
	server.iomultiplexer.ModifyWatchingFd(conn.Fd, mul.OpRead)
}

func (server *AsyncServer) handleWritingError(err error, conn *connection.Conn) {
	if err == core.ErrorNotFullyWritten {
		//Data not fully written, resubscribe with write event and return to wait for write event
		fmt.Printf("Got write error: %v\n", err.Error())
		server.iomultiplexer.ModifyWatchingFd(conn.Fd, mul.OpWrite)
		return
	}
	// Handle other errors (e.g., client disconnected)	 	 ``
	fmt.Printf("Got unexpected write error: %v\n", err.Error())
	server.CloseConnecttion(conn)
}

// Function to handle client disconnection or cleanup
func (server *AsyncServer) CloseConnecttion(client *connection.Conn) {
	// Close the socket, cleanup resources
	// Remove FD from epoll interest list
	server.iomultiplexer.RemoveWatchFd(client.Fd)
	client.Close()
	// Remove client from your client management structures
	delete(connection.ConnectedClients, client.Fd)
	delete(connection.ConnectedReplicas, client.Fd)

	ip, port := client.GetRemoteAddress()
	fmt.Printf("Client disconnected. IP = %s, Port = %d\n", ip.String(), port)
}

func (server *AsyncServer) close() {
	server.iomultiplexer.Close()
	unix.Close(server.fd)
}

func (server *AsyncServer) propagateCmd(rawCmd []byte) {
	if len(connection.ConnectedReplicas) == 0 {
		return
	}

	for _, replica := range connection.ConnectedReplicas {
		err := replica.Propagate(rawCmd)
		if err != nil {
			fmt.Println("Error propergate command to slave: " + err.Error())
			continue
		}
	}
}

func (server *AsyncServer) promoteToSlave(conn *connection.Conn) {
	connection.ConnectedReplicas[conn.Fd] = connection.NewReplica(conn)
	delete(connection.ConnectedClients, conn.Fd)
}

func (server *AsyncServer) setupMaster() error {
	if info.Role == "master" && server.master == nil {
		return nil
	}

	fd := server.master.Fd
	//Set new client socket fd as non-block so it wont block
	//the current thread while waiting for NIC doing its job
	err := unix.SetNonblock(fd, true)
	if err != nil {
		fmt.Printf("Error setting file descriptor %d as non-block: %v\n", fd, err)
		return err
	}

	err = server.iomultiplexer.AddWatchFd(fd, mul.OpRead)
	if err != nil {
		return fmt.Errorf("error adding master fd to epoll: " + err.Error())
	}

	return nil
}

func (server *AsyncServer) getConn(fd int) (*connection.Conn, bool) {
	if conn, exist := connection.ConnectedClients[fd]; exist {
		return conn, true
	}

	if replica, exist := connection.ConnectedReplicas[fd]; exist {
		return replica.GetConn(), true
	}

	if server.master != nil && server.master.Fd == fd {
		return server.master, true
	}

	return nil, false
}

