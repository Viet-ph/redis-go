package server

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/internal/command"
	custom_err "github.com/Viet-ph/redis-go/internal/error"
	"github.com/Viet-ph/redis-go/internal/info"
	"github.com/Viet-ph/redis-go/internal/proto"
	"github.com/Viet-ph/redis-go/internal/rdb"

	"github.com/Viet-ph/redis-go/internal/connection"
	"github.com/Viet-ph/redis-go/internal/datastore"
	mul "github.com/Viet-ph/redis-go/internal/multiplexer"
	"github.com/Viet-ph/redis-go/internal/queue"
	"golang.org/x/sys/unix"
)

type AsyncServer struct {
	iomultiplexer mul.Iomuliplexer
	fd            int
	store         *datastore.Datastore
	master        *connection.Conn
	taskQueue     *queue.TaskQueue
	cmdHandler    *command.Handler
}

func NewAsyncServer(masterConn *connection.Conn, masterDatastore *datastore.Datastore) (*AsyncServer, error) {
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

	taskQueue := queue.NewTaskQueue()
	handler := command.NewCmdHandler(taskQueue)
	command.SetupCommands(handler)

	// Master instance should read and unmarshall RDB file if has any
	var (
		expiry  map[string]time.Time
		storage map[string]*datastore.Data
		ds      *datastore.Datastore
	)
	if masterConn == nil && masterDatastore == nil {
		rawRdb, err := rdb.ReadRdbFile()
		if err != nil {
			return nil, err
		}

		if len(rawRdb) != 0 {
			storage, expiry, err = rdb.RdbUnMarshall(rawRdb)
			if err != nil {
				return nil, err
			}
			fmt.Println(storage)
		} else {
			storage = make(map[string]*datastore.Data)
			expiry = make(map[string]time.Time)
		}
		ds = datastore.NewDatastore(storage, expiry)
		rdb.PersistData(ds)
	} else {
		ds = masterDatastore
	}

	server := &AsyncServer{
		fd:         serverFD,
		store:      ds,
		master:     masterConn,
		taskQueue:  taskQueue,
		cmdHandler: handler,
	}

	return server, nil
}

func (server *AsyncServer) Start() {
	defer server.close()

	// Start listening
	err := unix.Listen(server.fd, config.MaximumClients)
	if err != nil {
		fmt.Println("error while listening", err)
		os.Exit(1)
	}

	//Start of IO multiplexing
	fmt.Println("ready to accept connections")

	// Create an epoll instance
	server.iomultiplexer, err = mul.New(config.MaximumClients)
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
		//Check task queue for any tasks that available
		server.taskQueue.DrainQueue()

		events, err := server.iomultiplexer.Poll(100)
		if len(events) > 0 {
			fmt.Println("polled " + strconv.Itoa(len(events)) + " events")
		}
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
	command.OffsTracking[conn] = &command.OffsTracker{CapturedOffs: 0}

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

	var rawCommand []byte
	if info.Role == "master" {
		// Make a seperate byte slice of the received command to propagate to replicas
		rawCommand = make([]byte, bytesRead)
		copy(rawCommand, underlyBuf[:bytesRead])
	}

	//If it's a command, parse it into command object
	cmd, err := command.Parse(buffer)
	if err != nil {
		return fmt.Errorf("error parsing command")
	}
	fmt.Println(cmd)

	// Keep track of latest client and replica serving
	err = server.cmdHandler.SetCurrentConn(conn)
	if err != nil {
		return err
	}

	//Execute command
	result, readyToRespond := command.ExecuteCmd(cmd, server.store)

	//Send result as response back to client and handle any possible errors
	if readyToRespond {
		server.respond(conn, cmd, result)
	}

	//Propagate command to slaves if has any
	if info.Role == "master" && command.IsWriteCommand(cmd) {
		server.propagateCmd(rawCommand)
		tracker := command.OffsTracking[conn]
		tracker.CapturedOffs += bytesRead
	}

	if command.IsWriteCommand(cmd) {
		// Master and replicas must keep track of the offset
		info.ReplicationOffset += bytesRead
	}
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
		rawRdb, _ := rdb.ReadRdbFile()
		if len(rawRdb) == 0 {
			rawRdb, _ = rdb.RdbMarshall(server.store)
		}
		err = conn.QueueDatas(byteSliceResult, rawRdb)
		fmt.Printf("Accepted replicatiobn: %d\n", conn.Fd)
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
	if err == custom_err.ErrorNotFullyWritten {
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

	// Delete entry from offset tracking map because only master should keep
	// track of replications offset
	delete(command.OffsTracking, conn)
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
