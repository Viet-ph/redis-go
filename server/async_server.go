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
	"github.com/Viet-ph/redis-go/datastore"
	mul "github.com/Viet-ph/redis-go/internal/multiplexer"
	"golang.org/x/sys/unix"
)

type AsyncServer struct {
	connectedClients  map[int]*core.Conn
	connectedReplicas map[int]*core.Conn
	iomultiplexer     mul.Iomuliplexer
	fd                int
	maxClients        int
	store             *datastore.Datastore
}

func NewAsyncServer() (*AsyncServer, error) {
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
		connectedClients:  make(map[int]*core.Conn),
		connectedReplicas: make(map[int]*core.Conn),
		fd:                serverFD,
		maxClients:        100,
		store:             datastore.NewDatastore(),
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
					if client, exists := server.connectedClients[fd]; exists {
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
	server.connectedClients[int(connFD)] = core.NewConn(connFD, sa)

	//Print out client's ip and port
	ip, port, err := server.connectedClients[int(connFD)].GetAddress()
	if err != nil {
		fmt.Println("Error getting client address: ", err)
		return err
	}
	fmt.Printf("Client connected: IP = %s, Port = %d\n", ip.String(), port)

	return nil
}

func (server *AsyncServer) handleReadableEvent(conn *core.Conn) error {
	//Read message sent from client
	underlyBuf := make([]byte, 0, config.DefaultMessageSize)
	buffer := bytes.NewBuffer(underlyBuf)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	//If it's a command, parse it into command object
	decoder := core.NewDecoder(buffer)
	cmd, err := decoder.Parse()
	if err != nil {
		return fmt.Errorf("error parsing command")
	}

	//Propagate command to slaves if possible
	if core.IsWriteCommand(cmd) {
		server.propagateCmd(underlyBuf[:bytesRead])
	}

	//Execute command
	result := core.ExecuteCmd(cmd, server.store)

	//Queue datas to write and write immediately after
	if strings.Contains(cmd.Cmd, "PSYNC") {
		rdb, _ := core.RdbMarshall()
		err = conn.QueueDatas(result, rdb)
		server.AddNewSlave(conn)
	} else {
		err = conn.QueueDatas(result)
	}
	if err != nil {
		server.handleWritingError(err, conn)
	}

	return nil
}

func (server *AsyncServer) handleWritableEvent(conn *core.Conn) {
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

func (server *AsyncServer) handleWritingError(err error, conn *core.Conn) {
	if err == core.ErrorNotFullyWritten {
		//No data could be written, resubscribe with write event and return to wait for write event
		fmt.Printf("Got write error: %v\n", err.Error())
		server.iomultiplexer.ModifyWatchingFd(conn.Fd, mul.OpWrite)
		return
	}
	// Handle other errors (e.g., client disconnected)	 	 ``
	fmt.Printf("Got unexpected write error: %v\n", err.Error())
	server.CloseConnecttion(conn)
}

// Function to handle client disconnection or cleanup
func (server *AsyncServer) CloseConnecttion(client *core.Conn) {
	// Close the socket, cleanup resources
	// Remove FD from epoll interest list
	server.iomultiplexer.RemoveWatchFd(client.Fd)
	client.Close()
	// Remove client from your client management structures
	delete(server.connectedClients, client.Fd)
	delete(server.connectedReplicas, client.Fd)

	ip, port, _ := client.GetAddress()
	fmt.Printf("Client disconnected. IP = %s, Port = %d\n", ip.String(), port)
}

func (server *AsyncServer) close() {
	server.iomultiplexer.Close()
	unix.Close(server.fd)
}

func (server *AsyncServer) propagateCmd(rawCmd []byte) {
	if len(server.connectedReplicas) == 0 {
		return
	}

	//fmt.Println("Propagate " + string(rawCmd))

	for _, slave := range server.connectedReplicas {
		err := slave.QueueDatas(rawCmd)
		if err != nil {
			fmt.Println("Error propergate command to slave: " + err.Error())
			continue
		}
	}
}

func (server *AsyncServer) AddNewSlave(conn *core.Conn) {
	server.connectedReplicas[conn.Fd] = conn
	delete(server.connectedClients, conn.Fd)
}

func (server *AsyncServer) getConn(fd int) (*core.Conn, bool) {
	if conn, exist := server.connectedClients[fd]; exist {
		return conn, true
	}

	if conn, exist := server.connectedReplicas[fd]; exist {
		return conn, true
	}

	return nil, false
}
