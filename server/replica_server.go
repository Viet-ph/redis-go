package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/datastore"
)

type RepServer struct {
	store *datastore.Datastore
}

func NewRepServer() *RepServer {
	return &RepServer{
		store: datastore.NewDatastore(),
	}
}

func (server *RepServer) Start(conn net.Conn) {
	defer conn.Close()

	//Handle master "write" commands
	reader := bufio.NewReader(conn)
	for {
		//reader.Reset(conn)
		buffer := make([]byte, config.DefaultMessageSize)
		n, err := reader.Read(buffer)
		if err != nil {
			fmt.Println(err)
			continue
		}

		decoder := core.NewDecoder(bytes.NewBuffer(buffer[:n]))
		command, err := decoder.Parse()
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println("Got command: " + command.Cmd)

		result := core.ExecuteCmd(command, server.store)
		fmt.Println("Command execute result: " + string(result))
	}
}
