package core

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Viet-ph/redis-go/config"
	"github.com/google/uuid"
)

var (
	Master string = ""
	Role   string = "master"

	MasterHost string = ""
	MasterPort int    = 0

	ReplicationId     uuid.UUID
	ReplicationOffset int
)

func SetupMasterSlave() error {
	fmt.Println("Setting master-slave...")
	if len(Master) > 0 {
		masterSocket := strings.Split(Master, " ")
		if len(masterSocket) != 2 {
			return errors.New("incorrect master IP address or PORT")
		}
		Role = "slave"
		MasterHost = masterSocket[0]
		MasterPort, _ = strconv.Atoi(masterSocket[1])

		fmt.Println("Pinging master ...")
		err := doHandShake()
		if err != nil {
			return err
		}
	}
	ReplicationId = uuid.New()
	ReplicationOffset = 0

	return nil
}

// Synchronous behavior, means write or read -> master will block the current goroutine
func doHandShake() error {
	address := fmt.Sprintf("%s:%d", MasterHost, MasterPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	handShakeCommands := map[string]string{
		"PING":       "*1\r\n$4\r\nPING\r\n",
		"REPLCONF 1": "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n",
		"REPLCONF 2": "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
		"PSYNC":      "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
	}

	//Ping
	response, err := sendDataSync(conn, handShakeCommands["PING"])
	if err != nil {
		return err
	}
	fmt.Println("Ping response: " + response.(string))

	//Rep config 1
	response, err = sendDataSync(conn, handShakeCommands["REPLCONF 1"])
	if err != nil {
		return err
	}
	fmt.Println("Rep config 1 response: " + response.(string))

	//Rep config 2
	response, err = sendDataSync(conn, handShakeCommands["REPLCONF 2"])
	if err != nil {
		return err
	}
	fmt.Println("Rep config 1 response: " + response.(string))

	//PSYNC
	response, err = sendDataSync(conn, handShakeCommands["PSYNC"])
	if err != nil {
		return err
	}
	fmt.Println("Psync response: " + response.(string))
	RdbUnMarshall(conn)

	return nil
}

// Goroutine blocking operation
func sendDataSync(conn net.Conn, data string) (response any, err error) {
	buffer := make([]byte, config.DefaultMessageSize)
	decoder := NewDecoder(bytes.NewBuffer(buffer))
	_, err = conn.Write([]byte(data))
	if err != nil {
		return "", err
	}
	_, err = conn.Read(buffer)
	if err != nil {
		return "", err
	}
	decodedResponse, err := decoder.Decode()
	if err != nil {
		return "", err
	}
	// decoder.ResetBufOffset()
	return decodedResponse, nil
}
