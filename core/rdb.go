package core

import (
	"encoding/hex"
	"fmt"
	"net"

	"github.com/Viet-ph/redis-go/config"
)

const EmptyRdbHexString = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

func RdbMarshall() ([]byte, error) {
	content, err := hex.DecodeString(EmptyRdbHexString)
	if err != nil {
		return nil, err
	}

	length := fmt.Sprintf("$%d\r\n", len(content))
	bytes := append([]byte(length), content...)

	return bytes, nil
}

func RdbUnMarshall(conn net.Conn) {
	buffer := make([]byte, config.DefaultMessageSize)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Receive non rdb file")
		return
	}

	fmt.Println(string(buffer[:n]))
}
