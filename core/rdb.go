package core

import (
	"encoding/hex"
	"fmt"
	"net"

	"github.com/Viet-ph/redis-go/config"
)

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
