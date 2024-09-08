package connection

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/core/info"
	"github.com/Viet-ph/redis-go/core/proto"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

var (
	ConnectedReplicas map[int]*Replica = make(map[int]*Replica)
)

type Replica struct {
	conn   *Conn
	offset int
}

func NewReplica(conn *Conn) *Replica {
	return &Replica{
		conn:   conn,
		offset: 0,
	}
}

func (rep *Replica) Propagate(datas ...[]byte) error {
	if len(datas) == 0 {
		return errors.New("no datas to propagate")
	}

	return rep.conn.QueueDatas(datas...)
}

func (rep *Replica) GetConn() *Conn {
	return rep.conn
}

func (rep *Replica) SetOffset(offs int) {
	rep.offset = offs
}

func SetupMasterSlave() (net.Conn, error) {
	fmt.Println("Setting master-slave...")
	info.ReplicationId = uuid.New()
	info.ReplicationOffset = 0
	if len(info.Master) > 0 {
		masterSocket := strings.Split(info.Master, " ")
		if len(masterSocket) != 2 {
			return nil, errors.New("incorrect master IP address or PORT")
		}
		info.Role = "slave"
		info.MasterHost = masterSocket[0]
		info.MasterPort, _ = strconv.Atoi(masterSocket[1])

		fmt.Println("Pinging master ...")
		conn, err := doHandShake()
		if err != nil {
			return nil, err
		}
		return conn, nil
	}

	return nil, nil
}

// Synchronous behavior, means write or read -> master will block the current goroutine
func doHandShake() (net.Conn, error) {
	address := fmt.Sprintf("%s:%d", info.MasterHost, info.MasterPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	masterAddr := conn.RemoteAddr().String()
	localAddr := conn.LocalAddr().String()

	fmt.Println("Master addre: " + masterAddr)
	fmt.Println("Local addre: " + localAddr)

	handShakeCommands := map[string]string{
		"PING":       "*1\r\n$4\r\nPING\r\n",
		"REPLCONF 1": "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n",
		"REPLCONF 2": "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
		"PSYNC":      "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
	}

	//Ping
	response, err := sendHandshake(conn, handShakeCommands["PING"])
	if err != nil {
		return nil, err
	}
	fmt.Println("Ping response: " + response.(string))

	//Rep config 1
	response, err = sendHandshake(conn, handShakeCommands["REPLCONF 1"])
	if err != nil {
		return nil, err
	}
	fmt.Println("Rep config 1 response: " + response.(string))

	//Rep config 2
	response, err = sendHandshake(conn, handShakeCommands["REPLCONF 2"])
	if err != nil {
		return nil, err
	}
	fmt.Println("Rep config 2 response: " + response.(string))

	//PSYNC
	err = handleReSync([]byte(handShakeCommands["PSYNC"]), conn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Goroutine blocking operation
func sendHandshake(conn net.Conn, data string) (response any, err error) {
	buffer := make([]byte, config.DefaultMessageSize)
	decoder := proto.NewDecoder(bytes.NewBuffer(buffer))
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

	return decodedResponse, nil
}

func handleReSync(syncCmd []byte, conn net.Conn) error {
	_, err := conn.Write(syncCmd)
	if err != nil {
		return err
	}

	container := make([]byte, config.DefaultMessageSize)
	n, err := conn.Read(container)
	if err != nil {
		return err
	}
	crlf := bytes.Index(container, []byte{'\r', '\n'})
	syncResponse := container[:crlf+2]
	RdbContainer := container[len(syncResponse):n]

	//In linux, 2 sequencial writes from server doesn't come in 1 package like darwin.
	//Here read 1 more time to get the rdb
	if len(RdbContainer) == 0 {
		RdbContainer = make([]byte, config.DefaultMessageSize)
		_, err := conn.Read(RdbContainer)
		if err != nil {
			return err
		}
	}

	decoder := proto.NewDecoder(bytes.NewBuffer([]byte(syncResponse)))
	docodedResponse, err := decoder.Decode()
	if err != nil {
		return err
	}
	fmt.Println("Psync response: " + docodedResponse.(string))

	core.RdbUnMarshall(RdbContainer)

	return nil
}

func getFileDescriptor(conn net.Conn) (int, error) {
	// Type assert to *net.TCPConn
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return -1, fmt.Errorf("connection is not of type *net.TCPConn")
	}

	// Get the underlying file descriptor
	file, err := tcpConn.File()
	if err != nil {
		return -1, err
	}

	conn.LocalAddr()

	return int(file.Fd()), nil
}

func NetConnToConn(netConn net.Conn) (*Conn, error) {
	if netConn == nil {
		return nil, errors.New("nil argument")
	}

	fd, err := getFileDescriptor(netConn)
	if err != nil {
		return nil, err
	}

	remoteSockAddr, err := unix.Getpeername(fd)
	if err != nil {
		return nil, err
	}

	masterConn, err := NewConn(fd, remoteSockAddr)
	if err != nil {
		return nil, err
	}

	return masterConn, nil
}

func IsMaster(conn *Conn) bool {
	ip, port := conn.GetRemoteAddress()
	return ip.String() == info.MasterHost && port == info.MasterPort
}

func IsReplica(conn *Conn) bool {
	_, ok := ConnectedReplicas[conn.Fd]
	return ok
}

func GetReplicas() []*Replica {
	if len(ConnectedReplicas) == 0 {
		return nil
	}

	replicas := make([]*Replica, 0, len(ConnectedReplicas))
	for _, replica := range ConnectedReplicas {
		replicas = append(replicas, replica)
	}

	return replicas
}
