package core

import (
	"fmt"
	"net"
	"syscall"
)

type Conn struct {
	Fd         int
	WriteQueue [][]byte
	sa         syscall.Sockaddr
}

func NewConn(connFd int, sa syscall.Sockaddr) *Conn {
	return &Conn{
		Fd: connFd,
		sa: sa,
	}
}

func (conn *Conn) Read(buf []byte) (int, error) {
	return syscall.Read(conn.Fd, buf)
}

func (conn *Conn) DrainQueue() error {
	for len(conn.WriteQueue) > 0 {
		data := conn.WriteQueue[0]
		n, err := syscall.Write(conn.Fd, data)
		if err != nil {
			if err == syscall.EAGAIN {
				// Socket is not ready for writing, return and wait for EPOLLOUT event
				return ErrorNotFullyWritten
			}
			return err
		}
		if n < len(data) {
			// Partial write maybe due to network error, keep the remaining data in the queue
			conn.WriteQueue[0] = data[n:]
			return ErrorNotFullyWritten
		}

		// Full write, remove the data from the queue
		fmt.Println("Response sent: " + string(data))
		conn.WriteQueue = conn.WriteQueue[1:]
	}

	return nil
}

func (conn *Conn) QueueDatas(data ...[]byte) error {
	conn.WriteQueue = append(conn.WriteQueue, data...)
	// Try to write immediately
	err := conn.DrainQueue()
	if err != nil {
		return err
	}

	if len(conn.WriteQueue) > 0 {
		return ErrorNotFullyWritten
	}

	return nil
}

func (conn *Conn) Close() error {
	return syscall.Close(conn.Fd)
}

func (conn *Conn) GetAddress() (net.IP, int, error) {
	var (
		ip   net.IP
		port int
	)

	switch addr := conn.sa.(type) {
	case *syscall.SockaddrInet4:
		ip = net.IPv4(addr.Addr[0], addr.Addr[1], addr.Addr[2], addr.Addr[3])
		port = addr.Port
	case *syscall.SockaddrInet6:
		ip = net.IP(addr.Addr[:])
		port = addr.Port
	default:
		return ip, port, fmt.Errorf("unknown address type")
	}

	return ip, port, nil
}
