package core

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"syscall"

	"github.com/Viet-ph/redis-go/config"
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

func (conn *Conn) Read(buf *bytes.Buffer) error {
	temp := make([]byte, config.DefaultMessageSize)
	//For loop to drain all the unknown size incomming message
	for {
		bytesRead, err := syscall.Read(conn.Fd, temp)
		fmt.Println("Bytes read: " + strconv.Itoa(bytesRead))
		if bytesRead == 0 || err == syscall.ECONNRESET || err == syscall.EPIPE {
			//Graceful Close Detection:
			//When syscall.Read returns 0, it indicates that the client has closed the connection gracefully.
			//This is the most common way to detect a normal disconnection.

			//Other Errors:
			//Certain errors like ECONNRESET or EPIPE during a read or write operation
			//indicate that the client has forcefully closed the connection,
			//server should handle these errors by cleaning up the client’s resources.
			return ErrorClientDisconnected
		}
		if err != nil {
			if err == syscall.EAGAIN && buf.Len() > 0 {
				//We drained all the massage and no available message left in kernel buffer
				break
			} else if err == syscall.EAGAIN {
				// No data available yet, return to event loop
				return nil
			}
			// Handle other errors
			return ErrorReadingSocket
		}

		buf.Write(temp)

		//If number of bytes read smaller than temp buffer size,
		//we got all data in one go. Break here.
		if bytesRead < config.DefaultMessageSize {
			break
		}
	}

	return nil
	// return syscall.Read(conn.Fd, buf)
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
