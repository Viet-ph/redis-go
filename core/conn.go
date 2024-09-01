package core

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/Viet-ph/redis-go/config"
	"golang.org/x/sys/unix"
)

type Conn struct {
	Fd         int
	WriteQueue [][]byte
	sa         unix.Sockaddr
}

func NewConn(connFd int, sa unix.Sockaddr) *Conn {
	return &Conn{
		Fd: connFd,
		sa: sa,
	}
}

func (conn *Conn) Read(buf *bytes.Buffer) error {
	temp := make([]byte, config.DefaultMessageSize)
	//For loop to drain all the unknown size incomming message
	for {
		bytesRead, err := unix.Read(conn.Fd, temp)
		fmt.Println("Bytes read: " + strconv.Itoa(bytesRead))
		if bytesRead == 0 || err == unix.ECONNRESET || err == unix.EPIPE {
			//Graceful Close Detection:
			//When unix.Read returns 0, it indicates that the client has closed the connection gracefully.
			//This is the most common way to detect a normal disconnection.

			//Other Errors:
			//Certain errors like ECONNRESET or EPIPE during a read or write operation
			//indicate that the client has forcefully closed the connection,
			//server should handle these errors by cleaning up the client’s resources.
			return ErrorClientDisconnected
		}
		if err != nil {
			if err == unix.EAGAIN && buf.Len() > 0 {
				//We drained all the massage and no available message left in kernel buffer
				break
			} else if err == unix.EAGAIN {
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
}

func (conn *Conn) DrainQueue() error {
	for len(conn.WriteQueue) > 0 {
		data := conn.WriteQueue[0]
		n, err := unix.Write(conn.Fd, data)
		fmt.Printf("Bytes wrote: %d, data length : %d\n", n, len(data))
		if err != nil {
			if err == unix.EAGAIN {
				// Socket is not ready for writing, return and wait for write event
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
	return unix.Close(conn.Fd)
}

func (conn *Conn) GetAddress() (net.IP, int, error) {
	var (
		ip   net.IP
		port int
	)

	switch addr := conn.sa.(type) {
	case *unix.SockaddrInet4:
		ip = net.IPv4(addr.Addr[0], addr.Addr[1], addr.Addr[2], addr.Addr[3])
		port = addr.Port
	case *unix.SockaddrInet6:
		ip = net.IP(addr.Addr[:])
		port = addr.Port
	default:
		return ip, port, fmt.Errorf("unknown address type")
	}

	return ip, port, nil
}
