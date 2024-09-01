package multiplexer

import (
	"golang.org/x/sys/unix"
)

// EventOp constants for read and write operations.
const (
	OpRead  = unix.EPOLLIN  // For Linux
	OpWrite = unix.EPOLLOUT // For Linux
)

type event = unix.EpollEvent

func GetFdFromEvent(event event) int {
	return int(event.Fd)
}
