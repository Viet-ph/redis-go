package multiplexer

import (
	"golang.org/x/sys/unix"
)

// EventOp constants for read and write operations.
const (
	OpRead  = unix.EVFILT_READ  // For Darwin
	OpWrite = unix.EVFILT_WRITE // For Darwin
)

type event = unix.Kevent_t

func GetFdFromEvent(event event) int {
	return int(event.Ident)
}
