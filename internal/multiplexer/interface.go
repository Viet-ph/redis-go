package multiplexer

import "time"

type Iomuliplexer interface {
	AddWatchFd(fd int, pollEvents ...int) error
	RemoveWatchFd(fd int) error
	ModifyWatchingFd(fd int, pollEvents ...int) error
	Poll(timeout time.Duration) ([]event, error)
	IsReadable(event event) bool
	IsWritable(event event) bool
	Close() error
}
