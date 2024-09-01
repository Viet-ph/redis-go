//go:build linux

package multiplexer

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

type Epoll struct {
	fd         int
	pollEvents []unix.EpollEvent
}

func New(maxClients int) (*Epoll, error) {
	if maxClients < 0 {
		return nil, fmt.Errorf("invalid number of max clients")
	}

	epollFD, err := unix.EpollCreate1(0)
	if err != nil {
		return &Epoll{}, err
	}

	return &Epoll{
		fd:         epollFD,
		pollEvents: make([]unix.EpollEvent, maxClients),
	}, nil
}

func (epoll *Epoll) AddWatchFd(fd int, pollEvents ...int) error {
	if len(pollEvents) == 0 {
		return fmt.Errorf("missing epoll events")
	}

	var events uint32
	for _, pollEvent := range pollEvents {
		events |= uint32(pollEvent)
	}

	err := unix.EpollCtl(epoll.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
	if err != nil {
		return fmt.Errorf("error adding fd to watch list")
	}

	return nil
}

func (epoll *Epoll) RemoveWatchFd(fd int) error {
	err := unix.EpollCtl(epoll.fd, unix.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return fmt.Errorf("error removing fd from watch list")
	}

	return nil
}

func (epoll *Epoll) ModifyWatchingFd(fd int, pollEvents ...int) error {
	var newEvents uint32
	for _, event := range pollEvents {
		newEvents |= uint32(event)
	}

	err := unix.EpollCtl(epoll.fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: newEvents,
		Fd:     int32(fd),
	})
	if err != nil {
		return fmt.Errorf("error modify current fd in watch list")
	}

	return nil
}

func (epoll *Epoll) Poll(timeout time.Duration) ([]event, error) {
	var msec int
	if timeout == -1 {
		msec = -1
	} else {
		msec = int(timeout / time.Millisecond)
	}
	numEvents, err := unix.EpollWait(epoll.fd, epoll.pollEvents, msec)
	if err != nil {
		return nil, fmt.Errorf("error waiting for events: %v", err.Error())
	}

	return epoll.pollEvents[:numEvents], nil
}

func (epoll *Epoll) IsReadable(event event) bool {
	return unix.EPOLLIN == event.Events&unix.EPOLLIN
}

func (epoll *Epoll) IsWritable(event event) bool {
	return unix.EPOLLOUT == event.Events&unix.EPOLLOUT
}

func (epoll *Epoll) Close() error {
	return unix.Close(epoll.fd)
}
