package multiplexer

import (
	"fmt"
	"syscall"
)

type Epoll struct {
	fd         int
	pollEvents []syscall.EpollEvent
}

func NewEpoll(maxClients int) (*Epoll, error) {
	if maxClients < 0 {
		return nil, fmt.Errorf("invalid number of max clients")
	}

	epollFD, err := syscall.EpollCreate1(0)
	if err != nil {
		return &Epoll{}, err
	}

	return &Epoll{
		fd:         epollFD,
		pollEvents: make([]syscall.EpollEvent, maxClients),
	}, nil
}

func (epoll *Epoll) AddWatchFd(fd int, pollEvents ...uint32) error {
	if len(pollEvents) == 0 {
		return fmt.Errorf("missing epoll events")
	}

	var events uint32
	for _, pollEvent := range pollEvents {
		events |= pollEvent
	}

	err := syscall.EpollCtl(epoll.fd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
	if err != nil {
		return fmt.Errorf("error adding fd to watch list")
	}

	return nil
}

func (epoll *Epoll) RemoveWatchFd(fd int) error {
	err := syscall.EpollCtl(epoll.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return fmt.Errorf("error removing fd from watch list")
	}

	return nil
}

func (epoll *Epoll) ModifyWatchingFd(fd int, pollEvents ...uint32) error {
	var newEvents uint32
	for _, event := range pollEvents {
		newEvents |= event
	}

	err := syscall.EpollCtl(epoll.fd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Events: newEvents,
		Fd:     int32(fd),
	})
	if err != nil {
		return fmt.Errorf("error modify current fd in watch list")
	}

	return nil
}

func (epoll *Epoll) Poll(timeout int) ([]syscall.EpollEvent, error) {
	numEvents, err := syscall.EpollWait(epoll.fd, epoll.pollEvents, timeout)
	if err != nil {
		return nil, fmt.Errorf("error waiting for events: %v", err.Error())
	}

	return epoll.pollEvents[:numEvents], nil
}

func (epoll *Epoll) IsReadable(epollEvent syscall.EpollEvent) bool {
	return syscall.EPOLLIN == epollEvent.Events&syscall.EPOLLIN
}

func (epoll *Epoll) IsWritable(epollEvent syscall.EpollEvent) bool {
	return syscall.EPOLLOUT == epollEvent.Events&syscall.EPOLLOUT
}

func (epoll *Epoll) Close() error {
	return syscall.Close(epoll.fd)
}
