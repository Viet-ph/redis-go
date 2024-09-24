//go:build darwin

package multiplexer

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

type Kqueue struct {
	fd       int
	kqEvents []unix.Kevent_t
}

func New(maxClients int) (*Kqueue, error) {
	if maxClients < 0 {
		return nil, fmt.Errorf("invalid number of max clients")
	}

	kqFD, err := unix.Kqueue()
	if err != nil {
		return &Kqueue{}, err
	}

	return &Kqueue{
		fd:       kqFD,
		kqEvents: make([]unix.Kevent_t, maxClients),
	}, nil
}

func (kq *Kqueue) AddWatchFd(fd int, pollEvents ...int) error {
	if len(pollEvents) == 0 {
		return fmt.Errorf("missing epoll events")
	}

	var events int16
	for _, pollEvent := range pollEvents {
		events |= int16(pollEvent)
	}
	event := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: events,
		Flags:  unix.EV_ADD | unix.EV_ENABLE,
	}

	subscribed, err := unix.Kevent(kq.fd, []unix.Kevent_t{event}, nil, nil)
	if err != nil || subscribed == -1 {
		return fmt.Errorf("error adding fd to watch list")
	}

	return nil
}

func (kq *Kqueue) RemoveWatchFd(fd int) error {
	event := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: unix.EVFILT_READ | unix.EVFILT_WRITE,
		Flags:  unix.EV_DELETE,
	}

	_, err := unix.Kevent(kq.fd, []unix.Kevent_t{event}, nil, nil)
	if err != nil {
		return fmt.Errorf("error removing fd from watch list")
	}

	return nil
}

func (kq *Kqueue) ModifyWatchingFd(fd int, pollEvents ...int) error {
	if len(pollEvents) == 0 {
		return fmt.Errorf("missing epoll events")
	}

	err := kq.RemoveWatchFd(fd)
	if err != nil {
		return fmt.Errorf("error modifying fd in watch list")
	}

	err = kq.AddWatchFd(fd, pollEvents...)
	if err != nil {
		return fmt.Errorf("error modifying fd in watch list")
	}

	return nil
}

func (kq *Kqueue) Poll(timeout time.Duration) ([]event, error) {
	var (
		numEvents int
		err       error
	)
	if timeout == -1 {
		numEvents, err = unix.Kevent(kq.fd, nil, kq.kqEvents, nil)
	} else {
		numEvents, err = unix.Kevent(kq.fd, nil, kq.kqEvents, &unix.Timespec{
			Nsec: int64(timeout * 1000),
		})
	}

	if err != nil {
		return nil, fmt.Errorf("error waiting for events: %v", err.Error())
	}

	return kq.kqEvents[:numEvents], nil
}

func (kq *Kqueue) IsReadable(event event) bool {
	return event.Filter == unix.EVFILT_READ
}

func (kq *Kqueue) IsWritable(event event) bool {
	return event.Filter == unix.EVFILT_WRITE
}

func (kq *Kqueue) Close() error {
	return unix.Close(kq.fd)
}
