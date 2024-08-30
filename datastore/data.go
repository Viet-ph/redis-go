package datastore

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

type data struct {
	value     any
	hasExpiry bool
	expiredAt time.Time
	createdAt time.Time
}

func NewData(value any) *data {
	return &data{
		value:     value,
		createdAt: time.Now().UTC(),
	}
}

func (data *data) SetExpiry(duration string, inMillisecond bool) error {
	var (
		nanoseconds  int64
		milliseconds int64
		seconds      int64
		err          error
	)
	if inMillisecond {
		milliseconds, err = strconv.ParseInt(duration, 10, 64)
		if err != nil { //|| milliseconds > int64(math.MaxInt64)/int64(time.Millisecond) {
			fmt.Println(err)
			return errors.New("value is not an integer or out of range")
		}
		nanoseconds = milliseconds * int64(time.Millisecond)
	} else {
		seconds, err = strconv.ParseInt(duration, 10, 64)
		fmt.Println("duration time in seconds: " + strconv.Itoa(int(seconds)))
		if err != nil || seconds > int64(math.MaxInt64)/int64(time.Second) {
			return errors.New("value is not an integer or out of range")
		}
		nanoseconds = seconds * int64(time.Second)
	}

	data.hasExpiry = true
	data.expiredAt = data.createdAt.Add(time.Duration(nanoseconds))
	return nil
}

func (data *data) isExpired() bool {
	return data.expiredAt.Before(time.Now().UTC())
}
