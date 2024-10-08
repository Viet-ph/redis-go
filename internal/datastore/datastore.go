package datastore

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Viet-ph/redis-go/internal/info"
)

type Datastore struct {
	store        map[string]*Data
	expiry       map[string]time.Time
	KeyChangesCh chan struct{}
	mu           *sync.RWMutex
}

func NewDatastore(store map[string]*Data, expiry map[string]time.Time) *Datastore {
	if store == nil {
		store = make(map[string]*Data)
	}

	if expiry == nil {
		expiry = make(map[string]time.Time)
	}
	return &Datastore{
		store:        store,
		expiry:       expiry,
		KeyChangesCh: make(chan struct{}),
		mu:           &sync.RWMutex{},
	}
}

func (ds *Datastore) Set(key string, value any, options []string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.store[key] = NewData(value)
	if len(options) > 0 {
		err := ds.setOptions(key, options)
		if err != nil {
			delete(ds.store, key)
			return err
		}
	}

	if info.Role == "master" {
		ds.KeyChangesCh <- struct{}{}
	}
	return nil
}

func (ds *Datastore) setOptions(key string, options []string) error {
	availableOptions := []string{"PX", "EX"}
	for i := 0; i < len(options); {
		fmt.Println(i + 1)
		fmt.Println(len(options))
		if slices.Contains(availableOptions, strings.ToUpper(options[i])) && i+1 < len(options) {
			err := ds.setDataExpiry(key, options[i+1], options[i] == "PX")
			if err != nil {
				return err
			}
			i += 2
		} else {
			return errors.New("syntax error")
		}
	}

	return nil
}

func (ds *Datastore) setDataExpiry(key, duration string, inMillisecond bool) error {
	if _, exist := ds.expiry[key]; exist {
		return errors.New("syntax error")
	}

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

	// data.hasExpiry = true
	ds.expiry[key] = time.Now().UTC().Add(time.Duration(nanoseconds))
	return nil
}

func (ds *Datastore) GetExpiry(key string) (time.Time, bool) {
	expireAt, exist := ds.expiry[key]
	if exist {
		return expireAt, true
	}

	return time.Time{}, false
}

func (ds *Datastore) IsExpired(key string) bool {
	expireAt, exist := ds.expiry[key]
	if exist {
		return expireAt.Before(time.Now().UTC())
	}

	return false
}

func (ds *Datastore) Get(key string) (value any, exists bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	data, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	if ds.IsExpired(key) {
		ds.Del(key)
		return nil, false
	}

	return data.value, true
}

func (ds *Datastore) Del(key string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if info.Role == "master" {
		ds.KeyChangesCh <- struct{}{}
	}
	delete(ds.store, key)
	delete(ds.expiry, key)
}

func (ds *Datastore) DeepCopy() (map[string]*Data, map[string]time.Time) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	store := make(map[string]*Data, len(ds.store))
	expiry := make(map[string]time.Time, len(ds.expiry))

	for k, v := range ds.store {
		store[k] = v
	}

	for k, v := range ds.expiry {
		expiry[k] = v
	}

	return store, expiry
}
