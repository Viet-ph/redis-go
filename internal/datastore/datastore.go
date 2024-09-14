package datastore

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Datastore struct {
	store  map[string]*Data
	expiry map[string]time.Time
}

func NewDatastore(store map[string]*Data, expiry map[string]time.Time) *Datastore {
	if store == nil {
		store = make(map[string]*Data)
	}

	if expiry == nil {
		expiry = make(map[string]time.Time)
	}
	return &Datastore{
		store:  store,
		expiry: expiry,
	}
}

func (ds *Datastore) Set(key string, value any, options []string) error {
	ds.store[key] = NewData(value)
	if len(options) > 0 {
		err := ds.setOptions(key, options)
		if err != nil {
			delete(ds.store, key)
			return err
		}
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
	// data := ds.store[key]
	// if data.hasExpiry {
	// 	return errors.New("syntax error")
	// }
	// fmt.Println("Setting expiry...")
	// err := data.SetExpiry(duration, inMillisecond)
	// fmt.Println(err)
	// if err != nil {
	// 	return err
	// }

	// return nil
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

// func (ds *Datastore) HasExpiry(key string) bool {
// 	_, exist := ds.expiry[key]
// 	return exist
// }

func (ds *Datastore) GetExpiry(key string) (time.Time, bool) {
	expireAt, exist := ds.expiry[key]
	if exist {
		return expireAt, true
	}

	return time.Time{}, false
}

func (ds *Datastore) isExpired(key string) bool {
	expireAt, exist := ds.expiry[key]
	if exist {
		return expireAt.Before(time.Now().UTC())
	}

	return false
}

func (ds *Datastore) Get(key string) (value any, exists bool) {
	data, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	if ds.isExpired(key) {
		delete(ds.store, key)
		delete(ds.expiry, key)
		return nil, false
	}

	return data.value, true
}

func (ds *Datastore) GetStoreSize() (int, int) {
	return len(ds.store), len(ds.expiry)
}

func (ds *Datastore) GetStorage() map[string]*Data {
	return ds.store
}
