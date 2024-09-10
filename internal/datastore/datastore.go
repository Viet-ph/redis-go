package datastore

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type Datastore struct {
	store map[string]*data
}

func NewDatastore() *Datastore {
	return &Datastore{
		store: make(map[string]*data),
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
	data := ds.store[key]
	if data.hasExpiry {
		return errors.New("syntax error")
	}
	fmt.Println("Setting expiry...")
	err := data.SetExpiry(duration, inMillisecond)
	fmt.Println(err)
	if err != nil {
		return err
	}

	return nil
}

func (ds *Datastore) Get(key string) (value any, exists bool) {
	data, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	if data.hasExpiry && data.isExpired() {
		delete(ds.store, key)
		return nil, false
	}

	return data.value, true
}
