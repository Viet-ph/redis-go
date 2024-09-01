package core

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Viet-ph/redis-go/datastore"
)

type hmap map[string]string

func handlePing(args []string, store *datastore.Datastore) any {
	if len(args) > 1 {
		return "wrong number of arguments for 'ping' command"
	}

	if len(args) == 0 {
		return "PONG"
	} else {
		return args[0]
	}
}

// SET GET Handlers
func handleSet(args []string, store *datastore.Datastore) any {
	if len(args) < 2 {
		return errors.New("ERR wrong number of arguments for 'set'command")
	}

	key := args[0]
	value := args[1]
	options := args[2:]

	err := store.Set(key, value, options)
	if err != nil {
		return err
	}

	return "OK"
}

func handleGet(args []string, store *datastore.Datastore) any {
	if len(args) != 1 {
		return errors.New("ERR wrong number of arguments for 'get' command")
	}

	data, exists := store.Get(args[0])
	if !exists {
		return ErrorKeyNotExists
	}

	stringData, ok := data.(string)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return stringData
}

// HSET HGET HGETALL Handlers
func handleHset(args []string, store *datastore.Datastore) any {
	if len(args[1:])%2 != 0 {
		return errors.New("ERR wrong number of arguments for 'hset' command")
	}

	hmap := make(hmap)
	key := args[0]

	for i := 1; i < len(args); i += 2 {
		hmap[args[i]] = args[i+1]
	}

	err := store.Set(key, hmap, nil)
	if err != nil {
		return err
	}

	return "OK"
}

func handleHGet(args []string, store *datastore.Datastore) any {
	if len(args) != 2 {
		return errors.New("ERR wrong number of arguments for 'hget' command")
	}

	data, exists := store.Get(args[0])
	if !exists {
		return ErrorKeyNotExists
	}

	hmap, ok := data.(hmap)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hmapValue, exists := hmap[args[1]]
	if !exists {
		return ErrorKeyNotExists
	}

	return hmapValue
}

func handleHGetAll(args []string, store *datastore.Datastore) any {
	if len(args) != 1 {
		return errors.New("ERR wrong number of arguments for 'hgetall' command")
	}

	data, exists := store.Get(args[0])
	if !exists {
		return ErrorKeyNotExists
	}

	hmap, ok := data.(hmap)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hmapKV := make([]string, 0, len(hmap)*2)
	for key, value := range hmap {
		hmapKV = append(hmapKV, key, value)
	}

	fmt.Println(hmapKV)
	return hmapKV
}

func handleInfo(args []string, store *datastore.Datastore) any {
	role := "role:" + Role
	info := []string{
		"# Replication",
		role,
		"master_replid:" + strings.Replace(ReplicationId.String(), "-", "", -1),
		"master_repl_offset:" + strconv.Itoa(ReplicationOffset),
		"",
	}

	return info
}

// REPLICATION CONFIGURATION
func handleReplConf(args []string, store *datastore.Datastore) any {
	return "OK"
}

// REPLICATION SYNC
func handlePsync(args []string, store *datastore.Datastore) any {
	result := fmt.Sprintf("FULLRESYNC %s %d", ReplicationId, ReplicationOffset)
	
	return result
}
