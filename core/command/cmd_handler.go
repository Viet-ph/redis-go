package command

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/core/info"
	"github.com/Viet-ph/redis-go/datastore"
	"github.com/Viet-ph/redis-go/internal/connection"
	"github.com/Viet-ph/redis-go/internal/queue"
)

type hmap map[string]string

type Handler struct {
	taskQueue *queue.TaskQueue
	currConn  *connection.Conn
}

func NewCmdHandler(taskQueue *queue.TaskQueue) *Handler {
	return &Handler{
		taskQueue: taskQueue,
	}
}

func (handler *Handler) SetCurrentConn(conn *connection.Conn) error {
	if conn.IsClosed {
		return core.ErrorClientDisconnected
	}
	handler.currConn = conn
	return nil
}

func (handler *Handler) Ping(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) > 1 {
		return "wrong number of arguments for 'ping' command", true
	}

	if len(args) == 0 {
		return "PONG", true
	} else {
		return args[0], true
	}
}

// SET GET Handlers
func (handler *Handler) Set(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) < 2 {
		return errors.New("ERR wrong number of arguments for 'set'command"), true
	}

	key := args[0]
	value := args[1]
	options := args[2:]

	err := store.Set(key, value, options)
	if err != nil {
		return err, true
	}

	return "OK", true
}

func (handler *Handler) Get(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) != 1 {
		return errors.New("ERR wrong number of arguments for 'get' command"), true
	}

	data, exists := store.Get(args[0])
	if !exists {
		return core.ErrorKeyNotExists, true
	}

	stringData, ok := data.(string)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), true
	}

	return stringData, true
}

// HSET HGET HGETALL Handlers
func (handler *Handler) Hset(args []string, store *datastore.Datastore) (any, bool) {
	if len(args[1:])%2 != 0 {
		return errors.New("ERR wrong number of arguments for 'hset' command"), true
	}

	hmap := make(hmap)
	key := args[0]

	for i := 1; i < len(args); i += 2 {
		hmap[args[i]] = args[i+1]
	}

	err := store.Set(key, hmap, nil)
	if err != nil {
		return err, true
	}

	return "OK", true
}

func (handler *Handler) HGet(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) != 2 {
		return errors.New("ERR wrong number of arguments for 'hget' command"), true
	}

	data, exists := store.Get(args[0])
	if !exists {
		return core.ErrorKeyNotExists, true
	}

	hmap, ok := data.(hmap)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), true
	}

	hmapValue, exists := hmap[args[1]]
	if !exists {
		return core.ErrorKeyNotExists, true
	}

	return hmapValue, true
}

func (handler *Handler) HGetAll(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) != 1 {
		return errors.New("ERR wrong number of arguments for 'hgetall' command"), true
	}

	data, exists := store.Get(args[0])
	if !exists {
		return core.ErrorKeyNotExists, true
	}

	hmap, ok := data.(hmap)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), true
	}

	hmapKV := make([]string, 0, len(hmap)*2)
	for key, value := range hmap {
		hmapKV = append(hmapKV, key, value)
	}

	fmt.Println(hmapKV)
	return hmapKV, true
}

func (handler *Handler) Info(args []string, store *datastore.Datastore) (any, bool) {
	role := "role:" + info.Role
	info := []string{
		"# Replication",
		role,
		"master_replid:" + strings.Replace(info.ReplicationId.String(), "-", "", -1),
		"master_repl_offset:" + strconv.Itoa(info.ReplicationOffset),
		"",
	}

	return info, true
}

// REPLICATION CONFIGURATION
func (handler *Handler) ReplConf(args []string, store *datastore.Datastore) (any, bool) {
	if strings.ToUpper(args[0]) == "GETACK" && args[1] == "*" {
		repOffset := strconv.Itoa(info.ReplicationOffset)
		return []string{"REPLCONF", "ACK", repOffset}, true
	}

	if strings.ToUpper(args[0]) == "ACK" {
		repOffset, _ := strconv.Atoi(args[1])
		// The channel will remain opened and exists if timeout is not occured
		// and the wait command still blocking tyo receive acks from other replicas
		if ackCh, ok := repOffs[handler.currConn]; ok {
			ackCh <- repOffset
		}

		connection.ConnectedReplicas[handler.currConn.Fd].SetOffset(repOffset)
		return nil, false
	}

	return "OK", true
}

// REPLICATION SYNC
func (handler *Handler) Psync(args []string, store *datastore.Datastore) (any, bool) {
	result := fmt.Sprintf("FULLRESYNC %s %d", info.ReplicationId, info.ReplicationOffset)

	return result, true
}

func (handler *Handler) Wait(args []string, store *datastore.Datastore) (any, bool) {
	numReps, err := strconv.Atoi(args[0])
	if err != nil {
		return errors.New("WRONGTYPE Operation against 'wait' command holding the wrong kind of value"), true
	}

	timeout, err := strconv.Atoi(args[1])
	if err != nil {
		return errors.New("WRONGTYPE Operation against 'wait' command holding the wrong kind of value"), true
	}

	// Create new entry int acks map. This entry contains a channel to reiceive
	// offsets from others replicas. The key is the coonection that made the wait cmd.
	repOffs[handler.currConn] = make(chan int)

	// Spawn a new goroutine here inorder to not block other clients while getting
	// offsets from the replicas. This will give us the capability to handle other
	// wait commands from other clients.
	go func() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()

		numAcks := getAcks(numReps, handler.currConn, timeoutCtx)
		task := queue.NewTask(handler.currConn, respondWaitCmd, numAcks)
		handler.taskQueue.Add(*task)
	}()

	return nil, false
}
