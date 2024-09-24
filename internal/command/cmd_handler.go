package command

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/internal/connection"
	"github.com/Viet-ph/redis-go/internal/datastore"
	custom_err "github.com/Viet-ph/redis-go/internal/error"
	"github.com/Viet-ph/redis-go/internal/info"
	"github.com/Viet-ph/redis-go/internal/queue"
	"github.com/Viet-ph/redis-go/internal/rdb"
)

type hmap map[string]string

type Handler struct {
	taskQueue  *queue.TaskQueue
	currClient *connection.Conn
	currRep    *connection.Conn
}

func NewCmdHandler(taskQueue *queue.TaskQueue) *Handler {
	return &Handler{
		taskQueue: taskQueue,
	}
}

func (handler *Handler) SetCurrentConn(conn *connection.Conn) error {
	if conn.IsClosed {
		return custom_err.ErrorClientDisconnected
	}

	if connection.IsReplica(conn) {
		handler.currRep = conn
	} else {
		handler.currClient = conn
	}
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
		return custom_err.ErrorKeyNotExists, true
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
		return custom_err.ErrorKeyNotExists, true
	}

	hmap, ok := data.(hmap)
	if !ok {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), true
	}

	hmapValue, exists := hmap[args[1]]
	if !exists {
		return custom_err.ErrorKeyNotExists, true
	}

	return hmapValue, true
}

func (handler *Handler) HGetAll(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) != 1 {
		return errors.New("ERR wrong number of arguments for 'hgetall' command"), true
	}

	data, exists := store.Get(args[0])
	if !exists {
		return custom_err.ErrorKeyNotExists, true
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
		// The channel will remain opened and exists if timeout is not occured.
		// The wait command still blocking to receive acks from other replicas
		if offsTracker, ok := OffsTracking[handler.currClient]; ok {
			fmt.Println("Sending offset to channel...")
			offsTracker.AckCh <- repOffset
		}

		connection.ConnectedReplicas[handler.currRep.Fd].SetOffset(repOffset)
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

	// Close, delete the old and create new entry in acks map. This entry contains a channel to reiceive
	// offsets from others replicas. The key is the connection that made the wait cmd.
	offsTracker := OffsTracking[handler.currClient]
	if offsTracker.AckCh != nil {
		close(offsTracker.AckCh)
	}
	// This channel must be buffered so it wont block the event loop when our gouroutine
	// done getting acks from replicas and wont drain any further. Because even the 'wait'
	// command is handled, other replicas may still sending replconf ack to master when
	// the 'numReps' given by client is smaller than connected replicas.
	offsTracker.AckCh = make(chan int, config.MaximumReplicas)

	// Spawn a new goroutine here inorder to not block other clients while getting
	// offsets from the replicas. This will give us the capability to handle other
	// wait commands from other clients.
	go func(currentClient *connection.Conn) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()

		numAcks := GetRepOffsets(numReps, offsTracker, timeoutCtx)
		task := queue.NewTask(respondWaitCmd, currentClient, numAcks)
		handler.taskQueue.Add(*task)
	}(handler.currClient)

	return nil, false
}

func (handler *Handler) Config(args []string, store *datastore.Datastore) (any, bool) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "GET":
		if value, exist := config.GetConfigValue(args[1]); exist {
			return []string{args[1], value.(string)}, true
		}
		return errors.New("configuration parameter not found"), true
	default:
		return errors.New("config subcommand not found"), true
	}
}

func (handler *Handler) Save(args []string, store *datastore.Datastore) (any, bool) {
	rdbMarshalled, err := rdb.RdbMarshall(store)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = rdb.WriteRdbFile(rdbMarshalled)
	if err != nil {
		fmt.Println(err.Error())
	}
	return "OK", true
}

func (handler *Handler) BgSave(args []string, store *datastore.Datastore) (any, bool) {
	go func() {
		handler.Save(args, store)
	}()
	return "OK", true
}

func (handler *Handler) Command(args []string, store *datastore.Datastore) (any, bool) {
	if len(args) == 0 {
		return errors.New("ERR wrong number of arguments for 'command' command"), true
	}

	subcmd := args[0]
	switch strings.ToLower(subcmd) {
	case "count":
		return len(commands), true
	case "list":
		cmdNames := make([]string, len(commands))
		i := 0
		for k := range commands {
			cmdNames[i] = k
			i++
		}
		return cmdNames, true
	case "docs":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'command' command"), true
		}

		cmdName := strings.ToUpper(args[1])
		cmdDescription := strings.Split(commands[cmdName].description, ".")
		for i, description := range cmdDescription {
			description = strings.ReplaceAll(description, "\n", "")
			description = strings.ReplaceAll(description, "\t", "")
			description = strings.TrimSpace(description)
			cmdDescription[i] = description
		}

		return cmdDescription, true
	default:
		return fmt.Errorf("unknown subcommand '%s'", subcmd), true
	}
}
