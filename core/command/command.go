package command

import (
	"bytes"
	"errors"
	"slices"
	"strings"

	"github.com/Viet-ph/redis-go/core/proto"
	"github.com/Viet-ph/redis-go/datastore"
)

type Command struct {
	Cmd  string
	Args []string
}

type CmdMetaData struct {
	name        string
	description string

	//handler will return with value and true if the result is ready,
	//otherwise returns nil value and false (result needs to be sent later on)
	handler func([]string, *datastore.Datastore) (any, bool)
}

func GetCommands(handler *Handler) map[string]CmdMetaData {
	return map[string]CmdMetaData{
		"PING": {
			name: "PING",
			description: `PING returns with an encoded "PONG" If any message is 
						added with the ping command,the message will be returned.`,
			handler: handler.Ping,
		},
		"SET": {
			name: "SET",
			description: `Set key to hold the string value. If key already holds a value,
						it is overwritten, regardless of its type. Any previous time to 
						live associated with the key is discarded on successful SET operation.`,
			handler: handler.Set,
		},
		"GET": {
			name: "GET",
			description: `Get the value of key. If the key does not exist the special value nil is returned. 
						An error is returned if the value stored at key is not a string, because GET only handles string values.`,
			handler: handler.Get,
		},
		"HSET": {
			name: "HSET",
			description: `Sets the specified fields to their respective values in the hash stored at key.
						This command overwrites the values of specified fields that exist in the hash. 
						If key doesn't exist, a new key holding a hash is created.`,
			handler: handler.Hset,
		},
		"HGET": {
			name:        "HGET",
			description: `Returns the value associated with field in the hash stored at key.`,
			handler:     handler.HGet,
		},
		"HGETALL": {
			name: "HGETALL",
			description: `Returns all fields and values of the hash stored at key. 
						In the returned value, every field name is followed by its value, 
						so the length of the reply is twice the size of the hash.`,
			handler: handler.HGetAll,
		},
		"INFO": {
			name: "INFO",
			description: `The INFO command returns information and statistics about the server 
						in a format that is simple to parse by computers and easy to read by humans.`,
			handler: handler.Info,
		},
		"REPLCONF": {
			name: "REPLCONF",
			description: `The REPLCONF command is an internal command. 
						It is used by a Redis master to configure a connected replica.`,
			handler: handler.ReplConf,
		},
		"PSYNC": {
			name: "PSYNC",
			description: `Initiates a replication stream from the master.
						The PSYNC command is called by Redis replicas for initiating a replication stream from the master.`,
			handler: handler.Psync,
		},
		"WAIT": {
			name: "WAIT",
			description: `This command blocks the current client until all the previous write commands
						are successfully transferred and acknowledged by at least the number of replicas
						you specify in the numreplicas argument. If the value you specify for the timeout
						argument (in milliseconds) is reached, the command returns even if the specified
						 number of replicas were not yet reached.`,
			handler: handler.Wait,
		},
	}
}

func Parse(rawCmdBuf *bytes.Buffer) (Command, error) {
	decoder := proto.NewDecoder(rawCmdBuf)
	value, err := decoder.Decode()
	if err != nil {
		return Command{}, err
	}

	interfaceArr := value.([]any)
	strArr := make([]string, 0, len(interfaceArr))

	for _, elem := range interfaceArr {
		strArr = append(strArr, elem.(string))
	}

	return Command{
		Cmd:  strings.ToUpper(strArr[0]),
		Args: strArr[1:],
	}, nil
}

func ExecuteCmd(cmd Command, store *datastore.Datastore, handler *Handler) (any, bool) {
	var (
		result any
		ready  bool
	)
	if cmdMetaData, ok := GetCommands(handler)[cmd.Cmd]; ok {
		result, ready = cmdMetaData.handler(cmd.Args, store)
		//fmt.Printf("Result after executed: %v\n", result)
	} else {
		result = errors.New("unknown command")
		ready = true
	}

	return result, ready
}

func IsWriteCommand(cmd Command) bool {
	writeCommands := []string{"SET", "HSET"}
	return slices.Contains(writeCommands, cmd.Cmd)
}
