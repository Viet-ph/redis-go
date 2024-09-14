package command

import (
	"bytes"
	"errors"
	"slices"
	"strings"

	"github.com/Viet-ph/redis-go/internal/datastore"
	"github.com/Viet-ph/redis-go/internal/info"
	"github.com/Viet-ph/redis-go/internal/proto"
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

var commands map[string]CmdMetaData

func SetupCommands(handler *Handler) {
	commands = map[string]CmdMetaData{
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
		"CONFIG": {
			name:        "CONFIG",
			description: `This is a container command for runtime configuration commands.`,
			handler:     handler.Config,
		},
		"SAVE": {
			name: "SAVE",
			description: `The SAVE commands performs a synchronous save of the dataset producing a point 
						in time snapshot of all the data inside the Redis instance, in the form of an RDB file.`,
			handler: handler.Save,
		},
		"BGSAVE": {
			name: "BGSAVE",
			description: `Save the DB in background. Normally the OK code is immediately returned. 
						Redis forks, the parent continues to serve the clients, the child saves the DB on disk then exits.`,
			handler: handler.Save,
		},
	}
}

func GetCmdMetadata(cmdName string) (CmdMetaData, bool) {
	metaData, exist := commands[cmdName]
	return metaData, exist
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

func ExecuteCmd(cmd Command, store *datastore.Datastore) (any, bool) {
	var (
		result any
		ready  bool
	)
	if cmdMetaData, ok := GetCmdMetadata(cmd.Cmd); ok {
		result, ready = cmdMetaData.handler(cmd.Args, store)
		//fmt.Printf("Result after executed: %v\n", result)
	} else {
		return errors.New("unknown command"), true
	}

	// If current role is slave, just execute any 'write' commands and forget.
	if IsWriteCommand(cmd) && info.Role == "slave" {
		return nil, false
	}

	return result, ready
}

func IsWriteCommand(cmd Command) bool {
	writeCommands := []string{"SET", "HSET"}
	return slices.Contains(writeCommands, cmd.Cmd)
}
