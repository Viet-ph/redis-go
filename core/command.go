package core

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Viet-ph/redis-go/datastore"
)

type Command struct {
	Cmd  string
	Args []string
}

type CmdMetaData struct {
	name        string
	description string
	handler     func([]string, *datastore.Datastore) any
}

var Commands = map[string]CmdMetaData{
	"PING": {
		name: "PING",
		description: `PING returns with an encoded "PONG" If any message is 
					added with the ping command,the message will be returned.`,
		handler: handlePing,
	},
	"SET": {
		name: "SET",
		description: `Set key to hold the string value. If key already holds a value,
					 it is overwritten, regardless of its type. Any previous time to 
					 live associated with the key is discarded on successful SET operation.`,
		handler: handleSet,
	},
	"GET": {
		name: "GET",
		description: `Get the value of key. If the key does not exist the special value nil is returned. 
					An error is returned if the value stored at key is not a string, because GET only handles string values.`,
		handler: handleGet,
	},
	"HSET": {
		name: "HSET",
		description: `Sets the specified fields to their respective values in the hash stored at key.
					This command overwrites the values of specified fields that exist in the hash. 
					If key doesn't exist, a new key holding a hash is created.`,
		handler: handleHset,
	},
	"HGET": {
		name:        "HGET",
		description: `Returns the value associated with field in the hash stored at key.`,
		handler:     handleHGet,
	},
	"HGETALL": {
		name: "HGETALL",
		description: `Returns all fields and values of the hash stored at key. 
					In the returned value, every field name is followed by its value, 
					so the length of the reply is twice the size of the hash.`,
		handler: handleHGetAll,
	},
	"INFO": {
		name: "INFO",
		description: `The INFO command returns information and statistics about the server 
					in a format that is simple to parse by computers and easy to read by humans.`,
		handler: handleInfo,
	},
	"REPLCONF": {
		name: "REPLCONF",
		description: `The REPLCONF command is an internal command. 
					It is used by a Redis master to configure a connected replica.`,
		handler: handleReplConf,
	},
	"PSYNC": {
		name: "PSYNC",
		description: `Initiates a replication stream from the master.
					The PSYNC command is called by Redis replicas for initiating a replication stream from the master.`,
		handler: handlePsync,
	},
}

func ExecuteCmd(cmd Command, store *datastore.Datastore) []byte {
	encoder := NewEncoder()
	var result any
	if cmdMetaData, ok := Commands[cmd.Cmd]; ok {
		result = cmdMetaData.handler(cmd.Args, store)
		err := encoder.Encode(result, true)
		fmt.Println(result)
		if err != nil {
			encoder.Reset()
			result = errors.New("error encoding resp")
			encoder.Encode(result, false)
		}
	} else {
		result = errors.New("unknown command")
		encoder.Encode(result, false)
	}

	return encoder.GetBufValue()
}

func IsWriteCommand(cmd Command) bool {
	writeCommands := []string{"SET", "HSET"}
	return slices.Contains(writeCommands, cmd.Cmd)
}
