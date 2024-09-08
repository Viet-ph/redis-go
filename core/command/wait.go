package command

import (
	"context"
	"fmt"

	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/core/info"
	"github.com/Viet-ph/redis-go/core/proto"
	"github.com/Viet-ph/redis-go/internal/connection"
)

// A map that hold repOffs counter for each client/connection
var repOffs = make(map[*connection.Conn]chan int)

func GetRepOffsets(numReplicas int, globalOffsAtTime int, conn *connection.Conn, ctx context.Context) int {
	totalAck := 0
	ackCh := repOffs[conn]
	replicas := connection.GetReplicas()
	cmd := []string{"REPLCONF", "GETACK", "*"}
	encoder := proto.NewEncoder()
	encoder.Encode(cmd, false)
	encodedCmd := encoder.GetBufValue()
	//info.ReplicationOffset += len(encodedCmd)
	for _, replica := range replicas {
		//TODO: Handle writing error
		_ = replica.Propagate(encodedCmd)
	}

loop:
	for {
		fmt.Println("Getting acks...")
		select {
		case offset := <-ackCh:
			fmt.Printf("Rep offset: %d, server offset: %d\n", offset, info.ReplicationOffset)
			if offset >= globalOffsAtTime {
				totalAck += 1
			}
			if totalAck == numReplicas {
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}

	// Close offsets receiving channel and remove it to prevent any other incomming data
	// close(ackCh)
	// delete(repOffs, conn)
	return totalAck
}

func respondWaitCmd(args ...any) error {
	var (
		conn      *connection.Conn
		totalAcks int
	)

	for _, arg := range args {
		switch value := arg.(type) {
		case *connection.Conn:
			conn = value
		case int:
			totalAcks = value
		default:
			return core.ErrorWrongCallBackArgumentType
		}
	}

	encoder := proto.NewEncoder()
	err := encoder.Encode(totalAcks, false)
	if err != nil {
		return err
	}

	return conn.QueueDatas(encoder.GetBufValue())
}
