package command

import (
	"context"
	"fmt"

	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/core/info"
	"github.com/Viet-ph/redis-go/core/proto"
	"github.com/Viet-ph/redis-go/internal/connection"
)

type OffsTracker struct {
	// Offset captured after processed write commands for each context
	CapturedOffs int

	// Channel to acknowledge replication offsets
	AckCh chan int
}

// A map that hold OffsTracking counter for each client context
var OffsTracking = make(map[*connection.Conn]*OffsTracker)

func GetRepOffsets(numReplicas int, conn *connection.Conn, ctx context.Context) int {
	totalAck := 0
	offsTracker := OffsTracking[conn]
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
		case offset := <-offsTracker.AckCh:
			fmt.Printf("Rep offset: %d, server offset: %d\n", offset, info.ReplicationOffset)
			if offset >= offsTracker.CapturedOffs {
				totalAck += 1
			}
			if totalAck == numReplicas {
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}

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
