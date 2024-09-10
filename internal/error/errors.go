package custom_err

import "errors"

var (
	ErrorIncompleteRESP     = errors.New("incomplete RESP data")
	ErrorKeyNotExists       = errors.New("target key doesn't exist")
	ErrorNotFullyWritten    = errors.New("data not fully written to socket")
	ErrorClientDisconnected = errors.New("client disconnected")
	ErrorReadingSocket      = errors.New("failed to copy data from kernal space to user space")

	ErrorRequeueTask               = errors.New("task executed with failure, needs to be requeued")
	ErrorWrongCallBackArgumentType = errors.New("callback argument(s) underlying type not correct")
)
