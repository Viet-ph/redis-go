package rdb

import (
	"bytes"
	"encoding/binary"

	//"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/internal/datastore"
)

const EmptyRdbHexString = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

// Endianess
var GlobalEndian = binary.LittleEndian

// Op codes
const (
	EOF          byte = 0xFF
	SELECTDB     byte = 0xFE
	EXPIRETIME   byte = 0xFD
	EXPIRETIMEMS byte = 0xFC
	RESIZEDB     byte = 0xFB
	AUX          byte = 0xFA
)

// String format
type stringFormat int

const (
	LengthPrefixed stringFormat = -1
	Int8           stringFormat = 0x00
	Int16          stringFormat = 0x01
	Int32          stringFormat = 0x02
)

const BitsPerWord = 32 << (^uint(0) >> 63)

func RdbMarshall(ds *datastore.Datastore) ([]byte, error) {
	// content, err := hex.DecodeString(EmptyRdbHexString)
	// if err != nil {
	// 	return nil, err
	// }

	// length := fmt.Sprintf("$%d\r\n", len(content))
	// bytes := append([]byte(length), content...)
	var buf bytes.Buffer

	// Marshall header
	header := marshallHeader()
	buf.Write(header)

	// Marshall auxiliary
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	auxiliary, err := marshallAuxi(auxiliary{
		redisVer:  "6.0.16",
		redisBits: strconv.Itoa(BitsPerWord),
		ctime:     time.Now().UTC().String(),
		usedMem:   strconv.Itoa(int(m.Alloc / 1024 / 1024)),
	})
	if err != nil {
		return nil, err
	}
	buf.Write(auxiliary)

	// Marshall database
	dbMarshalled, err := marshallDb(ds)
	if err != nil {
		return nil, err
	}
	buf.Write(dbMarshalled)

	// Marshall footer
	footer := marshallFooter()
	buf.Write(footer)

	return buf.Bytes(), nil
}
func RdbUnMarshall(rdb []byte) {
	fmt.Println("Unmarshal RDB: " + string(rdb))
}

func rdbExist() bool {
	_, err := os.Stat(config.RdbDir)
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	return true
}
