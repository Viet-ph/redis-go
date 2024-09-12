package rdb

import (
	"bufio"
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
	EOF      byte = 0xFF
	SELECTDB byte = 0xFE
	//EXPIRETIME   byte = 0xFD
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
		redisVer:  config.RedisVer,
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

func RdbUnMarshall(rdb []byte) (map[string]*datastore.Data, map[string]time.Time, error) {
	buf := bytes.NewReader(rdb)

	// Unmarshal header
	header, err := unmarshalHeader(buf)
	if err != nil {
		return nil, nil, err
	}
	fmt.Println("Unmarshal RDB header: " + string(header))

	// Unmarshal auxiliary data
	auxi, err := unmarshalAuxi(buf)
	if err != nil {
		return nil, nil, err
	}
	fmt.Printf(
		`Unmarshal RDB auxiliary:
	redisVer: %s 
	redisBits: %s
	ctime: %s
	memUsed: %s`,
		auxi.redisVer, auxi.redisBits, auxi.ctime, auxi.usedMem)

	// Unmarshal database
	store, expiry, err := unmarshalDb(buf)
	if err != nil {
		return nil, nil, err
	}

	//// Unmarshal footer
	err = unmarshalFooter(buf)
	if err != nil {
		return nil, nil, err
	}

	return store, expiry, nil
}

func rdbExist() bool {
	rdbFilePath := config.RdbDir + "/" + config.RdbFileName + ".rdb"
	_, err := os.Stat(rdbFilePath)
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	return true
}

func WriteRdbFile(rdbMarshalled []byte) error {
	rdbFilePath := config.RdbDir + "/" + config.RdbFileName + ".rdb"
	file, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	// Create a buffered writer
	writer := bufio.NewWriter(file)

	// Write content to the file
	_, err = writer.Write(rdbMarshalled)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}

	// Flush and close the writer
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing writer:", err)
		return err
	}

	return nil
}

func ReadRdbFile() ([]byte, error) {
	if rdbExist() {
		rdbFilePath := config.RdbDir + "/" + config.RdbFileName + ".rdb"
		dat, err := os.ReadFile(rdbFilePath)
		return dat, err
	}

	return nil, nil
}
