package rdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"

	//"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/internal/datastore"
	"golang.org/x/sys/unix"
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
type StringFormat int

const (
	LengthPrefixed StringFormat = -1
	Int8           StringFormat = 0x00
	Int16          StringFormat = 0x01
	Int32          StringFormat = 0x02
)

// ^uint(0): flips all bits to 1
// ^uint(0) >> 63: rightshift all bits by 63 places, results 0 on 32bit system and 1 on 64bit system
// 32 << (0 or 1): If the result of ^uint(0) >> 63 is 1 (on a 64-bit system), this becomes 32 << 1, which equals 64, otherwise 32.
const BitsPerWord = 32 << (^uint(0) >> 63)

func RdbMarshall(ds *datastore.Datastore) ([]byte, error) {
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

	fmt.Printf("RDB len: %d\n", len(buf.Bytes()))

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
		`
Unmarshal RDB auxiliary:
redisVer: %s 
redisBits: %s
ctime: %s
memUsed: %s

`,
		auxi.redisVer, auxi.redisBits, auxi.ctime, auxi.usedMem)

	// Unmarshal database
	store, expiry, err := unmarshalDb(buf)
	if err != nil {
		return nil, nil, err
	}

	// Unmarshal footer
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
	file, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	// Advisory lock here in case multiple redis processes using 1 rdb file.
	// Advisory locking is not an enforced locking scheme. It will work only if the
	// participating processes are cooperating by explicitly acquiring locks.
	// Otherwise, advisory locks will be ignored if a process is not aware of locks at all.

	// An example may help to understand the cooperative locking scheme easier.
	// Let’s take our rdb file as an example.
	// -- First, we assume that the file dump.rdb still contains the key-value of [mykey]myvalue.
	// -- Process A acquires an exclusive lock on the dump.rdb file, then opens and reads the file to get the current value: [mykey]myvalue.
	// We must understand that the advisory lock was not set by the operating system or file system.
	// Therefore, even if process A locks the file, process B is still free to read, write, or even delete the file via system calls.
	// If process B executes file operations without trying to acquire a lock, we say process B is not cooperating with process A.
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	// Unlock here may redundant since file.close would unblock it.
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN)

	// Write content to the file
	_, err = file.Write(rdbMarshalled)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}

	fmt.Println("RDB wrote sucessfullly.")

	return nil
}

func ReadRdbFile() ([]byte, error) {
	var data []byte
	if !rdbExist() {
		//File not exist, do nothing
		return nil, nil
	}

	rdbFilePath := config.RdbDir + "/" + config.RdbFileName + ".rdb"
	file, err := os.Open(rdbFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Advisory lock here
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return nil, err
	}
	// Unlock here may redundant since file.close would unblock it.
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN)

	var size int
	if info, err := file.Stat(); err == nil {
		size64 := info.Size()
		if int64(int(size64)) == size64 {
			size = int(size64)
		}
	}
	size++ // one byte for final read at EOF

	// If a file claims a small size, read at least 512 bytes.
	// In particular, files in Linux's /proc claim size 0 but
	// then do not work right if read in small pieces,
	// so an initial read of 1 byte would not work correctly.
	if size < 512 {
		size = 512
	}

	data = make([]byte, 0, size)
	for {
		n, err := file.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}

		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
	}
}

// Spawn a goroutine to track total number of key changes
// and marshall -> write rdb the data
func PersistData(ds *datastore.Datastore) {
	numKeyChanges := 0
	ticker := time.NewTicker(time.Duration(config.Interval) * time.Second)
	go func() {
		for {
			select {
			case <-ds.KeyChangesCh:
				numKeyChanges++
			case <-ticker.C:
				if numKeyChanges >= config.NumKeyChanges {
					fmt.Println("Saving RDB...")
					rdbMarshalled, err := RdbMarshall(ds)
					if err != nil {
						fmt.Println(err.Error())
					}
					err = WriteRdbFile(rdbMarshalled)
					if err != nil {
						fmt.Println(err.Error())
					}
				}
			}
		}
	}()
}

func EmptyRdb() []byte {
	content, _ := hex.DecodeString(EmptyRdbHexString)
	// if err != nil {
	// 	return nil, err
	// }

	length := fmt.Sprintf("$%d\r\n", len(content))
	return append([]byte(length), content...)
}
