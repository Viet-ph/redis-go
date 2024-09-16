package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/internal/datastore"
)

type auxiliary struct {
	redisVer  string
	redisBits string
	ctime     string
	usedMem   string
}

func marshallHeader() []byte {
	header := "REDIS" + config.RdbVer
	return []byte(header)
}

func marshallFooter() []byte {
	return []byte{EOF} //EOF
}

func marshallAuxi(auxi auxiliary) ([]byte, error) {
	marshalldAuxi := []byte{AUX}
	redisVer, err := marshallString(auxi.redisVer, LengthPrefixed)
	if err != nil {
		return nil, err
	}
	redisBits, err := marshallString(auxi.redisBits, Int8)
	if err != nil {
		return nil, err
	}
	ctime, err := marshallString(auxi.ctime, LengthPrefixed)
	if err != nil {
		return nil, err
	}
	usedMem, err := marshallString(auxi.usedMem, Int32)
	if err != nil {
		return nil, err
	}
	marshalldAuxi = append(marshalldAuxi, redisVer...)
	marshalldAuxi = append(marshalldAuxi, redisBits...)
	marshalldAuxi = append(marshalldAuxi, ctime...)
	marshalldAuxi = append(marshalldAuxi, usedMem...)

	return marshalldAuxi, nil
}

func marshallDb(ds *datastore.Datastore) ([]byte, error) {
	var buf bytes.Buffer
	store, expiry := ds.DeepCopy()

	// Indicates the start of a database subsection.
	buf.Write([]byte{SELECTDB, 0x00})

	// Indicates that key-value hash table size information follows.
	buf.WriteByte(RESIZEDB)
	encodedStoreSize, err := getLenghEncoding(uint32(len(store)), LengthPrefixed)
	if err != nil {
		return nil, err
	}
	buf.Write(encodedStoreSize)

	// Indicates that expiry hash table table size information follows.
	encodedExpirySize, err := getLenghEncoding(uint32(len(expiry)), LengthPrefixed)
	if err != nil {
		return nil, err
	}
	buf.Write(encodedExpirySize)

	// Key-Value pair starts

	for key, data := range store {
		// "expiry time in ms", followed by 8 byte unsigned long
		if expireAt, hasExpiry := expiry[key]; hasExpiry {
			//If data is already expired, skip it
			if ds.IsExpired(key) {
				continue
			}
			buf.WriteByte(EXPIRETIMEMS)
			timestamp := expireAt.Unix()
			binary.Write(&buf, GlobalEndian, timestamp)
		}

		kvMarshalled, err := marshallKeyValue(key, data.GetValue())
		if err != nil {
			return nil, err
		}

		buf.Write(kvMarshalled)
	}

	return buf.Bytes(), nil
}

func marshallKeyValue(key string, value any) ([]byte, error) {
	var (
		buf             bytes.Buffer
		stringfm        stringFormat
		keyMarshalled   []byte
		valueMarshalled []byte
		err             error
	)

	keyMarshalled, err = marshallString(key, LengthPrefixed)
	if err != nil {
		return nil, err
	}
	buf.Write(keyMarshalled)

	switch v := value.(type) {
	case string:
		buf.WriteByte(0x00) // 1 Byte flag indicate string encoding
		stringfm = getStringFormat(v)
		valueMarshalled, err = marshallString(value.(string), stringfm)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("value type not supported")
	}

	buf.Write(valueMarshalled)

	return buf.Bytes(), nil
}

func marshallString(data string, stringfm stringFormat) ([]byte, error) {
	var buf bytes.Buffer
	length := len(data)
	lengthmarshalled, err := getLenghEncoding(uint32(length), stringfm)
	if err != nil {
		return nil, err
	}

	if stringfm == LengthPrefixed {
		buf.Write(append(lengthmarshalled, []byte(data)...))
	} else {
		buf.Write(lengthmarshalled)
		switch stringfm {
		case Int8:
			int8Val, _ := strconv.Atoi(data)
			buf.WriteByte(byte(int8Val))
		case Int16:
			marshalldData := make([]byte, 2)
			int16Val, _ := strconv.Atoi(data)
			GlobalEndian.PutUint16(marshalldData, uint16(int16Val))
			buf.Write(marshalldData)
		case Int32:
			marshalldData := make([]byte, 4)
			int32Val, _ := strconv.Atoi(data)
			GlobalEndian.PutUint32(marshalldData, uint32(int32Val))
			buf.Write(marshalldData)
		}
	}

	fmt.Printf("Marshalled %s - %d bytes\n", data, buf.Len())
	return buf.Bytes(), nil
}

func getLenghEncoding(length uint32, stringType stringFormat) ([]byte, error) {
	var buf bytes.Buffer
	if stringType == LengthPrefixed {
		switch {
		// Case 1: Length fits in 6 bits (0-63)
		case length <= 0x3F:
			// First two bits are 00, so we can store length directly in the next 6 bits
			firstByte := byte(length & 0x3F) // Get the 6 least significant bits
			buf.WriteByte(firstByte)         // No need for bitwise OR with 0x00 because the two most significant bits are already zero

		// Case 2: Length fits in 14 bits (64-16383)
		case length <= 0x3FFF: // 0x3FFF = 16383
			// First two bits are 01, and the next 14 bits represent the length
			firstByte := byte((length >> 8) & 0x3F) // The top 6 bits of the length
			firstByte |= 0x40                       // Set the first two bits to 01
			secondByte := byte(length & 0xFF)       // The lower 8 bits
			buf.WriteByte(firstByte)
			buf.WriteByte(secondByte)

		// Case 3: Length fits in 32 bits (16384 or larger)
		default:
			// First two bits are 10, followed by 4 bytes of length
			firstByte := byte(0x80) // Set the first two bits to 10
			buf.WriteByte(firstByte)

			// Write the next 4 bytes for the 32-bit length
			lengthBytes := make([]byte, 4)
			GlobalEndian.PutUint32(lengthBytes, length) // Use the specified endianness to write the 32-bit length
			buf.Write(lengthBytes)
		}
	} else {
		// Special encoding (11)
		firstByte := byte(0xC0) // Set the first two bits to 11
		if stringType > 3 {
			return nil, errors.New("unknown string format")
		}
		buf.WriteByte(firstByte | byte(stringType&0x3F)) // marshall special format in the remaining 6 bits
	}

	return buf.Bytes(), nil
}

// Function to check if a string can be parsed as an integer
func isInteger(s string) (int64, bool) {
	// Try to parse the string as an integer
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func getStringFormat(s string) stringFormat {
	if intVal, ok := isInteger(s); ok {
		switch {
		case intVal >= -128 && intVal <= 127:
			return Int8
		case intVal >= -32768 && intVal <= 32767:
			return Int16
		case intVal >= -2147483648 && intVal <= 2147483647:
			return Int32
		}
	}
	return LengthPrefixed
}
