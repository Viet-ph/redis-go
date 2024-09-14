package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Viet-ph/redis-go/internal/datastore"
)

func unmarshalHeader(buf *bytes.Reader) (string, error) {
	header := make([]byte, 9) // "REDIS0011" (9 bytes)
	_, err := buf.Read(header)
	if err != nil {
		return "", err
	}

	if string(header[:5]) != "REDIS" {
		return "", errors.New("invalid RDB header")
	}

	version := string(header[5:])
	return version, nil
}

func unmarshalFooter(buf *bytes.Reader) error {
	eof, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if eof != EOF {
		return errors.New("invalid EOF marker")
	}
	return nil
}

func unmarshalAuxi(buf *bytes.Reader) (auxiliary, error) {
	var auxi auxiliary

	// Verify that the next byte indicates the AUX section
	auxOpCode, err := buf.ReadByte()
	if err != nil || auxOpCode != AUX {
		return auxi, errors.New("invalid AUX marker")
	}

	// Unmarshal auxiliary fields
	auxi.redisVer, err = unmarshalString(buf)
	if err != nil {
		return auxi, err
	}
	fmt.Println("Redis-ver: " + auxi.redisVer)

	auxi.redisBits, err = unmarshalString(buf)
	if err != nil {
		return auxi, err
	}
	fmt.Println("Redis-bits: " + auxi.redisBits)

	auxi.ctime, err = unmarshalString(buf)
	if err != nil {
		return auxi, err
	}
	fmt.Println("ctime: " + auxi.ctime)

	auxi.usedMem, err = unmarshalString(buf)
	if err != nil {
		return auxi, err
	}
	fmt.Println("Used Memory: " + auxi.usedMem)

	return auxi, nil
}

func unmarshalKeyValue(buf *bytes.Reader) (string, string, error) {
	key, err := unmarshalString(buf)
	if err != nil {
		return "", "", err
	}

	valueType, err := buf.ReadByte()
	if err != nil {
		return "", "", err
	}

	var value string
	switch valueType {
	case 0x00: // String encoding
		value, err = unmarshalString(buf)
		if err != nil {
			return "", "", err
		}
	default:
		return "", "", errors.New("unknown value type")
	}

	return key, value, nil
}

func unmarshalDb(buf *bytes.Reader) (map[string]*datastore.Data, map[string]time.Time, error) {
	var (
		store  map[string]*datastore.Data
		expiry map[string]time.Time
	)
	// Read SELECTDB flag and database index
	selectDbOpCode, err := buf.ReadByte()
	if err != nil || selectDbOpCode != SELECTDB {
		return nil, nil, errors.New("invalid SELECTDB marker")
	}

	// Read and discard the DB index (in this example, it's 0x00)
	_, err = buf.ReadByte()
	if err != nil {
		return nil, nil, err
	}

	// Read RESIZEDB flag and hash table sizes (store size and expiry size)
	resizeDbFlag, err := buf.ReadByte()
	if err != nil || resizeDbFlag != RESIZEDB {
		return nil, nil, errors.New("invalid RESIZEDB marker")
	}

	// Read key-value store size and initialize hash table
	storeSize, _, err := unmarshalLength(buf)
	if err != nil {
		return nil, nil, err
	}
	store = make(map[string]*datastore.Data, storeSize)

	// Read expiry hash table size and initialize hash table
	expirySize, _, err := unmarshalLength(buf)
	if err != nil {
		return nil, nil, err
	}
	expiry = make(map[string]time.Time, expirySize)

	// Now read key-value pairs
	for i := 0; i < storeSize; i++ {
		var (
			expireAt  time.Time
			isExpired bool
			hasExpiry bool
		)
		// Check if entry has expiry
		expireTimeOpCode, err := buf.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		hasExpiry = expireTimeOpCode == EXPIRETIMEMS
		if hasExpiry {
			var timestamp int64
			binary.Read(buf, GlobalEndian, &timestamp)
			expireAt = time.Unix(timestamp, 0)
			isExpired = expireAt.Before(time.Now().UTC())
		} else {
			// Must unread here to set buf offset to previous position
			_ = buf.UnreadByte()
		}

		key, value, err := unmarshalKeyValue(buf)
		if err != nil {
			return nil, nil, err
		}

		// Discard storage entry if it's already expired
		if isExpired {
			continue
		}

		// Add entry into key-value has table and expiry hash table
		store[key] = datastore.NewData(value)
		if hasExpiry {
			expiry[key] = expireAt
		}
	}

	return store, expiry, nil
}

func unmarshalString(buf *bytes.Reader) (string, error) {
	length, stringFormat, err := unmarshalLength(buf)
	if err != nil {
		return "", err
	}

	switch stringFormat {
	case LengthPrefixed:
		stringData := make([]byte, length)
		_, err = buf.Read(stringData)
		if err != nil {
			return "", err
		}
		return string(stringData), nil
	case Int8:
		int8Val, err := buf.ReadByte()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(int8Val)), nil
	case Int16:
		var int16Val int16
		err := binary.Read(buf, GlobalEndian, &int16Val)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(int16Val)), nil
	case Int32:
		var int32Val int32
		err := binary.Read(buf, GlobalEndian, &int32Val)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(int32Val)), nil
	default:
		return "", errors.New("unknown string format")
	}
}

func unmarshalLength(buf *bytes.Reader) (int, stringFormat, error) {
	firstByte, err := buf.ReadByte()
	if err != nil {
		return 0, LengthPrefixed, err
	}
	fmt.Printf("Length encoding: %d - %b\n", firstByte, firstByte)

	// Check first two bits
	sizeEncodingBits := firstByte >> 6
	switch sizeEncodingBits {
	case 0x00:
		// Case 1: Length fits in 6 bits (0-63)
		return int(firstByte & 0x3F), LengthPrefixed, nil
	case 0x01:
		// Case 2: Length fits in 14 bits (64-16383)
		secondByte, err := buf.ReadByte()
		if err != nil {
			return 0, LengthPrefixed, err
		}
		length := ((int(firstByte & 0x3F)) << 8) | int(secondByte)
		return length, LengthPrefixed, nil
	case 0x02:
		// Case 3: Length fits in 32 bits (16384 or larger)
		var length uint32
		err := binary.Read(buf, GlobalEndian, &length)
		if err != nil {
			return 0, LengthPrefixed, err
		}
		return int(length), LengthPrefixed, nil
	case 0x03:
		// Special encoding (11)
		format := firstByte & 0x3F
		switch format {
		case 0:
			return 1, Int8, nil
		case 1:
			return 2, Int16, nil
		case 2:
			return 4, Int32, nil
		}
	default:
	}

	return 0, LengthPrefixed, errors.New("invalid length encoding")
}
