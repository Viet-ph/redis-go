package proto

import (
	"bytes"
	"fmt"
	"strconv"

	custom_err "github.com/Viet-ph/redis-go/internal/error"
)

// Constants for RESP protocol
const (
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	IntegerPrefix      = ':'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
	CRLF               = "\r\n"
)

type Decoder struct {
	buf *bytes.Buffer
}

func NewDecoder(buf *bytes.Buffer) *Decoder {
	return &Decoder{
		buf: buf,
	}
}

// func (decoder *Decoder) ResetBufOffset() {
// 	decoder.buf.Reset()
// 	decoder.buf.Write(make([]byte, config.DefaultMessageSize))
// }

// func (decoder *Decoder) GetBufferLen() int {
// 	return decoder.buf.Len()
// }

func (decoder *Decoder) Decode() (any, error) {
	prefix, _, err := decoder.buf.ReadRune()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case SimpleStringPrefix:
		return decoder.decodeSimpleString()
	// case ErrorPrefix:
	// 	return decodeError()
	case IntegerPrefix:
		return decoder.decodeInteger()
	case BulkStringPrefix:
		return decoder.decodeBulkString()
	case ArrayPrefix:
		return decoder.decodeArray()
	default:
		return nil, fmt.Errorf("unknown prefix: %v", prefix)
	}
}

func (decoder *Decoder) decodeSimpleString() (string, error) {
	line, err := decoder.buf.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line[:len(line)-2], nil // Remove CRLF
}

func (decoder *Decoder) decodeInteger() (int64, error) {
	line, err := decoder.buf.ReadString('\n')
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(line[:len(line)-2], 10, 64) // Remove CRLF
}

func (decoder *Decoder) decodeBulkString() (string, error) {
	line, err := decoder.buf.ReadString('\n')
	if err != nil {
		return "", err
	}

	length, err := strconv.Atoi(line[:len(line)-2]) // Remove CRLF
	if err != nil {
		return "", err
	}
	if length == -1 {
		return "", nil // Null bulk string
	}

	data, err := decoder.buf.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	//fmt.Println("Decoded string: " + string(data[:length]))
	return string(data[:length]), nil
}

func (decoder *Decoder) decodeArray() ([]any, error) {
	line, err := decoder.buf.ReadString('\n')
	firstByte := line[0]
	if err != nil {
		return nil, err
	}

	arrLength, err := strconv.Atoi(string(firstByte))
	if err != nil {
		return nil, err
	}

	elements := make([]any, arrLength)
	for i := range arrLength {
		elem, err := decoder.Decode()
		if err != nil {
			return nil, err
		}
		elements[i] = elem
	}

	return elements, nil

}

// Encoder is responsible for encoding RESP messages to an io.Writer.
type Encoder struct {
	buf *bytes.Buffer
}

// NewEncoder creates a new Encoder.
func NewEncoder() *Encoder {
	return &Encoder{
		buf: bytes.NewBuffer(make([]byte, 0)),
	}
}

// Encode takes a Go value and encodes it as a RESP message.
func (encoder *Encoder) Encode(data any, isSimple bool) error {
	switch v := data.(type) {
	case string:
		return encoder.encodeString(v, isSimple)
	case error:
		return encoder.encodeError(v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		_, err := encoder.buf.WriteString(fmt.Sprintf("%c%d%s", IntegerPrefix, data, CRLF))
		if err != nil {
			return err
		}
		return nil
	case []string:
		return encoder.encodeArray(v)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}

func (encoder *Encoder) encodeError(err error) error {
	var writeData string
	if err == custom_err.ErrorKeyNotExists {
		writeData = fmt.Sprintf("%c%d%s", BulkStringPrefix, -1, CRLF)
	} else {
		writeData = fmt.Sprintf("%c%s%s", ErrorPrefix, err.Error(), CRLF)
	}
	_, err = encoder.buf.WriteString(writeData)
	if err != nil {
		return err
	}
	return nil
}

func (encoder *Encoder) encodeString(data string, isSimple bool) error {
	var writeData string
	if isSimple {
		writeData = fmt.Sprintf("%c%s%s", SimpleStringPrefix, data, CRLF)
	} else {
		writeData = fmt.Sprintf("%c%d%s%s%s", BulkStringPrefix, len(data), CRLF, data, CRLF)
	}
	_, err := encoder.buf.WriteString(writeData)
	if err != nil {
		return err
	}

	return nil
}

func (encoder *Encoder) encodeArray(datas []string) error {
	_, err := encoder.buf.WriteString(fmt.Sprintf("%c%d%s", ArrayPrefix, len(datas), CRLF))
	if err != nil {
		return err
	}

	for _, data := range datas {
		err := encoder.Encode(data, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (encoder *Encoder) GetBufValue() []byte {
	return encoder.buf.Bytes()
}

func (encoder *Encoder) Reset() {
	encoder.buf.Reset()
}
