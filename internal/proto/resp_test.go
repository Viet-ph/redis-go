package proto_test

import (
	"bytes"
	"testing"

	custom_err "github.com/Viet-ph/redis-go/internal/error"
	"github.com/Viet-ph/redis-go/internal/proto"
)

func TestDecodeSimpleString(t *testing.T) {
	// Simulate a RESP encoded simple string "+OK\r\n"
	encodedData := []byte("+OK\r\n")
	buf := bytes.NewBuffer(encodedData)
	decoder := proto.NewDecoder(buf)

	result, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "OK"
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestDecodeInteger(t *testing.T) {
	// Simulate a RESP encoded integer ":1000\r\n"
	encodedData := []byte(":1000\r\n")
	buf := bytes.NewBuffer(encodedData)
	decoder := proto.NewDecoder(buf)

	result, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := int64(1000)
	if result != expected {
		t.Errorf("Expected %d but got %d", expected, result)
	}
}

func TestDecodeBulkString(t *testing.T) {
	// Simulate a RESP encoded bulk string "$5\r\nhello\r\n"
	encodedData := []byte("$5\r\nhello\r\n")
	buf := bytes.NewBuffer(encodedData)
	decoder := proto.NewDecoder(buf)

	result, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "hello"
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestDecodeNullBulkString(t *testing.T) {
	// Simulate a RESP encoded null bulk string "$-1\r\n"
	encodedData := []byte("$-1\r\n")
	buf := bytes.NewBuffer(encodedData)
	decoder := proto.NewDecoder(buf)

	result, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "" {
		t.Errorf("Expected empty string but got %q", result)
	}
}

func TestDecodeArray(t *testing.T) {
	// Simulate a RESP encoded array "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
	encodedData := []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
	buf := bytes.NewBuffer(encodedData)
	decoder := proto.NewDecoder(buf)

	result, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := []any{"hello", "world"}
	if len(result.([]any)) != len(expected) {
		t.Errorf("Expected array length %d but got %d", len(expected), len(result.([]any)))
	}

	for i, val := range expected {
		if result.([]any)[i] != val {
			t.Errorf("Expected array element %d to be %q but got %q", i, val, result.([]any)[i])
		}
	}
}

func TestEncodeSimpleString(t *testing.T) {
	encoder := proto.NewEncoder()

	err := encoder.Encode("OK", true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "+OK\r\n"
	result := string(encoder.GetBufValue())
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestEncodeInteger(t *testing.T) {
	encoder := proto.NewEncoder()

	err := encoder.Encode(1000, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := ":1000\r\n"
	result := string(encoder.GetBufValue())
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestEncodeBulkString(t *testing.T) {
	encoder := proto.NewEncoder()

	err := encoder.Encode("hello", false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "$5\r\nhello\r\n"
	result := string(encoder.GetBufValue())
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestEncodeArray(t *testing.T) {
	encoder := proto.NewEncoder()

	err := encoder.Encode([]string{"hello", "world"}, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
	result := string(encoder.GetBufValue())
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

func TestEncodeError(t *testing.T) {
	encoder := proto.NewEncoder()

	testError := custom_err.ErrorKeyNotExists
	err := encoder.Encode(testError, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "$-1\r\n"
	result := string(encoder.GetBufValue())
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}
