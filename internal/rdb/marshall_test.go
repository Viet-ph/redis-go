package rdb

import (
	"bytes"
	"testing"
)

func TestGetLengthEncoding(t *testing.T) {
	tests := []struct {
		length     uint32
		stringType StringFormat
		expected   []byte
		shouldErr  bool
	}{
		// Case 1: Length fits in 6 bits (0-63)
		{
			length:     42,
			stringType: LengthPrefixed,
			expected:   []byte{0x2A}, // 0x2A == 42
		},
		// Case 2: Length fits in 14 bits (64-16383)
		{
			length:     300,
			stringType: LengthPrefixed,
			expected:   []byte{0x41, 0x2C}, // 0x41 is 01xxxxxx, 0x2C is the least significant bits (300 = 0x12C)
		},
		// Case 3: Length fits in 32 bits (16384 or larger)
		{
			length:     70000,
			stringType: LengthPrefixed,
			expected:   []byte{0x80, 0x70, 0x11, 0x01, 0x00}, // 0x80 + little-endian encoding of 70000 (0x00011170)
		},
		// Special encoding (11) for stringType other than LengthPrefixed
		{
			length:     0, // Length isn't used for this case
			stringType: Int8,
			expected:   []byte{0xC0}, // Special encoding for Int8
		},
		{
			length:     0, // Length isn't used for this case
			stringType: Int16,
			expected:   []byte{0xC1}, // Special encoding for Int16
		},
		{
			length:     0, // Length isn't used for this case
			stringType: Int32,
			expected:   []byte{0xC2}, // Special encoding for Int32
		},
		// Case 4: Invalid stringType
		{
			length:     0,
			stringType: 4, // Invalid type
			shouldErr:  true,
		},
	}

	for _, tc := range tests {
		result, err := getLenghEncoding(tc.length, tc.stringType)

		// Check if there was an unexpected error
		if (err != nil) != tc.shouldErr {
			t.Errorf("Unexpected error for length %d, stringType %d: %v", tc.length, tc.stringType, err)
		}

		// If an error was expected, skip further checks
		if tc.shouldErr {
			continue
		}

		// Check if the result matches the expected output
		if !bytes.Equal(result, tc.expected) {
			t.Errorf("For length %d, stringType %d, expected %v but got %v", tc.length, tc.stringType, tc.expected, result)
		}
	}
}

func TestMarshallString(t *testing.T) {
	tests := []struct {
		data      string
		stringfm  StringFormat
		expected  []byte
		shouldErr bool
	}{
		// Edge case 1: Empty string
		{
			data:     "",
			stringfm: LengthPrefixed,
			expected: []byte{0x00}, // Length-prefixed empty string (length 0)
		},
		// Edge case 2: Short string (1 character)
		{
			data:     "a",
			stringfm: LengthPrefixed,
			expected: []byte{0x01, 'a'}, // Length-prefixed string with 1 character
		},
		// Edge case 3: Long string (64+ characters)
		{
			data:     "thisisaverylongstringthatexceedssixtythreecharactersfortestingpurposes",
			stringfm: LengthPrefixed,
			expected: []byte{0x40, 0x46, 't', 'h', 'i', 's', 'i', 's', 'a', 'v', 'e', 'r', 'y', 'l', 'o', 'n', 'g', 's', 't', 'r', 'i', 'n', 'g', 't', 'h', 'a', 't', 'e', 'x', 'c', 'e', 'e', 'd', 's', 's', 'i', 'x', 't', 'y', 't', 'h', 'r', 'e', 'e', 'c', 'h', 'a', 'r', 'a', 'c', 't', 'e', 'r', 's', 'f', 'o', 'r', 't', 'e', 's', 't', 'i', 'n', 'g', 'p', 'u', 'r', 'p', 'o', 's', 'e', 's'},
		},
		// Edge case 4: Integer string (small value)
		{
			data:     "127",
			stringfm: Int8,
			expected: []byte{0xC0, 0x7F}, // Special encoding for Int8 (127)
		},
		// Edge case 5: Integer string (negative value)
		{
			data:     "-128",
			stringfm: Int8,
			expected: []byte{0xC0, 0x80}, // Special encoding for Int8 (-128)
		},
		// Edge case 6: Integer string (Int16 value in little-endian)
		{
			data:     "32767",
			stringfm: Int16,
			expected: []byte{0xC1, 0xFF, 0x7F}, // Little-endian encoding for Int16 (32767 -> 0x7FFF -> FF 7F)
		},
		// Edge case 7: Integer string (negative Int16 value in little-endian)
		{
			data:     "-32768",
			stringfm: Int16,
			expected: []byte{0xC1, 0x00, 0x80}, // Little-endian encoding for Int16 (-32768 -> 0x8000 -> 00 80)
		},
		// Edge case 8: Integer string (Int32 value in little-endian)
		{
			data:     "2147483647",
			stringfm: Int32,
			expected: []byte{0xC2, 0xFF, 0xFF, 0xFF, 0x7F}, // Little-endian encoding for Int32 (2147483647 -> 0x7FFFFFFF -> FF FF FF 7F)
		},
		// Edge case 9: Integer string (negative Int32 value in little-endian)
		{
			data:     "-2147483648",
			stringfm: Int32,
			expected: []byte{0xC2, 0x00, 0x00, 0x00, 0x80}, // Little-endian encoding for Int32 (-2147483648 -> 0x80000000 -> 00 00 00 80)
		},
	}

	for _, tc := range tests {
		// Call the function
		result, err := marshallString(tc.data, tc.stringfm)

		// Check if there was an unexpected error
		if (err != nil) != tc.shouldErr {
			t.Errorf("Unexpected error for %q: %v", tc.data, err)
		}

		// Check if the result matches the expected output
		if !bytes.Equal(result, tc.expected) {
			t.Errorf("For %q, expected %v but got %v", tc.data, tc.expected, result)
		}
	}
}
