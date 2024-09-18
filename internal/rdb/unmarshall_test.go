package rdb

import (
	"bytes"
	"testing"
)

func TestUnmarshalKeyValue(t *testing.T) {
	tests := []struct {
		encodedData []byte
		expectedKey string
		expectedVal string
		shouldErr   bool
	}{
		// Case 1: value type (0x00), key "testKey", value "testValue"
		{
			encodedData: []byte{0x00, 0x07, 't', 'e', 's', 't', 'K', 'e', 'y', 0x09, 't', 'e', 's', 't', 'V', 'a', 'l', 'u', 'e'},
			expectedKey: "testKey",
			expectedVal: "testValue",
		},
		// Case 2: Unknown value type
		{
			encodedData: []byte{0xFF, 0x07, 't', 'e', 's', 't', 'K', 'e', 'y', 0x05, 'v', 'a', 'l', 'u', 'e'}, // Unknown type 0xFF
			shouldErr:   true,
		},
		// Case 3: Wrong length encoding
		{
			encodedData: []byte{0x00, 0x07, 't', 'e', 's', 't', 'K', 'e', 'y', 0x05, 'v', 'a', 'l'}, // Corrupted string (incomplete encoded data)
			shouldErr:   true,
		},
	}

	for _, tc := range tests {
		buf := bytes.NewReader(tc.encodedData)
		key, value, err := unmarshalKeyValue(buf)

		if (err != nil) != tc.shouldErr {
			t.Errorf("Unexpected error for data %v: %v", tc.encodedData, err)
		}

		if !tc.shouldErr {
			if key != tc.expectedKey {
				t.Errorf("Expected key %q but got %q", tc.expectedKey, key)
			}
			if value != tc.expectedVal {
				t.Errorf("Expected value %q but got %q", tc.expectedVal, value)
			}
		}
	}
}

func TestUnmarshalString(t *testing.T) {
	tests := []struct {
		encodedData []byte
		expected    string
		shouldErr   bool
	}{
		// Case 1: Corrupted string (incomplete data)
		{
			encodedData: []byte{0x05, 'h', 'e', 'l', 'l'}, // Missing 'o'
			shouldErr:   true,
		},
		// Case 2: Int8 encoding (127)
		{
			encodedData: []byte{0xC0, 0x7F}, // Special encoding for Int8 (127)
			expected:    "127",
		},
		// Case 3: Int16 encoding (32767)
		{
			encodedData: []byte{0xC1, 0xFF, 0x7F}, // Special encoding for Int16 (32767)
			expected:    "32767",
		},
		// Case 4: Int32 encoding (2147483647)
		{
			encodedData: []byte{0xC2, 0xFF, 0xFF, 0xFF, 0x7F}, // Special encoding for Int32 (2147483647)
			expected:    "2147483647",
		},
	}

	for _, tc := range tests {
		buf := bytes.NewReader(tc.encodedData)
		result, err := unmarshallString(buf)

		if (err != nil) != tc.shouldErr {
			t.Errorf("Unexpected error for data %v: %v", tc.encodedData, err)
		}

		if !tc.shouldErr && result != tc.expected {
			t.Errorf("Expected %q but got %q", tc.expected, result)
		}
	}
}

func TestUnmarshalLength_EdgeCases(t *testing.T) {
	tests := []struct {
		input          []byte
		expected       int
		expectedFormat StringFormat
		shouldErr      bool
	}{
		// Case 1: Invalid length encoding (corrupted data)
		{
			input:     []byte{0xFF}, // Invalid encoding
			shouldErr: true,
		},
		// Case 2: Special encoding Int8 with edge value (127)
		{
			input:          []byte{0xC0}, // Special encoding for Int8
			expected:       1,
			expectedFormat: Int8,
		},
		// Case 3: Special encoding Int16 with edge value (32767)
		{
			input:          []byte{0xC1}, // Special encoding for Int16
			expected:       2,
			expectedFormat: Int16,
		},
		// Case 4: Special encoding Int32 with edge value (2147483647)
		{
			input:          []byte{0xC2}, // Special encoding for Int32
			expected:       4,
			expectedFormat: Int32,
		},
	}

	for _, tc := range tests {
		buf := bytes.NewReader(tc.input)
		result, format, err := unmarshalLength(buf)

		if (err != nil) != tc.shouldErr {
			t.Errorf("Unexpected error for input %v: %v", tc.input, err)
		}

		if !tc.shouldErr && result != tc.expected {
			t.Errorf("Expected length %d but got %d", tc.expected, result)
		}

		if !tc.shouldErr && format != tc.expectedFormat {
			t.Errorf("Expected format %v but got %v", tc.expectedFormat, format)
		}
	}
}
