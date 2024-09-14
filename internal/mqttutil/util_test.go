package mqttutil

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodecByte(t *testing.T) {
	testValue := byte(0x64)

	var buffer bytes.Buffer
	err := EncodeByte(&buffer, testValue)
	assert.NoError(t, err, "EncodeByte([0x%X]) failed with error", testValue)

	res, err := DecodeByte(&buffer)
	assert.NoError(t, err, "DecodeByte([%s]) returned an error", buffer)
	assert.Equal(t, testValue, res, "DecodeByte did not return expected result")
}

func TestCodecBool(t *testing.T) {
	testValue := bool(true)

	var buffer bytes.Buffer
	err := EncodeBool(&buffer, testValue)
	assert.NoError(t, err, "encodeBool(%t) failed with error", testValue)

	res, err := DecodeBool(&buffer)
	assert.NoError(t, err, "decodeBool([%s]) returned an error", buffer)
	assert.Equal(t, testValue, res, "decodeBool did not return expected result")
}

func TestCodecBigEndianUint16(t *testing.T) {
	testValue := uint16(128)

	var buffer bytes.Buffer
	err := EncodeBigEndianUint16(&buffer, testValue)
	assert.NoError(t, err, "encodeBigEndianUint16([%d]) failed with error", testValue)

	res, err := DecodeBigEndianUint16(&buffer)
	assert.NoError(t, err, "decodeBigEndianUint16([%s]) returned an error", buffer)
	assert.Equal(t, testValue, res, "decodeBigEndianUint16 did not return expected result")
}

func TestCodecBigEndianUint32(t *testing.T) {
	testValue := uint32(4096)

	var buffer bytes.Buffer
	err := EncodeBigEndianUint32(&buffer, testValue)
	assert.NoError(t, err, "encodeBigEndianUint32([%d]) failed with error", testValue)

	res, err := DecodeBigEndianUint32(&buffer)

	assert.NoError(t, err, "decodeBigEndianUint32([%s]) returned an error", buffer)
	assert.Equal(t, testValue, res, "decodeBigEndianUint32 did not return expected result")
}

func TestCodecVarUint32(t *testing.T) {

	nums := map[uint32][]byte{
		0:         {0x00},
		127:       {0x7F},
		128:       {0x80, 0x01},
		16383:     {0xFF, 0x7F},
		16384:     {0x80, 0x80, 0x01},
		2097151:   {0xFF, 0xFF, 0x7F},
		2097152:   {0x80, 0x80, 0x80, 0x01},
		268435455: {0xFF, 0xFF, 0xFF, 0x7F},
	}

	for n, encoded := range nums {
		var buffer bytes.Buffer

		err := EncodeVarUint32(&buffer, n)
		assert.NoError(t, err, "EncodeVarUint32(%d) failed with error", n)
		assert.Equal(t, encoded, buffer.Bytes(), "EncodeVarUint32 did not return expected result")

		value, consumed, err := DecodeVarUint32(&buffer)
		assert.NoError(t, err, "DecodeVarUint32(%s) failed with error", buffer)
		assert.Equal(t, n, value, "DecodeVarUint32(%s) did not return expected result", buffer)
		assert.Equal(t, len(encoded), consumed, "DecodeVarUint32(%s) did not return expected result", buffer)
	}

	// Test decode error too big
	_, _, err := DecodeVarUint32(bytes.NewBuffer([]byte{0x80, 0x80, 0x80, 0x80, 0x01}))
	assert.Error(t, err, "DecodeVarUint32 did not return an error for a value that is above permissible")

	var buffer bytes.Buffer
	// Test encode error too big
	err = EncodeVarUint32(&buffer, maxVarUint32+1)
	assert.Error(t, err, "EncodeVarUint32 did not return an error for a value(%d) that is above permissible", maxVarUint32+1)

	// Test decode underflow(EOF) error, empty byte array
	_, _, err = DecodeVarUint32(bytes.NewBuffer([]byte{}))
	assert.Error(t, err, "DecodeVarUint32 did not return an error(underflow - EOF) for zero bytes input buffer")

	// Test decode mid underflow(EOF) error, invalid byte array
	_, _, err = DecodeVarUint32(bytes.NewBuffer([]byte{0x80, 0x80, 0x80}))
	assert.Error(t, err, "DecodeVarUint32 did not return an error(mid underflow - EOF) for an invalid input buffer")
}

func TestCodecUTF8String(t *testing.T) {
	strings := map[string][]byte{
		"hello":  {0x00, 0x05, 'h', 'e', 'l', 'l', 'o'},
		"\uFEFF": {0x00, 0x03, 0xEF, 0xBB, 0xBF}, // MQTT-1.5.4-3
	}

	for str, encoded := range strings {
		buf := bytes.NewBuffer(encoded)
		res, _, err := DecodeUTF8String(buf)
		assert.NoError(t, err, "DecodeUTF8String(%v) failed with error", buf)
		assert.Equal(t, str, res, "DecodeUTF8String(%s)  did not return expected result", buf)

		var buffer bytes.Buffer
		_ = EncodeUTF8String(&buffer, str)
		assert.NoError(t, err, "EncodeUTF8String(%s) failed with error", str)
		// assert.Equal(t, encoded, buffer.Bytes(), "encodeUTF8String(%s)  did not return expected result", str)
	}

	// Test decode underflow(EOF) error, empty byte array
	_, _, err := DecodeUTF8String(bytes.NewBuffer([]byte{}))
	assert.EqualError(t, err, "EOF", "DecodeUTF8String did not return an error(underflow - EOF) for zero bytes input buffer")

	// Test decode mid underflow(EOF) error, invalid byte array
	_, _, err = DecodeUTF8String(bytes.NewBuffer([]byte{0x00, 0x05, 'h', 'e'}))
	assert.EqualError(t, err, "EOF", "DecodeUTF8String did not return an error(mid underflow - EOF) for zero bytes input buffer")
}

func TestValidUTF8(t *testing.T) {
	runeByte := make([]byte, 1)
	for i := 0x00; i <= 0x1f; i++ { //\u0000 - \u001f invalid
		runeByte[0] = byte(i)
		if validateUTF8Chars(runeByte) == true {
			t.Fatalf("validateUTF8Chars(%v) did not return %t but %t", runeByte, false, true)
		}
	}
	for i := 0x7F; i <= 0x9f; i++ { //\u007f - \u009f invalid
		runeByte[0] = byte(i)
		if validateUTF8Chars(runeByte) == true {
			t.Fatalf("validateUTF8Chars(%v) did not return %t but %t", runeByte, false, true)
		}
	}
}
