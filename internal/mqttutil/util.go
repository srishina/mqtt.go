package mqttutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	maxVarUint32 = 268435455
)

var (
	ErrInvalidUTF8String = errors.New("invalid or malformed utf-8 string")
)

// DecodeByte read and returns the next byte from the reader
// returns EOF when no byte is available to read
func DecodeByte(r io.Reader) (byte, error) {
	value := make([]byte, 1)
	_, err := r.Read(value)
	if err != nil {
		return 0, err
	}

	return value[0], nil
}

// EncodeByte appends the byte val to the buffer
func EncodeByte(buf *bytes.Buffer, val byte) error {
	return buf.WriteByte(val)
}

// DecodeBool read and returns the next byte  from the reader
// and converts to bool
// returns EOF when no byte is available to read
func DecodeBool(r io.Reader) (bool, error) {
	value, err := DecodeByte(r)
	return value != 0, err
}

// EncodeBool appends the boo as byte to the buffer
func EncodeBool(buf *bytes.Buffer, val bool) error {
	return EncodeByte(buf, BoolToByte(val))
}

func DecodeBigEndianUint16(r io.Reader) (uint16, error) {
	value := make([]byte, 2)
	_, err := r.Read(value)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func EncodeBigEndianUint16(buf *bytes.Buffer, value uint16) error {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, value)
	_, err := buf.Write(bytes)
	return err
}

func DecodeBigEndianUint32(r io.Reader) (uint32, error) {
	value := make([]byte, 4)
	_, err := r.Read(value)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}

func EncodeBigEndianUint32(buf *bytes.Buffer, value uint32) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, value)
	_, err := buf.Write(bytes)
	return err
}

func DecodeVarUint32(r io.Reader) (uint32, int, error) {
	consumed := 0
	multiplier := 1
	value := 0

	for {
		encodedByte, err := DecodeByte(r)
		if err != nil {
			return 0, 0, err
		}

		consumed++

		value += int(encodedByte&0x7f) * multiplier

		if multiplier > 128*128*128 {
			return 0, 0, fmt.Errorf("Variable integer contains value of %d which is more than the permissible", value)
		}

		if consumed > 4 {
			return 0, 0, fmt.Errorf("Variable integer contained more than maximum bytes %d", consumed)
		}

		if encodedByte&0x80 == 0 {
			break
		}
		multiplier *= 128
	}
	return uint32(value), consumed, nil
}

func EncodedVarUint32Size(val uint32) uint32 {
	var size uint32
	for ok := true; ok; ok = !(val == 0) {
		encodedByte := val % 0x80
		val = val / 0x80

		if val > 0 {
			encodedByte = encodedByte | 0x80
		}
		size++
	}
	return size
}

func EncodeVarUint32(buf *bytes.Buffer, val uint32) error {
	if val > maxVarUint32 {
		return fmt.Errorf("Variable integer contains value of %d which is more than the permissible", val)
	}

	for ok := true; ok; ok = !(val == 0) {
		encodedByte := val % 0x80
		val = val / 0x80

		if val > 0 {
			encodedByte |= 0x80
		}

		err := buf.WriteByte(byte(encodedByte))
		if err != nil {
			return err
		}
	}

	return nil
}

func DecodeBinaryData(r io.Reader) ([]byte, int, error) {
	buflen, err := DecodeBigEndianUint16(r)
	if err != nil {
		return nil, 0, err
	}

	if buflen == 0 {
		return []byte{}, 2, nil
	}

	payload, nn, err := DecodeBinaryDataNoLength(r, int(buflen))
	return payload, nn + 2, err
}

func DecodeBinaryDataNoLength(r io.Reader, byteToRead int) ([]byte, int, error) {
	payload := make([]byte, byteToRead)
	nn, err := r.Read(payload)
	if err != nil {
		return nil, 0, err
	}

	if nn < byteToRead {
		return nil, 0, io.EOF
	}

	return payload, byteToRead, err
}

func EncodeBinaryData(buf *bytes.Buffer, val []byte) error {
	err := EncodeBigEndianUint16(buf, uint16(len(val)))
	if err != nil {
		return err
	}
	_, err = buf.Write(val)
	return err
}

func EncodeBinaryDataNoLen(buf *bytes.Buffer, val []byte) error {
	_, err := buf.Write(val)
	return err
}

func DecodeUTF8String(r io.Reader) (string, int, error) {
	buf, nn, err := DecodeBinaryData(r)
	if err != nil {
		return "", nn, err
	}

	if !validateUTF8Chars(buf) {
		return "", nn, ErrInvalidUTF8String
	}

	return string(buf[:]), nn, nil
}

func EncodeUTF8String(buf *bytes.Buffer, val string) error {
	err := EncodeBigEndianUint16(buf, uint16(len(val)))
	if err != nil {
		return err
	}
	_, err = buf.WriteString(val)
	return err
}

func validateUTF8Chars(buf []byte) bool {

	for len(buf) > 0 {
		r, size := utf8.DecodeRune(buf)

		if r == utf8.RuneError || !utf8.ValidRune(r) {
			return false
		}

		if r >= '\u0000' && r <= '\u001f' {
			return false
		}

		if r >= '\u007f' && r <= '\u009f' {
			return false
		}

		if size == 0 {
			return true
		}

		buf = buf[size:]
	}

	return true
}

func BoolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func ByteToBool(b byte) bool {
	return b != 0
}

// SliceIndex returns the index of the element position
// returns -1 if not found
func SliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}
