package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecUnsubscribePacket(t *testing.T) {
	encoded := []byte{
		0xA2, 0x0F,
		0x00, 0x10, // Packet identifier 16
		0x00, // no properties
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x05, 'h', 'e', 'l', 'l', 'o',
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding UNSUBSCRIBE fixed header returned error")

	assert.Equal(t, packettype.UNSUBSCRIBE, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x0F), remainingLength)

	s := Unsubscribe{}
	err = s.decode(reader, remainingLength)
	assert.NoError(t, err, "Unsubscribe.decode returned an error")

	assert.Equal(t, uint16(16), s.packetID)
	assert.Equal(t, 2, len(s.TopicFilters))
	assert.Equal(t, "foo", s.TopicFilters[0])
	assert.Equal(t, "hello", s.TopicFilters[1])

	var buf bytes.Buffer
	err = s.encode(&buf)
	assert.NoError(t, err, "Unsubscribe.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Unsubscribe.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
