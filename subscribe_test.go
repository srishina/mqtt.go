package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecSubscribePacket(t *testing.T) {
	encoded := []byte{
		0x82, 0x17,
		0x00, 0x12, // Packet identifier 18
		0x00,                            // no properties
		0x00, 0x03, 'a', '/', 'b', 0x01, // a/b with QoS 1
		0x00, 0x03, 'c', '/', 'd', 0x02,
		0x00, 0x05, 'e', '/', 'f', '/', 'g', 0x00,
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding SUBSCRIBE fixed header returned error")

	assert.Equal(t, packettype.SUBSCRIBE, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x17), remainingLength)

	s := Subscribe{}
	err = s.decode(reader, remainingLength)
	assert.NoError(t, err, "Subscribe.decode returned an error")

	assert.Equal(t, uint16(0x12), s.packetID)
	assert.Equal(t, 3, len(s.Subscriptions))
	assert.Equal(t, "a/b", s.Subscriptions[0].TopicFilter)
	assert.Equal(t, byte(1), s.Subscriptions[0].QoSLevel)
	assert.False(t, s.Subscriptions[0].NoLocal)
	assert.False(t, s.Subscriptions[0].RetainAsPublished)
	assert.Equal(t, "c/d", s.Subscriptions[1].TopicFilter)
	assert.Equal(t, byte(2), s.Subscriptions[1].QoSLevel)
	assert.Equal(t, "e/f/g", s.Subscriptions[2].TopicFilter)
	assert.Equal(t, byte(0), s.Subscriptions[2].QoSLevel)

	var buf bytes.Buffer
	err = s.encode(&buf)
	assert.NoError(t, err, "Subscribe.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Subscribe.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
