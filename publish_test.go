package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecPublishPacket(t *testing.T) {
	encoded := []byte{
		0x3B, // PUBPACKID, DUP, 1, RETAIN
		0x0D,
		0x00, 0x03, 'a', '/', 'b',
		0x00, 0x12, // Packet identifier 18
		0x00, // no properties
		'h', 'e', 'l', 'l', 'o',
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding PUBLISH fixed header returned error")

	qos, dup, retain := decodePublishHeader(byte0)
	assert.Equal(t, packettype.PUBLISH, packettype.PacketType(byte0>>4))
	assert.Equal(t, byte(1), qos)
	assert.True(t, dup)
	assert.True(t, retain)
	assert.Equal(t, uint32(0x0D), remainingLength)

	p := Publish{QoSLevel: qos, DUPFlag: dup, Retain: retain}
	err = p.decode(reader, remainingLength)
	assert.NoError(t, err, "Publish.decode returned an error")

	assert.Equal(t, byte(1), p.QoSLevel)
	assert.True(t, p.DUPFlag)
	assert.True(t, p.Retain)
	assert.Equal(t, uint16(0x12), p.packetID)
	assert.Equal(t, "a/b", p.TopicName)
	assert.Equal(t, "hello", string(p.Payload))

	var buf bytes.Buffer
	err = p.encode(&buf)
	assert.NoError(t, err, "Publish.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Publish.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
