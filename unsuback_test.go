package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecUnsubAckPacket(t *testing.T) {
	encoded := []byte{
		0xB0, 0x06,
		0x00, 0x10, // Packet identifier 16
		0x00, // no properties
		byte(UnsubAckNoSubscriptionExisted),
		byte(UnsubAckReasonCodeNotAuthorized), byte(UnsubAckReasonCodeSuccess),
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding UNSUBACK fixed header returned error")

	assert.Equal(t, packettype.UNSUBACK, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x06), remainingLength)

	us := UnsubAck{}
	err = us.decode(reader, remainingLength)
	assert.NoError(t, err, "UnsubAck.decode returned an error")

	assert.Equal(t, uint16(16), us.packetID)
	assert.Equal(t, 3, len(us.Payload))
	assert.Equal(t, UnsubAckNoSubscriptionExisted, us.Payload[0])
	assert.Equal(t, UnsubAckReasonCodeNotAuthorized, us.Payload[1])
	assert.Equal(t, UnsubAckReasonCodeSuccess, us.Payload[2])

	var buf bytes.Buffer
	err = us.encode(&buf)
	assert.NoError(t, err, "UnsubAck.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("UnsubAck.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
