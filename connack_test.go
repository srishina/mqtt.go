package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecConnAckPacket(t *testing.T) {
	encoded := []byte{0x20, 0x03,
		0x01,                                 // session present
		byte(ConnAckReasonCodeNotAuthorized), // Reason code
		0x00,                                 // no properties
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding CONNACK fixed header returned error")

	assert.Equal(t, packettype.CONNACK, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x03), remainingLength)

	ca := ConnAck{}
	err = ca.decode(reader, remainingLength)
	assert.NoError(t, err, "ConnAck.decode returned an error")

	assert.True(t, ca.SessionPresent)
	assert.Equal(t, ConnAckReasonCodeNotAuthorized, ca.ReasonCode)

	var buf bytes.Buffer
	err = ca.encode(&buf)
	assert.NoError(t, err, "ConnAck.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("ConnAck.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
