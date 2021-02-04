package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecSubAckPacket(t *testing.T) {
	encoded := []byte{
		0x90, 0x05,
		0x00, 0x12, // Packet identifier 18
		0x00, // no properties
		byte(SubAckReasonCodeGrantedQoS1),
		byte(SubAckReasonCodeGrantedQoS2),
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding SUBACK fixed header returned error")

	assert.Equal(t, packettype.SUBACK, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x05), remainingLength)

	sa := SubAck{}
	err = sa.decode(reader, remainingLength)
	assert.NoError(t, err, "SubAck.Decode returned an error")

	assert.Equal(t, uint16(0x12), sa.packetID)
	assert.Equal(t, 2, len(sa.ReasonCodes))
	assert.Equal(t, SubAckReasonCodeGrantedQoS1, sa.ReasonCodes[0])
	assert.Equal(t, SubAckReasonCodeGrantedQoS2, sa.ReasonCodes[1])

	var buf bytes.Buffer
	err = sa.encode(&buf)
	assert.NoError(t, err, "SubAck.Encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("SubAck.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
