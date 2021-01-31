package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestCodecDisconnectPacket(t *testing.T) {
	encoded := []byte{0xE0, 0x02,
		byte(DisconnectReasonCodeServerShuttingDown), // Reason code
		0x00, // no properties
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	assert.NoError(t, err, "Decoding DISCONNECT fixed header returned error")

	assert.Equal(t, packettype.DISCONNECT, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x02), remainingLength)

	d := Disconnect{}
	err = d.decode(reader, remainingLength)
	assert.NoError(t, err, "Disconnect.decode returned an error")

	assert.Equal(t, DisconnectReasonCodeServerShuttingDown, d.ReasonCode)

	var buf bytes.Buffer
	err = d.encode(&buf)
	assert.NoError(t, err, "Disconnect.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Disconnect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
