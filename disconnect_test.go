package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/require"
)

func TestCodecDisconnectPacket(t *testing.T) {
	encoded := []byte{0xE0, 0x02,
		byte(DisconnectReasonCodeServerShuttingDown), // Reason code
		0x00, // no properties
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding DISCONNECT fixed header returned error")

	require.Equal(t, packettype.DISCONNECT, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x02), remainingLength)

	d := Disconnect{}
	err = d.decode(reader, remainingLength)
	require.NoError(t, err, "Disconnect.decode returned an error")

	require.Equal(t, DisconnectReasonCodeServerShuttingDown, d.ReasonCode)

	var buf bytes.Buffer
	err = d.encode(&buf)
	require.NoError(t, err, "Disconnect.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Disconnect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecDisconnectPacketWithProperties(t *testing.T) {
	encoded := []byte{0xE0, 0x07,
		byte(DisconnectReasonCodeServerShuttingDown), // Reason code
		0x05,
		0x11,                   // SessionIntervalPropertyID
		0x00, 0x00, 0x00, 0x05, // session interval = 5
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding DISCONNECT fixed header returned error")

	require.Equal(t, packettype.DISCONNECT, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x07), remainingLength)

	d := Disconnect{}
	err = d.decode(reader, remainingLength)
	require.NoError(t, err, "Disconnect.decode returned an error")

	require.NotNil(t, d.Properties)
	require.NotNil(t, d.Properties.SessionExpiryInterval)
	require.Equal(t, uint32(5), *d.Properties.SessionExpiryInterval)

	var buf bytes.Buffer
	err = d.encode(&buf)
	require.NoError(t, err, "Disconnect.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Disconnect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
