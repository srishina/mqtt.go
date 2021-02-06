package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/require"
)

func TestInvalidConnectProtocolVersion(t *testing.T) {
	encoded := []byte{0x00, 0x04, 'T', 'T', 'Q', 'M', 0x05}

	c := Connect{}
	err := c.decode(bytes.NewBuffer(encoded), 4)
	require.EqualError(t, err, ErrInvalidProtocolName.Error(), "Connect.Decode did not return an error code for invalid protocol name")
}

func TestInvalidConnectFlags(t *testing.T) {
	packets := map[error][]byte{
		ErrInvalidConnectFlags: {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x01},
		ErrInvalidWillQos:      {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x1C}, // 3.1.2.6
		ErrInvalidWillRetain:   {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x20}, // 3.1.2.7
	}

	for e, encoded := range packets {
		c := Connect{}
		err := c.decode(bytes.NewBuffer(encoded), 4)
		require.EqualError(t, err, e.Error(), "Connect.Decode did not return an expected error code")
	}
}

func TestCodecConnectPacket(t *testing.T) {
	encoded := []byte{0x10, 0x1B,
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x05,       // protocol version
		0xC2,       // Username=1, password=1, retain=0, qos=0, will=0, clean start=1, reserved=0
		0x00, 0x18, // Keep alive - 24
		0x00,       // properties
		0x00, 0x00, // client id
		0x00, 0x05, 'h', 'e', 'l', 'l', 'o', // username
		0x00, 0x05, 'w', 'o', 'r', 'l', 'd', // username
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding CONNECT fixed header returned error")

	require.Equal(t, packettype.CONNECT, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x1B), remainingLength)

	c := Connect{}
	err = c.decode(reader, remainingLength)
	require.NoError(t, err, "Connect.decode did returned an error")

	require.Equal(t, "MQTT", c.protocolName)
	require.Equal(t, byte(0x05), c.protocolVersion)
	require.True(t, c.CleanStart)
	require.Equal(t, uint16(24), c.KeepAlive)
	require.Equal(t, c.ClientID, "")
	require.Equal(t, "hello", c.UserName)
	require.Equal(t, []byte{'w', 'o', 'r', 'l', 'd'}, c.Password)

	var buf bytes.Buffer
	err = c.encode(&buf)
	require.NoError(t, err, "Connect.encode did returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Connect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
func TestCodecConnectPacketWithProperties(t *testing.T) {
	encoded := []byte{0x10, 0x23,
		0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, // MQTT
		0x05, // protocol version
		0xC2,
		0x00, 0x18, // Keep alive - 24
		0x08,             // properties
		0x21, 0x00, 0x0A, // receive maximum
		0x27, 0x00, 0x00, 0x04, 0x00, // maximum packet size
		0x00, 0x00, // client id
		0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, // username - "hello"
		0x00, 0x05, 0x77, 0x6F, 0x72, 0x6C, 0x64, // password - "world"
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding CONNECT fixed header returned error")

	require.Equal(t, packettype.CONNECT, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x23), remainingLength)

	c := Connect{}
	err = c.decode(reader, remainingLength)
	require.NoError(t, err, "Connect.decode did returned an error")
	require.NotNil(t, c.Properties)
	require.NotNil(t, c.Properties.ReceiveMaximum)
	require.Equal(t, uint16(10), *c.Properties.ReceiveMaximum)
	require.NotNil(t, uint32(1024), *c.Properties.MaximumPacketSize)

	var buf bytes.Buffer
	err = c.encode(&buf)
	require.NoError(t, err, "Connect.encode did returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Connect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
