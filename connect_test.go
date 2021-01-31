package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func TestInvalidConnectProtocolVersion(t *testing.T) {
	encoded := []byte{0x00, 0x04, 'T', 'T', 'Q', 'M', 0x05}

	c := Connect{}
	err := c.decode(bytes.NewBuffer(encoded), 4)
	assert.EqualError(t, err, ErrInvalidProtocolName.Error(), "Connect.Decode did not return an error code for invalid protocol name")
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
		assert.EqualError(t, err, e.Error(), "Connect.Decode did not return an expected error code")
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
	assert.NoError(t, err, "Decoding CONNECT fixed header returned error")

	assert.Equal(t, packettype.CONNECT, packettype.PacketType(byte0>>4))
	assert.Equal(t, uint32(0x1B), remainingLength)

	c := Connect{}
	err = c.decode(reader, remainingLength)
	assert.NoError(t, err, "Connect.decode did returned an error")

	assert.Equal(t, "MQTT", c.protocolName)
	assert.Equal(t, byte(0x05), c.protocolVersion)
	assert.True(t, c.CleanStart)
	assert.Equal(t, uint16(24), c.KeepAlive)
	assert.Equal(t, c.ClientID, "")
	assert.Equal(t, "hello", c.UserName)
	assert.Equal(t, []byte{'w', 'o', 'r', 'l', 'd'}, c.Password)

	var buf bytes.Buffer
	err = c.encode(&buf)
	assert.NoError(t, err, "Connect.encode did returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Connect.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
