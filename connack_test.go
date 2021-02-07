package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/stretchr/testify/require"
)

func TestCodecConnAckPacket(t *testing.T) {
	encoded := []byte{0x20, 0x03,
		0x01,                                 // session present
		byte(ConnAckReasonCodeNotAuthorized), // Reason code
		0x00,                                 // no properties
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding CONNACK fixed header returned error")

	require.Equal(t, packettype.CONNACK, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x03), remainingLength)

	ca := ConnAck{}
	err = ca.decode(reader, remainingLength)
	require.NoError(t, err, "ConnAck.decode returned an error")

	require.True(t, ca.SessionPresent)
	require.Equal(t, ConnAckReasonCodeNotAuthorized, ca.ReasonCode)

	var buf bytes.Buffer
	err = ca.encode(&buf)
	require.NoError(t, err, "ConnAck.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("ConnAck.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecConnAckInvalidPacket(t *testing.T) {
	encoded := []byte{0x20, 0x03,
		0x01,                                 // session present
		byte(ConnAckReasonCodeNotAuthorized), // reason code not authorized
		0x0A,
		byte(properties.SessionExpiryIntervalID),
		0x00, 0x00, 0x00, 0x0A,
		byte(properties.SessionExpiryIntervalID),
		0x00, 0x00, 0x00, 0x0A,
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding CONNACK fixed header returned error")

	require.Equal(t, packettype.CONNACK, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x03), remainingLength)

	ca := ConnAck{}
	err = ca.decode(reader, remainingLength)
	require.Error(t, err, "ConnAck.decode did not return an error")
}

func TestCodecConnAckPacketWithProperties(t *testing.T) {
	encoded := []byte{0x20, 0x0B,
		0x01,                                 // session present
		byte(ConnAckReasonCodeNotAuthorized), // Reason code
		0x08,                                 // properties
		byte(properties.ReceiveMaximumID),
		0x00, 0x0A, // receive maximum
		byte(properties.MaximumQoSID),
		0x01, // max qos = 1
		byte(properties.TopicAliasMaximumID),
		0x00, 0x0A, // receive maximum
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding CONNACK fixed header returned error")

	require.Equal(t, packettype.CONNACK, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x0B), remainingLength)

	ca := ConnAck{}
	err = ca.decode(reader, remainingLength)
	require.NoError(t, err, "ConnAck.decode returned an error")

	require.NotNil(t, ca.Properties)
	require.NotNil(t, ca.Properties.TopicAliasMaximum)
	require.Equal(t, uint16(10), *ca.Properties.TopicAliasMaximum)
	require.NotNil(t, ca.Properties.MaximumQoS)
	require.Equal(t, byte(1), *ca.Properties.MaximumQoS)
	require.NotNil(t, ca.Properties.ReceiveMaximum)
	require.Equal(t, uint16(10), *ca.Properties.ReceiveMaximum)

	var buf bytes.Buffer
	err = ca.encode(&buf)
	require.NoError(t, err, "ConnAck.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("ConnAck.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
