package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "Decoding PUBLISH fixed header returned error")

	qos, dup, retain := decodePublishHeader(byte0)
	require.Equal(t, packettype.PUBLISH, packettype.PacketType(byte0>>4))
	require.Equal(t, byte(1), qos)
	require.True(t, dup)
	require.True(t, retain)
	require.Equal(t, uint32(0x0D), remainingLength)

	p := Publish{QoSLevel: qos, DUPFlag: dup, Retain: retain}
	err = p.decode(reader, remainingLength)
	require.NoError(t, err, "Publish.decode returned an error")

	require.Equal(t, byte(1), p.QoSLevel)
	require.True(t, p.DUPFlag)
	require.True(t, p.Retain)
	require.Equal(t, uint16(0x12), p.packetID)
	require.Equal(t, "a/b", p.TopicName)
	require.Equal(t, "hello", string(p.Payload))

	var buf bytes.Buffer
	err = p.encode(&buf)
	require.NoError(t, err, "Publish.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Publish.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecPublishPacketWithProperties(t *testing.T) {
	const topicAliasID = 0x10
	encoded := []byte{
		0x3D, // PUBPACKID, DUP, 2, RETAIN
		0x13,
		0x00, 0x03, 0x61, 0x2F, 0x62,
		0x00, 0x12, // Packet identifier 18
		0x03,
		byte(properties.TopicAliasID),
		0x00, topicAliasID,
		0x57, 0x65, 0x6C, 0x63, 0x6F, 0x6D, 0x65, 0x21,
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding PUBLISH fixed header returned error")
	qos, dup, retain := decodePublishHeader(byte0)

	p := Publish{QoSLevel: qos, DUPFlag: dup, Retain: retain}
	err = p.decode(reader, remainingLength)
	require.NoError(t, err, "Publish.decode returned an error")

	require.NotNil(t, p.Properties)
	require.NotNil(t, p.Properties.TopicAlias)
	require.Equal(t, uint16(topicAliasID), *p.Properties.TopicAlias)

	var buf bytes.Buffer
	err = p.encode(&buf)
	require.NoError(t, err, "Publish.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Publish.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecPublishPacketWithQoS0(t *testing.T) {
	// no packet identifier present when QoS is 0
	encoded := []byte{
		0x31, // PUBLISH, NO-DUP, 0, RETAIN
		0x0E,
		0x00, 0x03, 0x61, 0x2F, 0x62,
		0x00, // no properties
		0x57, 0x65, 0x6C, 0x63, 0x6F, 0x6D, 0x65, 0x21,
	}

	const topic = "a/b"
	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding PUBLISH fixed header returned error")
	qos, dup, retain := decodePublishHeader(byte0)

	p := Publish{QoSLevel: qos, DUPFlag: dup, Retain: retain}
	err = p.decode(reader, remainingLength)
	require.NoError(t, err, "Publish.decode returned an error")
	require.Equal(t, uint16(0), p.packetID)
	require.Equal(t, topic, p.TopicName)

	var buf bytes.Buffer
	err = p.encode(&buf)
	require.NoError(t, err, "Publish.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Publish.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
