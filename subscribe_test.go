package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/require"
)

func TestCodecSubscribePacket(t *testing.T) {
	encoded := []byte{
		0x82, 0x17,
		0x00, 0x12, // Packet identifier 18
		0x00,                            // no properties
		0x00, 0x03, 'a', '/', 'b', 0x01, // a/b with QoS 1
		0x00, 0x03, 'c', '/', 'd', 0x02,
		0x00, 0x05, 'e', '/', 'f', '/', 'g', 0x00,
	}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding SUBSCRIBE fixed header returned error")

	require.Equal(t, packettype.SUBSCRIBE, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x17), remainingLength)

	s := Subscribe{}
	err = s.decode(reader, remainingLength)
	require.NoError(t, err, "Subscribe.decode returned an error")

	require.Equal(t, uint16(0x12), s.packetID)
	require.Equal(t, 3, len(s.Subscriptions))
	require.Equal(t, "a/b", s.Subscriptions[0].TopicFilter)
	require.Equal(t, byte(1), s.Subscriptions[0].QoSLevel)
	require.False(t, s.Subscriptions[0].NoLocal)
	require.False(t, s.Subscriptions[0].RetainAsPublished)
	require.Equal(t, "c/d", s.Subscriptions[1].TopicFilter)
	require.Equal(t, byte(2), s.Subscriptions[1].QoSLevel)
	require.Equal(t, "e/f/g", s.Subscriptions[2].TopicFilter)
	require.Equal(t, byte(0), s.Subscriptions[2].QoSLevel)

	var buf bytes.Buffer
	err = s.encode(&buf)
	require.NoError(t, err, "Subscribe.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Subscribe.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecSubscribePacketWithProperties(t *testing.T) {
	encoded := []byte{
		0x82, 0x0B,
		0x00, 0x12, // Packet identifier 18
		0x02,
		0x0B,
		0x0A,
		0x00, 0x03, 0x61, 0x2F, 0x62, 0x01, // a/b with QoS 1
	}
	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding SUBSCRIBE fixed header returned error")

	require.Equal(t, packettype.SUBSCRIBE, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x0B), remainingLength)

	s := Subscribe{}
	err = s.decode(reader, remainingLength)
	require.NoError(t, err, "Subscribe.decode returned an error")
	require.NotNil(t, s.Properties)
	require.NotNil(t, s.Properties.SubscriptionIdentifier)
	require.Equal(t, uint32(10), *s.Properties.SubscriptionIdentifier)

	var buf bytes.Buffer
	err = s.encode(&buf)
	require.NoError(t, err, "Subscribe.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("Subscribe.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
