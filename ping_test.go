package mqtt

import (
	"bytes"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/require"
)

func TestCodecPingReqPacket(t *testing.T) {
	encoded := []byte{0xC0, 0x00}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding PINGREQ fixed header returned error")

	require.Equal(t, packettype.PINGREQ, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x00), remainingLength)

	p := pingReq{}
	err = p.decode(reader, 0)
	require.NoError(t, err, "PingReq.decode returned an error")

	var buf bytes.Buffer
	err = p.encode(&buf)
	require.NoError(t, err, "PingReq.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("PingReq.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}

func TestCodecPingRespPacket(t *testing.T) {
	encoded := []byte{0xD0, 0x00}

	reader := bytes.NewBuffer(encoded)
	byte0, remainingLength, err := readFixedHeader(reader)
	require.NoError(t, err, "Decoding PINGRESP fixed header returned error")

	require.Equal(t, packettype.PINGRESP, packettype.PacketType(byte0>>4))
	require.Equal(t, uint32(0x00), remainingLength)

	p := pingResp{}
	err = p.decode(reader, 0)
	require.NoError(t, err, "PingResp.decode returned an error")

	var buf bytes.Buffer
	err = p.encode(&buf)
	require.NoError(t, err, "PingResp.encode returned an error")

	if !bytes.Equal(encoded, buf.Bytes()) {
		t.Errorf("PingReq.encode did not return expected bytes %v but %v ", encoded, buf.Bytes())
	}
}
