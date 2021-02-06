package mqtt

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/stretchr/testify/require"
)

func getType(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
}

func TestCodecPublishResponseNoProps(t *testing.T) {
	encoded := [][]byte{
		{
			0x40, // PUBACK,
			0x03,
			0x00, 0x12, // Packet identifier 18
			byte(PubAckReasonCodeNoMatchingSubscribers),
		},
		{
			0x50, // PUBREC,
			0x03,
			0x00, 0x12, // Packet identifier 18
			byte(PubRecQuotaExceeded),
		},
		{
			0x62, // PUBREL,
			0x03,
			0x00, 0x12, // Packet identifier 18
			byte(PubRelPacketIdentifierNotFound),
		},
		{
			0x70, // PUBCOMP,
			0x03,
			0x00, 0x12, // Packet identifier 18
			byte(PubCompPacketIdentifierNotFound),
		},
	}

	packetTypes := []packet{
		&PubAck{},
		&PubRec{},
		&PubRel{},
		&PubComp{},
	}

	packetIDs := []packettype.PacketType{
		packettype.PUBACK,
		packettype.PUBREC,
		packettype.PUBREL,
		packettype.PUBCOMP,
	}

	for i, el := range encoded {
		reader := bytes.NewBuffer(el)
		byte0, remainingLength, err := readFixedHeader(reader)
		require.NoError(t, err, "Decoding fixed header returned error")

		require.Equal(t, packetIDs[i], packettype.PacketType(byte0>>4))
		require.Equal(t, uint32(0x03), remainingLength)
		pt := packetTypes[i]
		err = pt.decode(reader, remainingLength)
		require.NoError(t, err, "decode %s returned an error", getType(pt))
		switch pt.(type) {
		case *PubAck:
			require.Equal(t, PubAckReasonCodeNoMatchingSubscribers, pt.(*PubAck).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubAck).packetID)
		case *PubRec:
			require.Equal(t, PubRecQuotaExceeded, pt.(*PubRec).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRec).packetID)
		case *PubRel:
			require.Equal(t, PubRelPacketIdentifierNotFound, pt.(*PubRel).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRel).packetID)
		case *PubComp:
			require.Equal(t, PubCompPacketIdentifierNotFound, pt.(*PubComp).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubComp).packetID)
		default:
			// unrecognized message
			require.Fail(t, "unrecognized packet type")
		}
		var buf bytes.Buffer
		err = pt.encode(&buf)
		require.NoError(t, err, "encode returned an error")

		if !bytes.Equal(el, buf.Bytes()) {
			t.Errorf("encode did not return expected bytes %v but %v ", el, buf.Bytes())
		}
	}
}

func TestCodecPublishResponseWithSuccessCodeAndProps(t *testing.T) {
	encoded := [][]byte{
		{
			0x40, // PUBACK,
			0x09,
			0x00, 0x12, // Packet identifier 18
			byte(PubAckReasonCodeSuccess),
			0x05,
			byte(properties.ReasonStringID),
			0x00, 0x02, 'a', 'b',
		},
		{
			0x50, // PUBREC,
			0x09,
			0x00, 0x12, // Packet identifier 18
			byte(PubRecReasonCodeSuccess),
			0x05,
			byte(properties.ReasonStringID),
			0x00, 0x02, 'a', 'b',
		},
		{
			0x62, // PUBREL,
			0x09,
			0x00, 0x12, // Packet identifier 18
			byte(PubRelReasonCodeSuccess),
			0x05,
			byte(properties.ReasonStringID),
			0x00, 0x02, 'a', 'b',
		},
		{
			0x70, // PUBCOMP,
			0x09,
			0x00, 0x12, // Packet identifier 18
			byte(PubCompReasonCodeSuccess),
			0x05,
			byte(properties.ReasonStringID),
			0x00, 0x02, 'a', 'b',
		},
	}

	packetTypes := []packet{
		&PubAck{},
		&PubRec{},
		&PubRel{},
		&PubComp{},
	}

	packetIDs := []packettype.PacketType{
		packettype.PUBACK,
		packettype.PUBREC,
		packettype.PUBREL,
		packettype.PUBCOMP,
	}

	for i, el := range encoded {
		reader := bytes.NewBuffer(el)
		byte0, remainingLength, err := readFixedHeader(reader)
		require.NoError(t, err, "Decoding fixed header returned error")

		require.Equal(t, packetIDs[i], packettype.PacketType(byte0>>4))
		require.Equal(t, uint32(0x09), remainingLength)
		pt := packetTypes[i]
		err = pt.decode(reader, remainingLength)
		require.NoError(t, err, "decode %s returned an error", getType(pt))
		switch pt.(type) {
		case *PubAck:
			require.Equal(t, PubAckReasonCodeSuccess, pt.(*PubAck).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubAck).packetID)
		case *PubRec:
			require.Equal(t, PubRecReasonCodeSuccess, pt.(*PubRec).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRec).packetID)
		case *PubRel:
			require.Equal(t, PubRelReasonCodeSuccess, pt.(*PubRel).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRel).packetID)
		case *PubComp:
			require.Equal(t, PubCompReasonCodeSuccess, pt.(*PubComp).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubComp).packetID)
		default:
			// unrecognized message
			require.Fail(t, "unrecognized packet type")
		}
		var buf bytes.Buffer
		err = pt.encode(&buf)
		require.NoError(t, err, "encode returned an error")

		if !bytes.Equal(el, buf.Bytes()) {
			t.Errorf("encode did not return expected bytes %v but %v ", el, buf.Bytes())
		}
	}
}

func TestCodecPublishResponseWithSuccessCodeAndNoProps(t *testing.T) {
	encoded := [][]byte{
		{
			0x40, // PUBACK,
			0x02,
			0x00, 0x12, // Packet identifier 18
		},
		{
			0x50, // PUBREC,
			0x02,
			0x00, 0x12, // Packet identifier 18
		},
		{
			0x62, // PUBREL,
			0x02,
			0x00, 0x12, // Packet identifier 18
		},
		{
			0x70, // PUBCOMP,
			0x02,
			0x00, 0x12, // Packet identifier 18
		},
	}

	packetTypes := []packet{
		&PubAck{},
		&PubRec{},
		&PubRel{},
		&PubComp{},
	}

	packetIDs := []packettype.PacketType{
		packettype.PUBACK,
		packettype.PUBREC,
		packettype.PUBREL,
		packettype.PUBCOMP,
	}

	for i, el := range encoded {
		reader := bytes.NewBuffer(el)
		byte0, remainingLength, err := readFixedHeader(reader)
		require.NoError(t, err, "Decoding fixed header returned error")

		require.Equal(t, packetIDs[i], packettype.PacketType(byte0>>4))
		require.Equal(t, uint32(0x02), remainingLength)
		pt := packetTypes[i]
		err = pt.decode(reader, remainingLength)
		require.NoError(t, err, "decode %s returned an error", getType(pt))
		switch pt.(type) {
		case *PubAck:
			require.Equal(t, PubAckReasonCodeSuccess, pt.(*PubAck).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubAck).packetID)
		case *PubRec:
			require.Equal(t, PubRecReasonCodeSuccess, pt.(*PubRec).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRec).packetID)
		case *PubRel:
			require.Equal(t, PubRelReasonCodeSuccess, pt.(*PubRel).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubRel).packetID)
		case *PubComp:
			require.Equal(t, PubCompReasonCodeSuccess, pt.(*PubComp).ReasonCode)
			require.Equal(t, uint16(0x12), pt.(*PubComp).packetID)
		default:
			// unrecognized message
			require.Fail(t, "unrecognized packet type")
		}
		var buf bytes.Buffer
		err = pt.encode(&buf)
		require.NoError(t, err, "encode returned an error")

		if !bytes.Equal(el, buf.Bytes()) {
			t.Errorf("encode did not return expected bytes %v but %v ", el, buf.Bytes())
		}
	}
}
