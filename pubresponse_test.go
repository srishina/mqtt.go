package mqtt

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
)

func getType(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
}

func TestCodecPublishResponse(t *testing.T) {
	encoded := [][]byte{
		{
			0x40, // PUBACK,
			0x04,
			0x00, 0x12, // Packet identifier 18
			byte(PubAckReasonCodeNoMatchingSubscribers),
			0x00, // no properties
		},
		{
			0x50, // PUBREC,
			0x04,
			0x00, 0x12, // Packet identifier 18
			byte(PubRecQuotaExceeded),
			0x00, // no properties
		},
		{
			0x62, // PUBREL,
			0x04,
			0x00, 0x12, // Packet identifier 18
			byte(PubRelPacketIdentifierNotFound),
			0x00, // no properties
		},
		{
			0x70, // PUBCOMP,
			0x04,
			0x00, 0x12, // Packet identifier 18
			byte(PubCompPacketIdentifierNotFound),
			0x00, // no properties
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
		assert.NoError(t, err, "Decoding fixed header returned error")

		assert.Equal(t, packetIDs[i], packettype.PacketType(byte0>>4))
		assert.Equal(t, uint32(0x04), remainingLength)
		pt := packetTypes[i]
		err = pt.decode(reader, remainingLength)
		assert.NoError(t, err, "decode %s returned an error", getType(pt))
		switch pt.(type) {
		case *PubAck:
			assert.Equal(t, PubAckReasonCodeNoMatchingSubscribers, pt.(*PubAck).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubAck).packetID)
		case *PubRec:
			assert.Equal(t, PubRecQuotaExceeded, pt.(*PubRec).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubRec).packetID)
		case *PubRel:
			assert.Equal(t, PubRelPacketIdentifierNotFound, pt.(*PubRel).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubRel).packetID)
		case *PubComp:
			assert.Equal(t, PubCompPacketIdentifierNotFound, pt.(*PubComp).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubComp).packetID)
		default:
			// unrecognized message
			assert.Fail(t, "unrecognized packet type")
		}
		var buf bytes.Buffer
		err = pt.encode(&buf)
		assert.NoError(t, err, "encode returned an error")

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
		assert.NoError(t, err, "Decoding fixed header returned error")

		assert.Equal(t, packetIDs[i], packettype.PacketType(byte0>>4))
		assert.Equal(t, uint32(0x02), remainingLength)
		pt := packetTypes[i]
		err = pt.decode(reader, remainingLength)
		assert.NoError(t, err, "decode %s returned an error", getType(pt))
		switch pt.(type) {
		case *PubAck:
			assert.Equal(t, PubAckReasonCodeSuccess, pt.(*PubAck).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubAck).packetID)
		case *PubRec:
			assert.Equal(t, PubRecReasonCodeSuccess, pt.(*PubRec).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubRec).packetID)
		case *PubRel:
			assert.Equal(t, PubRelReasonCodeSuccess, pt.(*PubRel).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubRel).packetID)
		case *PubComp:
			assert.Equal(t, PubCompReasonCodeSuccess, pt.(*PubComp).ReasonCode)
			assert.Equal(t, uint16(0x12), pt.(*PubComp).packetID)
		default:
			// unrecognized message
			assert.Fail(t, "unrecognized packet type")
		}
		var buf bytes.Buffer
		err = pt.encode(&buf)
		assert.NoError(t, err, "encode returned an error")

		if !bytes.Equal(el, buf.Bytes()) {
			t.Errorf("encode did not return expected bytes %v but %v ", el, buf.Bytes())
		}
	}
}
