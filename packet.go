package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
)

var (
	ErrProtocol = errors.New("Protocol error")
)

// controlPacket MQTT control packet codec interface
type controlPacket interface {
	encode(w io.Writer) error
	decode(r io.Reader, remainingLen uint32) error
}

// PROTOCOLVERSIONv5 MQTT protocol version
var PROTOCOLVERSIONv5 = byte(0x05)

func readFrom(r io.Reader) (controlPacket, error) {
	byte0, remainingLength, err := readFixedHeader(r)
	if err != nil {
		return nil, err
	}

	p, err := newPacketWithHeader(byte0)
	if err != nil {
		return nil, err
	}

	body := make([]byte, remainingLength)
	if _, err = io.ReadFull(r, body); err != nil {
		return nil, err
	}
	err = p.decode(bytes.NewBuffer(body), remainingLength)
	return p, err
}

func writeTo(p controlPacket, w io.Writer) error {
	return p.encode(w)
}

// newPacketWithHeader aa
func newPacketWithHeader(byte0 byte) (controlPacket, error) {
	pktType := packettype.PacketType(byte0 >> 4)
	switch pktType {
	case packettype.CONNECT:
		return &Connect{}, nil
	case packettype.CONNACK:
		return &ConnAck{}, nil
	case packettype.PUBLISH:
		qos, dup, retain := decodePublishHeader(byte0)
		return &Publish{QoSLevel: qos, DUPFlag: dup, Retain: retain}, nil
	case packettype.PUBACK:
		return &PubAck{}, nil
	case packettype.PUBREC:
		return &PubRec{}, nil
	case packettype.PUBREL:
		return &PubRel{}, nil
	case packettype.PUBCOMP:
		return &PubComp{}, nil
	case packettype.SUBSCRIBE:
		return &Subscribe{}, nil
	case packettype.SUBACK:
		return &SubAck{}, nil
	case packettype.UNSUBSCRIBE:
		return &Unsubscribe{}, nil
	case packettype.UNSUBACK:
		return &UnsubAck{}, nil
	case packettype.PINGREQ:
		return &pingReq{}, nil
	case packettype.PINGRESP:
		return &pingResp{}, nil
	case packettype.DISCONNECT:
		return &Disconnect{}, nil
		// case packettype.AUTH:
		// 	return &Auth{}, nil
	}
	return nil, fmt.Errorf("unsupported packet type, 0x%x", pktType)
}

func readFixedHeader(r io.Reader) (byte, uint32, error) {
	byte0, err := mqttutil.DecodeByte(r)
	if err != nil {
		return 0, 0, err
	}

	remainingLength, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return 0, 0, err
	}

	return byte0, remainingLength, nil
}
