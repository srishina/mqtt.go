package mqtt

import (
	"bytes"
	"errors"
	"io"
	"math"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
)

var (
	ErrInvalidProtocolName = errors.New("invalid protocol name")
	ErrInvalidConnectFlags = errors.New("invalid connect flags - Malformed packet")
	ErrInvalidWillQos      = errors.New("invalid QoS - Malformed packet")
	ErrInvalidQosFlags     = errors.New("invalid QoS flag- Malformed packet")
	ErrInvalidWillRetain   = errors.New("invalid retain flag - Malformed packet")
)

// ConnectProperties MQTT connect properties packet
type ConnectProperties struct {
	SessionExpiryInterval *uint32
	ReceiveMaximum        *uint16
	MaximumPacketSize     *uint32
	TopicAliasMaximum     *uint16
	RequestProblemInfo    *bool
	RequestResponseInfo   *bool
	UserProperty          map[string]string
	AuthenticationMethod  string
	AuthenticationData    []byte
}

const sessionExpiryIntervalDefault uint32 = 0
const receiveMaximumDefault uint16 = math.MaxUint16
const maximumPacketSizeDefault uint32 = math.MaxUint32
const topicAliasMaximumDefault uint16 = 0
const requestProblemInfoDefault bool = true
const requestResponseInfoDefault bool = false

func (cp *ConnectProperties) length() uint32 {
	propertyLen := uint32(0)

	propertyLen += properties.EncodedSize.FromUint32(cp.SessionExpiryInterval)
	propertyLen += properties.EncodedSize.FromUint16(cp.ReceiveMaximum)
	propertyLen += properties.EncodedSize.FromUint32(cp.MaximumPacketSize)
	propertyLen += properties.EncodedSize.FromUint16(cp.TopicAliasMaximum)
	propertyLen += properties.EncodedSize.FromBool(cp.RequestProblemInfo)
	propertyLen += properties.EncodedSize.FromBool(cp.RequestResponseInfo)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(cp.UserProperty)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.AuthenticationMethod)
	propertyLen += properties.EncodedSize.FromBinaryData(cp.AuthenticationData)

	return propertyLen
}

func (cp *ConnectProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromUint32(
		buf, properties.SessionExpiryIntervalID, cp.SessionExpiryInterval); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.ReceiveMaximumID, cp.ReceiveMaximum); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint32(
		buf, properties.MaximumPacketSizeID, cp.MaximumPacketSize); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.TopicAliasMaximumID, cp.TopicAliasMaximum); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.RequestProblemInfoID, cp.RequestProblemInfo); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.RequestResponseInfoID, cp.RequestResponseInfo); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, cp.UserProperty); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.AuthenticationMethodID, cp.AuthenticationMethod); err != nil {
		return err
	}

	if err := properties.Encoder.FromBinaryData(
		buf, properties.AuthenticationDataID, cp.AuthenticationData); err != nil {
		return err
	}

	return nil
}

func (cp *ConnectProperties) decode(r io.Reader, propertyLen uint32) error {

	var id uint32
	var err error
	for err == nil && propertyLen > 0 {
		id, _, err = mqttutil.DecodeVarUint32(r)
		if err != nil {
			return err
		}
		propID := properties.PropertyID(id)
		propertyLen -= mqttutil.EncodedVarUint32Size(id)
		switch propID {
		case properties.SessionExpiryIntervalID:
			cp.SessionExpiryInterval, err = properties.DecoderOnlyOnce.ToUint32(r, propID, cp.SessionExpiryInterval)
			propertyLen -= 4
		case properties.ReceiveMaximumID:
			cp.ReceiveMaximum, err = properties.DecoderOnlyOnce.ToUint16(r, propID, cp.ReceiveMaximum)
			propertyLen -= 2
		case properties.MaximumPacketSizeID:
			cp.MaximumPacketSize, err = properties.DecoderOnlyOnce.ToUint32(r, propID, cp.MaximumPacketSize)
			propertyLen -= 4
		case properties.TopicAliasMaximumID:
			cp.TopicAliasMaximum, err = properties.DecoderOnlyOnce.ToUint16(r, propID, cp.TopicAliasMaximum)
			propertyLen -= 2
		case properties.RequestProblemInfoID:
			cp.RequestProblemInfo, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.RequestProblemInfo)
			propertyLen--
		case properties.RequestResponseInfoID:
			cp.RequestResponseInfo, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.RequestResponseInfo)
			propertyLen--
		case properties.UserPropertyID:
			if cp.UserProperty == nil {
				cp.UserProperty = make(map[string]string)
			}

			key, value, err2 := properties.Decoder.ToUTF8StringPair(r)
			if err2 != nil {
				return err2
			}

			cp.UserProperty[key] = value
			propertyLen -= uint32(len(key) + len(value) + 4)
		case properties.AuthenticationMethodID:
			cp.AuthenticationMethod, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.AuthenticationMethod)
			propertyLen -= uint32(len(cp.AuthenticationMethod) + 2)
		case properties.AuthenticationDataID:
			cp.AuthenticationData, err = properties.DecoderOnlyOnce.ToBinaryData(r, propID, cp.AuthenticationData)
			propertyLen -= uint32(len(cp.AuthenticationData) + 2)
		}
	}

	return err
}

// Connect MQTT connect packet
type Connect struct {
	protocolName    string
	protocolVersion byte
	CleanStart      bool
	KeepAlive       uint16
	WillFlag        bool
	Properties      *ConnectProperties
	ClientID        string
	UserName        string
	Password        []byte
}

func (c *Connect) propertyLength() uint32 {
	if c.Properties != nil {
		return c.Properties.length()
	}
	return 0
}

func (c *Connect) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if c.Properties != nil {
		return c.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (c *Connect) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		c.Properties = &ConnectProperties{}
		return c.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the Connect packet and perform protocol validation
func (c *Connect) encode(w io.Writer) error {
	propertyLen := c.propertyLength()
	// calculate the remaining length
	// 10 = protocolname + version + flags + keepalive
	remainingLength := 10 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen) + uint32(2+len(c.ClientID))

	// TODO support will Flag

	connectFlags := byte(0)
	if c.CleanStart {
		connectFlags |= 0x02
	}

	if len(c.UserName) > 0 {
		connectFlags |= 0x80
		remainingLength += uint32(2 + len(c.UserName))
	}

	if len(c.Password) > 0 {
		connectFlags |= 0x40
		remainingLength += uint32(2 + len(c.Password))
	}

	var packet bytes.Buffer
	packet.Grow(int(remainingLength + 1 + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, byte(packettype.CONNECT<<4)); err != nil {
		return err
	}

	if err := mqttutil.EncodeVarUint32(&packet, remainingLength); err != nil {
		return err
	}

	if _, err := packet.Write([]byte{0x0, 0x4, 'M', 'Q', 'T', 'T', 0x05}); err != nil {
		return err
	}

	packet.WriteByte(connectFlags)

	if err := mqttutil.EncodeBigEndianUint16(&packet, c.KeepAlive); err != nil {
		return err
	}

	if err := c.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	if err := mqttutil.EncodeUTF8String(&packet, c.ClientID); err != nil {
		return err
	}

	if len(c.UserName) > 0 {
		if err := mqttutil.EncodeUTF8String(&packet, c.UserName); err != nil {
			return err
		}
	}

	if len(c.Password) > 0 {
		if err := mqttutil.EncodeBinaryData(&packet, c.Password); err != nil {
			return err
		}
	}

	_, err := packet.WriteTo(w)

	return err
}

func (c *Connect) decode(r io.Reader, remainingLen uint32) error {
	var err error
	var pname [6]byte
	if _, err = r.Read(pname[:]); err != nil {
		return err
	}

	if !bytes.Equal(pname[:], []byte{0, 4, 'M', 'Q', 'T', 'T'}) {
		return ErrInvalidProtocolName
	}

	c.protocolName = "MQTT"
	c.protocolVersion, err = mqttutil.DecodeByte(r)
	if err != nil {
		return err
	}

	connectFlag, err := mqttutil.DecodeByte(r)
	if err != nil {
		return err
	}
	c.CleanStart = (connectFlag & 0x02) > 0
	passwordFlag := (connectFlag & 0x40) > 0
	usernameFlag := (connectFlag & 0x80) > 0

	if err := c.validateConnectFlag(connectFlag); err != nil {
		return err
	}

	c.KeepAlive, err = mqttutil.DecodeBigEndianUint16(r)
	if err != nil {
		return err
	}

	err = c.decodeProperties(r)
	if err != nil {
		return err
	}

	c.ClientID, _, err = mqttutil.DecodeUTF8String(r)
	if err != nil {
		return err
	}

	if usernameFlag {
		c.UserName, _, err = mqttutil.DecodeUTF8String(r)
	}

	if passwordFlag {
		c.Password, _, err = mqttutil.DecodeBinaryData(r)
	}

	return err
}

func (c *Connect) validateConnectFlag(connectFlag byte) error {
	reserved := connectFlag & 0x01
	if reserved != 0 {
		return ErrInvalidConnectFlags
	}

	willFlag := (connectFlag & 0x04) > 0
	willQoS := 0x03 & (connectFlag >> 0x03)
	willRetain := (connectFlag & 0x20) > 0
	// 3.1.2.6
	if (willFlag && (willQoS > 2)) || (!willFlag && willQoS != 0) {
		return ErrInvalidWillQos
	}

	// 3.1.2.7
	if !willFlag && willRetain {
		return ErrInvalidWillRetain
	}

	return nil
}
