package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/srishina/mqtt.go/internal/reasoncode"
)

// UnsubAckReasonCode MQTT reason code that indicates the result of SUBSCRIBE operation
type UnsubAckReasonCode byte

const (
	UnsubAckReasonCodeSuccess            UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.Success)
	UnsubAckNoSubscriptionExisted        UnsubAckReasonCode = 0x11
	UnsubAckReasonCodeUnspecifiedError   UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.UnspecifiedError)
	UnsubAckReasonCodeImplSpecificError  UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.ImplSpecificError)
	UnsubAckReasonCodeNotAuthorized      UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.NotAuthorized)
	UnsubAckReasonCodeTopicFilterInvalid UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.TopicFilterInvalid)
	UnsubAckPacketIdentifierInUse        UnsubAckReasonCode = UnsubAckReasonCode(reasoncode.PacketIdentifierInUse)
)

var unsubAckReasonCodeText = map[UnsubAckReasonCode]string{
	UnsubAckReasonCodeSuccess:            reasoncode.Success.Text(),
	UnsubAckNoSubscriptionExisted:        "No subscription existed",
	UnsubAckReasonCodeUnspecifiedError:   reasoncode.UnspecifiedError.Text(),
	UnsubAckReasonCodeImplSpecificError:  reasoncode.ImplSpecificError.Text(),
	UnsubAckReasonCodeNotAuthorized:      reasoncode.NotAuthorized.Text(),
	UnsubAckReasonCodeTopicFilterInvalid: reasoncode.TopicFilterInvalid.Text(),
	UnsubAckPacketIdentifierInUse:        reasoncode.PacketIdentifierInUse.Text(),
}

var unsubAckReasonCodeDesc = map[UnsubAckReasonCode]string{
	UnsubAckReasonCodeSuccess:     "The subscription is deleted.",
	UnsubAckNoSubscriptionExisted: "No matching Topic Filter is being used by the Client.",
	UnsubAckReasonCodeUnspecifiedError: `The unsubscribe could not be completed and the Server either does not wish
											to reveal the reason or none of the other Reason Codes apply.`,
	UnsubAckReasonCodeImplSpecificError:  "The UNSUBSCRIBE is valid but the Server does not accept it.",
	UnsubAckReasonCodeNotAuthorized:      "The Client is not authorized to unsubscribe.",
	UnsubAckReasonCodeTopicFilterInvalid: "The Topic Filter is correctly formed but is not allowed for this Client.",
	UnsubAckPacketIdentifierInUse:        "The specified Packet Identifier is already in use.",
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code UnsubAckReasonCode) Text() string {
	return unsubAckReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code UnsubAckReasonCode) Desc() string {
	return unsubAckReasonCodeDesc[code]
}

// UnsubAckProperties MQTT UNSUBACK properties
type UnsubAckProperties struct {
	ReasonString string
	UserProperty map[string]string
}

func (usp *UnsubAckProperties) String() string {
	var fields []string
	if len(usp.ReasonString) > 0 {
		fields = append(fields, fmt.Sprintf("Reason string: %s", usp.ReasonString))
	}
	return fmt.Sprintf("{%s}", strings.Join(fields, ","))
}

func (usp *UnsubAckProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromUTF8String(usp.ReasonString)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(usp.UserProperty)
	return propertyLen
}

func (usp *UnsubAckProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromUTF8String(
		buf, properties.ReasonStringID, usp.ReasonString); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, usp.UserProperty); err != nil {
		return err
	}

	return nil
}

func (usp *UnsubAckProperties) decode(r io.Reader, propertyLen uint32) error {
	var id uint32
	var err error
	for err == nil && propertyLen > 0 {
		id, _, err = mqttutil.DecodeVarUint32(r)
		if err != nil {
			return err
		}
		propertyLen -= mqttutil.EncodedVarUint32Size(id)
		propID := properties.PropertyID(id)
		switch propID {
		case properties.ReasonStringID:
			usp.ReasonString, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, usp.ReasonString)
			propertyLen -= uint32(len(usp.ReasonString) + 2)
		case properties.UserPropertyID:
			if usp.UserProperty == nil {
				usp.UserProperty = make(map[string]string)
			}

			key, value, err2 := properties.Decoder.ToUTF8StringPair(r)
			if err2 != nil {
				return err2
			}

			usp.UserProperty[key] = value
			propertyLen -= uint32(len(key) + len(value) + 4)
		default:
			return fmt.Errorf("UNSUBACK: wrong property with identifier %d", id)
		}
	}

	return err
}

// UnsubAck MQTT UNSUBACK packet
type UnsubAck struct {
	packetID    uint16
	Properties  *UnsubAckProperties
	ReasonCodes []UnsubAckReasonCode
}

func (us *UnsubAck) String() string {
	return fmt.Sprintf(`Reason codes: [% x] Properties: %s`, us.ReasonCodes, us.Properties)
}

func (us *UnsubAck) propertyLength() uint32 {
	if us.Properties != nil {
		return us.Properties.length()
	}
	return 0
}

func (us *UnsubAck) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if us.Properties != nil {
		return us.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (us *UnsubAck) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		us.Properties = &UnsubAckProperties{}
		return us.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the UNSUBACK packet
func (us *UnsubAck) encode(w io.Writer) error {
	propertyLen := us.propertyLength()
	// calculate the remaining length
	// 2 = session present + reason code
	remainingLength := 2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen) + uint32(len(us.ReasonCodes))
	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, byte(packettype.UNSUBACK<<4)); err != nil {
		return err
	}

	if err := mqttutil.EncodeVarUint32(&packet, remainingLength); err != nil {
		return err
	}

	if err := mqttutil.EncodeBigEndianUint16(&packet, us.packetID); err != nil {
		return err
	}

	if err := us.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	for _, b := range us.ReasonCodes {
		if err := mqttutil.EncodeByte(&packet, byte(b)); err != nil {
			return err
		}
	}

	_, err := packet.WriteTo(w)

	return err
}

// decode decode the UNSUBACK packet
func (us *UnsubAck) decode(r io.Reader, remainingLen uint32) error {
	var err error

	us.packetID, err = mqttutil.DecodeBigEndianUint16(r)
	if err != nil {
		return err
	}

	err = us.decodeProperties(r)
	if err != nil {
		return err
	}

	propertyLen := us.propertyLength()
	remainingLen -= uint32(2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen))
	payload, _, err := mqttutil.DecodeBinaryDataNoLength(r, int(remainingLen))
	for _, p := range payload {
		us.ReasonCodes = append(us.ReasonCodes, UnsubAckReasonCode(p))
	}

	return err
}
