package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/srishina/mqtt.go/internal/reasoncode"
)

// SubAckReasonCode MQTT reason code that indicates the result of SUBSCRIBE operation
type SubAckReasonCode byte

const (
	SubAckReasonCodeGrantedQoS0             SubAckReasonCode = SubAckReasonCode(reasoncode.Success)
	SubAckReasonCodeGrantedQoS1             SubAckReasonCode = 0x01
	SubAckReasonCodeGrantedQoS2             SubAckReasonCode = 0x02
	SubAckReasonCodeUnspecifiedError        SubAckReasonCode = SubAckReasonCode(reasoncode.UnspecifiedError)
	SubAckReasonCodeImplSpecificError       SubAckReasonCode = SubAckReasonCode(reasoncode.ImplSpecificError)
	SubAckReasonCodeNotAuthorized           SubAckReasonCode = SubAckReasonCode(reasoncode.NotAuthorized)
	SubAckReasonCodeTopicFilterInvalid      SubAckReasonCode = SubAckReasonCode(reasoncode.TopicFilterInvalid)
	SubAckPacketIdentifierInUse             SubAckReasonCode = SubAckReasonCode(reasoncode.PacketIdentifierInUse)
	SubAckQuotaExceeded                     SubAckReasonCode = SubAckReasonCode(reasoncode.QuotaExceeded)
	SubAckSharedSubscriptionsNotSupported   SubAckReasonCode = SubAckReasonCode(reasoncode.SharedSubscriptionsNotSupported)
	SubAckSubscriptionIdsNotSupported       SubAckReasonCode = SubAckReasonCode(reasoncode.SubscriptionIdsNotSupported)
	SubAckWildcardSubscriptionsNotSupported SubAckReasonCode = SubAckReasonCode(reasoncode.WildcardSubscriptionsNotSupported)
)

var subAckReasonCodeText = map[SubAckReasonCode]string{
	SubAckReasonCodeGrantedQoS0:             "Granted QoS 0",
	SubAckReasonCodeGrantedQoS1:             "Granted QoS 1",
	SubAckReasonCodeGrantedQoS2:             "Granted QoS 2",
	SubAckReasonCodeUnspecifiedError:        reasoncode.UnspecifiedError.Text(),
	SubAckReasonCodeImplSpecificError:       reasoncode.ImplSpecificError.Text(),
	SubAckReasonCodeNotAuthorized:           reasoncode.NotAuthorized.Text(),
	SubAckReasonCodeTopicFilterInvalid:      reasoncode.TopicFilterInvalid.Text(),
	SubAckPacketIdentifierInUse:             reasoncode.PacketIdentifierInUse.Text(),
	SubAckQuotaExceeded:                     reasoncode.QuotaExceeded.Text(),
	SubAckSharedSubscriptionsNotSupported:   reasoncode.SharedSubscriptionsNotSupported.Text(),
	SubAckSubscriptionIdsNotSupported:       reasoncode.SubscriptionIdsNotSupported.Text(),
	SubAckWildcardSubscriptionsNotSupported: reasoncode.WildcardSubscriptionsNotSupported.Text(),
}

var subAckReasonCodeDesc = map[SubAckReasonCode]string{
	SubAckReasonCodeGrantedQoS0:             "The subscription is accepted and the maximum QoS sent will be QoS 0. This might be a lower QoS than was requested.",
	SubAckReasonCodeGrantedQoS1:             "The subscription is accepted and the maximum QoS sent will be QoS 1. This might be a lower QoS than was requested.",
	SubAckReasonCodeGrantedQoS2:             "The subscription is accepted and any received QoS will be sent to this subscription.",
	SubAckReasonCodeUnspecifiedError:        "The subscription is not accepted and the Server either does not wish to reveal the reason or none of the other Reason Codes apply.",
	SubAckReasonCodeImplSpecificError:       "The SUBSCRIBE is valid but the Server does not accept it. ",
	SubAckReasonCodeNotAuthorized:           "The Client is not authorized to make this subscription.",
	SubAckReasonCodeTopicFilterInvalid:      "The Topic Filter is correctly formed but is not allowed for this Client.",
	SubAckPacketIdentifierInUse:             "The specified Packet Identifier is already in use.",
	SubAckQuotaExceeded:                     "An implementation or administrative imposed limit has been exceeded.",
	SubAckSharedSubscriptionsNotSupported:   "The Server does not support Shared Subscriptions for this Client.",
	SubAckSubscriptionIdsNotSupported:       "The Server does not support Subscription Identifiers; the subscription is not accepted.",
	SubAckWildcardSubscriptionsNotSupported: "The Server does not support Wildcard Subscriptions; the subscription is not accepted.",
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code SubAckReasonCode) Text() string {
	return subAckReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code SubAckReasonCode) Desc() string {
	return subAckReasonCodeDesc[code]
}

// SubAckProperties MQTT SUBACK properties
type SubAckProperties struct {
	ReasonString string
	UserProperty map[string]string
}

func (sp *SubAckProperties) propertyLen() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromUTF8String(sp.ReasonString)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(sp.UserProperty)
	return propertyLen
}

func (sp *SubAckProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	mqttutil.EncodeVarUint32(buf, propertyLen)

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ReasonStringID, sp.ReasonString); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, sp.UserProperty); err != nil {
		return err
	}

	return nil
}

func (sp *SubAckProperties) decode(r io.Reader) error {
	var id uint32
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	for err == nil && propertyLen > 0 {
		id, _, err = mqttutil.DecodeVarUint32(r)
		if err != nil {
			return err
		}
		propertyLen -= mqttutil.EncodedVarUint32Size(id)
		propID := properties.PropertyID(id)
		switch propID {
		case properties.ReasonStringID:
			sp.ReasonString, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, sp.ReasonString)
			propertyLen -= uint32(len(sp.ReasonString) + 2)
		case properties.UserPropertyID:
			if sp.UserProperty == nil {
				sp.UserProperty = make(map[string]string)
			}

			key, value, err2 := properties.Decoder.ToUTF8StringPair(r)
			if err2 != nil {
				return err2
			}

			sp.UserProperty[key] = value
			propertyLen -= uint32(len(key) + len(value) + 4)
		default:
			return fmt.Errorf("SUBACK: wrong property with identifier %d", id)
		}
	}

	return err
}

// SubAck MQTT SUBACK packet
type SubAck struct {
	packetID   uint16
	Properties SubAckProperties
	Payload    []SubAckReasonCode
}

// encode encode the SUBACK packet
func (s *SubAck) encode(w io.Writer) error {
	propertyLen := s.Properties.propertyLen()
	// calculate the remaining length
	// 2 = session present + reason code
	remainingLength := 2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen) + uint32(len(s.Payload))
	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	mqttutil.EncodeByte(&packet, byte(packettype.SUBACK<<4))
	mqttutil.EncodeVarUint32(&packet, remainingLength)

	if err := mqttutil.EncodeBigEndianUint16(&packet, s.packetID); err != nil {
		return err
	}

	if err := s.Properties.encode(&packet, propertyLen); err != nil {
		return err
	}

	if err := mqttutil.EncodeBinaryDataNoLen(&packet, *(*[]byte)(unsafe.Pointer(&s.Payload))); err != nil {
		return err
	}
	_, err := packet.WriteTo(w)

	return err
}

// decode decode the SUBACK packet
func (s *SubAck) decode(r io.Reader, remainingLen uint32) error {
	var err error

	s.packetID, err = mqttutil.DecodeBigEndianUint16(r)
	if err != nil {
		return err
	}

	err = s.Properties.decode(r)
	if err != nil {
		return err
	}

	propertyLen := s.Properties.propertyLen()
	remainingLen -= uint32(2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen))
	payload, _, err := mqttutil.DecodeBinaryDataNoLength(r, int(remainingLen))
	for _, p := range payload {
		s.Payload = append(s.Payload, SubAckReasonCode(p))
	}

	return err
}
