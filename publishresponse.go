package mqtt

import (
	"bytes"
	"fmt"
	"io"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
	"github.com/srishina/mqtt.go/internal/reasoncode"
)

// PublishResponseProperties MQTT PUBACK, PUBREC, PUBREL, PUBCOMP properties
type PublishResponseProperties struct {
	ReasonString string
	UserProperty map[string]string
}

func (sp *PublishResponseProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromUTF8String(sp.ReasonString)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(sp.UserProperty)
	return propertyLen
}

func (sp *PublishResponseProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
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

func (sp *PublishResponseProperties) decode(r io.Reader) error {
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
			return fmt.Errorf("wrong property with identifier %d", id)
		}
	}

	return err
}

// PubAckReasonCode MQTT reason code that indicates the result of PUBLISH operation
type PubAckReasonCode byte

const (
	PubAckReasonCodeSuccess               PubAckReasonCode = PubAckReasonCode(reasoncode.Success)
	PubAckReasonCodeNoMatchingSubscribers PubAckReasonCode = PubAckReasonCode(reasoncode.NoMatchingSubscribers)
	PubAckUnspecifiedError                PubAckReasonCode = PubAckReasonCode(reasoncode.UnspecifiedError)
	PubAckImplSpecificError               PubAckReasonCode = PubAckReasonCode(reasoncode.ImplSpecificError)
	PubAckImplNotAuthorized               PubAckReasonCode = PubAckReasonCode(reasoncode.NotAuthorized)
	PubAckTopicNameInvalid                PubAckReasonCode = PubAckReasonCode(reasoncode.TopicNameInvalid)
	PubAckPacketIdentifierInUse           PubAckReasonCode = PubAckReasonCode(reasoncode.PacketIdentifierInUse)
	PubAckQuotaExceeded                   PubAckReasonCode = PubAckReasonCode(reasoncode.QuotaExceeded)
	PubAckPayloadFormatInvalid            PubAckReasonCode = PubAckReasonCode(reasoncode.PayloadFormatInvalid)
)

var pubAckReasonCodeText = map[PubAckReasonCode]string{
	PubAckReasonCodeSuccess:               reasoncode.Success.Text(),
	PubAckReasonCodeNoMatchingSubscribers: reasoncode.NoMatchingSubscribers.Text(),
	PubAckUnspecifiedError:                reasoncode.UnspecifiedError.Text(),
	PubAckImplSpecificError:               reasoncode.ImplSpecificError.Text(),
	PubAckImplNotAuthorized:               reasoncode.NotAuthorized.Text(),
	PubAckTopicNameInvalid:                reasoncode.TopicNameInvalid.Text(),
	PubAckPacketIdentifierInUse:           reasoncode.PacketIdentifierInUse.Text(),
	PubAckQuotaExceeded:                   reasoncode.QuotaExceeded.Text(),
	PubAckPayloadFormatInvalid:            reasoncode.PayloadFormatInvalid.Text(),
}

var pubAckReasonCodeDesc = map[PubAckReasonCode]string{
	PubAckReasonCodeSuccess: "The message is accepted. Publication of the QoS 1 message proceeds.",
	PubAckReasonCodeNoMatchingSubscribers: `The message is accepted but there are no subscribers. This is sent only 
											by the Server. If the Server knows that there are no matching subscribers, 
											it MAY use this Reason Code instead of 0x00 (Success).`,
	PubAckUnspecifiedError: `The receiver does not accept the publish but either does not want to reveal the reason, 
									or it does not match one of the other values.`,
	PubAckImplSpecificError:     "The PUBLISH is valid but the receiver is not willing to accept it. ",
	PubAckImplNotAuthorized:     "The PUBLISH is not authorized.",
	PubAckTopicNameInvalid:      "The Topic Name is not malformed, but is not accepted by this Client or Server.",
	PubAckPacketIdentifierInUse: "The Packet Identifier is already in use. This might indicate a mismatch in the Session State between the Client and Server.",
	PubAckQuotaExceeded:         "An implementation or administrative imposed limit has been exceeded.",
	PubAckPayloadFormatInvalid:  "The payload format does not match the specified Payload Format Indicator.",
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubAckReasonCode) Text() string {
	return pubAckReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubAckReasonCode) Desc() string {
	return pubAckReasonCodeDesc[code]
}

func encodePublishResponse(byte0 byte, id uint16, code byte, props *PublishResponseProperties) (*bytes.Buffer, error) {
	propertyLen := props.length()
	// calculate the remaining length
	remainingLength := uint32(2) // packet id
	// The Reason Code and Property Length can be omitted
	// if the Reason Code is 0x00 (Success) and there are no Properties.
	// In this case the packet has a Remaining Length of 2.
	if code != 0 || propertyLen != 0 {
		remainingLength += (1 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen))
	}

	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	mqttutil.EncodeByte(&packet, byte0)
	mqttutil.EncodeVarUint32(&packet, remainingLength)

	if err := mqttutil.EncodeBigEndianUint16(&packet, id); err != nil {
		return nil, err
	}

	if remainingLength > 2 {
		if err := mqttutil.EncodeByte(&packet, code); err != nil {
			return nil, err
		}

		if err := props.encode(&packet, propertyLen); err != nil {
			return nil, err
		}
	}

	return &packet, nil
}

func decodePublishResponse(r io.Reader, remainingLen uint32) (uint16, byte, PublishResponseProperties, error) {
	var props PublishResponseProperties
	var code byte

	packetID, err := mqttutil.DecodeBigEndianUint16(r)
	if err != nil {
		return 0, 0, PublishResponseProperties{}, err
	}

	if remainingLen > 2 {
		code, err = mqttutil.DecodeByte(r)
		if err != nil {
			return 0, 0, PublishResponseProperties{}, err
		}

		if err := props.decode(r); err != nil {
			return 0, 0, PublishResponseProperties{}, err
		}
	}

	return packetID, code, props, nil
}

//PubAck MQTT PUBACK packet
type PubAck struct {
	packetID   uint16
	ReasonCode PubAckReasonCode
	Properties PublishResponseProperties
}

func (pa *PubAck) encode(w io.Writer) error {
	packet, err := encodePublishResponse(byte(packettype.PUBACK<<4), pa.packetID, byte(pa.ReasonCode), &pa.Properties)
	if err != nil {
		return err
	}

	_, err = packet.WriteTo(w)

	return err
}

func (pa *PubAck) decode(r io.Reader, remainingLen uint32) error {
	id, code, props, err := decodePublishResponse(r, remainingLen)
	if err != nil {
		return err
	}

	pa.packetID = id
	pa.ReasonCode = PubAckReasonCode(code)
	pa.Properties = props

	return nil
}

type PubRecReasonCode byte

const (
	PubRecReasonCodeSuccess               PubRecReasonCode = PubRecReasonCode(reasoncode.Success)
	PubRecReasonCodeNoMatchingSubscribers PubRecReasonCode = PubRecReasonCode(reasoncode.NoMatchingSubscribers)
	PubRecUnspecifiedError                PubRecReasonCode = PubRecReasonCode(reasoncode.UnspecifiedError)
	PubRecImplSpecificError               PubRecReasonCode = PubRecReasonCode(reasoncode.ImplSpecificError)
	PubRecImplNotAuthorized               PubRecReasonCode = PubRecReasonCode(reasoncode.NotAuthorized)
	PubRecTopicNameInvalid                PubRecReasonCode = PubRecReasonCode(reasoncode.TopicNameInvalid)
	PubRecPacketIdentifierInUse           PubRecReasonCode = PubRecReasonCode(reasoncode.PacketIdentifierInUse)
	PubRecQuotaExceeded                   PubRecReasonCode = PubRecReasonCode(reasoncode.QuotaExceeded)
	PubRecPayloadFormatInvalid            PubRecReasonCode = PubRecReasonCode(reasoncode.PayloadFormatInvalid)
)

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubRecReasonCode) Text() string {
	return pubAckReasonCodeText[PubAckReasonCode(code)]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubRecReasonCode) Desc() string {
	return pubAckReasonCodeDesc[PubAckReasonCode(code)]
}

// PubRec MQTT PUBACK packet
type PubRec struct {
	packetID   uint16
	ReasonCode PubRecReasonCode
	Properties PublishResponseProperties
}

func (pa *PubRec) encode(w io.Writer) error {
	packet, err := encodePublishResponse(byte(packettype.PUBREC<<4), pa.packetID, byte(pa.ReasonCode), &pa.Properties)
	if err != nil {
		return err
	}

	_, err = packet.WriteTo(w)

	return err
}

func (pa *PubRec) decode(r io.Reader, remainingLen uint32) error {
	id, code, props, err := decodePublishResponse(r, remainingLen)
	if err != nil {
		return err
	}

	pa.packetID = id
	pa.ReasonCode = PubRecReasonCode(code)
	pa.Properties = props

	return nil
}

// PubRelReasonCode MQTT reason code that indicates the result of PUBLISH operation
type PubRelReasonCode byte

const (
	PubRelReasonCodeSuccess        PubRelReasonCode = PubRelReasonCode(reasoncode.Success)
	PubRelPacketIdentifierNotFound PubRelReasonCode = PubRelReasonCode(reasoncode.PacketIdentifierNotFound)
)

var pubRelReasonCodeText = map[PubRelReasonCode]string{
	PubRelReasonCodeSuccess:        reasoncode.Success.Text(),
	PubRelPacketIdentifierNotFound: reasoncode.PacketIdentifierNotFound.Text(),
}

var pubRelReasonCodeDesc = map[PubRelReasonCode]string{
	PubRelReasonCodeSuccess: "Message released.",
	PubRelPacketIdentifierNotFound: `The Packet Identifier is not known. 
				This is not an error during recovery, but at other times indicates a mismatch between the Session State on the Client and Server.`,
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubRelReasonCode) Text() string {
	return pubRelReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubRelReasonCode) Desc() string {
	return pubRelReasonCodeDesc[code]
}

// PubRel MQTT PUBREL packet
type PubRel struct {
	packetID   uint16
	ReasonCode PubRelReasonCode
	Properties PublishResponseProperties
}

func (pr *PubRel) encode(w io.Writer) error {
	const fixedHeader = byte(0x62) // 01100010
	packet, err := encodePublishResponse(fixedHeader, pr.packetID, byte(pr.ReasonCode), &pr.Properties)
	if err != nil {
		return err
	}

	_, err = packet.WriteTo(w)

	return err
}

func (pr *PubRel) decode(r io.Reader, remainingLen uint32) error {
	id, code, props, err := decodePublishResponse(r, remainingLen)
	if err != nil {
		return err
	}

	pr.packetID = id
	pr.ReasonCode = PubRelReasonCode(code)
	pr.Properties = props

	return nil
}

// PubCompReasonCode MQTT reason code that indicates the result of PUBLISH operation
type PubCompReasonCode byte

const (
	PubCompReasonCodeSuccess        PubCompReasonCode = PubCompReasonCode(reasoncode.Success)
	PubCompPacketIdentifierNotFound PubCompReasonCode = PubCompReasonCode(reasoncode.PacketIdentifierNotFound)
)

var pubCompReasonCodeText = map[PubCompReasonCode]string{
	PubCompReasonCodeSuccess:        reasoncode.Success.Text(),
	PubCompPacketIdentifierNotFound: reasoncode.PacketIdentifierNotFound.Text(),
}

var pubCompReasonCodeDesc = map[PubCompReasonCode]string{
	PubCompReasonCodeSuccess: "Packet Identifier released. Publication of QoS 2 message is complete.",
	PubCompPacketIdentifierNotFound: `The Packet Identifier is not known. 
				This is not an error during recovery, but at other times indicates a mismatch between the Session State on the Client and Server.`,
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubCompReasonCode) Text() string {
	return pubCompReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code PubCompReasonCode) Desc() string {
	return pubCompReasonCodeDesc[code]
}

// PubComp MQTT PUBCOMP packet
type PubComp struct {
	packetID   uint16
	ReasonCode PubCompReasonCode
	Properties PublishResponseProperties
}

func (pc *PubComp) encode(w io.Writer) error {
	packet, err := encodePublishResponse(byte(packettype.PUBCOMP<<4), pc.packetID, byte(pc.ReasonCode), &pc.Properties)
	if err != nil {
		return err
	}

	_, err = packet.WriteTo(w)

	return err
}

func (pc *PubComp) decode(r io.Reader, remainingLen uint32) error {
	id, code, props, err := decodePublishResponse(r, remainingLen)
	if err != nil {
		return err
	}

	pc.packetID = id
	pc.ReasonCode = PubCompReasonCode(code)
	pc.Properties = props

	return nil
}
