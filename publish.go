package mqtt

import (
	"bytes"
	"fmt"
	"io"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/srishina/mqtt.go/internal/properties"
)

// PublishProperties MQTT PUBLISH properties
type PublishProperties struct {
	PayloadFormatIndicator  *bool
	MessageExpiryInterval   *uint32
	TopicAlias              *uint16
	ResponseTopic           string
	CorrelationData         []byte
	UserProperty            map[string]string
	SubscriptionIdentifiers []uint32
	ContentType             string
}

func (pp *PublishProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromBool(pp.PayloadFormatIndicator)
	propertyLen += properties.EncodedSize.FromUint32(pp.MessageExpiryInterval)
	propertyLen += properties.EncodedSize.FromUint16(pp.TopicAlias)
	propertyLen += properties.EncodedSize.FromUTF8String(pp.ResponseTopic)
	propertyLen += properties.EncodedSize.FromBinaryData(pp.CorrelationData)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(pp.UserProperty)
	propertyLen += properties.EncodedSize.FromVarUint32Array(pp.SubscriptionIdentifiers)
	propertyLen += properties.EncodedSize.FromUTF8String(pp.ContentType)
	return propertyLen
}

func (pp *PublishProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromBool(
		buf, properties.PayloadFormatIndicatorID, pp.PayloadFormatIndicator); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint32(
		buf, properties.MessageExpiryIntervalID, pp.MessageExpiryInterval); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.TopicAliasID, pp.TopicAlias); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ResponseTopicID, pp.ResponseTopic); err != nil {
		return err
	}

	if err := properties.Encoder.FromBinaryData(
		buf, properties.CorrelationDataID, pp.CorrelationData); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, pp.UserProperty); err != nil {
		return err
	}

	if err := properties.Encoder.FromVarUint32Array(
		buf, properties.SubscriptionIdentifierID, pp.SubscriptionIdentifiers); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ContentTypeID, pp.ContentType); err != nil {
		return err
	}

	return nil
}

func (pp *PublishProperties) decode(r io.Reader, propertyLen uint32) error {
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
		case properties.PayloadFormatIndicatorID:
			pp.PayloadFormatIndicator, err = properties.DecoderOnlyOnce.ToBool(r, propID, pp.PayloadFormatIndicator)
			propertyLen--
		case properties.MessageExpiryIntervalID:
			pp.MessageExpiryInterval, err = properties.DecoderOnlyOnce.ToUint32(r, propID, pp.MessageExpiryInterval)
			propertyLen -= 4
		case properties.TopicAliasID:
			pp.TopicAlias, err = properties.DecoderOnlyOnce.ToUint16(r, propID, pp.TopicAlias)
			propertyLen -= 2
		case properties.ResponseTopicID:
			pp.ResponseTopic, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, pp.ResponseTopic)
			propertyLen -= uint32(len(pp.ResponseTopic) + 2)
		case properties.CorrelationDataID:
			pp.CorrelationData, err = properties.DecoderOnlyOnce.ToBinaryData(r, propID, pp.CorrelationData)
			propertyLen -= uint32(len(pp.CorrelationData) + 2)
		case properties.UserPropertyID:
			if pp.UserProperty == nil {
				pp.UserProperty = make(map[string]string)
			}

			key, value, err2 := properties.Decoder.ToUTF8StringPair(r)
			if err2 != nil {
				return err2
			}

			pp.UserProperty[key] = value
			propertyLen -= uint32(len(key) + len(value) + 4)
		case properties.SubscriptionIdentifierID:
			v, readLen, e := mqttutil.DecodeVarUint32(r)
			err = e
			if err == nil {
				if v == 0 {
					err = fmt.Errorf("%s must not be 0", propID.Text())
				} else {
					pp.SubscriptionIdentifiers = append(pp.SubscriptionIdentifiers, v)
				}
			}

			propertyLen -= uint32(readLen)
		case properties.ContentTypeID:
			pp.ContentType, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, pp.ContentType)
			propertyLen -= uint32(len(pp.ContentType) + 2)
		default:
			return fmt.Errorf("PUBLISH: wrong property with identifier %d", id)
		}
	}

	return err
}

// Publish MQTT PUBLISH packet
type Publish struct {
	QoSLevel   byte
	DUPFlag    bool
	Retain     bool
	TopicName  string
	packetID   uint16
	Properties *PublishProperties
	Payload    []byte
}

func (p *Publish) propertyLength() uint32 {
	if p.Properties != nil {
		return p.Properties.length()
	}
	return 0
}

func (p *Publish) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	mqttutil.EncodeVarUint32(buf, propertyLen)
	if p.Properties != nil {
		return p.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (p *Publish) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		p.Properties = &PublishProperties{}
		return p.Properties.decode(r, propertyLen)
	}

	return nil
}

func (p *Publish) encode(w io.Writer) error {
	propertyLen := p.propertyLength()
	// calculate the remaining length
	remainingLength := propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)
	remainingLength += uint32(len(p.TopicName) + 2 + len(p.Payload))
	if p.QoSLevel > 0 {
		remainingLength += 2
	}

	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	byte0 := byte(packettype.PUBLISH<<4) | mqttutil.BoolToByte(p.DUPFlag)<<3 | p.QoSLevel<<1 | mqttutil.BoolToByte(p.Retain)
	mqttutil.EncodeByte(&packet, byte0)
	mqttutil.EncodeVarUint32(&packet, remainingLength)

	// Write topic name
	if err := mqttutil.EncodeUTF8String(&packet, p.TopicName); err != nil {
		return err
	}

	// A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.2.1-2].
	if p.QoSLevel > 0 {
		// write Packet ID
		if err := mqttutil.EncodeBigEndianUint16(&packet, p.packetID); err != nil {
			return err
		}
	}

	// publish properties
	if err := p.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	// Write payload
	if err := mqttutil.EncodeBinaryDataNoLen(&packet, p.Payload); err != nil {
		return err
	}

	_, err := packet.WriteTo(w)
	return err
}

func (p *Publish) decode(r io.Reader, remainingLen uint32) error {
	var err error
	p.TopicName, _, err = mqttutil.DecodeUTF8String(r)
	if err != nil {
		return err
	}
	remainingLen -= uint32(len(p.TopicName) + 2)

	// Packet ID is present only when QoS level is 1 or 2
	// A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.2.1-2].
	if p.QoSLevel > 0 {
		p.packetID, err = mqttutil.DecodeBigEndianUint16(r)
		if err != nil {
			return err
		}

		if p.packetID == 0 {
			return ErrProtocol
		}
		remainingLen -= 2
	}

	// publish properties
	err = p.decodeProperties(r)
	if err != nil {
		return err
	}

	propertyLen := p.propertyLength()
	remainingLen -= propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)

	// publish payload
	p.Payload, _, err = mqttutil.DecodeBinaryDataNoLength(r, int(remainingLen))
	if err != nil {
		return err
	}

	return nil
}

func decodePublishHeader(byte0 byte) (byte, bool, bool) {
	return ((byte0 >> 1) & 0x03), (byte0 & 0x08) > 0, (byte0 & 0x01) > 0
}
