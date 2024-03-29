package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/properties"
)

var (
	ErrNoTopicsPresent = errors.New("Subscription payload MUST contain atleast a topic - protocol error")
)

// SubscribeProperties MQTT SUBSCRIBE properties
type SubscribeProperties struct {
	SubscriptionIdentifier *uint32
	UserProperty           map[string]string
}

func (sp *SubscribeProperties) String() string {
	var fields []string
	if sp.SubscriptionIdentifier != nil {
		fields = append(fields, fmt.Sprintf("Subscription identifier: %d", *sp.SubscriptionIdentifier))
	}
	return fmt.Sprintf("{%s}", strings.Join(fields, ","))
}

func (sp *SubscribeProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromVarUin32((sp.SubscriptionIdentifier))
	propertyLen += properties.EncodedSize.FromUTF8StringPair(sp.UserProperty)
	return propertyLen
}

func (sp *SubscribeProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromVarUint32(
		buf, properties.SubscriptionIdentifierID, sp.SubscriptionIdentifier); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, sp.UserProperty); err != nil {
		return err
	}
	return nil
}

func (sp *SubscribeProperties) decode(r io.Reader, propertyLen uint32) error {
	var id uint32
	var err error
	for err == nil && propertyLen > 0 {
		id, _, err = mqttutil.DecodeVarUint32(r)
		if err != nil {
			return err
		}
		propertyLen -= mqttutil.EncodedVarUint32Size(id)
		propID := properties.PropertyID(id)
		switch properties.PropertyID(id) {
		case properties.SubscriptionIdentifierID:
			sp.SubscriptionIdentifier, err = properties.DecoderOnlyOnce.ToVarUint32(r, propID, sp.SubscriptionIdentifier)
			if err == nil && *sp.SubscriptionIdentifier == 0 {
				err = fmt.Errorf("%s must not be 0", propID.Text())
			}
			propertyLen -= mqttutil.EncodedVarUint32Size(*sp.SubscriptionIdentifier)
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
			return fmt.Errorf("SUBSCRIBE: wrong property with identifier %d", id)
		}
	}

	return err
}

// Subscription contains topic filter and subscription options for the MQTT subscribe
type Subscription struct {
	TopicFilter       string
	QoSLevel          byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

func (s *Subscription) String() string {
	return fmt.Sprintf(`Topic filter: %s QoS level: %d No Local? %t
		Retain as published? %t Retain handling: %d`, s.TopicFilter, s.QoSLevel, s.NoLocal, s.RetainAsPublished, s.RetainHandling)
}

// Subscribe MQTT SUBSCRIBE packet
type Subscribe struct {
	packetID      uint16
	Subscriptions []*Subscription
	Properties    *SubscribeProperties
}

func (s *Subscribe) String() string {
	return fmt.Sprintf(`Subscriptions: [% v] Properties: %s`, s.Subscriptions, s.Properties)
}

func (s *Subscribe) propertyLength() uint32 {
	if s.Properties != nil {
		return s.Properties.length()
	}
	return 0
}

func (s *Subscribe) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if s.Properties != nil {
		return s.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (s *Subscribe) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		s.Properties = &SubscribeProperties{}
		return s.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the SUBSCRIBE packet
func (s *Subscribe) encode(w io.Writer) error {
	const fixedHeader = byte(0x82) // 10000010
	if len(s.Subscriptions) == 0 {
		return ErrNoTopicsPresent
	}

	propertyLen := s.propertyLength()
	// calculate the remaining length
	// 2 = packet ID
	remainingLength := 2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)

	// add subscriptions length
	for _, subscription := range s.Subscriptions {
		// subscibe topic filter length, topic filter and it's optionss
		remainingLength += uint32(len(subscription.TopicFilter) + 2 + 1)
	}

	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, fixedHeader); err != nil {
		return err
	}

	if err := mqttutil.EncodeVarUint32(&packet, remainingLength); err != nil {
		return err
	}

	if err := mqttutil.EncodeBigEndianUint16(&packet, s.packetID); err != nil {
		return err
	}

	if err := s.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	for _, subscription := range s.Subscriptions {
		if err := mqttutil.EncodeUTF8String(&packet, subscription.TopicFilter); err != nil {
			return err
		}
		var b byte
		// write subscribe options
		b |= subscription.QoSLevel & 0x03
		if subscription.NoLocal {
			b |= 0x04
		}
		if subscription.RetainAsPublished {
			b |= 0x08
		}

		b |= (subscription.RetainHandling & 0x30)

		if err := mqttutil.EncodeByte(&packet, b); err != nil {
			return err
		}
	}

	_, err := packet.WriteTo(w)
	return err
}

// decode decode the SUBSCRIBE packet
func (s *Subscribe) decode(r io.Reader, remainingLen uint32) error {
	var err error

	s.packetID, err = mqttutil.DecodeBigEndianUint16(r)
	if err != nil {
		return err
	}

	err = s.decodeProperties(r)
	if err != nil {
		return err
	}

	propertyLen := s.propertyLength()
	remainingLen -= (uint32(2) + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen))

	for remainingLen > 0 {
		topicFilter, _, err := mqttutil.DecodeUTF8String(r)
		if err != nil {
			return err
		}

		b, err := mqttutil.DecodeByte(r)
		if err != nil {
			return err
		}

		qosLevel := (b & 0x03)
		nl := (b & 0x04) == 1
		rap := (b & 0x08) == 1
		rh := b & 0x30

		s.Subscriptions = append(s.Subscriptions,
			&Subscription{TopicFilter: topicFilter, QoSLevel: qosLevel, NoLocal: nl, RetainAsPublished: rap, RetainHandling: rh})
		remainingLen -= uint32(len(topicFilter) + 2 + 1)
	}

	if len(s.Subscriptions) == 0 {
		return ErrNoTopicsPresent
	}

	return nil
}
