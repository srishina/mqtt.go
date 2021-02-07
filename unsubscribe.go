package mqtt

import (
	"bytes"
	"fmt"
	"io"

	"github.com/srishina/mqtt.go/internal/mqttutil"
	"github.com/srishina/mqtt.go/internal/properties"
)

// UnsubscribeProperties MQTT UNSUBSCRIBE properties
type UnsubscribeProperties struct {
	UserProperty map[string]string
}

func (usp *UnsubscribeProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(usp.UserProperty)
	return propertyLen
}

func (usp *UnsubscribeProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, usp.UserProperty); err != nil {
		return err
	}
	return nil
}

func (usp *UnsubscribeProperties) decode(r io.Reader, propertyLen uint32) error {
	var id uint32
	var err error
	for err == nil && propertyLen > 0 {
		id, _, err = mqttutil.DecodeVarUint32(r)
		if err != nil {
			return err
		}
		propertyLen -= mqttutil.EncodedVarUint32Size(id)
		switch properties.PropertyID(id) {
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
			return fmt.Errorf("UNSUBSCRIBE: wrong property with identifier %d", id)
		}
	}

	return err
}

// Unsubscribe MQTT unsubscribe packet
type Unsubscribe struct {
	packetID     uint16
	Properties   *UnsubscribeProperties
	TopicFilters []string
}

func (us *Unsubscribe) propertyLength() uint32 {
	if us.Properties != nil {
		return us.Properties.length()
	}
	return 0
}

func (us *Unsubscribe) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if us.Properties != nil {
		return us.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (us *Unsubscribe) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		us.Properties = &UnsubscribeProperties{}
		return us.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the SUBSCRIBE packet
func (us *Unsubscribe) encode(w io.Writer) error {
	const fixedHeader = byte(0xA2)
	propertyLen := us.propertyLength()
	// calculate the remaining length
	// 2 = packet ID
	remainingLength := 2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)

	// add unsubscribe topic filters length
	for _, topicFilter := range us.TopicFilters {
		// unsubscibe topic filter length, topic filter
		remainingLength += uint32(len(topicFilter) + 2)
	}

	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, fixedHeader); err != nil {
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
	for _, t := range us.TopicFilters {
		if err := mqttutil.EncodeUTF8String(&packet, t); err != nil {
			return err
		}
	}

	_, err := packet.WriteTo(w)

	return err
}

// decode decode the UNSUBSCRIBE packet
func (us *Unsubscribe) decode(r io.Reader, remainingLen uint32) error {
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
	remainingLen -= (uint32(2) + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen))
	for remainingLen > 0 {
		topicFilter, _, err := mqttutil.DecodeUTF8String(r)
		if err != nil {
			return err
		}

		us.TopicFilters = append(us.TopicFilters, topicFilter)
		remainingLen -= uint32(len(topicFilter) + 2)
	}

	if len(us.TopicFilters) != 0 {
		return nil
	}
	return ErrNoTopicsPresent
}
