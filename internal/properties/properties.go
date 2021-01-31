package properties

import (
	"bytes"
	"fmt"
	"io"

	"github.com/srishina/mqtt.go/internal/mqttutil"
)

// PropertyID defines the MQTT property identifier
type PropertyID uint32

// MQTT 5.0 2.2.2.2 Property
const (
	// PayloadFormatIndicatorID Payload format indicator
	PayloadFormatIndicatorID PropertyID = 0x01

	// MessageExpiryIntervalID Message expiry interval
	MessageExpiryIntervalID PropertyID = 0x02

	// ContentTypeID Content type
	ContentTypeID PropertyID = 0x03

	// ResponseTopic response topic
	ResponseTopicID PropertyID = 0x08

	// CorrelationDataID Correlation data
	CorrelationDataID PropertyID = 0x09

	// SubscriptionIdentifierID Subscription Identifier
	SubscriptionIdentifierID PropertyID = 0x0B

	// SessionExpiryIntervalID session expiry property  identifier
	SessionExpiryIntervalID PropertyID = 0x11

	// AssignedClientIdentifierID Assigned Client Identifier
	AssignedClientIdentifierID PropertyID = 0x12

	// ServerKeepAliveID Server Keep Alive
	ServerKeepAliveID PropertyID = 0x13

	// AuthenticationMethodID maximum packet size id
	AuthenticationMethodID PropertyID = 0x15

	// AuthenticationDataID maximum packet size id
	AuthenticationDataID PropertyID = 0x16

	// RequestProblemInfoID maximum packet size id
	RequestProblemInfoID PropertyID = 0x17

	// WillDelayIntervalID Will Delay Interval
	WillDelayIntervalID PropertyID = 0x18

	// RequestResponseInfoID maximum packet size id
	RequestResponseInfoID PropertyID = 0x19

	// ResponseInformationID Response Information
	ResponseInformationID PropertyID = 0x1A

	// ServerReferenceID Server Reference
	ServerReferenceID PropertyID = 0x1C

	// ReasonStringID Reason String
	ReasonStringID PropertyID = 0x1F

	// ReceiveMaximumID receive maximum id
	ReceiveMaximumID PropertyID = 0x21

	// TopicAliasMaximumID maximum packet size id
	TopicAliasMaximumID PropertyID = 0x22

	// TopicAliasID Topic Alias
	TopicAliasID PropertyID = 0x23

	// MaximumQoSID Maximum QoS
	MaximumQoSID PropertyID = 0x24

	// RetainAvailableID Retain Available
	RetainAvailableID PropertyID = 0x25

	// UserPropertyID User property id
	UserPropertyID PropertyID = 0x26

	// MaximumPacketSizeID maximum packet size id
	MaximumPacketSizeID PropertyID = 0x27

	// WildcardSubscriptionAvailableID Wildcard Subscription Available
	WildcardSubscriptionAvailableID PropertyID = 0x28

	// SubscriptionIdentifierAvailableID Subscription Identifier Available
	SubscriptionIdentifierAvailableID PropertyID = 0x29

	// SharedSubscriptionAvailableID Shared Subscription Available
	SharedSubscriptionAvailableID PropertyID = 0x2A
)

var propertyIDText = map[PropertyID]string{
	PayloadFormatIndicatorID:          "Payload format indicator",
	MessageExpiryIntervalID:           "Message expiry interval",
	ContentTypeID:                     "Content type",
	ResponseTopicID:                   "response topic",
	CorrelationDataID:                 "Correlation data",
	SubscriptionIdentifierID:          "Subscription Identifier",
	SessionExpiryIntervalID:           "Session Expiry Interval",
	AssignedClientIdentifierID:        "Assigned Client Identifier",
	ServerKeepAliveID:                 "Server Keep Alive",
	AuthenticationMethodID:            "Authentication Method",
	AuthenticationDataID:              "Authentication Data",
	RequestProblemInfoID:              "Request Problem Information",
	RequestResponseInfoID:             "Request Response Information",
	WillDelayIntervalID:               "Will Delay Interval",
	ResponseInformationID:             "Response Information",
	ServerReferenceID:                 "Server Reference",
	ReasonStringID:                    "Reason String",
	ReceiveMaximumID:                  "Receive Maximum",
	TopicAliasMaximumID:               "Topic Alias Maximum",
	TopicAliasID:                      "Topic Alias",
	MaximumQoSID:                      "Maximum QoS",
	RetainAvailableID:                 "Retain Available",
	UserPropertyID:                    "User Property",
	MaximumPacketSizeID:               "Maximum Packet Size",
	WildcardSubscriptionAvailableID:   "Wildcard Subscription Available",
	SubscriptionIdentifierAvailableID: "Subscription Identifier Available",
	SharedSubscriptionAvailableID:     "Shared Subscription Available",
}

// Text returns a text for the MQTT property ID. Returns the empty
// string if the property ID is unknown.
func (code PropertyID) Text() string {
	return propertyIDText[code]
}

// EncodedSize utils for calculating encoded property size for various data types
var EncodedSize encodedSize

type encodedSize struct{}

func (encodedSize) FromByte(value *byte) uint32 {
	if value == nil {
		return 0
	}
	return 2
}

func (encodedSize) FromBool(value *bool) uint32 {
	if value == nil {
		return 0
	}
	return 2
}

func (encodedSize) FromUint16(value *uint16) uint32 {
	if value == nil {
		return 0
	}
	return 3
}

func (encodedSize) FromUint32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return 5
}

func (encodedSize) FromUTF8String(value string) uint32 {
	if len(value) == 0 {
		return 0
	}
	return uint32(len(value) + 3)
}

func (encodedSize) FromBinaryData(value []byte) uint32 {
	if len(value) == 0 {
		return 0
	}
	return uint32(len(value) + 3)
}

func (encodedSize) FromVarUin32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return uint32(mqttutil.EncodedVarUint32Size(*value)) + 1
}

func (encodedSize) FromVarUint32Array(values []uint32) uint32 {
	if len(values) == 0 {
		return 0
	}

	propertyLen := uint32(0)

	for _, v := range values {
		propertyLen += (mqttutil.EncodedVarUint32Size((v))) + 1
	}

	return propertyLen
}

func (encodedSize) FromUTF8StringPair(values map[string]string) uint32 {
	if values == nil || len(values) == 0 {
		return 0
	}

	propertyLen := uint32(0)

	for k, v := range values {
		propertyLen++
		propertyLen += uint32(4 + len(k) + len(v))
	}

	return propertyLen
}

// Encoder utils for encoding property for various data types
var Encoder encoder

type encoder struct{}

func (encoder) FromByte(buf *bytes.Buffer, id PropertyID, val *byte) error {
	if val == nil {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeByte(buf, *val)
}

func (encoder) FromBool(buf *bytes.Buffer, id PropertyID, val *bool) error {
	if val == nil {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeBool(buf, *val)
}

func (encoder) FromUint16(buf *bytes.Buffer, id PropertyID, val *uint16) error {
	if val == nil {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeBigEndianUint16(buf, *val)
}

func (encoder) FromUint32(buf *bytes.Buffer, id PropertyID, val *uint32) error {
	if val == nil {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeBigEndianUint32(buf, *val)
}

func (encoder) FromUTF8String(buf *bytes.Buffer, id PropertyID, val string) error {
	if len(val) == 0 {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeUTF8String(buf, val)
}

func (encoder) FromBinaryData(buf *bytes.Buffer, id PropertyID, val []byte) error {
	if len(val) == 0 {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeBinaryData(buf, val)
}

func (encoder) FromVarUint32(buf *bytes.Buffer, id PropertyID, val *uint32) error {
	if val == nil {
		return nil
	}
	if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
		return err
	}
	return mqttutil.EncodeVarUint32(buf, *val)
}

func (encoder) FromVarUint32Array(buf *bytes.Buffer, id PropertyID, values []uint32) error {
	if len(values) == 0 {
		return nil
	}
	for _, v := range values {
		if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
			return err
		}
		if err := mqttutil.EncodeVarUint32(buf, v); err != nil {
			return err
		}
	}

	return nil
}

func (encoder) FromUTF8StringPair(buf *bytes.Buffer, id PropertyID, values map[string]string) error {
	if values == nil || len(values) == 0 {
		return nil
	}
	for k, v := range values {
		if err := mqttutil.EncodeVarUint32(buf, uint32(id)); err != nil {
			return err
		}
		if err := mqttutil.EncodeUTF8String(buf, k); err != nil {
			return err
		}
		if err := mqttutil.EncodeUTF8String(buf, v); err != nil {
			return err
		}
	}

	return nil
}

func propertyAlreadyExists(id PropertyID) error {
	return fmt.Errorf("%s must not be included more than once", id.Text())
}

// DecoderOnlyOnce utils for decoding property for various data types
// the decoder allows to decode the values only once, i.e, when the argument
// v is empty or nil
var DecoderOnlyOnce decoderOnlyOnce

type decoderOnlyOnce struct{}

func (decoderOnlyOnce) ToByte(r io.Reader, id PropertyID, v *byte) (*byte, error) {
	if v != nil {
		return nil, propertyAlreadyExists(id)
	}

	val, err := mqttutil.DecodeByte(r)
	return &val, err
}

func (decoderOnlyOnce) ToBool(r io.Reader, id PropertyID, v *bool) (*bool, error) {
	if v != nil {
		return nil, propertyAlreadyExists(id)
	}

	value, err := mqttutil.DecodeBool(r)
	return &value, err
}

func (decoderOnlyOnce) ToUint16(r io.Reader, id PropertyID, v *uint16) (*uint16, error) {
	if v != nil {
		return nil, propertyAlreadyExists(id)
	}

	value, err := mqttutil.DecodeBigEndianUint16(r)
	return &value, err
}

func (decoderOnlyOnce) ToUint32(r io.Reader, id PropertyID, v *uint32) (*uint32, error) {
	if v != nil {
		return nil, propertyAlreadyExists(id)
	}

	value, err := mqttutil.DecodeBigEndianUint32(r)
	return &value, err
}

func (decoderOnlyOnce) ToUTF8String(r io.Reader, id PropertyID, v string) (string, error) {
	if len(v) != 0 {
		return "", propertyAlreadyExists(id)
	}

	value, _, err := mqttutil.DecodeUTF8String(r)
	return value, err
}

func (decoderOnlyOnce) ToBinaryData(r io.Reader, id PropertyID, v []byte) ([]byte, error) {
	if len(v) != 0 {
		return nil, propertyAlreadyExists(id)
	}

	value, _, err := mqttutil.DecodeBinaryData(r)
	return value, err
}

func (decoderOnlyOnce) ToVarUint32(r io.Reader, id PropertyID, v *uint32) (*uint32, error) {
	if v != nil {
		return nil, propertyAlreadyExists(id)
	}

	value, _, err := mqttutil.DecodeVarUint32(r)
	return &value, err
}

// Decoder utils for decoding property for various data types
var Decoder decoder

type decoder struct{}

func (decoder) ToUTF8StringPair(r io.Reader) (string, string, error) {
	key, _, err := mqttutil.DecodeUTF8String(r)
	if err != nil {
		return "", "", err
	}

	value, _, err := mqttutil.DecodeUTF8String(r)
	if err != nil {
		return "", "", err
	}

	return key, value, err
}
