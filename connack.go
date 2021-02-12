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

// ConnAckReasonCode MQTT reason code that indicates the result of an CONNECT operation
type ConnAckReasonCode byte

const (
	ConnAckReasonCodeSuccess                ConnAckReasonCode = ConnAckReasonCode(reasoncode.Success)
	ConnAckReasonCodeUnspecifiedError       ConnAckReasonCode = ConnAckReasonCode(reasoncode.UnspecifiedError)
	ConnAckReasonCodeMalformedPacket        ConnAckReasonCode = ConnAckReasonCode(reasoncode.MalformedPacket)
	ConnAckReasonCodeProtocolError          ConnAckReasonCode = ConnAckReasonCode(reasoncode.ProtocolError)
	ConnAckReasonCodeImplSpecificError      ConnAckReasonCode = ConnAckReasonCode(reasoncode.ImplSpecificError)
	ConnAckReasonCodeUnsupportedProtocolVer ConnAckReasonCode = 0x84
	ConnAckReasonCodeClientIDNotValud       ConnAckReasonCode = 0x85
	ConnAckReasonCodeBadUsernameOrPWD       ConnAckReasonCode = 0x86
	ConnAckReasonCodeNotAuthorized          ConnAckReasonCode = ConnAckReasonCode(reasoncode.NotAuthorized)
	ConnAckReasonCodeServerUnavailable      ConnAckReasonCode = 0x88
	ConnAckReasonCodeServerBusy             ConnAckReasonCode = ConnAckReasonCode(reasoncode.ServerBusy)
	ConnAckReasonCodeBanned                 ConnAckReasonCode = 0x8A
	ConnAckReasonCodeBadAuthMethod          ConnAckReasonCode = 0x8C
	ConnAckReasonCodeTopicNameInvalid       ConnAckReasonCode = ConnAckReasonCode(reasoncode.TopicNameInvalid)
	ConnAckReasonCodePacketTooLarge         ConnAckReasonCode = ConnAckReasonCode(reasoncode.PacketTooLarge)
	ConnAckReasonCodeQuotaExceeded          ConnAckReasonCode = ConnAckReasonCode(reasoncode.QuotaExceeded)
	ConnAckReasonCodePayloadFormatInvalid   ConnAckReasonCode = ConnAckReasonCode(reasoncode.PayloadFormatInvalid)
	ConnAckReasonCodeRetainNotSupported     ConnAckReasonCode = ConnAckReasonCode(reasoncode.RetainNotSupported)
	ConnAckReasonCodeQoSNotSupported        ConnAckReasonCode = ConnAckReasonCode(reasoncode.QoSNotSupported)
	ConnAckReasonCodeUseAnotherServer       ConnAckReasonCode = ConnAckReasonCode(reasoncode.UseAnotherServer)
	ConnAckReasonCodeServerMoved            ConnAckReasonCode = ConnAckReasonCode(reasoncode.ServerMoved)
	ConnAckReasonCodeConnectionRateExceeded ConnAckReasonCode = ConnAckReasonCode(reasoncode.ConnectionRateExceeded)
)

var connAckReasonCodeText = map[ConnAckReasonCode]string{
	ConnAckReasonCodeSuccess:                reasoncode.Success.Text(),
	ConnAckReasonCodeUnspecifiedError:       reasoncode.UnspecifiedError.Text(),
	ConnAckReasonCodeMalformedPacket:        reasoncode.MalformedPacket.Text(),
	ConnAckReasonCodeProtocolError:          reasoncode.ProtocolError.Text(),
	ConnAckReasonCodeImplSpecificError:      reasoncode.ImplSpecificError.Text(),
	ConnAckReasonCodeUnsupportedProtocolVer: "Unsupported Protocol Version",
	ConnAckReasonCodeClientIDNotValud:       "Client Identifier not valid",
	ConnAckReasonCodeBadUsernameOrPWD:       "Bad User Name or Password",
	ConnAckReasonCodeNotAuthorized:          reasoncode.NotAuthorized.Text(),
	ConnAckReasonCodeServerUnavailable:      "Server unavailable",
	ConnAckReasonCodeServerBusy:             "Server busy",
	ConnAckReasonCodeBanned:                 "Banned",
	ConnAckReasonCodeBadAuthMethod:          "Bad authentication method",
	ConnAckReasonCodeTopicNameInvalid:       reasoncode.TopicNameInvalid.Text(),
	ConnAckReasonCodePacketTooLarge:         reasoncode.PacketTooLarge.Text(),
	ConnAckReasonCodeQuotaExceeded:          reasoncode.QuotaExceeded.Text(),
	ConnAckReasonCodePayloadFormatInvalid:   reasoncode.PayloadFormatInvalid.Text(),
	ConnAckReasonCodeRetainNotSupported:     reasoncode.RetainNotSupported.Text(),
	ConnAckReasonCodeQoSNotSupported:        reasoncode.QoSNotSupported.Text(),
	ConnAckReasonCodeUseAnotherServer:       reasoncode.UseAnotherServer.Text(),
	ConnAckReasonCodeServerMoved:            reasoncode.ServerMoved.Text(),
	ConnAckReasonCodeConnectionRateExceeded: reasoncode.ConnectionRateExceeded.Text(),
}

var connAckReasonCodeDesc = map[ConnAckReasonCode]string{
	ConnAckReasonCodeSuccess:                "The Connection is accepted.",
	ConnAckReasonCodeUnspecifiedError:       "The Server does not wish to reveal the reason for the failure, or none of the other Reason Codes apply.",
	ConnAckReasonCodeMalformedPacket:        "Data within the CONNECT packet could not be correctly parsed. ",
	ConnAckReasonCodeProtocolError:          "Data in the CONNECT packet does not conform to this specification.",
	ConnAckReasonCodeImplSpecificError:      "The CONNECT is valid but is not accepted by this Server.",
	ConnAckReasonCodeUnsupportedProtocolVer: "The Server does not support the version of the MQTT protocol requested by the Client.",
	ConnAckReasonCodeClientIDNotValud:       "The Client Identifier is a valid string but is not allowed by the Server.",
	ConnAckReasonCodeBadUsernameOrPWD:       "The Server does not accept the User Name or Password specified by the Client ",
	ConnAckReasonCodeNotAuthorized:          "The Client is not authorized to connect.",
	ConnAckReasonCodeServerUnavailable:      "The MQTT Server is not available.",
	ConnAckReasonCodeServerBusy:             "The Server is busy. Try again later.",
	ConnAckReasonCodeBanned:                 "This Client has been banned by administrative action. Contact the server administrator.",
	ConnAckReasonCodeBadAuthMethod:          "The authentication method is not supported or does not match the authentication method currently in use.",
	ConnAckReasonCodeTopicNameInvalid:       "The Will Topic Name is not malformed, but is not accepted by this Server.",
	ConnAckReasonCodePacketTooLarge:         "The CONNECT packet exceeded the maximum permissible size.",
	ConnAckReasonCodeQuotaExceeded:          "An implementation or administrative imposed limit has been exceeded.",
	ConnAckReasonCodePayloadFormatInvalid:   "The Will Payload does not match the specified Payload Format Indicator.",
	ConnAckReasonCodeRetainNotSupported:     "The Server does not support retained messages, and Will Retain was set to 1.",
	ConnAckReasonCodeQoSNotSupported:        "The Server does not support the QoS set in Will QoS.",
	ConnAckReasonCodeUseAnotherServer:       "The Client should temporarily use another server.",
	ConnAckReasonCodeServerMoved:            "The Client should permanently use another server.",
	ConnAckReasonCodeConnectionRateExceeded: "The connection rate limit has been exceeded.",
}

// Text returns a text for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code ConnAckReasonCode) Text() string {
	return connAckReasonCodeText[code]
}

// Desc returns a description for the MQTT reason code. Returns the empty
// string if the reason code is unknown.
func (code ConnAckReasonCode) Desc() string {
	return connAckReasonCodeDesc[code]
}

// ConnAckProperties MQTT CONNACK properties
type ConnAckProperties struct {
	SessionExpiryInterval           *uint32
	ReceiveMaximum                  *uint16
	MaximumQoS                      *byte
	RetainAvailable                 *bool
	MaximumPacketSize               *uint32
	AssignedClientIdentifier        string
	TopicAliasMaximum               *uint16
	ReasonString                    string
	UserProperty                    map[string]string
	WildcardSubscriptionAvailable   *bool
	SubscriptionIdentifierAvailable *bool
	SharedSubscriptionAvailable     *bool
	ServerKeepAlive                 *uint16
	ResponseInformation             string
	ServerReference                 string
	AuthenticationMethod            string
	AuthenticationData              []byte
}

func (cp *ConnAckProperties) String() string {
	var fields []string
	if cp.SessionExpiryInterval != nil {
		fields = append(fields, fmt.Sprintf("Session expiry interval: %d", *cp.SessionExpiryInterval))
	}
	if cp.ReceiveMaximum != nil {
		fields = append(fields, fmt.Sprintf("Receive maximum: %d", *cp.ReceiveMaximum))
	}
	if cp.MaximumQoS != nil {
		fields = append(fields, fmt.Sprintf("Maximum QoS: %d", *cp.MaximumQoS))
	}
	if cp.RetainAvailable != nil {
		fields = append(fields, fmt.Sprintf("Retain available: %t", *cp.RetainAvailable))
	}
	if cp.MaximumPacketSize != nil {
		fields = append(fields, fmt.Sprintf("Maximum packet size: %d", *cp.MaximumPacketSize))
	}
	if len(cp.AssignedClientIdentifier) > 0 {
		fields = append(fields, fmt.Sprintf("Assigned client id: %s", cp.AssignedClientIdentifier))
	}
	if cp.TopicAliasMaximum != nil {
		fields = append(fields, fmt.Sprintf("Topic alias max: %d", *cp.TopicAliasMaximum))
	}
	if len(cp.ReasonString) > 0 {
		fields = append(fields, fmt.Sprintf("Reason string: %s", cp.ReasonString))
	}
	if cp.WildcardSubscriptionAvailable != nil {
		fields = append(fields, fmt.Sprintf("Wildcard subscription available: %t", *cp.WildcardSubscriptionAvailable))
	}
	if cp.SubscriptionIdentifierAvailable != nil {
		fields = append(fields, fmt.Sprintf("Subscription ID available: %t", *cp.SubscriptionIdentifierAvailable))
	}
	if cp.SharedSubscriptionAvailable != nil {
		fields = append(fields, fmt.Sprintf("Shared subscription available: %t", *cp.SharedSubscriptionAvailable))
	}
	if cp.ServerKeepAlive != nil {
		fields = append(fields, fmt.Sprintf("Server keep alive: %d", *cp.ServerKeepAlive))
	}
	if len(cp.ResponseInformation) > 0 {
		fields = append(fields, fmt.Sprintf("Response info: %s", cp.ResponseInformation))
	}
	if len(cp.ServerReference) > 0 {
		fields = append(fields, fmt.Sprintf("Server reference: %s", cp.ServerReference))
	}
	if len(cp.AuthenticationMethod) > 0 {
		fields = append(fields, fmt.Sprintf("Authentication Method: %s", cp.AuthenticationMethod))
	}
	if len(cp.AuthenticationData) > 0 {
		fields = append(fields, fmt.Sprintf("Authentication Data: ****"))
	}
	return fmt.Sprintf("{%s}", strings.Join(fields, ","))
}

func (cp *ConnAckProperties) length() uint32 {
	propertyLen := uint32(0)

	propertyLen += properties.EncodedSize.FromUint32(cp.SessionExpiryInterval)
	propertyLen += properties.EncodedSize.FromUint16(cp.ReceiveMaximum)
	propertyLen += properties.EncodedSize.FromByte(cp.MaximumQoS)
	propertyLen += properties.EncodedSize.FromBool(cp.RetainAvailable)
	propertyLen += properties.EncodedSize.FromUint32(cp.MaximumPacketSize)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.AssignedClientIdentifier)
	propertyLen += properties.EncodedSize.FromUint16(cp.TopicAliasMaximum)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.ReasonString)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(cp.UserProperty)
	propertyLen += properties.EncodedSize.FromBool(cp.WildcardSubscriptionAvailable)
	propertyLen += properties.EncodedSize.FromBool(cp.SubscriptionIdentifierAvailable)
	propertyLen += properties.EncodedSize.FromBool(cp.SharedSubscriptionAvailable)
	propertyLen += properties.EncodedSize.FromUint16(cp.ServerKeepAlive)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.ResponseInformation)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.ServerReference)
	propertyLen += properties.EncodedSize.FromUTF8String(cp.AuthenticationMethod)
	propertyLen += properties.EncodedSize.FromBinaryData(cp.AuthenticationData)

	return propertyLen
}

func (cp *ConnAckProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromUint32(
		buf, properties.SessionExpiryIntervalID, cp.SessionExpiryInterval); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.ReceiveMaximumID, cp.ReceiveMaximum); err != nil {
		return err
	}

	if err := properties.Encoder.FromByte(
		buf, properties.MaximumQoSID, cp.MaximumQoS); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.RetainAvailableID, cp.RetainAvailable); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint32(
		buf, properties.MaximumPacketSizeID, cp.MaximumPacketSize); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.AssignedClientIdentifierID, cp.AssignedClientIdentifier); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.TopicAliasMaximumID, cp.TopicAliasMaximum); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ReasonStringID, cp.ReasonString); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, cp.UserProperty); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.WildcardSubscriptionAvailableID, cp.WildcardSubscriptionAvailable); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.SubscriptionIdentifierAvailableID, cp.SubscriptionIdentifierAvailable); err != nil {
		return err
	}

	if err := properties.Encoder.FromBool(
		buf, properties.SharedSubscriptionAvailableID, cp.SharedSubscriptionAvailable); err != nil {
		return err
	}

	if err := properties.Encoder.FromUint16(
		buf, properties.ServerKeepAliveID, cp.ServerKeepAlive); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ResponseInformationID, cp.ResponseInformation); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ServerReferenceID, cp.ServerReference); err != nil {
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

func (cp *ConnAckProperties) decode(r io.Reader, propertyLen uint32) error {
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
		case properties.SessionExpiryIntervalID:
			cp.SessionExpiryInterval, err = properties.DecoderOnlyOnce.ToUint32(r, propID, cp.SessionExpiryInterval)
			propertyLen -= 4
		case properties.ReceiveMaximumID:
			cp.ReceiveMaximum, err = properties.DecoderOnlyOnce.ToUint16(r, propID, cp.ReceiveMaximum)
			if err == nil && *cp.ReceiveMaximum == 0 {
				err = fmt.Errorf("%s must not be 0", propID.Text())
			}
			propertyLen -= 2
		case properties.MaximumQoSID:
			cp.MaximumQoS, err = properties.DecoderOnlyOnce.ToByte(r, propID, cp.MaximumQoS)
			if err == nil && *cp.MaximumQoS != 0 && *cp.MaximumQoS != 1 {
				err = fmt.Errorf("%s wrong maximum Qos", propID.Text())
			}
			propertyLen--
		case properties.RetainAvailableID:
			cp.RetainAvailable, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.RetainAvailable)
			propertyLen--
		case properties.MaximumPacketSizeID:
			cp.MaximumPacketSize, err = properties.DecoderOnlyOnce.ToUint32(r, propID, cp.MaximumPacketSize)
			if err == nil && *cp.MaximumPacketSize == 0 {
				err = fmt.Errorf("%s must not be 0", propID.Text())
			}
			propertyLen -= 4
		case properties.AssignedClientIdentifierID:
			cp.AssignedClientIdentifier, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.AssignedClientIdentifier)
			propertyLen -= uint32(len(cp.AssignedClientIdentifier) + 2)
		case properties.TopicAliasMaximumID:
			cp.TopicAliasMaximum, err = properties.DecoderOnlyOnce.ToUint16(r, propID, cp.TopicAliasMaximum)
			propertyLen -= 2
		case properties.ReasonStringID:
			cp.ReasonString, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.ReasonString)
			propertyLen -= uint32(len(cp.ReasonString) + 2)
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
		case properties.WildcardSubscriptionAvailableID:
			cp.WildcardSubscriptionAvailable, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.WildcardSubscriptionAvailable)
			propertyLen--
		case properties.SubscriptionIdentifierAvailableID:
			cp.SubscriptionIdentifierAvailable, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.SubscriptionIdentifierAvailable)
			propertyLen--
		case properties.SharedSubscriptionAvailableID:
			cp.SharedSubscriptionAvailable, err = properties.DecoderOnlyOnce.ToBool(r, propID, cp.SharedSubscriptionAvailable)
			propertyLen--
		case properties.ServerKeepAliveID:
			cp.ServerKeepAlive, err = properties.DecoderOnlyOnce.ToUint16(r, propID, cp.ServerKeepAlive)
			propertyLen -= 2
		case properties.ResponseInformationID:
			cp.ResponseInformation, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.ResponseInformation)
			propertyLen -= uint32(len(cp.ResponseInformation) + 2)
		case properties.ServerReferenceID:
			cp.ServerReference, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.ServerReference)
			propertyLen -= uint32(len(cp.ServerReference) + 2)
		case properties.AuthenticationMethodID:
			cp.AuthenticationMethod, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, cp.AuthenticationMethod)
			propertyLen -= uint32(len(cp.AuthenticationMethod) + 2)
		case properties.AuthenticationDataID:
			cp.AuthenticationData, err = properties.DecoderOnlyOnce.ToBinaryData(r, propID, cp.AuthenticationData)
			propertyLen -= uint32(len(cp.AuthenticationData) + 2)
		default:
			return fmt.Errorf("CONNACK: wrong property with identifier %d", id)
		}
	}

	return err
}

// ConnAck MQTT CONNACK packet
type ConnAck struct {
	SessionPresent bool
	ReasonCode     ConnAckReasonCode
	Properties     *ConnAckProperties
}

func (c *ConnAck) String() string {
	return fmt.Sprintf(`Session present: %t  Reason code: %d Properties: %s`,
		c.SessionPresent, c.ReasonCode, c.Properties)
}

// propertyLength property length
func (c *ConnAck) propertyLength() uint32 {
	if c.Properties != nil {
		return c.Properties.length()
	}
	return 0
}

func (c *ConnAck) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if c.Properties != nil {
		return c.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (c *ConnAck) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}

	if propertyLen > 0 {
		c.Properties = &ConnAckProperties{}
		return c.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the CONNACK packet
func (c *ConnAck) encode(w io.Writer) error {
	propertyLen := c.propertyLength()

	// calculate the remaining length
	// 2 = session present + reason code
	remainingLength := 2 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)
	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, byte(packettype.CONNACK<<4)); err != nil {
		return err
	}

	if err := mqttutil.EncodeVarUint32(&packet, remainingLength); err != nil {
		return err
	}

	if err := mqttutil.EncodeBool(&packet, c.SessionPresent); err != nil {
		return err
	}

	if err := mqttutil.EncodeByte(&packet, byte(c.ReasonCode)); err != nil {
		return err
	}

	if err := c.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	_, err := packet.WriteTo(w)

	return err
}

// decode decode the CONNACK packet
func (c *ConnAck) decode(r io.Reader, remainingLen uint32) error {
	var err error

	c.SessionPresent, err = mqttutil.DecodeBool(r)
	if err != nil {
		return err
	}

	reasonCode, err := mqttutil.DecodeByte(r)
	if err != nil {
		return err
	}
	c.ReasonCode = ConnAckReasonCode(reasonCode)

	return c.decodeProperties(r)
}
