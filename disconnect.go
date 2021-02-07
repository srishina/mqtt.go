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

// DisconnectReasonCode indicates DISCONNECT MQTT reason code
type DisconnectReasonCode byte

const (
	DisconnectReasonCodeNormalDisconnect                  DisconnectReasonCode = DisconnectReasonCode(reasoncode.Success)
	DisconnectReasonCodeWithWillMessage                   DisconnectReasonCode = 0x04
	DisconnectReasonCodeUnspecifiedError                  DisconnectReasonCode = DisconnectReasonCode(reasoncode.UnspecifiedError)
	DisconnectReasonCodeMalformedPacket                   DisconnectReasonCode = DisconnectReasonCode(reasoncode.MalformedPacket)
	DisconnectReasonCodeProtocolError                     DisconnectReasonCode = DisconnectReasonCode(reasoncode.ProtocolError)
	DisconnectReasonCodeImplSpecificError                 DisconnectReasonCode = DisconnectReasonCode(reasoncode.ImplSpecificError)
	DisconnectReasonCodeNotAuthorized                     DisconnectReasonCode = DisconnectReasonCode(reasoncode.NotAuthorized)
	DisconnectReasonCodeServerBusy                        DisconnectReasonCode = DisconnectReasonCode(reasoncode.ServerBusy)
	DisconnectReasonCodeServerShuttingDown                DisconnectReasonCode = 0x8B
	DisconnectReasonCodeKeepAliveTimeout                  DisconnectReasonCode = 0x8D
	DisconnectReasonCodeSessionTakenOver                  DisconnectReasonCode = 0x8E
	DisconnectReasonCodeTopicFilterInvalid                DisconnectReasonCode = DisconnectReasonCode(reasoncode.TopicFilterInvalid)
	DisconnectReasonCodeTopicNameInvalid                  DisconnectReasonCode = DisconnectReasonCode(reasoncode.TopicNameInvalid)
	DisconnectReasonCodeReceiveMaximumExceeded            DisconnectReasonCode = 0x93
	DisconnectReasonCodeTopicAliasInvalid                 DisconnectReasonCode = 0x94
	DisconnectReasonCodePacketTooLarge                    DisconnectReasonCode = 0x95
	DisconnectReasonCodeMessageRateTooHigh                DisconnectReasonCode = 0x96
	DisconnectReasonCodeQuotaExceeded                     DisconnectReasonCode = DisconnectReasonCode(reasoncode.QuotaExceeded)
	DisconnectReasonCodeAdministrativeAction              DisconnectReasonCode = 0x98
	DisconnectReasonCodePayloadFormatInvalid              DisconnectReasonCode = DisconnectReasonCode(reasoncode.PayloadFormatInvalid)
	DisconnectReasonCodeRetainNotSupported                DisconnectReasonCode = DisconnectReasonCode(reasoncode.RetainNotSupported)
	DisconnectReasonCodeQoSNotSupported                   DisconnectReasonCode = DisconnectReasonCode(reasoncode.QoSNotSupported)
	DisconnectReasonCodeUseAnotherServer                  DisconnectReasonCode = DisconnectReasonCode(reasoncode.UseAnotherServer)
	DisconnectReasonServerMoved                           DisconnectReasonCode = DisconnectReasonCode(reasoncode.ServerMoved)
	DisconnectReasonCodeSharedSubscriptionsNotSupported   DisconnectReasonCode = DisconnectReasonCode(reasoncode.SharedSubscriptionsNotSupported)
	DisconnectReasonCodeConnectionRateExceeded            DisconnectReasonCode = DisconnectReasonCode(reasoncode.ConnectionRateExceeded)
	DisconnectReasonCodeMaximumConnectTime                DisconnectReasonCode = 0xA0
	DisconnectReasonCodeSubscriptionIdsNotSupported       DisconnectReasonCode = DisconnectReasonCode(reasoncode.SubscriptionIdsNotSupported)
	DisconnectReasonCodeWildcardSubscriptionsNotSupported DisconnectReasonCode = DisconnectReasonCode(reasoncode.WildcardSubscriptionsNotSupported)
)

var disconnectReasonCodeText = map[DisconnectReasonCode]string{
	DisconnectReasonCodeNormalDisconnect:                  reasoncode.Success.Text(),
	DisconnectReasonCodeWithWillMessage:                   "Disconnect with Will Message",
	DisconnectReasonCodeUnspecifiedError:                  reasoncode.UnspecifiedError.Text(),
	DisconnectReasonCodeMalformedPacket:                   reasoncode.MalformedPacket.Text(),
	DisconnectReasonCodeProtocolError:                     reasoncode.ProtocolError.Text(),
	DisconnectReasonCodeImplSpecificError:                 reasoncode.ImplSpecificError.Text(),
	DisconnectReasonCodeNotAuthorized:                     reasoncode.NotAuthorized.Text(),
	DisconnectReasonCodeServerBusy:                        reasoncode.ServerBusy.Text(),
	DisconnectReasonCodeServerShuttingDown:                "Server shutting down",
	DisconnectReasonCodeKeepAliveTimeout:                  "Keep Alive timeout",
	DisconnectReasonCodeSessionTakenOver:                  "Session taken over",
	DisconnectReasonCodeTopicFilterInvalid:                reasoncode.TopicFilterInvalid.Text(),
	DisconnectReasonCodeTopicNameInvalid:                  reasoncode.TopicNameInvalid.Text(),
	DisconnectReasonCodeReceiveMaximumExceeded:            "Receive Maximum exceeded",
	DisconnectReasonCodeTopicAliasInvalid:                 "Topic Alias invalid",
	DisconnectReasonCodePacketTooLarge:                    "Packet too large",
	DisconnectReasonCodeMessageRateTooHigh:                "Message rate too high",
	DisconnectReasonCodeQuotaExceeded:                     reasoncode.QuotaExceeded.Text(),
	DisconnectReasonCodeAdministrativeAction:              "Administrative action",
	DisconnectReasonCodePayloadFormatInvalid:              reasoncode.PayloadFormatInvalid.Text(),
	DisconnectReasonCodeRetainNotSupported:                reasoncode.RetainNotSupported.Text(),
	DisconnectReasonCodeQoSNotSupported:                   reasoncode.QoSNotSupported.Text(),
	DisconnectReasonCodeUseAnotherServer:                  reasoncode.UseAnotherServer.Text(),
	DisconnectReasonServerMoved:                           reasoncode.ServerMoved.Text(),
	DisconnectReasonCodeSharedSubscriptionsNotSupported:   reasoncode.SharedSubscriptionsNotSupported.Text(),
	DisconnectReasonCodeConnectionRateExceeded:            reasoncode.ConnectionRateExceeded.Text(),
	DisconnectReasonCodeMaximumConnectTime:                "Maximum connect time",
	DisconnectReasonCodeSubscriptionIdsNotSupported:       reasoncode.SubscriptionIdsNotSupported.Text(),
	DisconnectReasonCodeWildcardSubscriptionsNotSupported: reasoncode.WildcardSubscriptionsNotSupported.Text(),
}

var disconnectReasonCodeDesc = map[DisconnectReasonCode]string{
	DisconnectReasonCodeNormalDisconnect:                  "Close the connection normally. Do not send the Will Message.",
	DisconnectReasonCodeWithWillMessage:                   "The Client wishes to disconnect but requires that the Server also publishes its Will Message.",
	DisconnectReasonCodeUnspecifiedError:                  "The Connection is closed but the sender either does not wish to reveal the reason, or none of the other Reason Codes apply.",
	DisconnectReasonCodeMalformedPacket:                   "The received packet does not conform to this specification.",
	DisconnectReasonCodeProtocolError:                     "An unexpected or out of order packet was received.",
	DisconnectReasonCodeImplSpecificError:                 "The packet received is valid but cannot be processed by this implementation.",
	DisconnectReasonCodeNotAuthorized:                     "The request is not authorized.",
	DisconnectReasonCodeServerBusy:                        "The Server is busy and cannot continue processing requests from this Client.",
	DisconnectReasonCodeServerShuttingDown:                "The Server is shutting down.",
	DisconnectReasonCodeKeepAliveTimeout:                  "The Connection is closed because no packet has been received for 1.5 times the Keepalive time.",
	DisconnectReasonCodeSessionTakenOver:                  "Another Connection using the same ClientID has connected causing this Connection to be closed.",
	DisconnectReasonCodeTopicFilterInvalid:                "The Topic Filter is correctly formed, but is not accepted by this Sever.",
	DisconnectReasonCodeTopicNameInvalid:                  "The Topic Name is correctly formed, but is not accepted by this Client or Server.",
	DisconnectReasonCodeReceiveMaximumExceeded:            "The Client or Server has received more than Receive Maximum publication for which it has not sent PUBACK or PUBCOMP. ",
	DisconnectReasonCodeTopicAliasInvalid:                 "The Client or Server has received a PUBLISH packet containing a Topic Alias which is greater than the Maximum Topic Alias it sent in the CONNECT or CONNACK packet.",
	DisconnectReasonCodePacketTooLarge:                    "The packet size is greater than Maximum Packet Size for this Client or Server.",
	DisconnectReasonCodeMessageRateTooHigh:                "The received data rate is too high.",
	DisconnectReasonCodeQuotaExceeded:                     "An implementation or administrative imposed limit has been exceeded.",
	DisconnectReasonCodeAdministrativeAction:              "The Connection is closed due to an administrative action.",
	DisconnectReasonCodePayloadFormatInvalid:              "The payload format does not match the one specified by the Payload Format Indicator.",
	DisconnectReasonCodeRetainNotSupported:                "The Server has does not support retained messages.",
	DisconnectReasonCodeQoSNotSupported:                   "The Client specified a QoS greater than the QoS specified in a Maximum QoS in the CONNACK. ",
	DisconnectReasonCodeUseAnotherServer:                  "The Client should temporarily change its Server.",
	DisconnectReasonServerMoved:                           "The Server is moved and the Client should permanently change its server location.",
	DisconnectReasonCodeSharedSubscriptionsNotSupported:   "The Server does not support Shared Subscriptions.",
	DisconnectReasonCodeConnectionRateExceeded:            "This connection is closed because the connection rate is too high.",
	DisconnectReasonCodeMaximumConnectTime:                "The maximum connection time authorized for this connection has been exceeded.",
	DisconnectReasonCodeSubscriptionIdsNotSupported:       "The Server does not support Subscription Identifiers; the subscription is not accepted.",
	DisconnectReasonCodeWildcardSubscriptionsNotSupported: "The Server does not support Wildcard Subscriptions; the subscription is not accepted.",
}

// DisconnectProperties MQTT DISCONNECT properties
type DisconnectProperties struct {
	SessionExpiryInterval *uint32
	ReasonString          string
	UserProperty          map[string]string
	ServerReference       string
}

func (dp *DisconnectProperties) length() uint32 {
	propertyLen := uint32(0)
	propertyLen += properties.EncodedSize.FromUint32(dp.SessionExpiryInterval)
	propertyLen += properties.EncodedSize.FromUTF8String(dp.ReasonString)
	propertyLen += properties.EncodedSize.FromUTF8StringPair(dp.UserProperty)
	propertyLen += properties.EncodedSize.FromUTF8String(dp.ServerReference)
	return propertyLen
}

func (dp *DisconnectProperties) encode(buf *bytes.Buffer, propertyLen uint32) error {
	if err := properties.Encoder.FromUint32(
		buf, properties.SessionExpiryIntervalID, dp.SessionExpiryInterval); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ReasonStringID, dp.ReasonString); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8StringPair(
		buf, properties.UserPropertyID, dp.UserProperty); err != nil {
		return err
	}

	if err := properties.Encoder.FromUTF8String(
		buf, properties.ServerReferenceID, dp.ServerReference); err != nil {
		return err
	}

	return nil
}

func (dp *DisconnectProperties) decode(r io.Reader, propertyLen uint32) error {
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
			dp.SessionExpiryInterval, err = properties.DecoderOnlyOnce.ToUint32(r, propID, dp.SessionExpiryInterval)
			propertyLen -= 4
		case properties.ReasonStringID:
			dp.ReasonString, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, dp.ReasonString)
			propertyLen -= uint32(len(dp.ReasonString) + 2)
		case properties.UserPropertyID:
			if dp.UserProperty == nil {
				dp.UserProperty = make(map[string]string)
			}

			key, value, err2 := properties.Decoder.ToUTF8StringPair(r)
			if err2 != nil {
				return err2
			}

			dp.UserProperty[key] = value
			propertyLen -= uint32(len(key) + len(value) + 4)
		case properties.ServerReferenceID:
			dp.ServerReference, err = properties.DecoderOnlyOnce.ToUTF8String(r, propID, dp.ServerReference)
			propertyLen -= uint32(len(dp.ServerReference) + 2)
		default:
			return fmt.Errorf("DISCONNECT: wrong property with identifier %d", id)
		}
	}
	return err
}

// Disconnect MQTT DISCONNECT packet
type Disconnect struct {
	ReasonCode DisconnectReasonCode
	Properties *DisconnectProperties
}

func (d *Disconnect) propertyLength() uint32 {
	if d.Properties != nil {
		return d.Properties.length()
	}
	return 0
}

func (d *Disconnect) encodeProperties(buf *bytes.Buffer, propertyLen uint32) error {
	if err := mqttutil.EncodeVarUint32(buf, propertyLen); err != nil {
		return err
	}

	if d.Properties != nil {
		return d.Properties.encode(buf, propertyLen)
	}
	return nil
}

func (d *Disconnect) decodeProperties(r io.Reader) error {
	propertyLen, _, err := mqttutil.DecodeVarUint32(r)
	if err != nil {
		return err
	}
	if propertyLen > 0 {
		d.Properties = &DisconnectProperties{}
		return d.Properties.decode(r, propertyLen)
	}

	return nil
}

// encode encode the DISCONNECT packet
func (d *Disconnect) encode(w io.Writer) error {
	propertyLen := d.propertyLength()
	// calculate the remaining length
	// 1 =  reason code
	remainingLength := 1 + propertyLen + mqttutil.EncodedVarUint32Size(propertyLen)
	var packet bytes.Buffer
	packet.Grow(int(1 + remainingLength + mqttutil.EncodedVarUint32Size(remainingLength)))
	if err := mqttutil.EncodeByte(&packet, byte(packettype.DISCONNECT<<4)); err != nil {
		return err
	}

	if err := mqttutil.EncodeVarUint32(&packet, remainingLength); err != nil {
		return err
	}

	if err := mqttutil.EncodeByte(&packet, byte(d.ReasonCode)); err != nil {
		return err
	}

	if err := d.encodeProperties(&packet, propertyLen); err != nil {
		return err
	}

	_, err := packet.WriteTo(w)

	return err
}

// decode decode the DISCONNECT packet
func (d *Disconnect) decode(r io.Reader, remainingLen uint32) error {
	var err error

	reasonCode, err := mqttutil.DecodeByte(r)
	if err != nil {
		return err
	}
	d.ReasonCode = DisconnectReasonCode(reasonCode)

	return d.decodeProperties(r)
}
