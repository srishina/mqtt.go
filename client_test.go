package mqtt

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/require"
)

type cstClient struct {
	*Client
}

const (
	requestURI = "ws://mqtt.eclipseprojects.io:80/mqtt"
)

var wg sync.WaitGroup

func newClient() (*Client, error) {
	url, _ := url.ParseRequestURI(requestURI)
	client := NewClient(&websocketConn{Host: url.String()})
	return client, nil
}

var mockSvr *mockServer

func setup(responses map[packettype.PacketType]packet) (*Client, error) {
	mockSvr = newMockServer()
	mockSvr.responses = responses
	go mockSvr.run()
	client := NewClient(&mockClientConn{conn: mockSvr.clientConn})
	return client, nil
}

func teardown() {
	if mockSvr != nil {
		mockSvr.stop()
	}
}

func TestBasic(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.SUBACK: &SubAck{
			Payload: []SubAckReasonCode{SubAckReasonCodeGrantedQoS1},
		},
		packettype.UNSUBACK: &UnsubAck{
			Payload: []UnsubAckReasonCode{UnsubAckReasonCodeSuccess},
		},
	}
	client, err := setup(responses)
	defer teardown()

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	recvr := NewMessageReceiver()
	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
	suback, err := client.Subscribe(context.Background(), s, recvr)
	require.NoError(t, err, "MQTT client subscribe failed")
	require.Equal(t, 1, len(suback.Payload))
	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.Payload[0])

	unsuback, err := client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe failed")
	require.Equal(t, 1, len(unsuback.Payload))
	require.Equal(t, UnsubAckReasonCodeSuccess, unsuback.Payload[0])

	// update the UNSUBACK response
	responses[packettype.UNSUBACK] = &UnsubAck{
		Payload: []UnsubAckReasonCode{UnsubAckNoSubscriptionExisted},
	}

	unsuback, err = client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe with non-existing subscription failed")
	require.Equal(t, 1, len(unsuback.Payload))
	require.Equal(t, UnsubAckNoSubscriptionExisted, unsuback.Payload[0])
}

func TestBasicWithKeepAlive(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.PINGRESP: &pingResp{},
	}
	client, err := setup(responses)
	defer teardown()

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true, KeepAlive: 5})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})
	time.Sleep(12 * time.Second)
}

// func TestPubSub(t *testing.T) {

// 	client, err := newClient()

// 	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
// 	require.NoError(t, err, "MQTT client connect failed")
// 	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
// 	defer client.Disconnect(context.Background(), &Disconnect{})

// 	recvr := NewMessageReceiver()
// 	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
// 	suback, err := client.Subscribe(context.Background(), s, recvr)
// 	require.NoError(t, err, "MQTT client subscribe failed")
// 	require.Equal(t, 1, len(suback.Payload))
// 	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.Payload[0])

// 	defer client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})

// 	payloads := []string{
// 		"Hello world!",
// 		"Welcome!",
// 		"Willkommen",
// 	}
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 0, Payload: []byte(payloads[0])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 0")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 1, Payload: []byte(payloads[1])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 1")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 2, Payload: []byte(payloads[2])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 2")

// 	time.Sleep(250 * time.Millisecond)

// 	for i := 1; i < len(payloads); i++ {
// 		msg, err := recvr.Recv()
// 		require.NoError(t, err, "Recv channel encountered an error")
// 		require.Contains(t, payloads, string(msg.Payload()))
// 	}
// }

// func TestPubSubWithCallback(t *testing.T) {

// 	client, err := newClient()

// 	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
// 	require.NoError(t, err, "MQTT client connect failed")
// 	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
// 	defer client.Disconnect(context.Background(), &Disconnect{})

// 	var receivedPayloads []string
// 	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
// 	suback, err := client.CallbackSubscribe(context.Background(), s, func(m Message) {
// 		receivedPayloads = append(receivedPayloads, string(m.Payload()))
// 	})

// 	require.NoError(t, err, "MQTT client subscribe failed")
// 	require.Equal(t, 1, len(suback.Payload))
// 	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.Payload[0])

// 	defer client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})

// 	payloads := []string{
// 		"Hello world!",
// 		"Welcome!",
// 		"Willkommen",
// 	}
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 0, Payload: []byte(payloads[0])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 0")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 1, Payload: []byte(payloads[1])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 1")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 2, Payload: []byte(payloads[2])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 2")

// 	time.Sleep(250 * time.Millisecond)
// 	require.ElementsMatch(t, payloads, receivedPayloads)
// }

// func TestPubSubWithAlias(t *testing.T) {

// 	client, err := newClient()

// 	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
// 	require.NoError(t, err, "MQTT client connect failed")
// 	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
// 	defer client.Disconnect(context.Background(), &Disconnect{})

// 	recvr := NewMessageReceiver()
// 	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
// 	suback, err := client.Subscribe(context.Background(), s, recvr)
// 	require.NoError(t, err, "MQTT client subscribe failed")
// 	require.Equal(t, 1, len(suback.Payload))
// 	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.Payload[0])

// 	defer client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})

// 	payloads := []string{
// 		"Hello world!",
// 		"Welcome!",
// 		"Willkommen",
// 	}
// 	topicAlias := uint16(2)
// 	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 0,
// 		Properties: PublishProperties{TopicAlias: &topicAlias}, Payload: []byte(payloads[0])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 0")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "", QoSLevel: 1,
// 		Properties: PublishProperties{TopicAlias: &topicAlias}, Payload: []byte(payloads[1])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 1")
// 	err = client.Publish(context.Background(), &Publish{TopicName: "", QoSLevel: 2,
// 		Properties: PublishProperties{TopicAlias: &topicAlias}, Payload: []byte(payloads[2])})
// 	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 2")

// 	time.Sleep(250 * time.Millisecond)

// 	for i := 1; i < len(payloads); i++ {
// 		msg, err := recvr.Recv()
// 		require.NoError(t, err, "Recv channel encountered an error")
// 		require.Contains(t, payloads, string(msg.Payload()))
// 	}
// }
