package mqtt

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/srishina/mqtt.go/internal/packettype"
	"github.com/stretchr/testify/assert"
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
	client := NewClient(&WebsocketConn{Host: url.String()})
	return client, nil
}

var mqttMock *mqttMockTester

type setupParams struct {
	triggerPublish     bool
	clientReadDeadline time.Duration
	reconnectDelay     int
}

func setup(responses map[packettype.PacketType]packet, params setupParams) (*Client, error) {
	mqttMock = &mqttMockTester{
		responses:                 responses,
		triggerPublishOnsubscribe: params.triggerPublish,
		clientReadDeadline:        params.clientReadDeadline,
	}

	var opts []ClientOption
	if params.reconnectDelay != 0 {
		opts = append(opts, WithInitialReconnectDelay(params.reconnectDelay))
	} else {
		opts = append(opts, WithInitialReconnectDelay(50))
	}

	client := NewClient(mqttMock, opts...)
	return client, nil
}

func TestBasic(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.SUBACK: &SubAck{
			ReasonCodes: []SubAckReasonCode{SubAckReasonCodeGrantedQoS1},
		},
		packettype.UNSUBACK: &UnsubAck{
			ReasonCodes: []UnsubAckReasonCode{UnsubAckReasonCodeSuccess},
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	recvr := NewMessageReceiver()
	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
	suback, err := client.Subscribe(context.Background(), s, recvr)
	require.NoError(t, err, "MQTT client subscribe failed")
	require.Equal(t, 1, len(suback.ReasonCodes))
	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.ReasonCodes[0])

	unsuback, err := client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe failed")
	require.Equal(t, 1, len(unsuback.ReasonCodes))
	require.Equal(t, UnsubAckReasonCodeSuccess, unsuback.ReasonCodes[0])

	// update the UNSUBACK response
	responses[packettype.UNSUBACK] = &UnsubAck{
		ReasonCodes: []UnsubAckReasonCode{UnsubAckNoSubscriptionExisted},
	}

	unsuback, err = client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe with non-existing subscription failed")
	require.Equal(t, 1, len(unsuback.ReasonCodes))
	require.Equal(t, UnsubAckNoSubscriptionExisted, unsuback.ReasonCodes[0])
}

func TestBasicWithKeepAlive(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.PINGRESP: &pingResp{},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true, KeepAlive: 2})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})
	time.Sleep(5 * time.Second)
}

func TestSubUnsubCallback(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.SUBACK: &SubAck{
			ReasonCodes: []SubAckReasonCode{SubAckReasonCodeGrantedQoS1},
		},
		packettype.UNSUBACK: &UnsubAck{
			ReasonCodes: []UnsubAckReasonCode{UnsubAckReasonCodeSuccess},
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
	suback, err := client.CallbackSubscribe(context.Background(), s, func(m *Publish) {
	})

	require.NoError(t, err, "MQTT client subscribe failed")
	require.Equal(t, 1, len(suback.ReasonCodes))
	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.ReasonCodes[0])

	unsuback, err := client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe failed")
	require.Equal(t, 1, len(unsuback.ReasonCodes))
	require.Equal(t, UnsubAckReasonCodeSuccess, unsuback.ReasonCodes[0])
}

func TestPublishQoS1(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.PUBACK: &PubAck{
			ReasonCode: PubAckReasonCodeSuccess,
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 1, Payload: []byte("Hello world!")})
	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 1")
}

func TestPublishQoS2(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.PUBREC: &PubRec{
			ReasonCode: PubRecReasonCodeSuccess,
		},
		packettype.PUBCOMP: &PubComp{
			ReasonCode: PubCompReasonCodeSuccess,
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	err = client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 2, Payload: []byte("Hello world!")})
	require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 2")
}

func recvPublish(t *testing.T, publishResponses map[packettype.PacketType]packet, payload string) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.SUBACK: &SubAck{
			ReasonCodes: []SubAckReasonCode{SubAckReasonCodeGrantedQoS1},
		},
		packettype.UNSUBACK: &UnsubAck{
			ReasonCodes: []UnsubAckReasonCode{UnsubAckReasonCodeSuccess},
		},
	}
	for k, v := range publishResponses {
		responses[k] = v
	}
	client, err := setup(responses, setupParams{triggerPublish: true})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	assert.NoError(t, err, "MQTT client connect failed")
	assert.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)
	defer client.Disconnect(context.Background(), &Disconnect{})

	pubRecvd := make(chan struct{}, 1)
	var receivedPayload string
	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING/#", QoSLevel: 2}}}
	suback, err := client.CallbackSubscribe(context.Background(), s, func(m *Publish) {
		receivedPayload = string(m.Payload)
		close(pubRecvd)
	})
	assert.NoError(t, err, "MQTT client subscribe failed")
	assert.Equal(t, 1, len(suback.ReasonCodes))
	assert.Equal(t, SubAckReasonCodeGrantedQoS1, suback.ReasonCodes[0])

	<-pubRecvd
	assert.Equal(t, payload, receivedPayload)

	unsuback, err := client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING/#"}})
	assert.NoError(t, err, "MQTT client unsubscribe failed")
	assert.Equal(t, 1, len(unsuback.ReasonCodes))
	assert.Equal(t, UnsubAckReasonCodeSuccess, unsuback.ReasonCodes[0])
}

func TestReceivePublishWithQoS0(t *testing.T) {
	payload := "Willkommen!"
	responses := map[packettype.PacketType]packet{
		packettype.PUBLISH: &Publish{TopicName: "TEST/GREETING", QoSLevel: 0, Payload: []byte(payload)},
	}
	recvPublish(t, responses, payload)
}

func TestReceivePublishWithQoS1(t *testing.T) {
	payload := "Willkommen!"
	responses := map[packettype.PacketType]packet{
		packettype.PUBLISH: &Publish{TopicName: "TEST/GREETING", QoSLevel: 1, Payload: []byte(payload)},
	}
	recvPublish(t, responses, payload)
}

func TestReceivePublishWithQoS2(t *testing.T) {
	payload := "Willkommen!"
	responses := map[packettype.PacketType]packet{
		packettype.PUBLISH: &Publish{TopicName: "TEST/GREETING", QoSLevel: 2, Payload: []byte(payload)},
		packettype.PUBREL:  &PubRel{ReasonCode: PubRelReasonCodeSuccess},
	}
	recvPublish(t, responses, payload)
}

func TestClientReconnect(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})
	reconencted := make(chan struct{})
	client.On(ReconnectedEvent, func(connack *ConnAck) {
		close(reconencted)
	})

	client.On(DisconnectedEvent, func(err error) {
	})
	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)

	// close the server
	mqttMock.svrConn.Close()

	select {
	case <-time.After(2 * time.Second):
		require.Fail(t, "Must not timeout, failed")
	case <-reconencted:
	}

	client.Disconnect(context.Background(), &Disconnect{})
}

func TestAutoSubscribeAfterReconnect(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
		packettype.SUBACK: &SubAck{
			ReasonCodes: []SubAckReasonCode{SubAckReasonCodeGrantedQoS1},
		},
		packettype.UNSUBACK: &UnsubAck{
			ReasonCodes: []UnsubAckReasonCode{UnsubAckReasonCodeSuccess},
		},
	}
	client, err := setup(responses, setupParams{triggerPublish: false})

	client.On(DisconnectedEvent, func(err error) {
	})

	resubscribed := make(chan struct{})
	client.On(ResubscribeEvent, func(result ResubscribeResult) {
		if result.Error != nil {
			t.Fail()
		}
		close(resubscribed)
	})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)

	recvr := NewMessageReceiver()
	s := &Subscribe{Subscriptions: []Subscription{{TopicFilter: "TEST/GREETING", QoSLevel: 1}}}
	suback, err := client.Subscribe(context.Background(), s, recvr)
	require.NoError(t, err, "MQTT client subscribe failed")
	require.Equal(t, 1, len(suback.ReasonCodes))
	require.Equal(t, SubAckReasonCodeGrantedQoS1, suback.ReasonCodes[0])

	// close the server
	mqttMock.svrConn.Close()

	select {
	case <-time.After(2 * time.Second):
		require.Fail(t, "Must not timeout, failed")
	case <-resubscribed:
	}

	unsuback, err := client.Unsubscribe(context.Background(), &Unsubscribe{TopicFilters: []string{"TEST/GREETING"}})
	require.NoError(t, err, "MQTT client unsubscribe failed")
	require.Equal(t, 1, len(unsuback.ReasonCodes))
	require.Equal(t, UnsubAckReasonCodeSuccess, unsuback.ReasonCodes[0])

	client.Disconnect(context.Background(), &Disconnect{})
}

func testPublishAfterReconnect(t *testing.T, respConnAck *ConnAck, disconnectPktCount int) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: respConnAck,
		packettype.PUBREC: &PubRec{
			ReasonCode: PubRecReasonCodeSuccess,
		},
		packettype.PUBCOMP: &PubComp{
			ReasonCode: PubCompReasonCodeSuccess,
		},
	}

	client, err := setup(responses, setupParams{triggerPublish: false})

	mqttMock.disconnectAtPacketCount = disconnectPktCount
	client.On(DisconnectedEvent, func(err error) {
	})

	var wg sync.WaitGroup
	start := make(chan struct{})
	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)

	wg.Add(100)
	// run 100 go routines
	for i := 0; i < 100; i++ {
		go func(n int) {
			defer wg.Done()
			<-start
			err := client.Publish(context.Background(), &Publish{TopicName: "TEST/GREETING", QoSLevel: 2, Payload: []byte("Hello world!")})
			require.NoError(t, err, "MQTT client PUBLISH failed, QoS is 2")
		}(i)

	}

	completed := make(chan struct{})
	go func() {
		defer close(completed)
		wg.Wait()
	}()
	close(start)

	select {
	case <-time.After(15 * time.Second):
		require.Fail(t, "Must not timeout, failed")
	case <-completed:
	}

	client.Disconnect(context.Background(), &Disconnect{})
}
func TestPublishAfterReconnectWithSession(t *testing.T) {
	// with session
	testPublishAfterReconnect(t, &ConnAck{
		ReasonCode:     ConnAckReasonCodeSuccess,
		SessionPresent: true,
	}, 15)
}

func TestPublishAfterReconnectWithoutSession(t *testing.T) {
	// without session
	recvMax := uint16(10)
	testPublishAfterReconnect(t, &ConnAck{
		ReasonCode:     ConnAckReasonCodeSuccess,
		SessionPresent: false,
		Properties: &ConnAckProperties{
			ReceiveMaximum: &recvMax,
		},
	}, 30)
}

func TestCloseClientInDisconnectedState(t *testing.T) {
	responses := map[packettype.PacketType]packet{
		packettype.CONNACK: &ConnAck{
			ReasonCode:     ConnAckReasonCodeSuccess,
			SessionPresent: false,
		},
	}
	client, err := setup(responses, setupParams{
		triggerPublish:     false,
		clientReadDeadline: 10 * time.Millisecond,
	})

	client.On(DisconnectedEvent, func(err error) {
	})

	connack, err := client.Connect(context.Background(), &Connect{CleanStart: true})
	require.NoError(t, err, "MQTT client connect failed")
	require.Equal(t, ConnAckReasonCodeSuccess, connack.ReasonCode)

	// close the server
	mqttMock.svrConn.Close()

	time.Sleep(250 * time.Millisecond)

	client.Disconnect(context.Background(), &Disconnect{})
}

func TestSusbcriptionCache(t *testing.T) {
	cache := subscriptionCache{}
	s := &Subscribe{Subscriptions: []Subscription{
		{TopicFilter: "TEST/GREETING", QoSLevel: 1},
		{TopicFilter: "TEST/GREETING2", QoSLevel: 1},
		{TopicFilter: "TEST/GREETING3", QoSLevel: 1},
	}}

	s2 := &Subscribe{Subscriptions: []Subscription{
		{TopicFilter: "FOO/GREETING", QoSLevel: 1},
		{TopicFilter: "FOO/GREETING2", QoSLevel: 1},
		{TopicFilter: "FOO/GREETING3", QoSLevel: 1},
	}}

	cache = append(cache, s)
	cache = append(cache, s2)
	require.Equal(t, 2, len(cache))
	cache.removeSubscriptionFromCache("TEST/GREETING2")
	require.Equal(t, 2, len(cache))
	require.Equal(t, 2, len(cache[0].Subscriptions))
	cache.removeSubscriptionFromCache("TEST/GREETING")
	cache.removeSubscriptionFromCache("TEST/GREETING3")
	require.Equal(t, 1, len(cache))
}
