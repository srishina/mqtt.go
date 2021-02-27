# mqtt.go
MQTTv5 client and server library

Go implementation of MQTTv5 protocol

(server library is work in progress)

# Installation

```bash
# Go client
go get github.com/srishina/mqtt.go
```

# Run the tests
```bash
go test ./... -race -v
```
note: few test can take time, namely, TestBasicWithKeepAlive, TestPublishAfterReconnectWithSession, TestPublishAfterReconnectWithoutSession

# Try out the examples
```bash
cd ./examples
```

Connect to a broker(basic client):
```bash
go run ./basic-client/main.go -b ws://mqtt.eclipseprojects.io:80/mqtt -k 120 -cs=true // keep alive = 120secs, clean start=true
```
Publish a message:
```bash
go run ./client-pub/main.go -b ws://mqtt.eclipseprojects.io:80/mqtt "TEST/GREETING" 1 "Willkommen"
```
Subscribe to a message:
```bash
go run ./client-sub/main.go -b ws://mqtt.eclipseprojects.io:80/mqtt "TEST/GREETING/#" 1
```
Will message
```bash
go run ./client-will-msg/main.go -b ws://mqtt.eclipseprojects.io:80/mqtt --will-delay-interval 5 "TEST/GREETING/WILL" 1 "The Will message" "TEST/GREETING/#" 1
```
# Network connection - client

The client library provides a possibility to provision a connection. The implementation of the "Connection" interface needs to be passed when initializing the client.
```go
    // Connection represents a connection that the MQTT client uses.
    // The implementation of the Connection is responsible for
    // initialization of the connection(tcp, ws etc...) with the broker.
    // WebsocketConn, TCPConn is provided as part of the library, other
    // connections can be written by the implementations
    type Connection interface {
        BrokerURL() string
        // Connect MQTT client calls Connect when it needs a io read writer.
        // If the Connect returns an error during reconnect then the MQTT client will
        // attempt a reconnect again. The reconnect interval depends on backoff delay
        Connect(ctx context.Context) (io.ReadWriter, error)
        // Closes the network connection
        Close()
    }
```

WebsocketConn, TCPConn implementations are provided as part of the library.
```go
e.g
	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
	}

	var conn mqtt.Connection
	switch u.Scheme {
	case "ws":
		fallthrough
	case "wss":
		conn = &mqtt.WebsocketConn{Host: broker}
	case "tcp":
		conn = &mqtt.TCPConn{Host: u.Host}
	default:
		log.Fatal("Invalid scheme name")
	}
	client := mqtt.NewClient(conn)
	mqttConnect := mqtt.Connect{KeepAlive: uint16(keepAlive), CleanStart: cleanStart, ClientID: clientID}
```

If the default implementations are not suitable and then more sophisticated implementations can be provisioned.


# Subscriber overview - client

In order to receive messages published to a topic, the client needs to subscribe to the interesting topics. The client can either use push or pull mechanism to receive messages. In the pull model the client can decide when to read the messages. The messages are queued internally in the library. The client may run the message receiver in a separate go routine. In the push model the library delivers message to the client asynchronously as the PUBLISH messages are received.

## Pull model
```go
e.g

	recvr := mqtt.NewMessageReceiver()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			p, err := recvr.Recv()
			if err != nil {
				return
			}
			log.Printf("PUBLISH recvd - Topic: %s QoS: %d Payload: %v\n", p.TopicName, p.QoSLevel, string(p.Payload))
		}
	}()
	// subscribe
	subscriptions := []*mqtt.Subscription{}
	subscriptions = append(subscriptions, &mqtt.Subscription{TopicFilter: "TEST/GREETING/#", QoSLevel: 2})

	suback, err := client.Subscribe(context.Background(), &mqtt.Subscribe{Subscriptions: subscriptions}, recvr)
	if err != nil {
		log.Fatal(err)
	}
```

## Push model
```go

    // The messages are delivered asynchronously. The library does not order messages in this case. The messages
    // are delivered as it arrives. The callbacks are executed from the library using a go routine.

	s := &Subscribe{Subscriptions: []*Subscription{{TopicFilter: "TEST/GREETING/#", QoSLevel: 2}}}
	suback, err := client.CallbackSubscribe(context.Background(), s, func(m *Publish) {
        log.Printf("PUBLISH received - Topic: %s QoS: %d Payload: %v\n", p.TopicName, p.QoSLevel, string(p.Payload))
	})

```

# How the network reconnect is handled in the library?

The client library supports reconnecting and automatically resubscribe / publish the pending messages.

MQTTv5 supports the possibility to set whether the session that is initiated with the broker should be clean or a continuation of the last session. In the later case, the session unique identifier is used. The specification also provides an extra property through which the client or the broker can decide how long a session should be kept. The client can set a session expiry interval. However, if the browser specifies a session expiry interval then that value takes precedence. If the client or broker does not specify session expiry interval then the session state is lost when the network connection is dropped.

So in summary, clean start + the session expiry interval + the CONNACK response from the broker determines how the client reconnects.

The library operates as below:

If the network connection is dropped, the library tries to reconnect with the broker with the CONNECT packet set by client. At the moment, the library does not provide a mechanism to override the CONNECT packet. Based on the broker response the client will perform one of the below.

1. If the broker still has the session state, then the pending messages will be send, which can also include partial PUBLISH messages with QoS 2. No resubscription is needed as broker has the subscriptions.
2. If the broker has no session state, then the client library resubscribes to the already subscribed topics and send pending messages. For QoS 1 & 2 the library restarts the publish flow again. Note that, in this scenario the resubscription may fail and the client will be notified of the status of the resubscription.

Connection retry uses exponential backoff with jitter.
```go
    an e.g

	var opts []ClientOption
    opts = append(opts, WithInitialReconnectDelay(50))
    // other as needed
	client := NewClient(mqttMock, opts...)

    please see func WithInitialReconnectDelay, WithMaxReconnectDelay, WithReconnectJitter for more information
```
