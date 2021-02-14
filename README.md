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


# How the network reconnect is handled in the library?

The client library supports reconnecting and automatically resubscribe / publish the pending messages.

MQTTv5 supports the possibility to set whether the session that is initiated with the broker should be clean or a continuation of the last session. In the later case, the session unique identifier is used. The specification also provides an extra property through which the client or the broker can decide how long a session should be kept. The client can set a session expiry interval. However, if the browser specifies a session expiry interval then that value takes precedence. If the client or broker does not specify session expiry interval then the session state is lost when the network connection is dropped.

So in sumamry, clean start + the session expiry interval + the connack response from the broker determines how the client reconnects.

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