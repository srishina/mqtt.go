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
