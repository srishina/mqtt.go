package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"

	mqtt "github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: client-pub [options] <topic> <qos> <payload>

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
		-id, --clientid <client ID> Client identifier - optional"
		-k, --keepalive <keep alive> Keep alive - optional, default: 0"
		-cs, --cleanstart <Clean start> Start clean - a new session is created in broker - optional, default: true"
	
	example:
		client-pub -b ws://mqtt.eclipseprojects.io:80/mqtt "TEST/GREETING" 1 "Willkommen"
`

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {
	var (
		broker     string
		clientID   string
		keepAlive  int
		cleanStart bool
	)

	flag.StringVar(&broker, "b", "", "MQTTv5 broker address")
	flag.StringVar(&broker, "broker", "", "MQTTv5 broker address")
	flag.StringVar(&clientID, "id", "", "Client identifier")
	flag.StringVar(&clientID, "clientid", "", "Client identifier")
	flag.IntVar(&keepAlive, "k", 0, "Keep alive")
	flag.IntVar(&keepAlive, "keepalive", 0, "Keep alive")
	flag.BoolVar(&cleanStart, "cs", true, "Start clean - a new session is created in broker")
	flag.BoolVar(&cleanStart, "cleanstart", true, "Start clean - a new session is created in broker")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 3 {
		usage()
	}

	if !isFlagPassed("b") && !isFlagPassed("broker") {
		log.Fatal("Error: MQTTV5 broker address is not given")
		usage()
	}

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

	_, err = client.Connect(context.Background(), &mqttConnect)
	if err != nil {
		log.Fatal(err)
	}

	topic, qos, payload := args[0], args[1], args[2]
	qosLevel, err := strconv.ParseInt(qos, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	if qosLevel > 2 {
		log.Fatal("Maximum QoS must be 2")
	}

	// publish
	err = client.Publish(context.Background(), &mqtt.Publish{TopicName: topic, QoSLevel: byte(qosLevel), Payload: []byte(payload)})
	if err != nil {
		log.Fatal(err)
	}

	// Disconnect from broker
	client.Disconnect(context.Background(), &mqtt.Disconnect{ReasonCode: mqtt.DisconnectReasonCodeNormalDisconnect})
}
