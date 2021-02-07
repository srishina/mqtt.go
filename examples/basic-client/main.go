package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	mqtt "github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: basic-client [options]

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
		-c, --conntype <connection type> Connection type - default: ws"
		-id, --clientid <client ID> Client identifier - optional"
		-k, --keepalive <keep alive> Keep alive - optional, default: 0"
		-cs, --cleanstart <Clean start> Start clean - a new session is created in broker - optional, default: true"
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
		connType   string
	)

	flag.StringVar(&broker, "b", "", "MQTTv5 broker address")
	flag.StringVar(&broker, "broker", "", "MQTTv5 broker address")
	flag.StringVar(&connType, "c", "ws", "Connection type (ws or tcp)")
	flag.StringVar(&connType, "conntype", "ws", "Connection type (ws or tcp)")

	flag.StringVar(&clientID, "id", "", "Client identifier")
	flag.StringVar(&clientID, "clientid", "", "Client identifier")
	flag.IntVar(&keepAlive, "k", 0, "Keep alive")
	flag.IntVar(&keepAlive, "keepalive", 0, "Keep alive")
	flag.BoolVar(&cleanStart, "cs", true, "Start clean - a new session is created in broker")
	flag.BoolVar(&cleanStart, "cleanstart", true, "Start clean - a new session is created in broker")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if !isFlagPassed("b") && !isFlagPassed("broker") {
		log.Fatal("Error: MQTTV5 broker address is not given")
		usage()
	}

	var conn mqtt.Connection
	conn = &mqtt.WebsocketConn{Host: broker}
	if connType == "tcp" {
		conn = &mqtt.TCPConn{Host: broker}
	}

	client := mqtt.NewClient(conn)
	mqttConnect := mqtt.Connect{KeepAlive: uint16(keepAlive), CleanStart: cleanStart, ClientID: clientID}

	connack, err := client.Connect(context.Background(), &mqttConnect)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Broker returned CONNACK - %v", connack)

	// Disconnect from broker
	client.Disconnect(context.Background(), &mqtt.Disconnect{ReasonCode: mqtt.DisconnectReasonCodeNormalDisconnect})
}
