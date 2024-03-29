package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	mqtt "github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: basic-client [options]

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
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

	var opts []mqtt.ClientOption
	opts = append(opts, mqtt.WithCleanStart(cleanStart))
	opts = append(opts, mqtt.WithKeepAlive(uint16(keepAlive)))
	opts = append(opts, mqtt.WithClientID(clientID))

	client := mqtt.NewClient(conn, opts...)
	connack, err := client.Connect(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Broker returned CONNACK - %s", connack)

	if keepAlive != 0 {
		exitIn := keepAlive * 2
		fmt.Printf("\r- Will exit in %dsecs OR Press Ctrl+C to exit\n", exitIn)
		shutdownCh := make(chan struct{})
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			select {
			case <-time.After(time.Duration(exitIn) * time.Second):
			case <-c:
			}
			close(shutdownCh)
		}()
		<-shutdownCh
	}

	// Disconnect from broker
	client.Disconnect(context.Background(), mqtt.DisconnectReasonCodeNormal, nil)
}
