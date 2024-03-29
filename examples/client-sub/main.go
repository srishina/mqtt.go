package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: client-sub [options] <topic-filter> <qos>

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
		-c, --conntype <connection type> Connection type - default: ws"
		-id, --clientid <client ID> Client identifier - optional"
		-k, --keepalive <keep alive> Keep alive - optional, default: 0"
		-cs, --cleanstart <Clean start> Start clean - a new session is created in broker - optional, default: true"
	
	example:
		client-sub -b ws://mqtt.eclipseprojects.io:80/mqtt "TEST/GREETING/#" 1
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
	if len(args) < 2 {
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

	var opts []mqtt.ClientOption
	opts = append(opts, mqtt.WithCleanStart(cleanStart))
	opts = append(opts, mqtt.WithKeepAlive(uint16(keepAlive)))
	opts = append(opts, mqtt.WithClientID(clientID))

	client := mqtt.NewClient(conn, opts...)
	_, err = client.Connect(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	topic, qos := args[0], args[1]
	qosLevel, err := strconv.ParseInt(qos, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	if qosLevel > 2 {
		log.Fatal("Maximum QoS must be 2")
	}

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
	subscriptions = append(subscriptions, &mqtt.Subscription{TopicFilter: topic, QoSLevel: byte(qosLevel)})

	suback, err := client.Subscribe(context.Background(), subscriptions, nil, recvr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("SUBACK: %v\n", suback)

	fmt.Println("\r- Press Ctrl+C to exit")
	shutdownCh := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		close(shutdownCh)
	}()

	<-shutdownCh
	log.Println("\r- Ctrl+C pressed in Terminal, unsubscribing")

	func() {
		withTimeOut, cancelFn := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		defer cancelFn()
		_, err = client.Unsubscribe(withTimeOut, []string{topic}, nil)
		if err != nil {
			log.Println("UNSUBSCRIBE returned error ", err)
		}
	}()

	// Disconnect from broker
	client.Disconnect(context.Background(), mqtt.DisconnectReasonCodeNormal, nil)

	wg.Wait()
}
