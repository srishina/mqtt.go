package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: client-sub [options] <will-topic> <will-qos> <will-payload> <topic-filter> <qos>

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
		-id, --clientid <client ID> Client identifier - optional"
		-k, --keepalive <keep alive> Keep alive - optional, default: 0"
		-cs, --cleanstart <Clean start> Start clean - a new session is created in broker - optional, default: true"
	
	example:
		client-will-msg -b ws://mqtt.eclipseprojects.io:80/mqtt "TEST/GREETING/WILL" 1 "The Will message" "TEST/GREETING/#" 1
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
		broker            string
		clientID          string
		keepAlive         int
		cleanStart        bool
		willDelayInterval int
	)

	flag.StringVar(&broker, "b", "", "MQTTv5 broker address")
	flag.StringVar(&broker, "broker", "", "MQTTv5 broker address")
	flag.StringVar(&clientID, "id", "", "Client identifier")
	flag.StringVar(&clientID, "clientid", "", "Client identifier")
	flag.IntVar(&keepAlive, "k", 0, "Keep alive")
	flag.IntVar(&keepAlive, "keepalive", 0, "Keep alive")
	flag.BoolVar(&cleanStart, "cs", true, "Start clean - a new session is created in broker")
	flag.BoolVar(&cleanStart, "cleanstart", true, "Start clean - a new session is created in broker")
	flag.IntVar(&willDelayInterval, "wi", 0, "Will delay interval")
	flag.IntVar(&willDelayInterval, "will-delay-interval", 0, "Will delay interval")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 5 {
		usage()
	}

	if !isFlagPassed("b") && !isFlagPassed("broker") {
		log.Println("Error: MQTTV5 broker address is not given")
		usage()
	}

	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
	}

	var conn mqtt.Connection
	var willConn mqtt.Connection
	switch u.Scheme {
	case "ws":
		fallthrough
	case "wss":
		conn = &mqtt.WebsocketConn{Host: broker}
		willConn = &mqtt.WebsocketConn{Host: broker}
	case "tcp":
		conn = &mqtt.TCPConn{Host: u.Host}
		willConn = &mqtt.TCPConn{Host: u.Host}
	default:
		log.Fatal("Invalid scheme name")
	}

	willQoSLevel, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	var willClientOpts []mqtt.ClientOption
	willClientOpts = append(willClientOpts, mqtt.WithCleanStart(cleanStart))
	willClientOpts = append(willClientOpts, mqtt.WithKeepAlive(uint16(keepAlive)))
	willClientOpts = append(willClientOpts, mqtt.WithClientID(clientID))
	willMsg := &mqtt.WillMessage{
		QoS:     byte(willQoSLevel),
		Topic:   args[0],
		Payload: []byte(args[2]),
	}
	if willDelayInterval > 0 {
		interval := uint32(willDelayInterval)
		willMsg.Properties = &mqtt.WillProperties{WillDelayInterval: &interval}
	}
	willClientOpts = append(willClientOpts, mqtt.WithWillMessage(willMsg))
	willClient := mqtt.NewClient(willConn, willClientOpts...)

	_, err = willClient.Connect(context.Background())
	if err != nil {
		log.Fatal(err)
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

	topic, qos := args[3], args[4]
	qosLevel, err := strconv.ParseInt(qos, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	if qosLevel > 2 {
		log.Fatal("Maximum QoS must be 2")
	}

	willMsgRecvd := make(chan struct{})
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
			log.Printf("Will message - Topic: %s QoS: %d Payload: %v\n", p.TopicName, p.QoSLevel, string(p.Payload))
			willMsgRecvd <- struct{}{}
			break
		}
	}()
	// subscribe
	subscriptions := []*mqtt.Subscription{}
	subscriptions = append(subscriptions, &mqtt.Subscription{TopicFilter: topic, QoSLevel: byte(qosLevel)})

	_, err = client.Subscribe(context.Background(), subscriptions, nil, recvr)
	if err != nil {
		log.Fatal(err)
	}

	// Disconnect the Will client
	willClient.Disconnect(context.Background(), mqtt.DisconnectReasonCodeWithWillMessage, nil)
	log.Printf("- Waiting for Will message, we should receive will message in %d secs\n", willDelayInterval)

	select {
	case <-willMsgRecvd:
		log.Println("\r- Will message received, unsubscribing")
		break
	}

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
