package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/srishina/mqtt.go"
)

var usageStr = `
	Usage: basic-server [options]

	Options:
		-b, --broker <broker address> MQTTv5 broker address"
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
		brokerAddr    string
		topicAliasMax uint
	)

	flag.StringVar(&brokerAddr, "b", "", "MQTTv5 broker address")
	flag.StringVar(&brokerAddr, "broker", "", "MQTTv5 broker address")
	flag.UintVar(&topicAliasMax, "ta", 0, "Topic alias maximum")
	flag.UintVar(&topicAliasMax, "topic-alias-max", 0, "Topic alias maximum")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if !isFlagPassed("b") && !isFlagPassed("broker") {
		log.Fatal("Error: MQTTV5 broker address is not given")
		usage()
	}

	url, err := url.ParseRequestURI(brokerAddr)
	if err != nil {
		log.Fatal(err)
	}

	opts := []mqtt.BrokerOption{}
	opts = append(opts, mqtt.WithMaxTopicAlias(uint16(topicAliasMax)))
	broker := mqtt.NewBroker(url, opts...)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := broker.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("\r- Press Ctrl+C to exit")
	shutdownCh := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		close(shutdownCh)
	}()

	select {
	case <-shutdownCh:
		log.Println("\r- Ctrl+C pressed in Terminal, stopping the broker")
		break
	}

	broker.Stop()

	wg.Wait()
}
