package mqtt

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/srishina/mqtt.go/internal/packettype"
)

type mqttMockTester struct {
	Connection
	svrConn                   net.Conn
	requests                  []packettype.PacketType
	responses                 map[packettype.PacketType]packet
	triggerPublishOnsubscribe bool
	publishAckd               bool
	disconnectAtPacketCount   int
	clientReadDeadline        time.Duration
	wg                        sync.WaitGroup
}

func (m *mqttMockTester) BrokerURL() string {
	return ""
}

// Connect just return the net.Conn as we already have a connected pipe
func (m *mqttMockTester) Connect(ctx context.Context) (io.ReadWriter, error) {
	conn, clientConn := net.Pipe()
	m.svrConn = conn
	// start the server
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
	}()

	if m.clientReadDeadline != 0 {
		clientConn.SetReadDeadline(time.Now().Add(m.clientReadDeadline))
	}

	return clientConn, nil
}

func (m *mqttMockTester) Close() {
	m.svrConn.Close()
	m.wg.Wait()
}

func (m *mqttMockTester) triggerPublish() {
	timer := time.NewTimer(0)
	go func() {
		<-timer.C
		if publishPkt, ok := m.responses[packettype.PUBLISH]; ok {
			publishPkt.(*Publish).packetID = 12
			if err := publishPkt.encode(m.svrConn); err != nil {
				log.Errorf("mock server - PUBLISH encoder error %v", err)
			}
		}
	}()
}

func (m *mqttMockTester) run() {
	recvdPacketCount := 0
	for {
		pkt, err := readFrom(m.svrConn)
		if err != nil {
			if err != io.ErrClosedPipe && err != io.EOF {
				fmt.Println("mock server error when reading packet ", err)
			}
			return
		}
		if m.disconnectAtPacketCount != 0 && recvdPacketCount == m.disconnectAtPacketCount {
			// Disconnect
			m.svrConn.Close()
			continue
		}
		recvdPacketCount++

		switch pkt.(type) {
		case *Connect:
			m.requests = append(m.requests, packettype.CONNECT)
			if respPkt, ok := m.responses[packettype.CONNACK]; ok {
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - CONNACK encoder error ", err)
				}
			}
		case *Subscribe:
			m.requests = append(m.requests, packettype.SUBSCRIBE)
			if respPkt, ok := m.responses[packettype.SUBACK]; ok {
				respPkt.(*SubAck).packetID = pkt.(*Subscribe).packetID
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - SUBACK encoder error ", err)
				}

				if m.triggerPublishOnsubscribe {
					m.triggerPublish()
				}
			}
		case *Unsubscribe:
			m.requests = append(m.requests, packettype.UNSUBSCRIBE)
			if respPkt, ok := m.responses[packettype.UNSUBACK]; ok {
				respPkt.(*UnsubAck).packetID = pkt.(*Unsubscribe).packetID
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - UNSUBACK encoder error ", err)
				}
			}
		case *pingReq:
			m.requests = append(m.requests, packettype.PINGREQ)
			if respPkt, ok := m.responses[packettype.PINGRESP]; ok {
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - PINGRESP encoder error ", err)
				}
			}
		case *Publish:
			m.requests = append(m.requests, packettype.PUBLISH)
			publishPkt := pkt.(*Publish)
			if publishPkt.QoSLevel == 1 {
				if respPkt, ok := m.responses[packettype.PUBACK]; ok {
					respPkt.(*PubAck).packetID = pkt.(*Publish).packetID
					if err := respPkt.encode(m.svrConn); err != nil {
						fmt.Println("mock server - PUBACK encoder error ", err)
					}
				}
			} else if publishPkt.QoSLevel == 2 {
				if respPkt, ok := m.responses[packettype.PUBREC]; ok {
					respPkt.(*PubRec).packetID = pkt.(*Publish).packetID
					if err := respPkt.encode(m.svrConn); err != nil {
						fmt.Println("mock server - PUBREC encoder error ", err)
					}
				}
			}
		case *PubRel:
			m.requests = append(m.requests, packettype.PUBREL)
			if respPkt, ok := m.responses[packettype.PUBCOMP]; ok {
				respPkt.(*PubComp).packetID = pkt.(*PubRel).packetID
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - PUBCOMP encoder error ", err)
				}
			}
		case *PubAck:
			m.requests = append(m.requests, packettype.PUBACK)
		case *PubRec:
			m.requests = append(m.requests, packettype.PUBREC)
			if respPkt, ok := m.responses[packettype.PUBREL]; ok {
				respPkt.(*PubRel).packetID = pkt.(*PubRec).packetID
				if err := respPkt.encode(m.svrConn); err != nil {
					fmt.Println("mock server - PUBREL encoder error ", err)
				}
			}
		case *PubComp:
			m.requests = append(m.requests, packettype.PUBCOMP)
		}
	}
}
