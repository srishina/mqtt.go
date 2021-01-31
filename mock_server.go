package mqtt

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/srishina/mqtt.go/internal/packettype"
)

type mockClientConn struct {
	ConnectionWithChangeNotifier
	changeNotifier ConnectionChangeNotifier
	conn           net.Conn
}

func (m *mockClientConn) BrokerURL() string {
	return ""
}

// Connect just return the net.Conn as we already have a connected pipe
func (m *mockClientConn) Connect(ctx context.Context) (io.ReadWriter, error) {
	return m.conn, nil
}

func (m *mockClientConn) Close() {
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}
}

func (m *mockClientConn) OnConnected() {
	if m.changeNotifier != nil {
		m.changeNotifier.OnConnected()
	}
}

func (m *mockClientConn) OnDisconnected(err error) {
	if m.changeNotifier != nil {
		m.changeNotifier.OnDisconnected(err)
	}
}

type mockServer struct {
	conn       net.Conn
	clientConn net.Conn
	requests   []packettype.PacketType
	responses  map[packettype.PacketType]packet
	done       chan struct{}
}

func newMockServer() *mockServer {
	conn, clientConn := net.Pipe()
	m := &mockServer{
		conn:       conn,
		clientConn: clientConn,
		done:       make(chan struct{}),
	}

	return m
}

func (m *mockServer) stop() {
	close(m.done)
	m.conn.Close()
}

func (m *mockServer) run() {
	for {
		select {
		case <-m.done:
			return
		default:
			pkt, err := readFrom(m.conn)
			if err != nil && err != io.EOF {
				fmt.Println("mock server error when reading packet ", err)
				return
			}
			switch pkt.(type) {
			case *Connect:
				m.requests = append(m.requests, packettype.CONNECT)
				if respPkt, ok := m.responses[packettype.CONNACK]; ok {
					if err := respPkt.encode(m.conn); err != nil {
						fmt.Println("mock server - CONNACK encoder error ", err)
					}
				}
			case *Subscribe:
				m.requests = append(m.requests, packettype.SUBSCRIBE)
				if respPkt, ok := m.responses[packettype.SUBACK]; ok {
					respPkt.(*SubAck).packetID = pkt.(*Subscribe).packetID
					if err := respPkt.encode(m.conn); err != nil {
						fmt.Println("mock server - SUBACK encoder error ", err)
					}
				}
			case *Unsubscribe:
				m.requests = append(m.requests, packettype.UNSUBSCRIBE)
				if respPkt, ok := m.responses[packettype.UNSUBACK]; ok {
					respPkt.(*UnsubAck).packetID = pkt.(*Unsubscribe).packetID
					if err := respPkt.encode(m.conn); err != nil {
						fmt.Println("mock server - UNSUBACK encoder error ", err)
					}
				}
			case *pingReq:
				m.requests = append(m.requests, packettype.PINGREQ)
				if respPkt, ok := m.responses[packettype.PINGRESP]; ok {
					if err := respPkt.encode(m.conn); err != nil {
						fmt.Println("mock server - PINGRESP encoder error ", err)
					}
				}
			}
		}
	}
}
