package mqtt

import (
	"context"
	"crypto/tls"
	"io"
	"net"
)

// TCPConn concrete implementation of Connection
// when used the MQTT client uses a TCP to
// connect to MQTT broker
type TCPConn struct {
	conn      net.Conn
	Host      string
	TLSConfig *tls.Config
}

// BrokerURL the broker URL
func (t *TCPConn) BrokerURL() string {
	return t.Host
}

// Connect connect to MQTT broker
func (t *TCPConn) Connect(ctx context.Context) (io.ReadWriter, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", t.Host)
	if err != nil {
		return nil, err
	}
	t.conn = conn
	return t.conn, nil
}

// Close closes the connection
func (t *TCPConn) Close() {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
}
