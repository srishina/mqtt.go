package mqtt

import (
	"context"
	"crypto/tls"
	"io"
	"net"
)

type tcpConn struct {
	Connection
	conn      net.Conn
	Host      string
	TLSConfig *tls.Config
	rw        io.ReadWriter
}

func (t *tcpConn) BrokerURL() string {
	return t.Host
}

func (t *tcpConn) Connect(ctx context.Context) (io.ReadWriter, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", t.Host)
	if err != nil {
		return nil, err
	}
	t.conn = conn
	return t.conn, nil
}

func (t *tcpConn) Close() {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
}
