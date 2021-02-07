package mqtt

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/srishina/mqtt.go/internal/mqttutil"
)

// WebsocketConn concrete implementation of Connection
// when used the MQTT client uses a WebSocket to
// connect to MQTT broker
type WebsocketConn struct {
	conn      *websocket.Conn
	Host      string
	TLSConfig *tls.Config
	rw        io.ReadWriter
}

// BrokerURL the broker URL
func (w *WebsocketConn) BrokerURL() string {
	return w.Host
}

// Connect connect to MQTT broker
func (w *WebsocketConn) Connect(ctx context.Context) (io.ReadWriter, error) {
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: false,
		TLSClientConfig:   w.TLSConfig,
		Subprotocols:      []string{"mqtt"},
	}
	ws, _, err := dialer.DialContext(ctx, w.Host, http.Header{})

	if err != nil {
		return nil, err
	}

	w.conn = ws
	w.rw = mqttutil.NewWebsocketReadWriter(ws)

	return w.rw, nil
}

// Close closes the connection
func (w *WebsocketConn) Close() {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
}
