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

type websocketConn struct {
	ConnectionWithChangeNotifier
	conn           *websocket.Conn
	changeNotifier ConnectionChangeNotifier
	Host           string
	TLSConfig      *tls.Config
	rw             io.ReadWriter
}

func (w *websocketConn) BrokerURL() string {
	return w.Host
}

func (w *websocketConn) Connect(ctx context.Context) (io.ReadWriter, error) {
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

func (w *websocketConn) Close() {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
}

func (w *websocketConn) OnConnected() {
	if w.changeNotifier != nil {
		w.changeNotifier.OnConnected()
	}
}

func (w *websocketConn) OnDisconnected(err error) {
	if w.changeNotifier != nil {
		w.changeNotifier.OnDisconnected(err)
	}
}
