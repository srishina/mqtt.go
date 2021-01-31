package mqttutil

import (
	"io"

	"github.com/gorilla/websocket"
)

type websocketRW struct {
	*websocket.Conn
	r io.Reader
}

func NewWebsocketReadWriter(c *websocket.Conn) io.ReadWriter {
	return &websocketRW{Conn: c}
}

func (wsc *websocketRW) Read(p []byte) (int, error) {
	for {
		if wsc.r == nil {
			var err error
			_, wsc.r, err = wsc.NextReader()
			if err != nil {
				return 0, err
			}
		}
		n, err := wsc.r.Read(p)
		if err == io.EOF {
			wsc.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}

}

func (wsc *websocketRW) Write(p []byte) (int, error) {
	err := wsc.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

// import (
// 	"context"
// 	"crypto/tls"
// 	"io"
// 	"net/http"
// 	"time"

// 	"github.com/gorilla/websocket"
// )

// type WebsocketConn struct {
// 	*websocket.Conn
// 	r io.Reader
// }

// func NewWebsocketFromConn(c *websocket.Conn) *WebsocketConn {
// 	return &WebsocketConn{Conn: c}
// }

// func NewWebSocket(ctx context.Context, host string, tlsc *tls.Config, requestHeader http.Header) (*WebsocketConn, error) {
// 	dialer := &websocket.Dialer{
// 		Proxy:             http.ProxyFromEnvironment,
// 		HandshakeTimeout:  10 * time.Second,
// 		EnableCompression: false,
// 		TLSClientConfig:   tlsc,
// 		Subprotocols:      []string{"mqtt"},
// 	}
// 	ws, _, err := dialer.DialContext(ctx, host, requestHeader)

// 	if err != nil {
// 		return nil, err
// 	}
// 	return &WebsocketConn{Conn: ws}, nil
// }

// func (wsc *WebsocketConn) SetDeadline(t time.Time) error {
// 	if err := wsc.SetReadDeadline(t); err != nil {
// 		return err
// 	}

// 	return wsc.SetWriteDeadline(t)
// }

// func (wsc *WebsocketConn) Read(p []byte) (int, error) {

// 	for {
// 		if wsc.r == nil {
// 			// Advance to next message.
// 			var err error
// 			_, wsc.r, err = wsc.NextReader()
// 			if err != nil {
// 				return 0, err
// 			}
// 		}
// 		n, err := wsc.r.Read(p)
// 		if err == io.EOF {
// 			// At end of message.
// 			wsc.r = nil
// 			if n > 0 {
// 				return n, nil
// 			}
// 			// No data read, continue to next message.
// 			continue
// 		}
// 		return n, err
// 	}

// }

// func (wsc *WebsocketConn) Write(p []byte) (int, error) {
// 	err := wsc.WriteMessage(websocket.BinaryMessage, p)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return len(p), nil
// }
