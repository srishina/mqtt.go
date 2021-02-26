package mqtt

import (
	"context"
	"io"
)

// Connection represents a connection that the MQTT client uses.
// The implementation of the Connection is responsible for
// initialization of the connection(tcp, ws etc...) with the broker.
// WebsocketConn, TCPConn is provided as part of the library, other
// connections can be written by the implementations
type Connection interface {
	BrokerURL() string
	// Connect MQTT client calls Connect when it needs a io read writer.
	// If the Connect returns an error during reconenct then the MQTT client will
	// attempt a reconnect again. The reconnect interval depends on backoff delay
	Connect(ctx context.Context) (io.ReadWriter, error)
	// Closes the network connection
	Close()
}
