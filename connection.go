package mqtt

import (
	"context"
	"io"
)

// Connection represents a connection that the MQTT client uses.
// The implementation of the MQTTConnection is responsible for
// initialization of the connection(tcp, ws etc...) with the broker
type Connection interface {
	BrokerURL() string
	// Connect MQTT client calls Connect when it needs a io read writer.
	// If the Connect returns an error the MQTT client will not ask
	// for a read writer anymore.
	Connect(ctx context.Context) (io.ReadWriter, error)
	Close()
}

// ConnectionChangeNotifier provides an interface for connection changes such connected or
// disconnected
type ConnectionChangeNotifier interface {
	// notifies when a new connection has been established
	OnConnected()
	// notifies when a connection has been disconncted by a normal network drop
	// or due to a MQTT DISCONNECT initiated by the broker
	// Please check MQTTv5 spec for the reasons why the broker may send a DISCONNECT.
	// The err represents the reason for the disconnection
	OnDisconnected(err error)
}

// ConnectionWithChangeNotifier is the interface that groups the MQTT connection and listen functionalities
type ConnectionWithChangeNotifier interface {
	Connection
	ConnectionChangeNotifier
}
