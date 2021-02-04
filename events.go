package mqtt

const (
	ReconnectingEvent = "reconnecting"
	DisconnectedEvent = "disconnected"
	ReconnectedEvent  = "reconnected"
	ResubscribeEvent  = "resubscribe"
	LogEvent          = "log"
)

// ResubscribeResult result after resubscription, returns
// the Subscription ack (SubAck) and an error code. If there
// is an error then the subscription may have failed
type ResubscribeResult struct {
	Subscribe *Subscribe
	SubAck    *SubAck
	Error     error
}

type ReconnectingEventFn = func(str string)
type DisconnectedEventFn = func(err error)
type ReconnectedEventFn = func(connack *ConnAck)
type ResubscribeEventFn = func(resubscribeResult ResubscribeResult)
