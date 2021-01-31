package mqtt

import (
	"fmt"
	"sync"
)

// MessageReceiver that allows receiving subscribed messages
type MessageReceiver struct {
	mu         sync.Mutex
	ch         chan Message
	backBuffer []Message
	closed     chan struct{}
}

// NewMessageReceiver new subscriber channel
func NewMessageReceiver() *MessageReceiver {
	return &MessageReceiver{
		ch:     make(chan Message, 1),
		closed: make(chan struct{}),
	}
}

func (m *MessageReceiver) Recv() (Message, error) {
	var element Message
	select {
	case element = <-m.ch:
	case <-m.closed:
		return nil, fmt.Errorf("Channel is closed")
	}

	m.mu.Lock()
	m.shift()
	m.mu.Unlock()
	return element, nil
}

func (m *MessageReceiver) send(p Message) error {
	m.mu.Lock()
	m.backBuffer = append(m.backBuffer, p)
	m.shift()
	defer m.mu.Unlock()
	return nil
}

func (m *MessageReceiver) close() {
	close(m.closed)
}

func (m *MessageReceiver) shift() {
	if len(m.backBuffer) > 0 {
		select {
		case m.ch <- m.backBuffer[0]:
			m.backBuffer = m.backBuffer[1:]
		default:
		}
	}
}
