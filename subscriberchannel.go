package mqtt

import (
	"fmt"
	"sync"
	"time"
)

// MessageReceiver that allows receiving subscribed messages
type MessageReceiver struct {
	mu         sync.Mutex
	ch         chan *Publish
	backBuffer []*Publish
	closed     chan struct{}
}

// NewMessageReceiver new subscriber channel
func NewMessageReceiver() *MessageReceiver {
	return &MessageReceiver{
		ch:     make(chan *Publish, 1),
		closed: make(chan struct{}),
	}
}

func (m *MessageReceiver) Recv() (*Publish, error) {
	var element *Publish
	select {
	case element = <-m.ch:
	case <-m.closed:
		return nil, fmt.Errorf("channel is closed")
	}

	m.mu.Lock()
	m.shift()
	m.mu.Unlock()
	return element, nil
}

func (m *MessageReceiver) recvTimeout(timeoutMs uint) (*Publish, error) {
	var element *Publish
	select {
	case element = <-m.ch:
	case <-time.After(time.Duration(int64(timeoutMs) * time.MicroSeconds)):
		return nil, nil
	case <-m.closed:
		return nil, fmt.Errorf("channel is closed")
	}

	m.mu.Lock()
	m.shift()
	m.mu.Unlock()
	return element, nil
}


func (m *MessageReceiver) send(p *Publish) error {
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
