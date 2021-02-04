package mqtt

import (
	"testing"
	"time"
)

func TestEventEmitter(t *testing.T) {
	emitter := newEventEmitter()
	go emitter.run()

	done := make(chan struct{})
	maxCalls := 12
	nRecvdEvents := 0
	emitter.on(ReconnectingEvent, func(str string) {
		nRecvdEvents++
		if nRecvdEvents == maxCalls {
			close(done)
		}
	})

	go func() {
		for i := 0; i < maxCalls; i++ {
			emitter.emit(ReconnectingEvent, "I am reconnecting!")
		}
	}()

	select {
	case <-time.After(1 * time.Second):
		t.Fail()
	case <-done:
	}
	emitter.close()
}
