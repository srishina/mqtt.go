package mqtt

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

type event struct {
	name  string
	value interface{}
}

type eventQueue struct {
	mu   sync.Mutex
	out  chan *event
	data []*event
}

func newEventQueue() *eventQueue {
	return &eventQueue{out: make(chan *event, 1)}
}

func (e *eventQueue) close() {
	// maybe should use sync.Once
	close(e.out)
}

// push adds an item to the queue
func (e *eventQueue) push(item *event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case e.out <- item:
	default:
		e.data = append(e.data, item)
	}
	e.shift()
}

// shift moves the next available item from the queue into the out channel
// the out channel value is returned to the user. must be locked by the caller
func (e *eventQueue) shift() {
	if len(e.data) > 0 {
		select {
		case e.out <- e.data[0]:
			e.data = e.data[1:]
		default:
		}
	}
}

// pop returns the element and the status of the queue (closed or not)
func (e *eventQueue) pop() (*event, bool) {
	item, ok := <-e.out
	if ok {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.shift()
		return item, false
	}

	return nil, true
}

// EventEmitter listens to a named event and triggers a callback when that event occurs
// The events are emitted as it occurs
type EventEmitter interface {
	On(eventName string, callback interface{}) error
	Off(eventName string, callback interface{}) error
}

type eventEmitter struct {
	eventq    *eventQueue
	wg        sync.WaitGroup
	mu        sync.Mutex
	listeners map[string]*[]interface{}
	stop      chan struct{}
}

func newEventEmitter() *eventEmitter {
	return &eventEmitter{
		eventq:    newEventQueue(),
		listeners: make(map[string]*[]interface{}),
		stop:      make(chan struct{}),
	}
}

func (e *eventEmitter) close() {
	if e.eventq != nil {
		e.eventq.close()
	}
	e.wg.Wait()
	e.eventq = nil
}

func (e *eventEmitter) on(eventName string, callback interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	listeners := e.listeners[eventName]
	if listeners == nil {
		l := make([]interface{}, 0)
		e.listeners[eventName], listeners = &l, &l
	}
	*listeners = append(*listeners, callback)
	return nil
}

func (e *eventEmitter) off(eventName string, callback interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	listeners := e.listeners[eventName]
	if listeners != nil {
		for i, val := range *listeners {
			if val == callback {
				len := len(*listeners)
				if i != len-1 {
					// Remove the element at index i from listeners.
					(*listeners)[i] = (*listeners)[len-1] // Copy last element to index i.
					(*listeners)[len-1] = nil             // Erase last element (write nil value).
				}
				(*listeners) = (*listeners)[:len-1] // Truncate slice.
			}
		}
	}
	return nil
}

func (e *eventEmitter) emit(eventName string, value interface{}) {
	e.eventq.push(&event{name: eventName, value: value})
}

func (e *eventEmitter) run() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			item, closed := e.eventq.pop()
			if closed {
				// quueue is closed
				return
			}

			listeners := e.listeners[item.name]
			if listeners != nil {
				// this could be potentially done using reflect, but for now keep it simple
				// as we don't have too many events and not writing a generic solution
				switch item.name {
				case ReconnectingEvent:
					for _, l := range *listeners {
						if fn, ok := l.(ReconnectingEventFn); ok {
							fn(item.value.(string))
						}
					}
				case DisconnectedEvent:
					for _, l := range *listeners {
						if fn, ok := l.(DisconnectedEventFn); ok {
							fn(item.value.(error))
						}
					}
				case ReconnectedEvent:
					for _, l := range *listeners {
						if fn, ok := l.(ReconnectedEventFn); ok {
							fn(item.value.(*ConnAck))
						}
					}
				case ResubscribeEvent:
					for _, l := range *listeners {
						if fn, ok := l.(ResubscribeEventFn); ok {
							fn(item.value.(ResubscribeResult))
						}
					}
				default:
					log.Errorf("Received invalid event, name: %s", item.name)
				}
			}
		}
	}()
}
