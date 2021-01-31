package mqttutil

import "sync/atomic"

type SyncQueue struct {
	items chan interface{}
	len   int32
	cap   int
	stop  chan struct{}
}

func NewSyncQueue(maxCapacity int) *SyncQueue {
	return &SyncQueue{
		items: make(chan interface{}, maxCapacity),
		stop:  make(chan struct{}),
	}
}

func (s *SyncQueue) Close() {
	close(s.stop)
}

// Length returns the length of the queue
func (s *SyncQueue) Length() int {
	return int(atomic.LoadInt32(&s.len))
}

// IsEmpty checks whether the queue is empty
func (s *SyncQueue) IsEmpty() bool {
	return s.Length() == 0
}

// Push add an element into the queue
func (s *SyncQueue) Push(x interface{}) {
	s.items <- x
	atomic.AddInt32(&s.len, 1)
}

// Pop Returns the element
// The function waits till an item is available or the queue is closed.
func (s *SyncQueue) Pop() (bool, interface{}) {
	select {
	case item := <-s.items:
		atomic.AddInt32(&s.len, -1)
		return false, item
	case <-s.stop:
		return true, nil
	}
}
