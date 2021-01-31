package mqtt

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// memstore packet heap store
type memstore struct {
	sync.RWMutex
	messages map[uint32]packet
}

// Put ..
func (ms *memstore) Insert(key uint32, pkt packet) error {
	ms.Lock()
	defer ms.Unlock()

	ms.messages[key] = pkt

	return nil
}

// GetByID ..
func (ms *memstore) GetByID(key uint32) packet {
	ms.Lock()
	defer ms.Unlock()
	return ms.messages[key]
}

// Remove ..
func (ms *memstore) DeleteByID(key uint32) {
	ms.Lock()
	defer ms.Unlock()
	val := ms.messages[key]
	if val == nil {
		log.Warnf("message key(%d) not found", key)
	} else {
		delete(ms.messages, key)
	}
}

// newMemStore ...
func newMemStore() *memstore {
	return &memstore{messages: make(map[uint32]packet)}
}
