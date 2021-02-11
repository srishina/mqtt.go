package mqtt

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// memstore packet heap store
type memstore struct {
	sync.RWMutex
	messages map[uint32]controlPacket
}

func (ms *memstore) Insert(key uint32, pkt controlPacket) error {
	ms.Lock()
	defer ms.Unlock()

	ms.messages[key] = pkt

	return nil
}

func (ms *memstore) GetByID(key uint32) controlPacket {
	ms.Lock()
	defer ms.Unlock()
	return ms.messages[key]
}

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

func (ms *memstore) DeleteAll() {
	ms.messages = make(map[uint32]controlPacket)
}

func (ms *memstore) CopyItems() []controlPacket {
	ms.Lock()
	defer ms.Unlock()
	packets := make([]controlPacket, 0)
	for _, v := range ms.messages {
		packets = append(packets, v)
	}
	return packets
}

func newMemStore() *memstore {
	return &memstore{messages: make(map[uint32]controlPacket)}
}
