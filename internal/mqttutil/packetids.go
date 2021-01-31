package mqttutil

import (
	"sync"
)

// PIDGenerator is 16 bit id generator as
// specified by the MQTT spec.
type PIDGenerator struct {
	sync.RWMutex
	index map[uint16]struct{}
}

const (
	pidMin uint16 = 1
	pidMax uint16 = 65535
)

func NewPIDGenerator() *PIDGenerator {
	return &PIDGenerator{index: make(map[uint16]struct{})}
}

func (pid *PIDGenerator) FreeID(id uint16) {
	pid.Lock()
	defer pid.Unlock()
	delete(pid.index, id)
}

func (pid *PIDGenerator) NextID() uint16 {
	pid.Lock()
	defer pid.Unlock()

	for i := pidMin; i < pidMax; i++ {
		if _, ok := pid.index[i]; !ok {
			pid.index[i] = struct{}{}
			return i
		}
	}
	return 0
}
