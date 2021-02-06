package mqttutil

import (
	"sync"
)

// PIDGenerator is 16 bit id generator as
// specified by the MQTT spec.
type PIDGenerator struct {
	sync.RWMutex
	index   map[uint16]struct{}
	lastPID uint16
}

const (
	pidMin uint16 = 1
	pidMax uint16 = 65535
)

// NewPIDGenerator the PID generator
func NewPIDGenerator() *PIDGenerator {
	return &PIDGenerator{index: make(map[uint16]struct{})}
}

// FreeID free the id
func (pid *PIDGenerator) FreeID(id uint16) {
	pid.Lock()
	defer pid.Unlock()
	delete(pid.index, id)
}

// NextID returns the next available ID, if not available
// returns 0
func (pid *PIDGenerator) NextID() uint16 {
	pid.Lock()
	defer pid.Unlock()

	for {
		if len(pid.index) >= int(pidMax) {
			return 0
		}
		pid.lastPID++
		if _, ok := pid.index[pid.lastPID]; ok {
			// in use
			continue
		}
		id := pid.lastPID
		if pid.lastPID == pidMax {
			pid.lastPID = 0
		}
		pid.index[id] = struct{}{}
		return id
	}
}
