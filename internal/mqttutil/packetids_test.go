package mqttutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPacketIDsBasic(t *testing.T) {
	pidgen := NewPIDGenerator()

	assert.Equal(t, pidgen.NextID(), uint16(1))
	assert.Equal(t, pidgen.NextID(), uint16(2))
	pidgen.FreeID(uint16(2))
	assert.Equal(t, pidgen.NextID(), uint16(3))
	assert.Equal(t, pidgen.NextID(), uint16(4))
}

func TestExhaustPacketIDs(t *testing.T) {
	pidgen := NewPIDGenerator()
	for i := uint16(0); i < pidMax; i++ {
		pidgen.NextID()
	}
	assert.Equal(t, pidgen.NextID(), uint16(0))
	pidgen.FreeID(uint16(2))
	pidgen.FreeID(uint16(3))
	assert.Equal(t, pidgen.NextID(), uint16(2))
	assert.Equal(t, pidgen.NextID(), uint16(3))
	assert.Equal(t, pidgen.NextID(), uint16(0))
	pidgen.FreeID(uint16(65535))
	assert.Equal(t, pidgen.NextID(), uint16(65535))
}
