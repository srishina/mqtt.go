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
	assert.Equal(t, pidgen.NextID(), uint16(2))
	assert.Equal(t, pidgen.NextID(), uint16(3))
}
