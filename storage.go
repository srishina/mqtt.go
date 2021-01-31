package mqtt

// Store ...
type Store interface {
	Insert(key uint32, pkt packet) error
	GetByID(key uint32) packet
	DeleteByID(key uint32)
}
