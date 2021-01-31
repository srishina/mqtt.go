package mqtt

import (
	"io"

	"github.com/srishina/mqtt.go/internal/packettype"
)

// PingReq MQTT ping request control packet
type pingReq struct {
}

// encode encode MQTT PINGREQ packet
func (p *pingReq) encode(w io.Writer) error {
	encoded := []byte{0xC0, 0x00}
	_, err := w.Write(encoded)
	return err
}

// decode decode MQTT PINGREQ packet
func (p *pingReq) decode(r io.Reader, len uint32) error {
	return nil
}

// pingResp MQTT PINGRESP control packet
type pingResp struct {
}

// encode encode MQTT PINGRESP packet
func (p *pingResp) encode(w io.Writer) error {
	_, err := w.Write([]byte{byte(packettype.PINGRESP << 4), 0})
	return err
}

// decode decode MQTT PINGRESP packet
func (p *pingResp) decode(r io.Reader, len uint32) error {
	return nil
}
