package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadMessage reads a single PostgreSQL wire message from r.
func ReadMessage(r io.Reader) (*Message, error) {
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, err
	}
	msgType := typeBuf[0]

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	payloadLen := length - 4
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Message{
		Type:    msgType,
		Payload: payload,
	}, nil
}
