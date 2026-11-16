package pgwire

import (
	"io"
)

// WriteMessage serializes a Message to w.
func WriteMessage(w io.Writer, msg *Message) error {
	return msg.Encode(w)
}

// WriteParseComplete sends '1' with length 4.
func WriteParseComplete(w io.Writer) error {
	msg := &Message{Type: MsgParseComplete}
	return msg.Encode(w)
}

// WriteBindComplete sends '2' with length 4.
func WriteBindComplete(w io.Writer) error {
	msg := &Message{Type: MsgBindComplete}
	return msg.Encode(w)
}

// WriteCloseComplete sends '3' with length 4.
func WriteCloseComplete(w io.Writer) error {
	msg := &Message{Type: MsgCloseComplete}
	return msg.Encode(w)
}

// WriteReadyForQuery sends 'Z' with transaction status.
func WriteReadyForQuery(w io.Writer, status byte) error {
	msg := &Message{
		Type:    MsgReadyForQuery,
		Payload: []byte{status},
	}
	return msg.Encode(w)
}
