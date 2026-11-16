package pgwire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Message types in PostgreSQL 3.0 protocol
const (
	MsgParse           byte = 'P'
	MsgBind            byte = 'B'
	MsgDescribe        byte = 'D'
	MsgExecute         byte = 'E'
	MsgSync            byte = 'S'
	MsgFlush           byte = 'H'
	MsgClose           byte = 'C'
	MsgQuery           byte = 'Q'
	MsgParseComplete   byte = '1'
	MsgBindComplete    byte = '2'
	MsgCloseComplete   byte = '3'
	MsgCommandComplete byte = 'C'
	MsgRowDescription  byte = 'T'
	MsgDataRow         byte = 'D'
	MsgReadyForQuery   byte = 'Z'
	MsgErrorResponse   byte = 'E'
	MsgNoticeResponse  byte = 'N'
	MsgNoData          byte = 'n'
	MsgParameterStatus byte = 'S'
	MsgBackendKeyData  byte = 'K'
)

// Message is a raw PostgreSQL wire protocol message.
type Message struct {
	Type    byte
	Payload []byte
}

// Length returns the length field value (payload len + 4).
func (m *Message) Length() int32 {
	return int32(len(m.Payload) + 4)
}

// Encode writes the wire representation of the message to w.
func (m *Message) Encode(w io.Writer) error {
	header := make([]byte, 5)
	header[0] = m.Type
	binary.BigEndian.PutUint32(header[1:5], uint32(m.Length()))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(m.Payload) > 0 {
		if _, err := w.Write(m.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Bytes returns the complete wire representation of the message.
func (m *Message) Bytes() []byte {
	var buf bytes.Buffer
	_ = m.Encode(&buf)
	return buf.Bytes()
}

// ParseMsg represents a parsed 'P' message.
type ParseMsg struct {
	Statement  string
	Query      string
	ParamOIDs  []uint32
}

// Encode serializes ParseMsg into a Message.
func (p *ParseMsg) Encode() *Message {
	var buf bytes.Buffer
	buf.WriteString(p.Statement)
	buf.WriteByte(0)
	buf.WriteString(p.Query)
	buf.WriteByte(0)

	_ = binary.Write(&buf, binary.BigEndian, int16(len(p.ParamOIDs)))
	for _, oid := range p.ParamOIDs {
		_ = binary.Write(&buf, binary.BigEndian, oid)
	}

	return &Message{
		Type:    MsgParse,
		Payload: buf.Bytes(),
	}
}

// DecodeParse unpacks a Parse message payload.
func DecodeParse(payload []byte) (*ParseMsg, error) {
	r := bytes.NewReader(payload)
	stmt, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("read stmt: %w", err)
	}
	query, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("read query: %w", err)
	}

	var numParams int16
	if err := binary.Read(r, binary.BigEndian, &numParams); err != nil {
		return nil, fmt.Errorf("read numParams: %w", err)
	}

	oids := make([]uint32, numParams)
	for i := 0; i < int(numParams); i++ {
		if err := binary.Read(r, binary.BigEndian, &oids[i]); err != nil {
			return nil, fmt.Errorf("read param oid %d: %w", i, err)
		}
	}

	return &ParseMsg{
		Statement: stmt,
		Query:     query,
		ParamOIDs: oids,
	}, nil
}

// BindMsg represents a parsed 'B' message.
type BindMsg struct {
	Portal            string
	Statement         string
	ParamFormatCodes  []int16
	ParamValues       [][]byte // nil indicates NULL
	ResultFormatCodes []int16
}

// Encode serializes BindMsg into a Message.
func (b *BindMsg) Encode() *Message {
	var buf bytes.Buffer
	buf.WriteString(b.Portal)
	buf.WriteByte(0)
	buf.WriteString(b.Statement)
	buf.WriteByte(0)

	_ = binary.Write(&buf, binary.BigEndian, int16(len(b.ParamFormatCodes)))
	for _, fc := range b.ParamFormatCodes {
		_ = binary.Write(&buf, binary.BigEndian, fc)
	}

	_ = binary.Write(&buf, binary.BigEndian, int16(len(b.ParamValues)))
	for _, val := range b.ParamValues {
		if val == nil {
			_ = binary.Write(&buf, binary.BigEndian, int32(-1))
		} else {
			_ = binary.Write(&buf, binary.BigEndian, int32(len(val)))
			buf.Write(val)
		}
	}

	_ = binary.Write(&buf, binary.BigEndian, int16(len(b.ResultFormatCodes)))
	for _, rfc := range b.ResultFormatCodes {
		_ = binary.Write(&buf, binary.BigEndian, rfc)
	}

	return &Message{
		Type:    MsgBind,
		Payload: buf.Bytes(),
	}
}

// DecodeBind unpacks a Bind message payload.
func DecodeBind(payload []byte) (*BindMsg, error) {
	r := bytes.NewReader(payload)
	portal, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("read portal: %w", err)
	}
	stmt, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("read stmt: %w", err)
	}

	var numParamFormats int16
	if err := binary.Read(r, binary.BigEndian, &numParamFormats); err != nil {
		return nil, fmt.Errorf("read numParamFormats: %w", err)
	}
	paramFormats := make([]int16, numParamFormats)
	for i := 0; i < int(numParamFormats); i++ {
		if err := binary.Read(r, binary.BigEndian, &paramFormats[i]); err != nil {
			return nil, err
		}
	}

	var numParamValues int16
	if err := binary.Read(r, binary.BigEndian, &numParamValues); err != nil {
		return nil, fmt.Errorf("read numParamValues: %w", err)
	}
	paramValues := make([][]byte, numParamValues)
	for i := 0; i < int(numParamValues); i++ {
		var valLen int32
		if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}
		if valLen == -1 {
			paramValues[i] = nil
		} else {
			val := make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return nil, err
			}
			paramValues[i] = val
		}
	}

	var numResultFormats int16
	if err := binary.Read(r, binary.BigEndian, &numResultFormats); err != nil {
		return nil, fmt.Errorf("read numResultFormats: %w", err)
	}
	resultFormats := make([]int16, numResultFormats)
	for i := 0; i < int(numResultFormats); i++ {
		if err := binary.Read(r, binary.BigEndian, &resultFormats[i]); err != nil {
			return nil, err
		}
	}

	return &BindMsg{
		Portal:            portal,
		Statement:         stmt,
		ParamFormatCodes:  paramFormats,
		ParamValues:       paramValues,
		ResultFormatCodes: resultFormats,
	}, nil
}

// DescribeMsg represents a 'D' message.
type DescribeMsg struct {
	TargetType byte // 'S' for statement, 'P' for portal
	Name       string
}

// Encode serializes DescribeMsg into a Message.
func (d *DescribeMsg) Encode() *Message {
	var buf bytes.Buffer
	buf.WriteByte(d.TargetType)
	buf.WriteString(d.Name)
	buf.WriteByte(0)

	return &Message{
		Type:    MsgDescribe,
		Payload: buf.Bytes(),
	}
}

// DecodeDescribe unpacks Describe payload.
func DecodeDescribe(payload []byte) (*DescribeMsg, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("describe payload too short: %d", len(payload))
	}
	targetType := payload[0]
	r := bytes.NewReader(payload[1:])
	name, err := readCString(r)
	if err != nil {
		return nil, err
	}
	return &DescribeMsg{
		TargetType: targetType,
		Name:       name,
	}, nil
}

// ExecuteMsg represents an 'E' message.
type ExecuteMsg struct {
	Portal  string
	MaxRows int32
}

// Encode serializes ExecuteMsg.
func (e *ExecuteMsg) Encode() *Message {
	var buf bytes.Buffer
	buf.WriteString(e.Portal)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, e.MaxRows)
	return &Message{
		Type:    MsgExecute,
		Payload: buf.Bytes(),
	}
}

// DecodeExecute unpacks Execute payload.
func DecodeExecute(payload []byte) (*ExecuteMsg, error) {
	r := bytes.NewReader(payload)
	portal, err := readCString(r)
	if err != nil {
		return nil, err
	}
	var maxRows int32
	if err := binary.Read(r, binary.BigEndian, &maxRows); err != nil {
		return nil, err
	}
	return &ExecuteMsg{
		Portal:  portal,
		MaxRows: maxRows,
	}, nil
}

// ErrorResponseMsg captures code and error string from ErrorResponse ('E').
type ErrorResponseMsg struct {
	Severity string
	Code     string // e.g. "26000", "42P05"
	Message  string
}

// DecodeErrorResponse extracts severity, SQLSTATE code, and message.
func DecodeErrorResponse(payload []byte) *ErrorResponseMsg {
	res := &ErrorResponseMsg{}
	r := bytes.NewReader(payload)
	for {
		b, err := r.ReadByte()
		if err != nil || b == 0 {
			break
		}
		str, err := readCString(r)
		if err != nil {
			break
		}
		switch b {
		case 'S':
			res.Severity = str
		case 'C':
			res.Code = str
		case 'M':
			res.Message = str
		}
	}
	return res
}

func readCString(r *bytes.Reader) (string, error) {
	var b []byte
	for {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == 0 {
			break
		}
		b = append(b, c)
	}
	return string(b), nil
}
