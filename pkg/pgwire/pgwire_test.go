package pgwire

import (
	"bytes"
	"reflect"
	"testing"
)

func TestParseMessage_RoundTrip(t *testing.T) {
	orig := &ParseMsg{
		Statement: "s0",
		Query:     "SELECT * FROM users WHERE id = $1 AND role = $2",
		ParamOIDs: []uint32{23, 25},
	}

	msg := orig.Encode()
	if msg.Type != MsgParse {
		t.Fatalf("expected MsgParse, got %c", msg.Type)
	}

	var buf bytes.Buffer
	if err := msg.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decodedMsg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	decoded, err := DecodeParse(decodedMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeParse failed: %v", err)
	}

	if decoded.Statement != orig.Statement {
		t.Errorf("expected stmt %s, got %s", orig.Statement, decoded.Statement)
	}
	if decoded.Query != orig.Query {
		t.Errorf("expected query %s, got %s", orig.Query, decoded.Query)
	}
	if !reflect.DeepEqual(decoded.ParamOIDs, orig.ParamOIDs) {
		t.Errorf("expected oids %v, got %v", orig.ParamOIDs, decoded.ParamOIDs)
	}
}

func TestBindMessage_RoundTrip(t *testing.T) {
	orig := &BindMsg{
		Portal:            "",
		Statement:         "s0",
		ParamFormatCodes:  []int16{0, 1},
		ParamValues:       [][]byte{[]byte("123"), nil, []byte("admin")},
		ResultFormatCodes: []int16{0},
	}

	msg := orig.Encode()
	if msg.Type != MsgBind {
		t.Fatalf("expected MsgBind, got %c", msg.Type)
	}

	decoded, err := DecodeBind(msg.Payload)
	if err != nil {
		t.Fatalf("DecodeBind failed: %v", err)
	}

	if decoded.Statement != orig.Statement {
		t.Errorf("expected stmt %s, got %s", orig.Statement, decoded.Statement)
	}
	if len(decoded.ParamValues) != 3 {
		t.Fatalf("expected 3 param values, got %d", len(decoded.ParamValues))
	}
	if decoded.ParamValues[1] != nil {
		t.Errorf("expected param 1 to be nil (NULL)")
	}
	if string(decoded.ParamValues[0]) != "123" || string(decoded.ParamValues[2]) != "admin" {
		t.Errorf("param values mismatch")
	}
}

func TestDescribeAndExecute_RoundTrip(t *testing.T) {
	desc := &DescribeMsg{
		TargetType: 'S',
		Name:       "stmt_1",
	}
	dMsg := desc.Encode()
	dDecoded, err := DecodeDescribe(dMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeDescribe failed: %v", err)
	}
	if dDecoded.TargetType != 'S' || dDecoded.Name != "stmt_1" {
		t.Errorf("unexpected describe: %+v", dDecoded)
	}

	exec := &ExecuteMsg{
		Portal:  "p1",
		MaxRows: 100,
	}
	eMsg := exec.Encode()
	eDecoded, err := DecodeExecute(eMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeExecute failed: %v", err)
	}
	if eDecoded.Portal != "p1" || eDecoded.MaxRows != 100 {
		t.Errorf("unexpected execute: %+v", eDecoded)
	}
}

func TestErrorResponse_Decoding(t *testing.T) {
	// Construct ErrorResponse for SQLSTATE 26000
	var payload bytes.Buffer
	payload.WriteByte('S')
	payload.WriteString("ERROR\x00")
	payload.WriteByte('C')
	payload.WriteString("26000\x00")
	payload.WriteByte('M')
	payload.WriteString("prepared statement \"s0\" does not exist\x00")
	payload.WriteByte(0) // terminator

	errResp := DecodeErrorResponse(payload.Bytes())
	if errResp.Code != "26000" {
		t.Errorf("expected code 26000, got %s", errResp.Code)
	}
	if errResp.Severity != "ERROR" {
		t.Errorf("expected severity ERROR, got %s", errResp.Severity)
	}
}
