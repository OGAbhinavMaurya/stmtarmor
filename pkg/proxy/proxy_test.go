package proxy

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/pgwire"
)

func TestProxy_EndToEnd(t *testing.T) {
	// 1. Mock PostgreSQL backend server
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen backend: %v", err)
	}
	defer backendLn.Close()

	var backendReceivedTypes []byte
	go func() {
		conn, err := backendLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read Startup packet (4-byte length + payload)
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lenBuf)
		if length > 4 {
			payload := make([]byte, length-4)
			_, _ = io.ReadFull(conn, payload)
		}

		// Send ReadyForQuery to client
		_ = pgwire.WriteReadyForQuery(conn, 'I')

		// Read commands
		for {
			msg, err := pgwire.ReadMessage(conn)
			if err != nil {
				return
			}
			backendReceivedTypes = append(backendReceivedTypes, msg.Type)

			switch msg.Type {
			case pgwire.MsgParse:
				_ = pgwire.WriteParseComplete(conn)
			case pgwire.MsgBind:
				_ = pgwire.WriteBindComplete(conn)
			case pgwire.MsgSync:
				_ = pgwire.WriteReadyForQuery(conn, 'I')
			}
		}
	}()

	// 2. Start StmtArmor proxy
	proxy := New(Config{
		ListenAddr: "127.0.0.1:0",
		TargetAddr: backendLn.Addr().String(),
	})
	go func() {
		_ = proxy.Start()
	}()
	defer func() {
		_ = proxy.Close()
	}()

	time.Sleep(50 * time.Millisecond)

	// 3. Connect client to StmtArmor proxy
	clientConn, err := net.Dial("tcp", proxy.Addr())
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer clientConn.Close()

	// Send startup packet
	startupPacket := []byte{0x00, 0x00, 0x00, 0x08, 0x00, 0x03, 0x00, 0x00}
	if _, err := clientConn.Write(startupPacket); err != nil {
		t.Fatalf("failed to write startup packet: %v", err)
	}

	// Read ReadyForQuery
	readyMsg, err := pgwire.ReadMessage(clientConn)
	if err != nil || readyMsg.Type != pgwire.MsgReadyForQuery {
		t.Fatalf("expected ReadyForQuery, got %v, err: %v", readyMsg, err)
	}

	// 4. Send Parse "s0"
	parseMsg := &pgwire.ParseMsg{
		Statement: "s0",
		Query:     "SELECT * FROM users WHERE id = $1",
		ParamOIDs: []uint32{23},
	}
	if err := parseMsg.Encode().Encode(clientConn); err != nil {
		t.Fatalf("failed to send Parse: %v", err)
	}

	parseComplete, err := pgwire.ReadMessage(clientConn)
	if err != nil || parseComplete.Type != pgwire.MsgParseComplete {
		t.Fatalf("expected ParseComplete, got: %v, err: %v", parseComplete, err)
	}

	// 5. Send second Parse of identical query (SHOULD BE SUPPRESSED BY PROXY!)
	if err := parseMsg.Encode().Encode(clientConn); err != nil {
		t.Fatalf("failed to send second Parse: %v", err)
	}
	parseComplete2, err := pgwire.ReadMessage(clientConn)
	if err != nil || parseComplete2.Type != pgwire.MsgParseComplete {
		t.Fatalf("expected synthetic ParseComplete, got: %v, err: %v", parseComplete2, err)
	}

	// 6. Send Bind
	bindMsg := &pgwire.BindMsg{
		Portal:    "",
		Statement: "s0",
	}
	if err := bindMsg.Encode().Encode(clientConn); err != nil {
		t.Fatalf("failed to send Bind: %v", err)
	}
	bindComplete, err := pgwire.ReadMessage(clientConn)
	if err != nil || bindComplete.Type != pgwire.MsgBindComplete {
		t.Fatalf("expected BindComplete, got: %v, err: %v", bindComplete, err)
	}

	metrics := proxy.GetMetrics()
	if metrics.ParsesTotal != 2 {
		t.Errorf("expected 2 parses total, got %d", metrics.ParsesTotal)
	}
	if metrics.ParsesSuppressed != 1 {
		t.Errorf("expected 1 parse suppressed, got %d", metrics.ParsesSuppressed)
	}
}
