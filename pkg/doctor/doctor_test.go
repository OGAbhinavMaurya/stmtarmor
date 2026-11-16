package doctor

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/pgwire"
)

func TestDoctor_Probe_Protected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read startup packet
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err == nil {
			length := binary.BigEndian.Uint32(lenBuf)
			if length > 4 {
				payload := make([]byte, length-4)
				_, _ = io.ReadFull(conn, payload)
			}
		}

		// Send ReadyForQuery
		_ = pgwire.WriteReadyForQuery(conn, 'I')

		// Read parse and sync
		for {
			msg, err := pgwire.ReadMessage(conn)
			if err != nil {
				return
			}
			if msg.Type == pgwire.MsgParse {
				_ = pgwire.WriteParseComplete(conn)
			}
			if msg.Type == pgwire.MsgSync {
				_ = pgwire.WriteReadyForQuery(conn, 'I')
			}
		}
	}()

	prober := New(Config{
		TargetAddr: ln.Addr().String(),
		Timeout:    1 * time.Second,
	})
	report, err := prober.Probe()
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	if report.Status != "PROTECTED" {
		t.Errorf("expected PROTECTED, got %s", report.Status)
	}
	if report.Vulnerable42P05 || report.Vulnerable26000 {
		t.Errorf("expected non-vulnerable report")
	}
}

func TestDoctor_Probe_Unreachable(t *testing.T) {
	prober := New(Config{
		TargetAddr: "127.0.0.1:59999", // closed port
		Timeout:    100 * time.Millisecond,
	})
	report, err := prober.Probe()
	if err == nil {
		t.Errorf("expected error for unreachable target")
	}
	if report.Status != "UNREACHABLE" {
		t.Errorf("expected UNREACHABLE status, got %s", report.Status)
	}
}

func TestSimulateReport(t *testing.T) {
	rep1 := SimulateReport("localhost:6432", true, false)
	if rep1.Status != "VULNERABLE" {
		t.Errorf("expected VULNERABLE for 26000")
	}

	rep2 := SimulateReport("localhost:6432", false, false)
	if rep2.Status != "PROTECTED" {
		t.Errorf("expected PROTECTED for clean pooler")
	}
}
