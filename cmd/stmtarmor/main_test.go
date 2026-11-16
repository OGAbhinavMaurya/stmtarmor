package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/doctor"
)

func TestMain_Usage(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	if !strings.Contains(out, "proxy") || !strings.Contains(out, "doctor") {
		t.Errorf("expected usage to contain proxy and doctor, got: %s", out)
	}
}

func TestRunDoctor_CLI(t *testing.T) {
	// Test doctor against unreachable address (should report without crashing)
	err := runDoctor([]string{"--target", "127.0.0.1:59998", "--timeout", "100ms"})
	if err != nil {
		t.Errorf("expected runDoctor to handle unreachable cleanly, got: %v", err)
	}
}

func TestSimulateReport_Integration(t *testing.T) {
	rep := doctor.SimulateReport("localhost:6432", true, true)
	if rep.Status != "VULNERABLE" {
		t.Errorf("expected VULNERABLE status, got %s", rep.Status)
	}
}

func TestRunDoctor_JSON(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	_ = runDoctor([]string{"--target", "127.0.0.1:59998", "--timeout", "50ms", "--json"})

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	if !strings.Contains(out, `"target_addr"`) {
		t.Errorf("expected JSON output containing target_addr, got: %s", out)
	}
}

func TestVersionOutput(t *testing.T) {
	if version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", version)
	}
}

func init() {
	// Ensure test runs quickly
	_ = time.Millisecond
}
