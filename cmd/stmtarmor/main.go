package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/doctor"
	"github.com/OGAbhinavMaurya/stmtarmor/pkg/proxy"
)

const version = "1.0.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	var err error
	switch command {
	case "proxy":
		err = runProxy(args)
	case "doctor":
		err = runDoctor(args)
	case "version", "-v", "--version":
		fmt.Printf("stmtarmor version %s\n", version)
		return
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	usage := `stmtarmor - PostgreSQL wire-protocol statement virtualizer & zero-collision transaction pooling accelerator

Usage:
  stmtarmor <command> [options]

Commands:
  proxy      Run transparent wire-protocol statement virtualizer proxy
  doctor     Diagnose target connection pooler for prepared statement collisions
  version    Show stmtarmor version
  help       Show this help message

Proxy Options:
  --listen   Local bind address [default: :6433]
  --target   Upstream PgBouncer or PostgreSQL address [default: 127.0.0.1:6432]
  --json     Output metrics in JSON format upon exit

Doctor Options:
  --target   Target database or connection pooler address [default: 127.0.0.1:6432]
  --timeout  Probe connection timeout [default: 5s]
  --json     Output results in JSON format

Examples:
  stmtarmor proxy --listen :6433 --target 127.0.0.1:6432
  stmtarmor doctor --target 127.0.0.1:6432
  stmtarmor doctor --target 127.0.0.1:6432 --json
`
	fmt.Print(usage)
}

func runProxy(args []string) error {
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	listen := fs.String("listen", ":6433", "Local listen address")
	target := fs.String("target", "127.0.0.1:6432", "Upstream database address")
	jsonOutput := fs.Bool("json", false, "Output metrics in JSON upon exit")

	if err := fs.Parse(args); err != nil {
		return err
	}

	p := proxy.New(proxy.Config{
		ListenAddr: *listen,
		TargetAddr: *target,
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("stmtarmor proxy listening on %s -> forwarding to %s\n", *listen, *target)
		serverErr <- p.Start()
	}()

	select {
	case sig := <-sigChan:
		fmt.Fprintf(os.Stderr, "\nReceived signal %v, shutting down stmtarmor proxy...\n", sig)
		_ = p.Close()
	case err := <-serverErr:
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			return fmt.Errorf("proxy server error: %w", err)
		}
	}

	metrics := p.GetMetrics()
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(metrics)
	}

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Proxy Summary:\n")
	fmt.Printf("  Total Connections:       %d\n", metrics.ConnectionsTotal)
	fmt.Printf("  Total Parses Intercepted: %d\n", metrics.ParsesTotal)
	fmt.Printf("  Parses Suppressed (Saved): %d\n", metrics.ParsesSuppressed)
	fmt.Printf("  JIT Parses Injected:     %d\n", metrics.JITParsesInjected)
	fmt.Printf("  Collisions Prevented:    %d\n", metrics.CollisionsPrevented)
	fmt.Println("--------------------------------------------------------------------------------")

	return nil
}

func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	target := fs.String("target", "127.0.0.1:6432", "Target pooler address")
	timeout := fs.Duration("timeout", 5*time.Second, "Probe timeout")
	jsonOutput := fs.Bool("json", false, "Output results in JSON format")

	if err := fs.Parse(args); err != nil {
		return err
	}

	prober := doctor.New(doctor.Config{
		TargetAddr: *target,
		Timeout:    *timeout,
	})

	report, _ := prober.Probe()

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}

	fmt.Println("================================================================================")
	fmt.Println("                            STMTARMOR DOCTOR REPORT                             ")
	fmt.Println("================================================================================")
	fmt.Printf(" Target Address:      %s\n", report.TargetAddr)
	fmt.Printf(" Status:              [%s]\n", report.Status)
	fmt.Printf(" 42P05 Hazard:        %v (prepared statement already exists)\n", report.Vulnerable42P05)
	fmt.Printf(" 26000 Hazard:        %v (prepared statement does not exist)\n", report.Vulnerable26000)
	fmt.Printf(" Summary:             %s\n", report.Summary)
	fmt.Printf(" Recommendation:      %s\n", report.Recommendation)
	fmt.Println("--------------------------------------------------------------------------------")
	if len(report.DiagnosticLog) > 0 {
		fmt.Println(" Diagnostic Trace:")
		for _, log := range report.DiagnosticLog {
			fmt.Printf("   * %s\n", log)
		}
	}
	fmt.Println("================================================================================")

	return nil
}
