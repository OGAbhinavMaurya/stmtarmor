package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/pgwire"
	"github.com/OGAbhinavMaurya/stmtarmor/pkg/virtualizer"
)

// Config configures the StmtArmor transparent wire proxy.
type Config struct {
	ListenAddr string
	TargetAddr string
}

// Metrics captures aggregate proxy stats.
type Metrics struct {
	ConnectionsTotal    uint64 `json:"connections_total"`
	ConnectionsActive   int64  `json:"connections_active"`
	ParsesTotal         uint64 `json:"parses_total"`
	ParsesSuppressed    uint64 `json:"parses_suppressed"`
	JITParsesInjected   uint64 `json:"jit_parses_injected"`
	CollisionsPrevented uint64 `json:"collisions_prevented"`
}

// Proxy is a transparent PostgreSQL wire-protocol proxy that virtualizes
// prepared statements in transaction pooling mode.
type Proxy struct {
	cfg      Config
	listener net.Listener
	mu       sync.Mutex
	running  bool
	metrics  Metrics
}

// New creates a new Proxy instance.
func New(cfg Config) *Proxy {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":6433"
	}
	if cfg.TargetAddr == "" {
		cfg.TargetAddr = "127.0.0.1:6432"
	}
	return &Proxy{cfg: cfg}
}

// Start starts listening and handling proxy connections.
func (p *Proxy) Start() error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("proxy already running")
	}

	ln, err := net.Listen("tcp", p.cfg.ListenAddr)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", p.cfg.ListenAddr, err)
	}
	p.listener = ln
	p.running = true
	p.mu.Unlock()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			p.mu.Lock()
			running := p.running
			p.mu.Unlock()
			if !running {
				return nil
			}
			continue
		}

		atomic.AddUint64(&p.metrics.ConnectionsTotal, 1)
		atomic.AddInt64(&p.metrics.ConnectionsActive, 1)

		go func(c net.Conn) {
			defer func() {
				_ = c.Close()
				atomic.AddInt64(&p.metrics.ConnectionsActive, -1)
			}()
			p.handleConnection(c)
		}(clientConn)
	}
}

// Addr returns the bound address of the listener.
func (p *Proxy) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listener != nil {
		return p.listener.Addr().String()
	}
	return p.cfg.ListenAddr
}

// Close stops the proxy listener.
func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

// GetMetrics returns snapshot of proxy metrics.
func (p *Proxy) GetMetrics() Metrics {
	return Metrics{
		ConnectionsTotal:    atomic.LoadUint64(&p.metrics.ConnectionsTotal),
		ConnectionsActive:   atomic.LoadInt64(&p.metrics.ConnectionsActive),
		ParsesTotal:         atomic.LoadUint64(&p.metrics.ParsesTotal),
		ParsesSuppressed:    atomic.LoadUint64(&p.metrics.ParsesSuppressed),
		JITParsesInjected:   atomic.LoadUint64(&p.metrics.JITParsesInjected),
		CollisionsPrevented: atomic.LoadUint64(&p.metrics.CollisionsPrevented),
	}
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	backendConn, err := net.Dial("tcp", p.cfg.TargetAddr)
	if err != nil {
		return
	}
	defer backendConn.Close()

	// 1. Initial Handshake / Startup Message pass-through
	// In PostgreSQL, the client initiates with a startup packet (4-byte length + payload)
	// without a message type byte.
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, lenBuf); err != nil {
		return
	}
	if _, err := backendConn.Write(lenBuf); err != nil {
		return
	}
	startupLen := int(lenBuf[0])<<24 | int(lenBuf[1])<<16 | int(lenBuf[2])<<8 | int(lenBuf[3])
	if startupLen > 4 {
		startupPayload := make([]byte, startupLen-4)
		if _, err := io.ReadFull(clientConn, startupPayload); err != nil {
			return
		}
		if _, err := backendConn.Write(startupPayload); err != nil {
			return
		}
	}

	virt := virtualizer.NewSessionVirtualizer()
	var swallowParseCompletes int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Backend -> Client loop
	go func() {
		defer cancel()
		for {
			msg, err := pgwire.ReadMessage(backendConn)
			if err != nil {
				return
			}

			// If backend returned ParseComplete ('1') from an injected JIT Parse, swallow it
			if msg.Type == pgwire.MsgParseComplete {
				if atomic.LoadInt32(&swallowParseCompletes) > 0 {
					atomic.AddInt32(&swallowParseCompletes, -1)
					continue
				}
			}

			// Handle ErrorResponse: absorb 42P05 collisions
			if msg.Type == pgwire.MsgErrorResponse {
				errResp := pgwire.DecodeErrorResponse(msg.Payload)
				if errResp.Code == "42P05" {
					// Statement already exists on backend: absorb collision
					atomic.AddUint64(&p.metrics.CollisionsPrevented, 1)
					continue
				}
			}

			if err := msg.Encode(clientConn); err != nil {
				return
			}
		}
	}()

	// Client -> Backend loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := pgwire.ReadMessage(clientConn)
		if err != nil {
			return
		}

		switch msg.Type {
		case pgwire.MsgParse:
			parsed, err := pgwire.DecodeParse(msg.Payload)
			if err != nil {
				_ = msg.Encode(backendConn)
				continue
			}

			suppress, rewrittenParse := virt.HandleParse(parsed)
			atomic.AddUint64(&p.metrics.ParsesTotal, 1)

			if suppress {
				atomic.AddUint64(&p.metrics.ParsesSuppressed, 1)
				atomic.AddUint64(&p.metrics.CollisionsPrevented, 1)
				// Synthesize ParseComplete immediately back to client!
				_ = pgwire.WriteParseComplete(clientConn)
				continue
			}

			// Forward rewritten Parse to backend
			_ = rewrittenParse.Encode().Encode(backendConn)

		case pgwire.MsgBind:
			bind, err := pgwire.DecodeBind(msg.Payload)
			if err != nil {
				_ = msg.Encode(backendConn)
				continue
			}

			jitParse, rewrittenBind := virt.HandleBind(bind)
			if jitParse != nil {
				atomic.AddUint64(&p.metrics.JITParsesInjected, 1)
				atomic.AddUint64(&p.metrics.CollisionsPrevented, 1)
				// Expect a ParseComplete from backend to swallow
				atomic.AddInt32(&swallowParseCompletes, 1)
				_ = jitParse.Encode().Encode(backendConn)
			}

			_ = rewrittenBind.Encode().Encode(backendConn)

		case pgwire.MsgDescribe:
			desc, err := pgwire.DecodeDescribe(msg.Payload)
			if err != nil {
				_ = msg.Encode(backendConn)
				continue
			}

			jitParse, rewrittenDesc := virt.HandleDescribe(desc)
			if jitParse != nil {
				atomic.AddUint64(&p.metrics.JITParsesInjected, 1)
				atomic.AddUint64(&p.metrics.CollisionsPrevented, 1)
				atomic.AddInt32(&swallowParseCompletes, 1)
				_ = jitParse.Encode().Encode(backendConn)
			}

			_ = rewrittenDesc.Encode().Encode(backendConn)

		default:
			// Forward all other messages (Sync, Execute, Flush, Close, Query) directly
			_ = msg.Encode(backendConn)
		}
	}
}
