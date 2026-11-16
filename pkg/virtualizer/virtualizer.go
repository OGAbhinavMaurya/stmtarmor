package virtualizer

import (
	"sync"
	"sync/atomic"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/pgwire"
)

// CanonicalStatement represents a unique parameterized query.
type CanonicalStatement struct {
	ID        string
	Query     string
	ParamOIDs []uint32
}

// Metrics tracks statement virtualization activity and savings.
type Metrics struct {
	ParsesTotal          uint64 `json:"parses_total"`
	ParsesSuppressed     uint64 `json:"parses_suppressed"`
	JITParsesInjected    uint64 `json:"jit_parses_injected"`
	CollisionsPrevented  uint64 `json:"collisions_prevented"`
	ActiveClientStmts    int    `json:"active_client_stmts"`
	ActiveBackendStmts   int    `json:"active_backend_stmts"`
}

// SessionVirtualizer manages statement mappings between one client session
// and its currently assigned backend connection.
type SessionVirtualizer struct {
	mu               sync.RWMutex
	clientStatements map[string]*CanonicalStatement // client statement name -> canonical stmt
	backendPrepared  map[string]bool                // canonical ID -> prepared on current backend
	metrics          Metrics
}

// NewSessionVirtualizer creates a new virtualization engine for a connection pair.
func NewSessionVirtualizer() *SessionVirtualizer {
	return &SessionVirtualizer{
		clientStatements: make(map[string]*CanonicalStatement),
		backendPrepared:  make(map[string]bool),
	}
}

// HandleParse processes a client 'P' message.
// If the statement is already prepared on the active backend connection,
// it returns suppress = true (so proxy can return synthetic ParseComplete '1' immediately).
// Otherwise, it returns suppress = false with the rewritten Parse message using the canonical name.
func (v *SessionVirtualizer) HandleParse(parsed *pgwire.ParseMsg) (bool, *pgwire.ParseMsg) {
	v.mu.Lock()
	defer v.mu.Unlock()

	atomic.AddUint64(&v.metrics.ParsesTotal, 1)

	canonicalID := FingerprintQuery(parsed.Query, parsed.ParamOIDs)
	canonicalStmt := &CanonicalStatement{
		ID:        canonicalID,
		Query:     parsed.Query,
		ParamOIDs: parsed.ParamOIDs,
	}

	// Register in client session mapping
	v.clientStatements[parsed.Statement] = canonicalStmt

	// Check if already prepared on the current backend
	if v.backendPrepared[canonicalID] {
		// Already on backend: suppress parse round-trip!
		atomic.AddUint64(&v.metrics.ParsesSuppressed, 1)
		atomic.AddUint64(&v.metrics.CollisionsPrevented, 1)
		return true, nil
	}

	// Needs preparation on backend under canonical name
	v.backendPrepared[canonicalID] = true
	rewritten := &pgwire.ParseMsg{
		Statement: canonicalID,
		Query:     parsed.Query,
		ParamOIDs: parsed.ParamOIDs,
	}
	return false, rewritten
}

// HandleBind processes a client 'B' message.
// Rewrites the statement identifier to its canonical name.
// If the backend has not yet prepared this canonical statement (e.g. after pooler rotation),
// it returns a JIT ParseMsg to be injected on the wire immediately before the Bind message.
func (v *SessionVirtualizer) HandleBind(bind *pgwire.BindMsg) (*pgwire.ParseMsg, *pgwire.BindMsg) {
	v.mu.Lock()
	defer v.mu.Unlock()

	clientName := bind.Statement
	canonicalStmt, found := v.clientStatements[clientName]

	targetStmtName := clientName
	var jitParse *pgwire.ParseMsg

	if found {
		targetStmtName = canonicalStmt.ID

		// Check if active backend has it
		if !v.backendPrepared[canonicalStmt.ID] {
			// JIT preparation needed!
			v.backendPrepared[canonicalStmt.ID] = true
			atomic.AddUint64(&v.metrics.JITParsesInjected, 1)
			atomic.AddUint64(&v.metrics.CollisionsPrevented, 1)

			jitParse = &pgwire.ParseMsg{
				Statement: canonicalStmt.ID,
				Query:     canonicalStmt.Query,
				ParamOIDs: canonicalStmt.ParamOIDs,
			}
		}
	}

	rewritten := &pgwire.BindMsg{
		Portal:            bind.Portal,
		Statement:         targetStmtName,
		ParamFormatCodes:  bind.ParamFormatCodes,
		ParamValues:       bind.ParamValues,
		ResultFormatCodes: bind.ResultFormatCodes,
	}

	return jitParse, rewritten
}

// HandleDescribe processes a client 'D' message.
// If target is a statement ('S'), rewrites to canonical identifier and injects JIT parse if needed.
func (v *SessionVirtualizer) HandleDescribe(desc *pgwire.DescribeMsg) (*pgwire.ParseMsg, *pgwire.DescribeMsg) {
	v.mu.Lock()
	defer v.mu.Unlock()

	targetName := desc.Name
	var jitParse *pgwire.ParseMsg

	if desc.TargetType == 'S' {
		if canonicalStmt, found := v.clientStatements[desc.Name]; found {
			targetName = canonicalStmt.ID
			if !v.backendPrepared[canonicalStmt.ID] {
				v.backendPrepared[canonicalStmt.ID] = true
				atomic.AddUint64(&v.metrics.JITParsesInjected, 1)
				atomic.AddUint64(&v.metrics.CollisionsPrevented, 1)

				jitParse = &pgwire.ParseMsg{
					Statement: canonicalStmt.ID,
					Query:     canonicalStmt.Query,
					ParamOIDs: canonicalStmt.ParamOIDs,
				}
			}
		}
	}

	return jitParse, &pgwire.DescribeMsg{
		TargetType: desc.TargetType,
		Name:       targetName,
	}
}

// HandleClose processes a client 'C' message.
func (v *SessionVirtualizer) HandleClose(targetType byte, name string) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if targetType == 'S' {
		delete(v.clientStatements, name)
	}
}

// ResetBackend clears the active backend prepared cache.
// Useful when a backend connection terminates or when PgBouncer cycles connections.
func (v *SessionVirtualizer) ResetBackend() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.backendPrepared = make(map[string]bool)
}

// MarkBackendPrepared registers that a statement is prepared on the backend.
func (v *SessionVirtualizer) MarkBackendPrepared(canonicalID string) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.backendPrepared[canonicalID] = true
}

// GetMetrics returns a copy of current metrics.
func (v *SessionVirtualizer) GetMetrics() Metrics {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return Metrics{
		ParsesTotal:         atomic.LoadUint64(&v.metrics.ParsesTotal),
		ParsesSuppressed:    atomic.LoadUint64(&v.metrics.ParsesSuppressed),
		JITParsesInjected:   atomic.LoadUint64(&v.metrics.JITParsesInjected),
		CollisionsPrevented: atomic.LoadUint64(&v.metrics.CollisionsPrevented),
		ActiveClientStmts:   len(v.clientStatements),
		ActiveBackendStmts:  len(v.backendPrepared),
	}
}
