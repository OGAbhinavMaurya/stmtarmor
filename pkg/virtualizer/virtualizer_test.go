package virtualizer

import (
	"testing"

	"github.com/OGAbhinavMaurya/stmtarmor/pkg/pgwire"
)

func TestFingerprintQuery(t *testing.T) {
	q1 := "SELECT *   FROM  users WHERE id = $1"
	q2 := "SELECT * FROM users WHERE id = $1"
	oids := []uint32{23}

	fp1 := FingerprintQuery(q1, oids)
	fp2 := FingerprintQuery(q2, oids)

	if fp1 != fp2 {
		t.Errorf("expected identical fingerprints for whitespace normalized queries, got %s != %s", fp1, fp2)
	}

	diffOids := []uint32{25}
	fp3 := FingerprintQuery(q1, diffOids)
	if fp1 == fp3 {
		t.Errorf("expected different fingerprints for different parameter OIDs")
	}
}

func TestVirtualizer_ParseSuppressionAndJIT(t *testing.T) {
	v := NewSessionVirtualizer()

	parse := &pgwire.ParseMsg{
		Statement: "s0",
		Query:     "SELECT * FROM items WHERE id = $1",
		ParamOIDs: []uint32{23},
	}

	// 1. Initial Parse: must NOT be suppressed
	suppress, rewrittenParse := v.HandleParse(parse)
	if suppress {
		t.Errorf("expected initial Parse not to be suppressed")
	}
	if rewrittenParse.Statement == "s0" {
		t.Errorf("expected rewritten statement name to be canonical, got s0")
	}
	canonicalID := rewrittenParse.Statement

	// 2. Second Parse of identical query: MUST be suppressed!
	suppress2, _ := v.HandleParse(parse)
	if !suppress2 {
		t.Errorf("expected second Parse of identical query to be suppressed")
	}

	// 3. Bind: should rewrite statement name to canonicalID, no JIT parse needed
	bind := &pgwire.BindMsg{
		Portal:    "",
		Statement: "s0",
	}
	jitParse, rewrittenBind := v.HandleBind(bind)
	if jitParse != nil {
		t.Errorf("expected no JIT parse when backend already prepared")
	}
	if rewrittenBind.Statement != canonicalID {
		t.Errorf("expected rewritten bind to use canonical ID %s, got %s", canonicalID, rewrittenBind.Statement)
	}

	// 4. Simulate backend rotation (e.g. PgBouncer pool_mode=transaction switches backend)
	v.ResetBackend()

	// 5. Client performs Bind on next transaction: JIT parse MUST be generated!
	jitParse2, rewrittenBind2 := v.HandleBind(bind)
	if jitParse2 == nil {
		t.Fatalf("expected JIT parse after backend reset")
	}
	if jitParse2.Statement != canonicalID || jitParse2.Query != parse.Query {
		t.Errorf("unexpected JIT parse content: %+v", jitParse2)
	}
	if rewrittenBind2.Statement != canonicalID {
		t.Errorf("expected rewritten bind: %s", rewrittenBind2.Statement)
	}

	// 6. Check metrics
	metrics := v.GetMetrics()
	if metrics.ParsesTotal != 2 {
		t.Errorf("expected 2 parses total, got %d", metrics.ParsesTotal)
	}
	if metrics.ParsesSuppressed != 1 {
		t.Errorf("expected 1 parse suppressed, got %d", metrics.ParsesSuppressed)
	}
	if metrics.JITParsesInjected != 1 {
		t.Errorf("expected 1 JIT parse injected, got %d", metrics.JITParsesInjected)
	}
	if metrics.CollisionsPrevented != 2 {
		t.Errorf("expected 2 collisions prevented, got %d", metrics.CollisionsPrevented)
	}
}

func TestVirtualizer_Describe(t *testing.T) {
	v := NewSessionVirtualizer()

	parse := &pgwire.ParseMsg{
		Statement: "query_1",
		Query:     "SELECT 1",
	}
	_, rewrittenParse := v.HandleParse(parse)

	desc := &pgwire.DescribeMsg{
		TargetType: 'S',
		Name:       "query_1",
	}
	jit, rewrittenDesc := v.HandleDescribe(desc)
	if jit != nil {
		t.Errorf("expected no JIT parse on prepared statement")
	}
	if rewrittenDesc.Name != rewrittenParse.Statement {
		t.Errorf("expected rewritten describe name %s, got %s", rewrittenParse.Statement, rewrittenDesc.Name)
	}
}
