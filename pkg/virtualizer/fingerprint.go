package virtualizer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

// FingerprintQuery computes a deterministic canonical statement identifier
// based on normalized SQL text and parameter OIDs.
func FingerprintQuery(query string, paramOIDs []uint32) string {
	normalized := strings.TrimSpace(query)
	// Collapse consecutive whitespaces
	normalized = strings.Join(strings.Fields(normalized), " ")

	h := sha256.New()
	h.Write([]byte(normalized))
	h.Write([]byte{0})

	oidBuf := make([]byte, 4)
	for _, oid := range paramOIDs {
		binary.BigEndian.PutUint32(oidBuf, oid)
		h.Write(oidBuf)
	}

	sum := h.Sum(nil)
	// Format as "sa_" followed by 12 hex characters
	return "sa_" + hex.EncodeToString(sum[:6])
}
