package ingest

import (
	"testing"
	"time"
)

func TestLateRefresherNoteMerges(t *testing.T) {
	r := NewLateRefresher(nil, time.Minute, nil)
	if _, _, ok := r.pending(); ok {
		t.Fatal("pending before any note")
	}
	t0 := time.Date(2026, 11, 10, 5, 0, 0, 0, time.UTC)
	r.Note(t0.Add(time.Hour), t0.Add(2*time.Hour))
	r.Note(t0, t0.Add(30*time.Minute))
	lo, hi, ok := r.pending()
	if !ok || !lo.Equal(t0) || !hi.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("range = %v..%v ok=%v", lo, hi, ok)
	}
}

func TestLateAfterLeavesPolicyMargin(t *testing.T) {
	policyStartOffset := 2 * time.Hour
	// start_offset - 1 min bucket - 1 min schedule interval must still leave a margin
	if LateAfter > policyStartOffset-2*time.Minute-time.Minute {
		t.Fatalf("LateAfter %s leaves no margin before the policy window edge", LateAfter)
	}
}
