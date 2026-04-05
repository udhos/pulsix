package main

import (
	"fmt"
	"testing"

	"github.com/udhos/pulsix/pulsix"
)

func assertUniqueAndTotal(t *testing.T, s *modelState, want int) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[uint64]struct{}, want)
	count := 0

	for _, m := range s.unsent {
		if _, ok := seen[m.IngressID]; ok {
			t.Fatalf("duplicate ingress ID in state: %d", m.IngressID)
		}
		seen[m.IngressID] = struct{}{}
		count++
	}
	for _, m := range s.unacked {
		if _, ok := seen[m.IngressID]; ok {
			t.Fatalf("duplicate ingress ID in state: %d", m.IngressID)
		}
		seen[m.IngressID] = struct{}{}
		count++
	}
	for _, m := range s.acked {
		if _, ok := seen[m.IngressID]; ok {
			t.Fatalf("duplicate ingress ID in state: %d", m.IngressID)
		}
		seen[m.IngressID] = struct{}{}
		count++
	}

	if count != want {
		t.Fatalf("message conservation failed: total=%d want=%d", count, want)
	}
}

func TestModelState_AckErrorNoMessageLoss(t *testing.T) {
	state := newModelState()

	const total = 6
	batch := make([]pulsix.Message, 0, total)
	for i := range total {
		batch = append(batch, pulsix.Message{Data: fmt.Appendf(nil, "m-%d", i)})
	}

	entries := state.addUnsent(batch)
	if len(entries) != total {
		t.Fatalf("addUnsent returned %d entries, want %d", len(entries), total)
	}
	assertUniqueAndTotal(t, state, total)

	for i, e := range entries {
		state.moveUnsentToUnacked(e.IngressID, uint64(100+i))
	}

	unsent, unacked, acked, _ := state.snapshot()
	if unsent != 0 || unacked != total || acked != 0 {
		t.Fatalf("unexpected state before Ack.Err: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)

	reverted := state.revertAllUnackedToUnsent()
	if reverted != total {
		t.Fatalf("reverted=%d want=%d", reverted, total)
	}

	unsent, unacked, acked, _ = state.snapshot()
	if unsent != total || unacked != 0 || acked != 0 {
		t.Fatalf("unexpected state after Ack.Err: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)

	pending := state.snapshotUnsent()
	for i, e := range pending {
		state.moveUnsentToUnacked(e.IngressID, uint64(1000+i))
	}

	moved := state.moveUnackedToAcked(5000)
	if moved != total {
		t.Fatalf("moved=%d want=%d", moved, total)
	}

	unsent, unacked, acked, _ = state.snapshot()
	if unsent != 0 || unacked != 0 || acked != total {
		t.Fatalf("unexpected final state: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)
}

func TestModelState_AckedUpToMovesPrefixOnly(t *testing.T) {
	state := newModelState()

	entries := state.addUnsent([]pulsix.Message{
		{Data: []byte("a")},
		{Data: []byte("b")},
		{Data: []byte("c")},
	})

	state.moveUnsentToUnacked(entries[0].IngressID, 10)
	state.moveUnsentToUnacked(entries[1].IngressID, 11)
	state.moveUnsentToUnacked(entries[2].IngressID, 12)

	moved := state.moveUnackedToAcked(11)
	if moved != 2 {
		t.Fatalf("moved=%d want=2", moved)
	}

	unsent, unacked, acked, _ := state.snapshot()
	if unsent != 0 || unacked != 1 || acked != 2 {
		t.Fatalf("unexpected state: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, 3)
}
