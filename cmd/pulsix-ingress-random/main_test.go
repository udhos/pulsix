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

	count := len(s.unsent) + len(s.unacked)
	count += int(s.stats.Acked)

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

	state.addUnsent(batch)
	if _, ok := state.peekUnsent(); !ok {
		t.Fatalf("peekUnsent returned empty, want messages")
	}
	assertUniqueAndTotal(t, state, total)

	for i := range total {
		if _, ok := state.peekUnsent(); !ok {
			t.Fatalf("peekUnsent returned empty at i=%d", i)
		}
		if !state.moveFrontUnsentToUnacked(uint64(100 + i)) {
			t.Fatalf("moveFrontUnsentToUnacked failed at i=%d", i)
		}
	}

	unsent, unacked, stats := state.snapshot()
	acked := stats.Acked
	if unsent != 0 || unacked != total || acked != 0 {
		t.Fatalf("unexpected state before Ack.Err: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)

	reverted := state.revertAllUnackedToUnsent()
	if reverted != total {
		t.Fatalf("reverted=%d want=%d", reverted, total)
	}

	unsent, unacked, stats = state.snapshot()
	acked = stats.Acked
	if unsent != total || unacked != 0 || acked != 0 {
		t.Fatalf("unexpected state after Ack.Err: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)

	for i := range total {
		if _, ok := state.peekUnsent(); !ok {
			t.Fatalf("peekUnsent returned empty at retry i=%d", i)
		}
		if !state.moveFrontUnsentToUnacked(uint64(1000 + i)) {
			t.Fatalf("moveFrontUnsentToUnacked failed at retry i=%d", i)
		}
	}

	moved := state.moveUnackedToAcked(5000)
	if moved != total {
		t.Fatalf("moved=%d want=%d", moved, total)
	}

	unsent, unacked, stats = state.snapshot()
	acked = stats.Acked
	if unsent != 0 || unacked != 0 || acked != total {
		t.Fatalf("unexpected final state: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, total)
}

func TestModelState_AckedUpToMovesPrefixOnly(t *testing.T) {
	state := newModelState()

	state.addUnsent([]pulsix.Message{
		{Data: []byte("a")},
		{Data: []byte("b")},
		{Data: []byte("c")},
	})

	if _, ok := state.peekUnsent(); !ok {
		t.Fatalf("peekUnsent returned empty for first message")
	}
	if !state.moveFrontUnsentToUnacked(10) {
		t.Fatal("moveFrontUnsentToUnacked failed for first message")
	}
	if !state.moveFrontUnsentToUnacked(11) {
		t.Fatal("moveFrontUnsentToUnacked failed for second message")
	}
	if !state.moveFrontUnsentToUnacked(12) {
		t.Fatal("moveFrontUnsentToUnacked failed for third message")
	}

	moved := state.moveUnackedToAcked(11)
	if moved != 2 {
		t.Fatalf("moved=%d want=2", moved)
	}

	unsent, unacked, stats := state.snapshot()
	acked := stats.Acked
	if unsent != 0 || unacked != 1 || acked != 2 {
		t.Fatalf("unexpected state: unsent=%d unacked=%d acked=%d", unsent, unacked, acked)
	}
	assertUniqueAndTotal(t, state, 3)
}
