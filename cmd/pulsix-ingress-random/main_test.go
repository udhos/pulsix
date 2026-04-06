package main

import (
	"testing"
)

func TestSimpleRandomRangeBounds(t *testing.T) {
	const low = 10
	const high = 20

	for range 2000 {
		got := simpleRandomRange(low, high)
		if got < low || got >= high {
			t.Fatalf("simpleRandomRange out of bounds: got=%d range=[%d,%d)", got, low, high)
		}
	}
}

func TestBuildRandomBatchShape(t *testing.T) {
	const (
		low         = 3
		high        = 7
		payloadSize = 11
	)

	batch := buildRandomBatch(low, high, payloadSize)

	if len(batch) < low || len(batch) >= high {
		t.Fatalf("unexpected batch length: got=%d want in [%d,%d)", len(batch), low, high)
	}

	for i, msg := range batch {
		if got := len(msg.Data); got != payloadSize {
			t.Fatalf("unexpected payload length at i=%d: got=%d want=%d", i, got, payloadSize)
		}
		for j, b := range msg.Data {
			if b != 'a' {
				t.Fatalf("unexpected payload byte at i=%d j=%d: got=%q want='a'", i, j, b)
			}
		}
	}
}
