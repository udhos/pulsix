package pub

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/udhos/pulsix/pulsix"
)

type mockStorage struct {
	capturedContent string
}

func (m *mockStorage) PutObject(_ context.Context, _ string, r io.Reader, _ int64) error {
	buf := new(strings.Builder)
	io.Copy(buf, r)
	m.capturedContent = buf.String()
	return nil
}

func (m *mockStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil
}

func TestSendBatch(t *testing.T) {
	storage := &mockStorage{}
	p := New(Options{Storage: storage, Prefix: "test"})

	msgs := []pulsix.Message{
		{Data: []byte("hello")},
		{Data: []byte("pulsix")},
	}

	// Expected:
	// p1:9:d:5:hello (4+1+1+1+2 + 5 = 9 internal, total string is 11)
	// p1:10:d:6:pulsix
	err := p.SendBatch(context.Background(), msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "p1:9:d:5:hellop1:10:d:6:pulsix"
	if storage.capturedContent != expected {
		t.Errorf("expected content %q, got %q", expected, storage.capturedContent)
	}
}

func TestSendBatchEmpty(t *testing.T) {
	// Provide a mock even if we expect an early return
	storage := &mockStorage{}
	p := New(Options{Storage: storage})

	err := p.SendBatch(context.Background(), []pulsix.Message{})
	if !errors.Is(err, ErrEmptyMessages) {
		t.Errorf("expected ErrEmptyMessages, got %v", err)
	}
}
