package pub

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type mockStorage struct {
	capturedContent string
}

func (m *mockStorage) PutObject(ctx context.Context, key string, r io.Reader, size int64) error {
	buf := new(strings.Builder)
	io.Copy(buf, r)
	m.capturedContent = buf.String()
	return nil
}

func (m *mockStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}

func TestSendBatch(t *testing.T) {
	storage := &mockStorage{}
	p := New(Options{Storage: storage, Prefix: "test"})

	msgs := [][]byte{[]byte("hello"), []byte("pulsix")}

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

	err := p.SendBatch(context.Background(), [][]byte{})
	if !errors.Is(err, ErrEmptyMessages) {
		t.Errorf("expected ErrEmptyMessages, got %v", err)
	}
}
