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
	LastContent string
	LastKey     string
}

func (m *mockStorage) PutObject(ctx context.Context, key string, r io.Reader, size int64) error {
	m.LastKey = key
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.LastContent = string(data)
	return nil
}

func (m *mockStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(m.LastContent)), nil
}

func TestSendBatch(t *testing.T) {
	const fixedID = "FIXED_ID_FOR_TESTING_1234567"

	// This is the exact string the encoder produces with your fixed ID
	// p1:59:m:45:{"message_id":"FIXED_ID_FOR_TESTING_1234567"}d:5:hello
	// Breakdown:
	// m tag: "m:45:{"message_id":"FIXED_ID_FOR_TESTING_1234567"}" (50 bytes)
	// d tag: "d:5:hello" (9 bytes)
	// Total body: 59 bytes.
	wantRecord1 := `p1:59:m:45:{"message_id":"FIXED_ID_FOR_TESTING_1234567"}d:5:hello`
	wantRecord2 := `p1:60:m:45:{"message_id":"FIXED_ID_FOR_TESTING_1234567"}d:6:pulsix`
	fullExpected := wantRecord1 + wantRecord2

	mockStore := &mockStorage{}
	pub := New(Options{
		Storage: mockStore,
		Prefix:  "test",
		GenerateIDFunc: func() string {
			return fixedID
		},
	})

	messages := []pulsix.Message{
		{Data: []byte("hello")},
		{Data: []byte("pulsix")},
	}

	err := pub.SendBatch(context.Background(), messages)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	got := mockStore.LastContent
	if got != fullExpected {
		t.Errorf("\nexpected: %s\ngot:      %s", fullExpected, got)
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
