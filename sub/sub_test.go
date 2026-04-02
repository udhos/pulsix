package sub

import (
	"bufio"
	"context"
	"io"
	"strings"
	"testing"
)

type MockNotification struct {
	Key     string
	Deleted bool
}

func (m *MockNotification) GetKey() string                 { return m.Key }
func (m *MockNotification) Delete(_ context.Context) error { m.Deleted = true; return nil }

type MockQueue struct {
	Notifications []Notification
}

func (m *MockQueue) ReceiveNotifications(_ context.Context) ([]Notification, error) {
	return m.Notifications, nil
}

type MockStorage struct {
	Data map[string]string
}

func (m *MockStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (m *MockStorage) GetObject(_ context.Context, key string) (io.ReadCloser, error) {
	content, ok := m.Data[key]
	if !ok {
		return nil, io.EOF
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func TestSub_ReceiveAndLifecycle(t *testing.T) {
	key := "test/path/batch.pulsix"
	// UPDATED: Content now matches the \n prefix format
	content := "\nPULSIX-SIZE:5\nhello"

	notif1 := &MockNotification{Key: key}
	mockQueue := &MockQueue{Notifications: []Notification{notif1}}
	mockStorage := &MockStorage{Data: map[string]string{key: content}}

	subscriber := New(Options{
		Queue:   mockQueue,
		Storage: mockStorage,
	})

	batches, err := subscriber.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	batch := batches[0]
	if !batch.Next() {
		t.Fatalf("Next() failed: %v", batch.Error())
	}

	if string(batch.Message()) != "hello" {
		t.Errorf("expected hello, got %s", batch.Message())
	}

	if err := batch.Done(); err != nil {
		t.Errorf("Done failed: %v", err)
	}

	if !notif1.Deleted {
		t.Error("expected notification to be deleted after Done()")
	}
}

func TestSub_FullStream(t *testing.T) {
	key := "test.batch"
	// UPDATED: Two messages with leading newlines
	content := "\nPULSIX-SIZE:5\nhello\nPULSIX-SIZE:5\nworld"

	mockQueue := &MockQueue{Notifications: []Notification{&MockNotification{Key: key}}}
	mockStorage := &MockStorage{Data: map[string]string{key: content}}
	subscriber := New(Options{Queue: mockQueue, Storage: mockStorage})

	batches, _ := subscriber.Receive(context.Background())
	batch := batches[0]

	if !batch.Next() || string(batch.Message()) != "hello" {
		t.Errorf("failed to read first message")
	}

	if !batch.Next() || string(batch.Message()) != "world" {
		t.Errorf("failed to read second message")
	}

	if batch.Next() {
		t.Error("expected end of stream")
	}

	batch.Done()
}

func TestParseHeader_LeadingNewline(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{
			name:    "Standard with leading newline",
			input:   "\nPULSIX-SIZE:5\nhello",
			want:    5,
			wantErr: false,
		},
		{
			name:    "Subsequent record (double newline handling)",
			input:   "\n\nPULSIX-SIZE:10\n",
			want:    10,
			wantErr: false,
		},
		{
			name:    "Missing leading newline but correct prefix",
			input:   "PULSIX-SIZE:5\n",
			want:    5,
			wantErr: false,
		},
		{
			name:    "Garbage input",
			input:   "\nNOT-PULSIX:5\n",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseHeader(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseHeader() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseHeader_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr string
	}{
		{
			name:    "EOF mid-header",
			input:   "\nPULSIX-SI",
			wantErr: "EOF",
		},
		{
			name:    "Overflow size",
			input:   "\nPULSIX-SIZE:9223372036854775808\n",
			wantErr: "invalid size",
		},
		{
			name:    "Negative size",
			input:   "\nPULSIX-SIZE:-1\n",
			wantErr: "invalid size",
		},
		{
			name:    "Valid header followed by data that looks like header",
			input:   "\nPULSIX-SIZE:5\n\nPULSIX-SIZE:10\n",
			want:    5,
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseHeader(r)

			// If we expect an error
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) && err != io.EOF {
					t.Errorf("expected error %q, got %v", tt.wantErr, err)
				}
				return
			}

			// If we expect success
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != tt.want {
				t.Errorf("got size %d, want %d", got, tt.want)
			}
		})
	}
}

func FuzzParseHeader(f *testing.F) {
	f.Add("\nPULSIX-SIZE:10\n")
	f.Fuzz(func(_ *testing.T, data string) {
		r := bufio.NewReader(strings.NewReader(data))
		_, _ = parseHeader(r)
		// We don't care about the result, only that it doesn't panic
	})
}
