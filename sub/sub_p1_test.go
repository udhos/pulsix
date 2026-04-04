package sub

import (
	"bufio"
	"strings"
	"testing"
)

// go test -v -count 1 -run '^TestNext_P1Format$' ./sub
// go test -v -count 1 -run '^TestNext_P1Format$' ./sub
func TestNext_P1Format(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantMeta    string
		wantAttrKey string // Changed to verify map keys
		wantAttrVal string
		wantData    string
		wantErr     bool
		errContains string
	}{
		{
			name:        "Valid packed p1 record",
			input:       "p1:51:m:20:j:{\"message_id\":\"001\"}a:9:j:{\"a\":\"b\"}d:5:hello",
			wantMeta:    "001",
			wantAttrKey: "a",
			wantAttrVal: "b",
			wantData:    "hello",
		},
		{
			name:     "Empty data field",
			input:    "p1:4:d:0:",
			wantData: "",
		},
		{
			name:        "Invalid version prefix",
			input:       "x1:22:m:3:001",
			wantErr:     true,
			errContains: "unexpected version",
		},
		{
			name:        "Truncated record (total length mismatch)",
			input:       "p1:10:d:5:hel",
			wantErr:     true,
			errContains: "unexpected EOF",
		},
		{
			name:     "Extensibility: Skip unknown tag 'z'",
			input:    "p1:18:z:5:ghostd:5:hello",
			wantData: "hello",
		},
		{
			name:     "Binary safety: Colon inside value",
			input:    "p1:10:d:6:a:b:c:",
			wantData: "a:b:c:",
		},
		{
			name:        "Reject unsupported metadata encoding",
			input:       "p1:18:m:3:x:001d:5:hello",
			wantErr:     true,
			errContains: "unsupported encoding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				bufr: bufio.NewReader(strings.NewReader(tt.input)),
			}

			ok := b.Next()

			if tt.wantErr {
				if ok {
					t.Errorf("expected error, but Next() returned true")
				}
				if tt.errContains != "" && b.err != nil {
					if !strings.Contains(b.err.Error(), tt.errContains) {
						t.Errorf("error %q does not contain %q", b.err, tt.errContains)
					}
				}
				return
			}

			if !ok {
				t.Fatalf("Next() failed unexpectedly: %v", b.err)
			}

			msg := b.Message()

			// 1. Verify Data (Tag 'd')
			if string(msg.Data) != tt.wantData {
				t.Errorf("got data %q, want %q", string(msg.Data), tt.wantData)
			}

			// 2. Verify Meta (Tag 'm') - mapped to MessageID
			if msg.Metadata.MessageID != tt.wantMeta {
				t.Errorf("got meta %q, want %q", msg.Metadata.MessageID, tt.wantMeta)
			}

			// 3. Verify Attr (Tag 'a') - mapped to Attributes map
			if tt.wantAttrKey != "" {
				val, exists := msg.Attributes[tt.wantAttrKey]
				if !exists {
					t.Errorf("expected attribute key %q not found", tt.wantAttrKey)
				}
				if val != tt.wantAttrVal {
					t.Errorf("got attr[%q] = %q, want %q", tt.wantAttrKey, val, tt.wantAttrVal)
				}
			}
		})
	}
}

func TestStream_MultipleP1Records(t *testing.T) {
	// Two records:
	// 1. p1:4:d:0: (empty)
	// 2. p1:9:d:5:world
	input := "p1:4:d:0:p1:9:d:5:world"
	b := &Batch{
		bufr: bufio.NewReader(strings.NewReader(input)),
	}

	// First Record
	if !b.Next() || len(b.Message().Data) != 0 {
		t.Errorf("failed to parse first empty record: %v", b.err)
	}

	// Second Record
	if !b.Next() || string(b.Message().Data) != "world" {
		t.Errorf("failed to parse second record: %v", b.err)
	}

	if b.Next() {
		t.Error("expected end of stream")
	}
}

func TestP1_RecordJumping(t *testing.T) {
	// This tests if the parser correctly honors total_record_length
	// even if the TLVs inside are weird or contain 'p1:'

	// Record 1: Claims 5 bytes, but we put "p1:XX" inside it.
	// A good parser skips exactly 5 bytes after the length colon.
	input := "p1:5:abcdep1:9:d:5:valid"

	b := &Batch{
		bufr: bufio.NewReader(strings.NewReader(input)),
	}

	// We skip the first "junk" record by calling Next
	b.Next()

	// The second call should find the start of the next valid p1
	if !b.Next() || string(b.Message().Data) != "valid" {
		t.Errorf("failed to jump to next record correctly: %v", b.err)
	}
}
