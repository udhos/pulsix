package pulsix

import (
	"io"
	"strings"
	"testing"
)

const (
	blockReadSize = 10_000_000
	bodySizeSmall = 100

	messagesFew  = 100
	messagesMany = 10_000
	messagesHuge = 1_000_000
)

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallOne(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, 1)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallFew(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesFew)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallMany(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesMany)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallHuge(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesHuge)
}

func benchMessageHelper(b *testing.B, bodySize, amount int) {
	buf := make([]byte, blockReadSize)
	messages := createTestMessages(bodySize, amount)
	reader := NewReaderFromMessages(messages, nil)
	for b.Loop() {
		drainReader(reader, buf)
	}
}

func createTestMessages(bodySize, amount int) []Message {
	messages := make([]Message, amount)
	body := []byte(strings.Repeat("a", bodySize))

	for i := range messages {
		messages[i] = Message{
			Data: body,
		}
	}

	return messages
}

func drainReader(r io.Reader, buf []byte) {
	for {
		_, err := r.Read(buf)
		if err != nil {
			return
		}
	}
}
