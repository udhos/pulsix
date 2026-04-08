package pulsix

import (
	"io"
	"strings"
	"testing"
)

/*
EncodeTLV BASELINE VERSION:

go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
goos: linux
goarch: amd64
pkg: github.com/udhos/pulsix/pulsix
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkMessageEncodeBodySmallOne-16      	  188761	      6114 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodySmallFew-16      	    7568	    230932 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodySmallMany-16     	      52	  23922363 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumOne-16     	  209595	      5586 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumFew-16     	    4795	    297643 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumMany-16    	      75	  23255807 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeOne-16      	   27426	     43396 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeFew-16      	     295	   4040231 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeMany-16     	       3	 402366738 ns/op	     522 B/op	       6 allocs/op
PASS
ok  	github.com/udhos/pulsix/pulsix	12.106s
*/

const (
	blockReadSize = 10_000_000

	bodySizeSmall  = 100
	bodySizeMedium = 10_000
	bodySizeLarge  = 1_000_000

	messagesOne  = 1
	messagesFew  = 100
	messagesMany = 10_000
)

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallOne(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesOne)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallFew(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesFew)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallMany(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesMany)
}

func BenchmarkMessageEncodeBodyMediumOne(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesOne)
}

func BenchmarkMessageEncodeBodyMediumFew(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesFew)
}

func BenchmarkMessageEncodeBodyMediumMany(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesMany)
}

func BenchmarkMessageEncodeBodyLargeOne(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesOne)
}

func BenchmarkMessageEncodeBodyLargeFew(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesFew)
}

func BenchmarkMessageEncodeBodyLargeMany(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesMany)
}

func benchMessageHelper(b *testing.B, bodySize, amount int) {
	buf := make([]byte, blockReadSize)
	messages := createTestMessages(bodySize, amount)
	headerBuf := make([]byte, 0, 128) // Reusable buffer for encoding message headers
	for b.Loop() {
		reader := NewReaderFromMessages(messages, nil, headerBuf)
		if err := drainReader(reader, buf); err != nil {
			b.Fatalf("drainReader: %v", err)
		}
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

func drainReader(r io.Reader, buf []byte) error {
	for {
		_, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
