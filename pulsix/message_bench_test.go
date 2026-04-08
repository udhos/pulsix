package pulsix

import (
	"io"
	"strings"
	"testing"
)

/*
BASELINE

go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
goos: linux
goarch: amd64
pkg: github.com/udhos/pulsix/pulsix
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkMessageEncodeBodySmallOne-16      	  248454	      5576 ns/op	     504 B/op	       8 allocs/op
BenchmarkMessageEncodeBodySmallFew-16      	    5160	    228059 ns/op	    2089 B/op	     206 allocs/op
BenchmarkMessageEncodeBodySmallMany-16     	      66	  27988904 ns/op	  160603 B/op	   20006 allocs/op
BenchmarkMessageEncodeBodyMediumOne-16     	  232702	      6617 ns/op	     520 B/op	      10 allocs/op
BenchmarkMessageEncodeBodyMediumFew-16     	    3858	    291794 ns/op	    3689 B/op	     406 allocs/op
BenchmarkMessageEncodeBodyMediumMany-16    	      44	  23038792 ns/op	  320660 B/op	   40006 allocs/op
BenchmarkMessageEncodeBodyLargeOne-16      	   27278	     44036 ns/op	     536 B/op	      10 allocs/op
BenchmarkMessageEncodeBodyLargeFew-16      	     294	   4057943 ns/op	    5296 B/op	     406 allocs/op
BenchmarkMessageEncodeBodyLargeMany-16     	       3	 404453544 ns/op	  481453 B/op	   40009 allocs/op
PASS
ok  	github.com/udhos/pulsix/pulsix	11.723s
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
	for b.Loop() {
		reader := NewReaderFromMessages(messages, nil)
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
