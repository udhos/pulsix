package pulsix

import (
	"io"
	"strings"
	"testing"
)

/*
EncodeTLV BASELINE VERSION:

go test -bench=BenchmarkMessageEncode -benchmem -run=^$ ./pulsix
goos: linux
goarch: amd64
pkg: github.com/udhos/pulsix/pulsix
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkMessageEncodeBodySmallOne-16           	  202802	      5802 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodySmallFew-16           	    4977	    250867 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodySmallMany-16          	      51	  21871001 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumOne-16          	  295630	      5210 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumFew-16          	    5278	    241288 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyMediumMany-16         	      51	  23104780 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeOne-16           	   27988	     42524 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeFew-16           	     300	   3958739 ns/op	     520 B/op	       6 allocs/op
BenchmarkMessageEncodeBodyLargeMany-16          	       3	 394676698 ns/op	     522 B/op	       6 allocs/op

BenchmarkMessageEncodeMetadataSmallOne-16       	  214899	      7052 ns/op	     648 B/op	       8 allocs/op
BenchmarkMessageEncodeMetadataSmallFew-16       	    2343	    473082 ns/op	   13326 B/op	     206 allocs/op
BenchmarkMessageEncodeMetadataSmallMany-16      	      30	  39873681 ns/op	 1280960 B/op	   20007 allocs/op
BenchmarkMessageEncodeMetadataLargeOne-16       	   62242	     19739 ns/op	   10820 B/op	       8 allocs/op
BenchmarkMessageEncodeMetadataLargeFew-16       	     691	   1677439 ns/op	 1030400 B/op	     207 allocs/op
BenchmarkMessageEncodeMetadataLargeMany-16      	       9	 130015942 ns/op	102890112 B/op	   20103 allocs/op

BenchmarkMessageEncodeAttributesSmallOne-16     	  191505	      7462 ns/op	     632 B/op	      10 allocs/op
BenchmarkMessageEncodeAttributesSmallFew-16     	    3048	    448829 ns/op	   11723 B/op	     406 allocs/op
BenchmarkMessageEncodeAttributesSmallMany-16    	      31	  37890229 ns/op	 1120876 B/op	   40007 allocs/op
BenchmarkMessageEncodeAttributesLargeOne-16     	  136551	      7793 ns/op	     808 B/op	      10 allocs/op
BenchmarkMessageEncodeAttributesLargeFew-16     	    2320	    478086 ns/op	   29335 B/op	     406 allocs/op
BenchmarkMessageEncodeAttributesLargeMany-16    	      24	  48118836 ns/op	 2881964 B/op	   40013 allocs/op
PASS
ok  	github.com/udhos/pulsix/pulsix	25.828s
*/

const (
	blockReadSize = 10_000_000

	bodySizeEmpty  = 0
	bodySizeSmall  = 100
	bodySizeMedium = 10_000
	bodySizeLarge  = 1_000_000

	messagesOne  = 1
	messagesFew  = 100
	messagesMany = 10_000
)

var (
	metadataEmpty = Metadata{}
	metadataSmall = Metadata{MessageID: strings.Repeat("x", 100)}
	metadataLarge = Metadata{MessageID: strings.Repeat("x", 10000)}
)

var attributesEmpty map[string]string
var attributesSmall = genAttributes(10, 10)   // 10 * 20 = 200 bytes
var attributesLarge = genAttributes(100, 100) // 100 * 200 = 20_000 bytes

func genAttributes(amount, size int) map[string]string {
	attributes := make(map[string]string)
	for range amount {
		key := strings.Repeat("k", size)
		value := strings.Repeat("v", size)
		attributes[key] = value
	}
	return attributes
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallOne(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesOne, metadataEmpty, attributesEmpty)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallFew(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesFew, metadataEmpty, attributesEmpty)
}

// go test -bench=BenchmarkMessage -benchmem -run=^$ ./pulsix
func BenchmarkMessageEncodeBodySmallMany(b *testing.B) {
	benchMessageHelper(b, bodySizeSmall, messagesMany, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyMediumOne(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesOne, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyMediumFew(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesFew, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyMediumMany(b *testing.B) {
	benchMessageHelper(b, bodySizeMedium, messagesMany, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyLargeOne(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesOne, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyLargeFew(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesFew, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeBodyLargeMany(b *testing.B) {
	benchMessageHelper(b, bodySizeLarge, messagesMany, metadataEmpty, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataSmallOne(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesOne, metadataSmall, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataSmallFew(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesFew, metadataSmall, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataSmallMany(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesMany, metadataSmall, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataLargeOne(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesOne, metadataLarge, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataLargeFew(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesFew, metadataLarge, attributesEmpty)
}

func BenchmarkMessageEncodeMetadataLargeMany(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesMany, metadataLarge, attributesEmpty)
}

func BenchmarkMessageEncodeAttributesSmallOne(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesOne, metadataEmpty, attributesSmall)
}

func BenchmarkMessageEncodeAttributesSmallFew(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesFew, metadataEmpty, attributesSmall)
}

func BenchmarkMessageEncodeAttributesSmallMany(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesMany, metadataEmpty, attributesSmall)
}

func BenchmarkMessageEncodeAttributesLargeOne(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesOne, metadataEmpty, attributesLarge)
}

func BenchmarkMessageEncodeAttributesLargeFew(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesFew, metadataEmpty, attributesLarge)
}

func BenchmarkMessageEncodeAttributesLargeMany(b *testing.B) {
	benchMessageHelper(b, bodySizeEmpty, messagesMany, metadataEmpty, attributesLarge)
}

func benchMessageHelper(b *testing.B, bodySize, amount int, metadata Metadata,
	attributes map[string]string) {
	buf := make([]byte, blockReadSize)
	messages := createTestMessages(bodySize, amount, metadata, attributes)
	headerBuf := make([]byte, 0, 128) // Reusable buffer for encoding message headers
	for b.Loop() {
		reader := NewReaderFromMessages(messages, nil, headerBuf)
		if err := drainReader(reader, buf); err != nil {
			b.Fatalf("drainReader: %v", err)
		}
	}
}

func createTestMessages(bodySize, amount int, metadata Metadata, attributes map[string]string) []Message {
	messages := make([]Message, amount)
	body := []byte(strings.Repeat("a", bodySize))

	for i := range messages {
		messages[i] = Message{
			Metadata:   metadata,
			Attributes: attributes,
			Data:       body,
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
