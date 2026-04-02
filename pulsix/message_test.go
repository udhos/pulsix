package pulsix

import (
	"bytes"
	"testing"
)

func TestMessage_EncodeTLV_Rigorous(t *testing.T) {
	m := Message{
		Metadata:   Metadata{MessageID: "789"},
		Attributes: map[string]string{"k": "v"},
		Data:       []byte{0xDE, 0xAD, 0xBE, 0xEF}, // Hex data
	}

	buf := new(bytes.Buffer)
	_ = m.EncodeTLV(buf)
	res := buf.Bytes()

	// 1. Find where the 'd' TLV starts
	// We look for "d:" followed by the length of our data (4) and another ":"
	marker := []byte("d:4:")
	dataStart := bytes.Index(res, marker)
	if dataStart == -1 {
		t.Fatal("Could not find data TLV marker 'd:4:'")
	}

	// 2. Extract the actual bytes following the marker
	// The marker itself is 4 bytes long ("d:4:")
	extractedData := res[dataStart+len(marker):]

	// 3. Verify exact match and length
	if !bytes.Equal(extractedData, m.Data) {
		t.Errorf("Data mismatch! Expected %x, got %x", m.Data, extractedData)
	}

	if len(extractedData) != 4 {
		t.Errorf("Length mismatch! Expected 4, got %d", len(extractedData))
	}
}
