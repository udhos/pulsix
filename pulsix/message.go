package pulsix

import (
	"encoding/json"
	"fmt"
	"io"
)

// Message represents a message.
type Message struct {
	// Data holds actual opaque user message data.
	Data []byte

	// Attributes hold user defined attributes (user defined metadata).
	Attributes map[string]string

	// Metadata holds internal metadata (not user defined).
	Metadata Metadata
}

// Metadata represents internal metadata.
type Metadata struct {
	MessageID string `json:"id"`
}

func (m *Message) hasMetadata() bool {
	return m.Metadata.MessageID != ""
}

// EncodeTLV encodes a single record body prefixed by its record length.
func (m *Message) EncodeTLV(w io.Writer) error {
	var attrBytes, metaBytes []byte
	var attrHeader, metaHeader string

	// 1. Prepare Metadata (only if MessageID is set)
	if m.hasMetadata() {
		metaBytes, _ = json.Marshal(m.Metadata)
		metaHeader = fmt.Sprintf("m:%d:j:", len(metaBytes)+2) // include encoding marker "j:"
	}

	// 2. Prepare Attributes (only if map is not empty)
	if len(m.Attributes) > 0 {
		attrBytes, _ = json.Marshal(m.Attributes)
		attrHeader = fmt.Sprintf("a:%d:j:", len(attrBytes)+2) // include encoding marker "j:"
	}

	// 3. Prepare Data (Always included)
	dataHeader := fmt.Sprintf("d:%d:", len(m.Data))

	// 4. Calculate Total Record Length dynamically
	totalLen := len(metaHeader) + len(metaBytes) +
		len(attrHeader) + len(attrBytes) +
		len(dataHeader) + len(m.Data)

	// 5. Write the record length prefix.
	if _, err := fmt.Fprintf(w, "%d:", totalLen); err != nil {
		return err
	}

	// 6. Conditional Writes
	if metaHeader != "" {
		io.WriteString(w, metaHeader)
		w.Write(metaBytes)
	}
	if attrHeader != "" {
		io.WriteString(w, attrHeader)
		w.Write(attrBytes)
	}

	// Always write data
	io.WriteString(w, dataHeader)
	_, err := w.Write(m.Data)

	return err
}
