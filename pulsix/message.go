package pulsix

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/segmentio/ksuid"
)

const (
	// VersionP1 is version 1.
	VersionP1 = "p1"
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

	// 1. Prepare Metadata
	if m.hasMetadata() {
		metaBytes, _ = json.Marshal(m.Metadata)
		metaHeader = fmt.Sprintf("m:%d:j:", len(metaBytes)+2) // include encoding marker "j:"
	}

	// 2. Prepare Attributes
	if len(m.Attributes) > 0 {
		attrBytes, _ = json.Marshal(m.Attributes)
		attrHeader = fmt.Sprintf("a:%d:j:", len(attrBytes)+2) // include encoding marker "j:"
	}

	// 3. Prepare Data
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

// NewReaderFromMessages is a helper function that creates a reader from
// a slice of messages, encoding them in the p1 format.
func NewReaderFromMessages(messages []Message, generateID func() string) io.Reader {
	pr, pw := io.Pipe()

	go func() {
		var err error
		defer func() {
			// Only close with an error if one actually occurred
			pw.CloseWithError(err)
		}()

		if _, err = io.WriteString(pw, VersionP1+":"); err != nil {
			return
		}

		for _, m := range messages {
			if generateID != nil {
				m.Metadata.MessageID = generateID()
			}
			if err = m.EncodeTLV(pw); err != nil {
				return
			}
		}
	}()

	return pr
}

// GenerateID generates a unique ID.
func GenerateID() string {
	id, _ := ksuid.NewRandom()
	return id.String()
}
