package pulsix

import (
	"encoding/json"
	"io"
	"strconv"

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
func (m *Message) EncodeTLV(w io.Writer, headerBuf []byte) error {
	var metaBytes, attrBytes []byte
	var err error

	// 1. Marshalling (This is the only part that should allocate)
	if m.hasMetadata() {
		metaBytes, _ = json.Marshal(m.Metadata)
	}
	if len(m.Attributes) > 0 {
		attrBytes, _ = json.Marshal(m.Attributes)
	}

	headerBuf = headerBuf[:0]

	// Calculate Metadata block size
	metaTotal := 0
	if len(metaBytes) > 0 {
		headerBuf = headerBuf[:0]
		headerBuf = append(headerBuf, 'm', ':')
		headerBuf = strconv.AppendInt(headerBuf, int64(len(metaBytes)+2), 10)
		headerBuf = append(headerBuf, ':', 'j', ':')
		metaTotal = len(headerBuf) + len(metaBytes)
	}

	// Calculate Attribute block size
	attrTotal := 0
	if len(attrBytes) > 0 {
		headerBuf = headerBuf[:0]
		headerBuf = append(headerBuf, 'a', ':')
		headerBuf = strconv.AppendInt(headerBuf, int64(len(attrBytes)+2), 10)
		headerBuf = append(headerBuf, ':', 'j', ':')
		attrTotal = len(headerBuf) + len(attrBytes)
	}

	// Calculate Data block size
	headerBuf = headerBuf[:0]
	headerBuf = append(headerBuf, 'd', ':')
	headerBuf = strconv.AppendInt(headerBuf, int64(len(m.Data)), 10)
	headerBuf = append(headerBuf, ':')
	dataTotal := len(headerBuf) + len(m.Data)

	// 3. The true Total Length
	totalLen := metaTotal + attrTotal + dataTotal

	// 4. EXECUTION - Now we write for real

	// Write Record Prefix "<total>:"
	headerBuf = headerBuf[:0]
	headerBuf = strconv.AppendInt(headerBuf, int64(totalLen), 10)
	headerBuf = append(headerBuf, ':')
	if _, err = w.Write(headerBuf); err != nil {
		return err
	}

	// Write Metadata
	if len(metaBytes) > 0 {
		headerBuf = headerBuf[:0]
		headerBuf = append(headerBuf, 'm', ':')
		headerBuf = strconv.AppendInt(headerBuf, int64(len(metaBytes)+2), 10)
		headerBuf = append(headerBuf, ':', 'j', ':')
		if _, err = w.Write(headerBuf); err != nil {
			return err
		}
		if _, err = w.Write(metaBytes); err != nil {
			return err
		}
	}

	// Write Attributes
	if len(attrBytes) > 0 {
		headerBuf = headerBuf[:0]
		headerBuf = append(headerBuf, 'a', ':')
		headerBuf = strconv.AppendInt(headerBuf, int64(len(attrBytes)+2), 10)
		headerBuf = append(headerBuf, ':', 'j', ':')
		if _, err = w.Write(headerBuf); err != nil {
			return err
		}
		if _, err = w.Write(attrBytes); err != nil {
			return err
		}
	}

	// Write Data
	headerBuf = headerBuf[:0]
	headerBuf = append(headerBuf, 'd', ':')
	headerBuf = strconv.AppendInt(headerBuf, int64(len(m.Data)), 10)
	headerBuf = append(headerBuf, ':')
	if _, err = w.Write(headerBuf); err != nil {
		return err
	}
	_, err = w.Write(m.Data)

	return err
}

// NewReaderFromMessages is a helper function that creates a reader from
// a slice of messages, encoding them in the p1 format.
func NewReaderFromMessages(messages []Message, generateID func() string,
	headerBuf []byte) io.Reader {

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
			if err = m.EncodeTLV(pw, headerBuf); err != nil {
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
