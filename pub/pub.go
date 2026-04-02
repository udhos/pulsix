// Package pub implements pulsix producer/publisher functionality.
package pub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/udhos/pulsix/pulsix"
)

const (
	// VersionP1 is version 1.
	VersionP1 = "p1"

	// TagData is TLV type for user data.
	TagData = 'd'

	// TagMeta is TLV type for internal metadata.
	TagMeta = 'm'

	// TagAttr is TLV type for user attributes.
	TagAttr = 'a'
)

// Pub is the main struct for the pulsix publisher. It provides methods to send messages to S3.
type Pub struct {
	options Options
}

// Options defines the configuration for the Pub struct.
type Options struct {
	Storage pulsix.Storage
	Prefix  string
}

// New creates a new Pub instance with the provided Storage implementation.
func New(options Options) *Pub {
	return &Pub{
		options: options,
	}
}

// generatePulsixKey generate a key in this format:
// <prefix>/YYYY-MM/DD/HH/MM/<id>.batch
func generatePulsixKey(prefix string) string {
	id, _ := ksuid.NewRandom()
	now := time.Now().UTC()
	return fmt.Sprintf("%s/%04d-%02d/%02d/%02d/%02d/%s.batch",
		prefix,
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(),
		id.String())
}

// ErrEmptyMessages is returned when SendBatch is called with an empty slice of messages.
var ErrEmptyMessages = errors.New("no messages to send")

// SendBatch persists a slice of messages to S3 as a single Pulsix batch.
// It returns only after S3 confirms the write (Synchronous Persistence).
// SendBatch encodes messages into the p1 format and sends them to storage.
func (p *Pub) SendBatch(ctx context.Context, messages [][]byte) error {
	if len(messages) == 0 {
		return ErrEmptyMessages
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		for _, msg := range messages {
			// 1. Prepare the Data TLV: "d:<len>:<data>"
			dataLenStr := strconv.Itoa(len(msg))
			tlvBody := fmt.Sprintf("d:%s:", dataLenStr)

			// Total length of the record body (the TLV part)
			totalRecordLen := len(tlvBody) + len(msg)

			// 2. Write the p1 Header: "p1:<total_len>:"
			header := fmt.Sprintf("%s:%d:", VersionP1, totalRecordLen)

			if _, err := pw.Write([]byte(header)); err != nil {
				return
			}

			// 3. Write the TLV Body
			if _, err := pw.Write([]byte(tlvBody)); err != nil {
				return
			}
			if _, err := pw.Write(msg); err != nil {
				return
			}
		}
	}()

	key := generatePulsixKey(p.options.Prefix)

	return p.options.Storage.PutObject(ctx, key, pr, -1)
}
