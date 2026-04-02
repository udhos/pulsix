// Package pub implements pulsix producer/publisher functionality.
package pub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/udhos/pulsix/pulsix"
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
func (p *Pub) SendBatch(ctx context.Context, messages [][]byte) error {
	if len(messages) == 0 {
		return ErrEmptyMessages // empty batch is not allowed
	}

	// 1. Generate a unique key (e.g., UUID or Timestamp)
	key := generatePulsixKey(p.options.Prefix)

	// 2. Wrap the messages in a MultiReader (Zero-copy)
	// We create a slice of readers: [Header, Msg, Header, Msg...]
	readers := make([]io.Reader, 0, len(messages)*2)
	var totalSize int64

	for _, m := range messages {
		header := fmt.Appendf(nil, "%s%d\n", pulsix.HeaderPrefix, len(m))
		readers = append(readers, bytes.NewReader(header))
		readers = append(readers, bytes.NewReader(m))
		totalSize += int64(len(header) + len(m))
	}

	fullStream := io.MultiReader(readers...)

	// 3. Hand off to the Storage layer
	return p.options.Storage.PutObject(ctx, key, fullStream, totalSize)
}
