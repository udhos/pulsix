// Package pub implements pulsix producer/publisher functionality.
package pub

import (
	"context"
	"errors"
	"fmt"
	"io"
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
func (p *Pub) SendBatch(ctx context.Context, messages []pulsix.Message) error {
	if len(messages) == 0 {
		return ErrEmptyMessages
	}

	pr, pw := io.Pipe()

	go func() {
		var err error
		defer func() {
			// Only close with an error if one actually occurred
			pw.CloseWithError(err)
		}()

		for i := range messages {
			// EncodeTLV now handles the p1:len: and all internal TLVs
			if err = messages[i].EncodeTLV(pw); err != nil {
				return
			}
		}
	}()

	key := generatePulsixKey(p.options.Prefix)

	// PutObject handles the stream. Notification happens outside this func
	// or via S3 bucket notification config.
	return p.options.Storage.PutObject(ctx, key, pr, -1)
}
