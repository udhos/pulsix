// Package pub implements pulsix producer/publisher functionality.
package pub

import (
	"context"
	"errors"

	"github.com/udhos/pulsix/pulsix"
)

// Pub is the main struct for the pulsix publisher. It provides methods to send messages to S3.
type Pub struct {
	options Options
}

// Options defines the configuration for the Pub struct.
type Options struct {
	Storage        pulsix.Storage
	Prefix         string
	GenerateIDFunc func() string
}

// New creates a new Pub instance with the provided Storage implementation.
func New(options Options) *Pub {
	if options.GenerateIDFunc == nil {
		options.GenerateIDFunc = pulsix.GenerateID
	}
	return &Pub{
		options: options,
	}
}

// ErrEmptyMessages is returned when SendBatch is called with an empty slice of messages.
var ErrEmptyMessages = errors.New("no messages to send")

// SendBatch persists a slice of messages to S3 as a single Pulsix batch.
// It returns only after S3 confirms the write (Synchronous Persistence).
// SendBatch encodes messages into the p1 format and sends them to storage.
func (p *Pub) SendBatch(ctx context.Context, messages []pulsix.Message,
	headerBuf []byte) error {

	if len(messages) == 0 {
		return ErrEmptyMessages
	}

	reader := pulsix.NewReaderFromMessages(messages, p.options.GenerateIDFunc,
		headerBuf)

	key := pulsix.GeneratePulsixKey(p.options.Prefix)

	// PutObject handles the stream. Notification happens outside this func
	// or via S3 bucket notification config.
	return p.options.Storage.PutObject(ctx, key, reader, -1)
}
