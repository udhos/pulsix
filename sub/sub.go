// Package sub implements pulsix consumer/subscriber functionality.
package sub

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/udhos/pulsix/pulsix"
)

// Sub is the main struct for the pulsix subscriber. It provides methods to receive messages from S3.
type Sub struct {
	options Options
}

// Notification represents a message notification from the queue.
type Notification interface {
	// GetKey returns the S3 key associated with the notification.
	GetKey() string

	// Delete removes the notification from the queue after processing.
	Delete() error
}

// Queue abstracts the queue-specific calls for receiving notifications.
type Queue interface {
	// ReceiveNotifications returns a list of notifications from the queue.
	ReceiveNotifications(ctx context.Context) ([]Notification, error)
}

// Options defines the configuration for the Sub struct.
type Options struct {
	Prefix  string
	Storage pulsix.Storage
	Queue   Queue
}

// New creates a new Sub instance with the provided Storage implementation.
func New(options Options) *Sub {
	return &Sub{
		options: options,
	}
}

// Receive fetches notifications from the queue, retrieves the corresponding S3 objects,
// and returns a list of Batches for processing.
func (s *Sub) Receive(ctx context.Context) ([]*Batch, error) {
	notifications, err := s.options.Queue.ReceiveNotifications(ctx)
	if err != nil {
		return nil, err
	}

	batches := make([]*Batch, 0, len(notifications))
	for _, n := range notifications {
		rc, err := s.options.Storage.GetObject(ctx, n.GetKey())
		if err != nil {
			return nil, err
		}

		batches = append(batches, &Batch{
			notification: n,
			reader:       rc,
			// Wrap the S3 stream in a buffered reader for line parsing
			bufr: bufio.NewReader(rc),
		})
	}

	return batches, nil
}

// Batch represents a batch of messages read from a single S3 object.
// It provides methods to iterate through the messages.
type Batch struct {
	notification Notification
	reader       io.ReadCloser
	bufr         *bufio.Reader
	current      []byte
	err          error
}

// Next advances the batch to the next message.
func (b *Batch) Next() bool {
	if b.err != nil {
		return false
	}

	// 1. Read the header line (e.g., "PULSIX-SIZE:123\n")
	line, err := b.bufr.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			b.err = fmt.Errorf("failed to read pulsix header: %w", err)
		}
		return false
	}

	// 2. Parse the size from the header
	if !strings.HasPrefix(line, pulsix.HeaderPrefix) {
		b.err = fmt.Errorf("invalid pulsix header format: %s", line)
		return false
	}

	// Trim prefix and the trailing newline
	sizeStr := strings.TrimSpace(line[len(pulsix.HeaderPrefix):])
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		b.err = fmt.Errorf("invalid pulsix size: %w", err)
		return false
	}

	// 3. Read exactly N bytes into the current buffer
	// We reuse b.current to minimize allocations
	if cap(b.current) < size {
		b.current = make([]byte, size)
	} else {
		b.current = b.current[:size]
	}

	_, err = io.ReadFull(b.bufr, b.current)
	if err != nil {
		b.err = fmt.Errorf("failed to read pulsix payload: %w", err)
		return false
	}

	return true
}

// Message returns the most recent message parsed by Next.
// Note: The underlying slice is reused; copy it if you need it to persist.
func (b *Batch) Message() []byte {
	return b.current
}

// Error returns any error encountered during streaming.
func (b *Batch) Error() error {
	return b.err
}

// Done should be called after processing the batch to clean up resources and delete the notification.
func (b *Batch) Done() error {
	var err error
	if b.reader != nil {
		err = b.reader.Close()
	}
	if delErr := b.notification.Delete(); delErr != nil {
		return delErr
	}
	return err
}

// GetKey returns the underlying S3 key for this batch.
// Useful for logging and tracing.
func (b *Batch) GetKey() string {
	if b.notification == nil {
		return ""
	}
	return b.notification.GetKey()
}
