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
	Delete(ctx context.Context) error
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
	currentMeta  string
	currentAttr  string
	err          error
}

// Next advances the batch to the next message.
func (b *Batch) Next() bool {
	if b.err != nil {
		return false
	}

	// 1. Read Version Prefix (e.g., "p1:")
	prefix, err := b.bufr.ReadString(':')
	if err != nil {
		if err != io.EOF {
			b.err = fmt.Errorf("failed to read version: %w", err)
		}
		return false
	}
	if prefix != "p1:" {
		b.err = fmt.Errorf("unexpected version: %q", prefix)
		return false
	}

	// 2. Read Total Record Length
	lenStr, err := b.bufr.ReadString(':')
	if err != nil {
		b.err = fmt.Errorf("failed to read record length: %w", err)
		return false
	}
	totalLen, err := strconv.Atoi(strings.TrimSuffix(lenStr, ":"))
	if err != nil {
		b.err = fmt.Errorf("invalid record length: %w", err)
		return false
	}

	// 3. Read the entire record body into memory based on totalLen
	recordBody := make([]byte, totalLen)
	_, err = io.ReadFull(b.bufr, recordBody)
	if err != nil {
		b.err = fmt.Errorf("payload read error (truncated record): %w", err)
		return false
	}

	// 4. Parse TLVs inside the record body
	return b.parseTLVs(recordBody)
}

func (b *Batch) parseTLVs(data []byte) bool {
	pos := 0
	found := false

	// Reset current fields for the new record
	b.current = nil
	b.currentMeta = ""
	b.currentAttr = ""

	for pos < len(data) {
		// 1. Ensure we have at least "T:" (2 bytes)
		if pos+2 > len(data) {
			break
		}

		tag := data[pos]
		if data[pos+1] != ':' {
			break // Malformed: missing colon after tag
		}
		pos += 2

		// 2. Extract the length (find the next colon)
		endLen := pos
		for endLen < len(data) && data[endLen] != ':' {
			endLen++
		}
		if endLen >= len(data) {
			break // Malformed: missing colon after length
		}

		valLen, err := strconv.Atoi(string(data[pos:endLen]))
		if err != nil {
			break // Malformed: length is not an integer
		}
		pos = endLen + 1

		// 3. Extract the value
		if pos+valLen > len(data) {
			break // Malformed: value length exceeds remaining data
		}

		value := data[pos : pos+valLen]
		pos += valLen

		// 4. Assign based on tag
		switch tag {
		case 'd':
			b.current = value
			found = true
		case 'm':
			b.currentMeta = string(value)
		case 'a':
			b.currentAttr = string(value)
		default:
			// Unknown tags (like 'z') are ignored but correctly skipped
			// because we already advanced 'pos' by 'valLen'
		}
	}

	return found
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
	var closeErr error
	if b.reader != nil {
		closeErr = b.reader.Close()
	}

	// Use the context from the batch if possible, or pass it in
	delErr := b.notification.Delete(context.Background())

	if delErr != nil {
		return fmt.Errorf("sqs delete failed: %w", delErr)
	}
	return closeErr
}

// GetKey returns the underlying S3 key for this batch.
// Useful for logging and tracing.
func (b *Batch) GetKey() string {
	if b.notification == nil {
		return ""
	}
	return b.notification.GetKey()
}
