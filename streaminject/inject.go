// Package streaminject implements a helper that consumes messages from
// a channel and forwards them to Pulsix using stream.Pub while handling
// ack/retry bookkeeping.
package streaminject

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/udhos/pulsix/pulsix"
	"github.com/udhos/pulsix/stream"
)

const (
	// DefaultBufferSize is used by New when BufferSize <= 0.
	DefaultBufferSize = 1000
)

// InjectMessage is a message accepted by Injector.
type InjectMessage struct {
	// Receipt is an opaque token supplied by the caller and echoed back through AckFunc.
	Receipt string
	// Data is the raw payload to be sent to Pulsix.
	Data []byte
}

// AckFunc is called after a message is successfully durably stored.
// The callback runs synchronously in Run().
type AckFunc func(receipt string)

// Options configuration for the Injector.
type Options struct {
	// StreamOptions are the base options for creating a stream.Pub.
	// The Injector will override AckCh.
	StreamOptions stream.Options
	BufferSize    int
	OnAck         AckFunc
}

// Injector forwards messages from C to a stream.Pub and tracks ack/retry state.
//
// Ownership model:
// 1. Caller writes messages to C and closes C when input ends.
// 2. Caller runs Run() and waits until it returns.
type Injector struct {
	opts Options

	// C is the input channel consumed by Run.
	C chan InjectMessage
}

// New creates an Injector with its own buffered input channel C.
func New(opts Options) *Injector {
	if opts.BufferSize <= 0 {
		opts.BufferSize = DefaultBufferSize
	}
	return &Injector{
		opts: opts,
		C:    make(chan InjectMessage, opts.BufferSize),
	}
}

// Run blocks while C is open, forwards messages using stream.Pub, and handles
// ack/retry bookkeeping until all in-flight messages are settled.
// If the underlying stream publisher encounters a hard failure, the Injector
// will automatically tear it down and recreate it to retry unacknowledged messages.
func (i *Injector) Run() error {
	var (
		ackCh       = make(chan stream.Ack, 100)
		unsent      = make([]InjectMessage, 0)
		unacked     = make(map[uint64]InjectMessage) // Maps offset -> InjectMessage
		inputClosed = false
		currentPub  *stream.Pub
		pubFailed   = false
	)

	createPub := func() {
		streamOpts := i.opts.StreamOptions
		streamOpts.AckCh = ackCh
		currentPub = stream.New(streamOpts)
		pubFailed = false
	}

	createPub()
	defer func() {
		if currentPub != nil {
			_ = currentPub.Close()
		}
	}()

	for {
		if !pubFailed {
			// Push as much as possible from unsent to sender in chunks.
			// Batching amortizes the cost of the SendBatch channel synchronization.
			for len(unsent) > 0 {
				chunkSize := min(len(unsent), 1000)

				msgs := make([]pulsix.Message, chunkSize)
				for i := 0; i < chunkSize; i++ {
					msgs[i] = pulsix.Message{Data: unsent[i].Data}
				}

				offset, err := currentPub.SendBatch(context.Background(), msgs)
				if err != nil {
					// Publisher is broken. Stop sending, wait for pending acks.
					slog.Error("streaminject send failed", "error", err)
					pubFailed = true
					pubToClose := currentPub
					go func() {
						_ = pubToClose.Close() // Close it to flush remaining things and trigger final acks.
					}()
					break
				}

				for i := 0; i < chunkSize; i++ {
					unacked[offset+uint64(i)] = unsent[i]
				}
				unsent = unsent[chunkSize:]
			}
		}

		// If input is closed and everything is drained, exit.
		if inputClosed && len(unsent) == 0 && len(unacked) == 0 {
			return nil
		}

		// If publisher failed and there are no more unacked messages waiting for a response,
		// we can recreate the publisher to resume sending the requeued unsent messages.
		if pubFailed && len(unacked) == 0 {
			// Backoff before recreating to avoid spinning tightly on failures
			time.Sleep(1 * time.Second)
			createPub()
			continue
		}

		select {
		case msg, ok := <-i.C:
			if !ok {
				inputClosed = true
				i.C = nil // prevent busy loop on closed channel
				continue
			}
			unsent = append(unsent, msg)

		case ack := <-ackCh:
			if ack.Err != nil {
				// The batch failed durably.
				if !pubFailed {
					pubFailed = true
					pubToClose := currentPub
					go func() {
						_ = pubToClose.Close()
					}()
				}

				// Find all unacked messages that fall into this failed batch's offset range.
				var failedOffsets []uint64
				for off := range unacked {
					if off >= ack.Offset && off < ack.Offset+ack.Amount {
						failedOffsets = append(failedOffsets, off)
					}
				}

				// Sort offsets to preserve original message order during requeue
				slices.Sort(failedOffsets)

				var requeued []InjectMessage
				for _, off := range failedOffsets {
					requeued = append(requeued, unacked[off])
					delete(unacked, off)
				}

				// Requeue failed messages at the FRONT of the unsent list
				unsent = append(requeued, unsent...)
			} else {
				// Success Ack
				for off := ack.Offset; off < ack.Offset+ack.Amount; off++ {
					if msg, exists := unacked[off]; exists {
						delete(unacked, off)
						if err := i.safeOnAck(msg.Receipt); err != nil {
							slog.Error("streaminject callback failed", "error", err)
							return err
						}
					}
				}
			}
		}
	}
}

func (i *Injector) safeOnAck(receipt string) (err error) {
	if i.opts.OnAck == nil {
		return nil
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("ack callback panic: %v", rec)
		}
	}()
	i.opts.OnAck(receipt)
	return nil
}
