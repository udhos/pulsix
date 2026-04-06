// Package inject implements a helper that consumes messages from a channel and
// forwards them to Pulsix using pub.Sender while handling ack/retry bookkeeping.
package inject

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

const (
	// DefaultBufferSize is used by New when bufferSize <= 0.
	DefaultBufferSize = 1000
)

// InjectMessage is a message accepted by Injector.
type InjectMessage struct {
	// Receipt is an opaque token supplied by the caller and echoed back through AckFunc.
	Receipt string
	// Data is the raw payload to be sent to Pulsix.
	Data []byte
}

// AckFunc is called after a message is moved from Unacked to Acked.
// The callback runs synchronously in Run().
type AckFunc func(receipt string)

// Injector forwards messages from C to a Sender and tracks ack/retry state.
//
// Ownership model (v1):
// 1. Caller writes messages to C and closes C when input ends.
// 2. Caller runs Run() and waits until it returns.
// 3. Caller closes sender only after Run() returns.
type Injector struct {
	sender *pub.Sender
	onAck  AckFunc

	// C is the input channel consumed by Run.
	C chan InjectMessage
}

// New creates an Injector with its own buffered input channel C.
func New(sender *pub.Sender, onAck AckFunc, bufferSize int) *Injector {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &Injector{
		sender: sender,
		onAck:  onAck,
		C:      make(chan InjectMessage, bufferSize),
	}
}

// Run blocks while C is open, forwards messages using Sender.Send, and handles
// ack/retry bookkeeping until all in-flight messages are settled.
func (i *Injector) Run() error {
	if i == nil {
		return fmt.Errorf("injector is nil")
	}
	if i.sender == nil {
		return fmt.Errorf("sender is nil")
	}

	ackCh := i.sender.AckChan()
	unsent := make([]InjectMessage, 0)
	unacked := make(map[uint64]InjectMessage)
	inputClosed := false

	for {
		// Push as much as possible from unsent to sender.
		for len(unsent) > 0 {
			msg := unsent[0]
			senderID, err := i.sender.Send(context.Background(), pulsix.Message{Data: msg.Data})
			if err != nil {
				slog.Error("inject send failed", "error", err)
				return fmt.Errorf("inject send failed: %w", err)
			}
			unacked[senderID] = msg
			unsent = unsent[1:]
		}

		if inputClosed && len(unsent) == 0 && len(unacked) == 0 {
			return nil
		}

		select {
		case msg, ok := <-i.C:
			if !ok {
				inputClosed = true
				continue
			}
			unsent = append(unsent, msg)

		case ack, ok := <-ackCh:
			if !ok {
				return fmt.Errorf("sender ack channel closed before injector drained")
			}

			if ack.Err != nil {
				for _, msg := range unacked {
					unsent = append(unsent, msg)
				}
				clear(unacked)
				continue
			}

			for id, msg := range unacked {
				if id > ack.AckedUpTo {
					continue
				}
				delete(unacked, id)
				if err := i.safeOnAck(msg.Receipt); err != nil {
					slog.Error("inject callback failed", "error", err)
					return err
				}
			}
		}
	}
}

func (i *Injector) safeOnAck(receipt string) (err error) {
	if i.onAck == nil {
		return nil
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("ack callback panic: %v", rec)
		}
	}()
	i.onAck(receipt)
	return nil
}
