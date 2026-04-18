// Package streaminject implements a helper that consumes messages from
// a channel and forwards them to Pulsix using stream.Pub while handling
// ack/retry bookkeeping.
package streaminject

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/udhos/pulsix/pulsix"
	"github.com/udhos/pulsix/stream"
)

const (
	// DefaultBufferSize is used by New when BufferSize <= 0.
	DefaultBufferSize = 1000

	// DefaultSendBatchSize controls how many messages are sent per SendBatch call.
	DefaultSendBatchSize = 16384
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
	SendBatchSize int
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
	if opts.SendBatchSize <= 0 {
		opts.SendBatchSize = DefaultSendBatchSize
	}
	return &Injector{
		opts: opts,
		C:    make(chan InjectMessage, opts.BufferSize),
	}
}

type pendingWindow struct {
	baseOffset uint64
	msgs       []InjectMessage
}

func (p *pendingWindow) len() int {
	return len(p.msgs)
}

func (p *pendingWindow) append(offset uint64, msgs []InjectMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	if len(p.msgs) == 0 {
		p.baseOffset = offset
		p.msgs = append(p.msgs, msgs...)
		return nil
	}

	expected := p.baseOffset + uint64(len(p.msgs))
	if offset != expected {
		return fmt.Errorf("non-contiguous offset: got=%d expected=%d", offset, expected)
	}

	p.msgs = append(p.msgs, msgs...)
	return nil
}

func (p *pendingWindow) consume(offset, amount uint64) ([]InjectMessage, error) {
	if amount == 0 {
		return nil, nil
	}
	if len(p.msgs) == 0 {
		return nil, fmt.Errorf("ack on empty pending window: offset=%d amount=%d", offset, amount)
	}
	if offset != p.baseOffset {
		return nil, fmt.Errorf("unexpected ack offset: got=%d expected=%d", offset, p.baseOffset)
	}
	if amount > uint64(len(p.msgs)) {
		return nil, fmt.Errorf("ack amount overflow: amount=%d pending=%d", amount, len(p.msgs))
	}

	n := int(amount)
	out := p.msgs[:n]
	p.msgs = p.msgs[n:]
	p.baseOffset += amount
	if len(p.msgs) == 0 {
		p.baseOffset = 0
	}

	return out, nil
}

// Run blocks while C is open, forwards messages using stream.Pub, and handles
// ack/retry bookkeeping until all in-flight messages are settled.
// If the underlying stream publisher encounters a hard failure, the Injector
// will automatically tear it down and recreate it to retry unacknowledged messages.
func (i *Injector) Run() error {
	pendingCap := max(max(i.opts.BufferSize*8, i.opts.SendBatchSize), DefaultBufferSize)

	var (
		ackCh       = make(chan stream.Ack, 4096)
		incoming    = make([]InjectMessage, 0, i.opts.BufferSize)
		retry       = make([]InjectMessage, 0, i.opts.BufferSize)
		pending     = pendingWindow{msgs: make([]InjectMessage, 0, pendingCap)}
		msgBuf      = make([]pulsix.Message, i.opts.SendBatchSize)
		sendBuf     = make([]InjectMessage, i.opts.SendBatchSize)
		inputClosed = false
		currentPub  *stream.Pub
		pubFailed   = false
	)

	ctx := context.Background()
	retryHead := 0
	incomingHead := 0

	callOnAckBatch := func(acked []InjectMessage) (err error) {
		if i.opts.OnAck == nil || len(acked) == 0 {
			return nil
		}
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("ack callback panic: %v", rec)
			}
		}()
		for _, msg := range acked {
			i.opts.OnAck(msg.Receipt)
		}
		return nil
	}

	nextToSend := func(maxValue int) []InjectMessage {
		if maxValue <= 0 {
			return nil
		}

		if retryHead < len(retry) {
			n := min(maxValue, len(retry)-retryHead)
			chunk := retry[retryHead : retryHead+n]
			retryHead += n
			if retryHead == len(retry) {
				retry = retry[:0]
				retryHead = 0
			}
			return chunk
		}

		if incomingHead < len(incoming) {
			n := min(maxValue, len(incoming)-incomingHead)
			chunk := incoming[incomingHead : incomingHead+n]
			incomingHead += n
			if incomingHead == len(incoming) {
				incoming = incoming[:0]
				incomingHead = 0
			}
			return chunk
		}

		return nil
	}

	hasPendingToSend := func() bool {
		return retryHead < len(retry) || incomingHead < len(incoming)
	}

	drainInput := func() {
		if inputClosed || i.C == nil {
			return
		}
		for len(incoming) < cap(incoming) {
			select {
			case msg, ok := <-i.C:
				if !ok {
					inputClosed = true
					i.C = nil
					return
				}
				incoming = append(incoming, msg)
			default:
				return
			}
		}
	}

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
		drainInput()

		if !pubFailed {
			// Push as much as possible from retry/incoming queues to sender in chunks.
			for hasPendingToSend() {
				chunk := nextToSend(i.opts.SendBatchSize)
				if len(chunk) == 0 {
					break
				}

				for j := range chunk {
					msgBuf[j].Data = chunk[j].Data
					sendBuf[j] = chunk[j]
				}

				offset, err := currentPub.SendBatch(ctx, msgBuf[:len(chunk)])
				if err != nil {
					// Publisher is broken. Stop sending, wait for pending acks.
					slog.Error("streaminject send failed", "error", err)
					pubFailed = true
					pubToClose := currentPub
					go func() {
						_ = pubToClose.Close() // Close it to flush remaining things and trigger final acks.
					}()

					// The chunk was not accepted by stream.Pub: enqueue it for retry first.
					retry = append(retry, sendBuf[:len(chunk)]...)
					break
				}

				if err := pending.append(offset, sendBuf[:len(chunk)]); err != nil {
					return err
				}
			}
		}

		// If input is closed and everything is drained, exit.
		if inputClosed && !hasPendingToSend() && pending.len() == 0 {
			return nil
		}

		// If publisher failed and there are no more unacked messages waiting for a response,
		// we can recreate the publisher to resume sending the requeued unsent messages.
		if pubFailed && pending.len() == 0 {
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
			incoming = append(incoming, msg)

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

				requeued, err := pending.consume(ack.Offset, ack.Amount)
				if err != nil {
					return err
				}
				retry = append(retry, requeued...)
			} else {
				// Success Ack
				acked, err := pending.consume(ack.Offset, ack.Amount)
				if err != nil {
					return err
				}
				if err := callOnAckBatch(acked); err != nil {
					slog.Error("streaminject callback failed", "error", err)
					return err
				}
			}
		}
	}
}
