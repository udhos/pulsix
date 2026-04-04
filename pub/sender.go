package pub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udhos/pulsix/pulsix"
)

const (
	// DefaultFlushThresholdAge is the default maximum age of a batch before flushing.
	DefaultFlushThresholdAge = time.Second

	// DefaultFlushThresholdMessages is the default maximum number of messages per batch.
	DefaultFlushThresholdMessages = 10_000

	// DefaultFlushThresholdBytes is the default maximum byte size of a batch.
	DefaultFlushThresholdBytes = int64(50 * 1024 * 1024) // 50MB

	// DefaultAckChannelSize is the default buffer size for AckChan.
	DefaultAckChannelSize = 100

	// HardFailDeadline is the default retry window before declaring a hard-fail boundary.
	// Keep this default for production; test code may override it to speed up failure-path tests.
	HardFailDeadline = 10 * time.Second
)

// SendOptions extends Options with thresholds for the async Send API.
type SendOptions struct {
	Options

	// FlushThresholdAge is the maximum age of a batch before it is flushed.
	// Defaults to DefaultFlushThresholdAge.
	FlushThresholdAge time.Duration

	// FlushThresholdMessages is the maximum number of messages per batch.
	// Defaults to DefaultFlushThresholdMessages.
	FlushThresholdMessages int

	// FlushThresholdBytes is the maximum size in bytes of a batch.
	// Defaults to DefaultFlushThresholdBytes.
	FlushThresholdBytes int64

	// AckChannelSize is the buffer size for AckChan.
	// Defaults to DefaultAckChannelSize.
	AckChannelSize int

	// HardFailDeadline is the maximum time to retry a failed batch before
	// reporting a hard-fail boundary. Defaults to the package HardFailDeadline.
	// This option exists mainly so tests can reduce waiting time; production code
	// should generally use the default unless there is a clear operational need.
	HardFailDeadline time.Duration
}

// Ack signals that all messages with ID <= AckedUpTo are durably persisted.
type Ack struct {
	// AckedUpTo is the highest ID in this batch. All IDs <= AckedUpTo are durable.
	AckedUpTo uint64

	// Err is set only for terminal hard-fail boundary events.
	// Transient retry failures are never emitted on AckChan.
	Err error
}

// pendingMessage holds a message and its assigned sequence ID.
type pendingMessage struct {
	msg pulsix.Message
	id  uint64
}

// senderState represents the internal state machine of the Sender.
type senderState int

const (
	stateNormal senderState = iota
	stateRetrying
	stateFailed
)

// Sender is the async producer. It accumulates messages into batches and flushes
// them automatically based on time, message count, and byte size thresholds.
// Send is goroutine-safe.
type Sender struct {
	pub       *Pub
	opts      SendOptions
	nextID    atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
	inbox     chan pendingMessage
	ackCh     chan Ack
	done      chan struct{}
	wg        sync.WaitGroup

	// State machine (read/write in flushLoop only, no locking needed)
	state        senderState
	lastAckedID  uint64 // Highest ID after successful flush
	failedBatch  []pendingMessage
	failureStart time.Time
}

// NewSender creates a Sender backed by a Pub instance.
// It starts the background flusher goroutine immediately.
func NewSender(opts SendOptions) *Sender {
	if opts.FlushThresholdAge == 0 {
		opts.FlushThresholdAge = DefaultFlushThresholdAge
	}
	if opts.FlushThresholdMessages == 0 {
		opts.FlushThresholdMessages = DefaultFlushThresholdMessages
	}
	if opts.FlushThresholdBytes == 0 {
		opts.FlushThresholdBytes = DefaultFlushThresholdBytes
	}
	if opts.AckChannelSize == 0 {
		opts.AckChannelSize = DefaultAckChannelSize
	}
	if opts.HardFailDeadline == 0 {
		opts.HardFailDeadline = HardFailDeadline
	}

	s := &Sender{
		pub:   New(opts.Options),
		opts:  opts,
		inbox: make(chan pendingMessage, opts.FlushThresholdMessages*2),
		ackCh: make(chan Ack, opts.AckChannelSize),
		done:  make(chan struct{}),
	}

	s.wg.Add(1)
	go s.flushLoop()

	return s
}

// Send enqueues a message for async delivery.
// It returns a monotonically increasing ID assigned to that message.
// All messages with ID <= the AckedUpTo value in a subsequent Ack are durable.
// Send is goroutine-safe.
func (s *Sender) Send(ctx context.Context, msg pulsix.Message) (uint64, error) {
	if s.closed.Load() {
		return 0, ErrSenderClosed
	}

	id := s.nextID.Add(1) - 1 // 0-indexed

	select {
	case s.inbox <- pendingMessage{msg: msg, id: id}:
		return id, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrSenderClosed
	}
}

// AckChan returns the channel on which batch acknowledgments are delivered.
// Each Ack means all messages with ID <= Ack.AckedUpTo are durably persisted.
// The channel is closed after Close returns.
func (s *Sender) AckChan() <-chan Ack {
	return s.ackCh
}

// Close flushes any pending messages, stops the background flusher,
// and closes the AckChan. Blocks until all in-flight work is complete.
// Terminal hard-fail boundaries are reported on AckChan as Ack{Err: err}.
func (s *Sender) Close() {
	s.closed.Store(true)
	s.closeOnce.Do(func() {
		close(s.done)
	})
	s.wg.Wait()
}

// ErrSenderClosed is returned by Send when the Sender has been closed.
var ErrSenderClosed = errSenderClosed{}

type errSenderClosed struct{}

func (errSenderClosed) Error() string { return "sender is closed" }

// flushLoop is the background goroutine that accumulates messages and flushes
// batches when any threshold is reached. It also handles retry logic for failed batches.
func (s *Sender) flushLoop() {
	defer s.wg.Done()
	defer close(s.ackCh)

	ticker := time.NewTicker(s.opts.FlushThresholdAge)
	defer ticker.Stop()

	var (
		batch     []pendingMessage
		batchSize int64
		attempt   int // Retry attempt count for exponential backoff
	)

	var retryTimer *time.Timer

	stopRetryTimer := func() {
		if retryTimer == nil {
			return
		}
		if !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryTimer = nil
	}

	scheduleRetry := func(delay time.Duration) {
		if retryTimer == nil {
			retryTimer = time.NewTimer(delay)
			return
		}
		if !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryTimer.Reset(delay)
	}

	// calculateBackoff returns the wait duration for the given retry attempt.
	calculateBackoff := func(attempt int) time.Duration {
		base := min(
			// 100ms, 200ms, 400ms, ...
			time.Duration(100<<uint(attempt))*time.Millisecond, 2*time.Second)
		return base
	}

	// flush attempts to send the batch and handles retry/failure logic.
	// Returns true if batch was successfully persisted.
	flush := func(batch []pendingMessage) bool {
		if len(batch) == 0 {
			return true
		}

		// Extract message data and track ID range.
		msgs := make([]pulsix.Message, len(batch))
		maxID := batch[len(batch)-1].id
		for i, pm := range batch {
			msgs[i] = pm.msg
		}

		// Attempt to send.
		err := s.pub.SendBatch(context.Background(), msgs)
		if err == nil {
			// Success: advance watermark and emit ack.
			s.lastAckedID = maxID
			s.state = stateNormal
			s.failedBatch = nil
			s.failureStart = time.Time{}
			stopRetryTimer()
			s.ackCh <- Ack{AckedUpTo: maxID}
			attempt = 0 // Reset retry counter on success.
			return true
		}

		// Failure: enter retry state if not already.
		if s.state == stateNormal {
			s.state = stateRetrying
			s.failedBatch = batch
			s.failureStart = time.Now()
			attempt = 0
			scheduleRetry(calculateBackoff(attempt))
		}

		// Check if we've exceeded the hard fail deadline.
		elapsed := time.Since(s.failureStart)
		if elapsed > s.opts.HardFailDeadline {
			s.state = stateFailed
			stopRetryTimer()
			s.ackCh <- Ack{Err: err}
			return false
		}

		// Still within retry window; prepare for next attempt.
		attempt++
		return false
	}

	// processPendingMessage accumulates a message into the batch.
	processPendingMessage := func(pm pendingMessage) {
		// A hard fail drops the failed batch boundary; new messages start a fresh cycle.
		if s.state == stateFailed {
			s.state = stateNormal
			s.failedBatch = nil
			s.failureStart = time.Time{}
			attempt = 0
			stopRetryTimer()
		}

		// Accumulate message into batch.
		batch = append(batch, pm)
		batchSize += int64(len(pm.msg.Data))

		// Check thresholds for immediate flush.
		if len(batch) >= s.opts.FlushThresholdMessages ||
			batchSize >= s.opts.FlushThresholdBytes {
			if flush(batch) {
				batch = nil
				batchSize = 0
				ticker.Reset(s.opts.FlushThresholdAge)
			} else if s.state == stateFailed {
				return
			}
			// If retrying, keep batch pinned and continue accumulating new messages.
		}
	}

	for {
		var retryC <-chan time.Time
		if retryTimer != nil {
			retryC = retryTimer.C
		}

		select {
		case pm, ok := <-s.inbox:
			if !ok {
				// Inbox closed; drain and flush.
				for {
					select {
					case pm := <-s.inbox:
						processPendingMessage(pm)
					default:
						flush(batch)
						return
					}
				}
			}

			processPendingMessage(pm)

		case <-ticker.C:
			// Time-based flush trigger.
			if s.state == stateNormal {
				if flush(batch) {
					batch = nil
					batchSize = 0
				}
			}

		case <-retryC:
			if s.state == stateRetrying {
				if flush(s.failedBatch) {
					// Do not clear current batch here; it may contain new messages
					// that arrived while the failed batch was being retried.
					ticker.Reset(s.opts.FlushThresholdAge)
				} else if s.state == stateFailed {
					// Drop only the failed retry batch. Keep any already-queued
					// new messages so the sender can continue after the boundary.
					s.failedBatch = nil
					s.failureStart = time.Time{}
					attempt = 0
					stopRetryTimer()
					s.state = stateNormal
					ticker.Reset(s.opts.FlushThresholdAge)
				} else {
					scheduleRetry(calculateBackoff(attempt))
				}
			}

		case <-s.done:
			// Shutdown initiated; flush pending and exit.
			stopRetryTimer()
			for {
				select {
				case pm := <-s.inbox:
					processPendingMessage(pm)
				default:
					flush(batch)
					return
				}
			}
		}
	}
}
