// Package stream implements a streaming publisher that starts uploading a batch
// as soon as the first message arrives and closes it by age, bytes, or silence.
package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

const (
	// DefaultFlushThresholdAge is the default maximum age of an open stream batch.
	DefaultFlushThresholdAge = pub.DefaultFlushThresholdAge

	// DefaultFlushThresholdBytes is the default byte threshold that closes a stream batch.
	DefaultFlushThresholdBytes = pub.DefaultFlushThresholdBytes

	// DefaultInboxSize is the default number of pending Send requests.
	DefaultInboxSize = 1024
)

// ErrEmptyMessages is returned when SendBatch receives an empty slice.
var ErrEmptyMessages = pub.ErrEmptyMessages

// ErrClosed is returned when SendBatch is called after Close.
var ErrClosed = errors.New("stream publisher is closed")

// Options defines the streaming publisher configuration.
type Options struct {
	Storage pulsix.Storage
	Prefix  string

	// GenerateIDFunc optionally injects a metadata ID into every message.
	GenerateIDFunc func() string

	// FlushThresholdAge closes the active batch after this duration since the
	// first message in the batch. Zero uses DefaultFlushThresholdAge.
	FlushThresholdAge time.Duration

	// FlushThresholdBytes closes the active batch after this many user payload
	// bytes have been written. Zero uses DefaultFlushThresholdBytes.
	FlushThresholdBytes int64

	// FlushThresholdSilence closes the active batch after this much idle time
	// since the last SendBatch append. Zero disables the silence trigger.
	FlushThresholdSilence time.Duration

	// InboxSize bounds concurrent Send requests waiting for the internal loop.
	InboxSize int
}

// Pub hosts the streaming publish state.
type Pub struct {
	opts Options

	inbox     chan sendRequest
	stopCh    chan struct{}
	loopDone  chan struct{}
	closed    atomic.Bool
	closeOnce sync.Once
	uploadWG  sync.WaitGroup

	errMu sync.Mutex
	err   error
}

type sendRequest struct {
	ctx    context.Context
	msgs   []pulsix.Message
	result chan error
}

type activeBatch struct {
	writer    *io.PipeWriter
	createdAt time.Time
	lastWrite time.Time
	dataBytes int64
}

// New creates a streaming publisher and starts its internal loop.
func New(opts Options) *Pub {
	if opts.FlushThresholdAge == 0 {
		opts.FlushThresholdAge = DefaultFlushThresholdAge
	}
	if opts.FlushThresholdBytes == 0 {
		opts.FlushThresholdBytes = DefaultFlushThresholdBytes
	}
	if opts.InboxSize <= 0 {
		opts.InboxSize = DefaultInboxSize
	}

	p := &Pub{
		opts:     opts,
		inbox:    make(chan sendRequest, opts.InboxSize),
		stopCh:   make(chan struct{}),
		loopDone: make(chan struct{}),
	}

	go p.run()

	return p
}

// SendBatch appends messages to the currently open streaming batch.
// The call returns after all messages have been encoded into the upload stream,
// not after the storage write becomes durable. The batch remains open until one
// of the configured close signals fires or Close is called.
func (p *Pub) SendBatch(ctx context.Context, messages []pulsix.Message) error {
	if len(messages) == 0 {
		return ErrEmptyMessages
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.closed.Load() {
		return ErrClosed
	}

	req := sendRequest{
		ctx:    ctx,
		msgs:   messages,
		result: make(chan error, 1),
	}

	select {
	case p.inbox <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.loopDone:
		if err := p.currentErr(); err != nil {
			return err
		}
		return ErrClosed
	}

	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.loopDone:
		if err := p.currentErr(); err != nil {
			return err
		}
		return ErrClosed
	}
}

// Close flushes the active batch and waits for all background uploads to finish.
func (p *Pub) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		close(p.stopCh)
		<-p.loopDone
		p.uploadWG.Wait()
	})

	return p.currentErr()
}

func (p *Pub) run() {
	defer close(p.loopDone)

	var (
		batch        *activeBatch
		headerBuf    = make([]byte, 0, 128)
		ageTimer     *time.Timer
		silenceTimer *time.Timer
		ageC         <-chan time.Time
		silenceC     <-chan time.Time
	)

	stopTimer := func(timer **time.Timer, timerC *<-chan time.Time) {
		if *timer == nil {
			*timerC = nil
			return
		}
		if !(*timer).Stop() {
			select {
			case <-(*timer).C:
			default:
			}
		}
		*timerC = nil
	}

	resetTimer := func(timer **time.Timer, timerC *<-chan time.Time, d time.Duration) {
		if d <= 0 {
			stopTimer(timer, timerC)
			return
		}
		if *timer == nil {
			*timer = time.NewTimer(d)
			*timerC = (*timer).C
			return
		}
		if !(*timer).Stop() {
			select {
			case <-(*timer).C:
			default:
			}
		}
		(*timer).Reset(d)
		*timerC = (*timer).C
	}

	stopAllTimers := func() {
		stopTimer(&ageTimer, &ageC)
		stopTimer(&silenceTimer, &silenceC)
	}

	closeBatch := func() error {
		if batch == nil {
			return nil
		}
		err := batch.writer.Close()
		batch = nil
		stopAllTimers()
		if err != nil {
			p.setErr(err)
		}
		return err
	}

	startBatch := func(now time.Time) error {
		key := pulsix.GeneratePulsixKey(p.opts.Prefix)
		reader, writer := io.Pipe()

		p.uploadWG.Go(func() {
			if err := p.opts.Storage.PutObject(context.Background(), key, reader, -1); err != nil {
				p.setErr(err)
			}
		})

		if _, err := io.WriteString(writer, pulsix.VersionP1+":"); err != nil {
			_ = writer.CloseWithError(err)
			p.setErr(err)
			return err
		}

		batch = &activeBatch{
			writer:    writer,
			createdAt: now,
			lastWrite: now,
		}

		resetTimer(&ageTimer, &ageC, p.opts.FlushThresholdAge)
		resetTimer(&silenceTimer, &silenceC, p.opts.FlushThresholdSilence)

		return nil
	}

	rejectPending := func(err error) {
		for {
			select {
			case req := <-p.inbox:
				req.result <- err
			default:
				return
			}
		}
	}

	for {
		select {
		case <-p.stopCh:
			_ = closeBatch()
			rejectPending(ErrClosed)
			stopAllTimers()
			return

		case <-ageC:
			_ = closeBatch()

		case <-silenceC:
			_ = closeBatch()

		case req := <-p.inbox:
			if err := p.currentErr(); err != nil {
				req.result <- err
				continue
			}

			var reqErr error
			for _, msg := range req.msgs {
				now := time.Now()
				if batch == nil {
					if err := startBatch(now); err != nil {
						reqErr = err
						break
					}
				}

				if p.opts.GenerateIDFunc != nil {
					msg.Metadata.MessageID = p.opts.GenerateIDFunc()
				}

				if err := msg.EncodeTLV(batch.writer, headerBuf); err != nil {
					_ = batch.writer.CloseWithError(err)
					batch = nil
					stopAllTimers()
					p.setErr(err)
					reqErr = err
					break
				}

				batch.lastWrite = now
				batch.dataBytes += int64(len(msg.Data))
				resetTimer(&silenceTimer, &silenceC, p.opts.FlushThresholdSilence)

				if p.opts.FlushThresholdBytes > 0 && batch.dataBytes >= p.opts.FlushThresholdBytes {
					if err := closeBatch(); err != nil {
						reqErr = err
						break
					}
				}
			}

			if reqErr != nil {
				req.result <- reqErr
				continue
			}

			req.result <- p.currentErr()
		}
	}
}

func (p *Pub) setErr(err error) {
	if err == nil {
		return
	}
	p.errMu.Lock()
	defer p.errMu.Unlock()
	if p.err == nil {
		p.err = err
	}
}

func (p *Pub) currentErr() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}
