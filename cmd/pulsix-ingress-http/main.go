// Package main implements an ingress model that accepts HTTP requests
// and injects them into Pulsix using the Sender API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/pulsix/inject"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

type ingressConfig struct {
	bucket          string
	prefix          string
	listenAddr      string
	flushInterval   time.Duration
	flushBytes      int64
	ackChannelSize  int
	inboxSize       int
	injectBuffer    int
	shutdownTimeout time.Duration
	readHeaderDelay time.Duration
}

type ingressStats struct {
	Requests      atomic.Uint64
	Accepted      atomic.Uint64
	Acked         atomic.Uint64
	Rejected      atomic.Uint64
	Failed        atomic.Uint64
	BytesReceived atomic.Uint64
}

type statsSnapshot struct {
	Requests      uint64 `json:"requests"`
	Accepted      uint64 `json:"accepted"`
	Acked         uint64 `json:"acked"`
	Rejected      uint64 `json:"rejected"`
	Failed        uint64 `json:"failed"`
	BytesReceived uint64 `json:"bytes_received"`
	Inflight      int    `json:"inflight"`
	Healthy       bool   `json:"healthy"`
}

type responseBody struct {
	Receipt string `json:"receipt,omitempty"`
	Bytes   int    `json:"bytes,omitempty"`
	Durable bool   `json:"durable,omitempty"`
	Status  string `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

type receiptTracker struct {
	mu      sync.Mutex
	waiters map[string]chan error
	failed  error
}

type ingressHTTP struct {
	ctx      context.Context
	injectCh chan<- inject.InjectMessage
	tracker  *receiptTracker
	stats    *ingressStats
}

func getenv(name, fallback string) string {
	if raw := os.Getenv(name); raw != "" {
		return raw
	}
	return fallback
}

func getenvDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", name, raw, err)
	}
	return d
}

func getenvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", name, raw, err)
	}
	return v
}

func getenvInt64(name string, fallback int64) int64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", name, raw, err)
	}
	return v
}

func loadConfig() ingressConfig {
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		log.Fatal("BUCKET environment variable is required")
	}

	return ingressConfig{
		bucket:          bucket,
		prefix:          getenv("PREFIX", "events"),
		listenAddr:      getenv("LISTEN_ADDR", ":8080"),
		flushInterval:   getenvDuration("BATCH_INTERVAL", pub.DefaultFlushThresholdAge),
		flushBytes:      getenvInt64("BATCH_BYTES", pub.DefaultFlushThresholdBytes),
		ackChannelSize:  getenvInt("ACK_CHANNEL_SIZE", pub.DefaultAckChannelSize),
		inboxSize:       getenvInt("INBOX_SIZE", pub.DefaultInboxSize),
		injectBuffer:    getenvInt("INJECT_BUFFER_SIZE", inject.DefaultBufferSize),
		shutdownTimeout: getenvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		readHeaderDelay: getenvDuration("READ_HEADER_TIMEOUT", 5*time.Second),
	}
}

func newReceiptTracker() *receiptTracker {
	return &receiptTracker{waiters: make(map[string]chan error)}
}

func (t *receiptTracker) register(receipt string) (<-chan error, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failed != nil {
		return nil, t.failed
	}
	ch := make(chan error, 1)
	t.waiters[receipt] = ch
	return ch, nil
}

func (t *receiptTracker) unregister(receipt string) {
	t.mu.Lock()
	delete(t.waiters, receipt)
	t.mu.Unlock()
}

func (t *receiptTracker) ack(receipt string) {
	t.mu.Lock()
	ch, ok := t.waiters[receipt]
	if ok {
		delete(t.waiters, receipt)
	}
	t.mu.Unlock()
	if !ok {
		return
	}
	ch <- nil
	close(ch)
}

func (t *receiptTracker) fail(err error) {
	t.mu.Lock()
	if t.failed == nil {
		t.failed = err
	}
	failed := t.failed
	waiters := t.waiters
	t.waiters = make(map[string]chan error)
	t.mu.Unlock()

	for _, ch := range waiters {
		ch <- failed
		close(ch)
	}
}

func (t *receiptTracker) failedErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failed
}

func (t *receiptTracker) inflight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.waiters)
}

func newIngressHTTP(ctx context.Context, injectCh chan<- inject.InjectMessage, tracker *receiptTracker, stats *ingressStats) *ingressHTTP {
	return &ingressHTTP{
		ctx:      ctx,
		injectCh: injectCh,
		tracker:  tracker,
		stats:    stats,
	}
}

func (s *ingressHTTP) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/messages", s.handleMessages)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	return mux
}

func (s *ingressHTTP) handleMessages(w http.ResponseWriter, r *http.Request) {
	s.stats.Requests.Add(1)

	if r.Method != http.MethodPost {
		s.reject(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := s.tracker.failedErr(); err != nil {
		s.fail(w, http.StatusServiceUnavailable, err)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		s.reject(w, http.StatusBadRequest, "unable to read request body")
		return
	}

	receipt := pulsix.GenerateID()
	waitCh, err := s.tracker.register(receipt)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err)
		return
	}

	msg := inject.InjectMessage{Receipt: receipt, Data: payload}

	select {
	case s.injectCh <- msg:
		s.stats.Accepted.Add(1)
		s.stats.BytesReceived.Add(uint64(len(payload)))
	case <-r.Context().Done():
		s.tracker.unregister(receipt)
		return
	case <-s.ctx.Done():
		s.tracker.unregister(receipt)
		if err := s.tracker.failedErr(); err != nil {
			s.fail(w, http.StatusServiceUnavailable, err)
			return
		}
		s.fail(w, http.StatusServiceUnavailable, context.Cause(s.ctx))
		return
	}

	select {
	case ackErr := <-waitCh:
		if ackErr != nil {
			s.fail(w, http.StatusServiceUnavailable, ackErr)
			return
		}
		writeJSON(w, http.StatusOK, responseBody{
			Receipt: receipt,
			Bytes:   len(payload),
			Durable: true,
			Status:  "stored",
		})
	case <-r.Context().Done():
		s.tracker.unregister(receipt)
		return
	case <-s.ctx.Done():
		s.tracker.unregister(receipt)
		if err := s.tracker.failedErr(); err != nil {
			s.fail(w, http.StatusServiceUnavailable, err)
			return
		}
		s.fail(w, http.StatusServiceUnavailable, context.Cause(s.ctx))
	}
}

func (s *ingressHTTP) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.reject(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := s.tracker.failedErr(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, responseBody{
			Status: "degraded",
			Error:  err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, responseBody{Status: "ok"})
}

func (s *ingressHTTP) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.reject(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, statsSnapshot{
		Requests:      s.stats.Requests.Load(),
		Accepted:      s.stats.Accepted.Load(),
		Acked:         s.stats.Acked.Load(),
		Rejected:      s.stats.Rejected.Load(),
		Failed:        s.stats.Failed.Load(),
		BytesReceived: s.stats.BytesReceived.Load(),
		Inflight:      s.tracker.inflight(),
		Healthy:       s.tracker.failedErr() == nil,
	})
}

func (s *ingressHTTP) reject(w http.ResponseWriter, status int, message string) {
	s.stats.Rejected.Add(1)
	writeJSON(w, status, responseBody{Error: message})
}

func (s *ingressHTTP) fail(w http.ResponseWriter, status int, err error) {
	s.stats.Failed.Add(1)
	message := "service unavailable"
	if err != nil {
		message = err.Error()
	}
	writeJSON(w, status, responseBody{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write json response failed: %v", err)
	}
}

func main() {
	cfg := loadConfig()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsConfig)
	store := pulsix.NewS3Storage(s3Client, cfg.bucket)
	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: store,
			Prefix:  cfg.prefix,
		},
		FlushThresholdAge:   cfg.flushInterval,
		FlushThresholdBytes: cfg.flushBytes,
		AckChannelSize:      cfg.ackChannelSize,
		InboxSize:           cfg.inboxSize,
	})

	tracker := newReceiptTracker()
	stats := &ingressStats{}

	inj := inject.New(sender, func(receipt string) {
		stats.Acked.Add(1)
		tracker.ack(receipt)
	}, cfg.injectBuffer)

	injectErr := make(chan error, 1)
	go func() {
		err := inj.Run()
		if err != nil {
			tracker.fail(err)
			cancel()
		}
		injectErr <- err
	}()

	app := newIngressHTTP(ctx, inj.C, tracker, stats)
	server := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: cfg.readHeaderDelay,
	}

	serveErr := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			tracker.fail(err)
			cancel()
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	log.Printf("pulsix-ingress-http starting: bucket=%s prefix=%s listen_addr=%s flush_interval=%s flush_bytes=%d inject_buffer=%d",
		cfg.bucket,
		cfg.prefix,
		cfg.listenAddr,
		cfg.flushInterval,
		cfg.flushBytes,
		cfg.injectBuffer,
	)

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("http shutdown failed: %v", err)
	}

	close(inj.C)

	if err := <-injectErr; err != nil {
		log.Printf("inject run error: %v", err)
	}

	sender.Close()

	if err := <-serveErr; err != nil {
		log.Printf("http serve error: %v", err)
	}

	log.Printf("final stats: requests=%d accepted=%d acked=%d rejected=%d failed=%d bytes_received=%d inflight=%d",
		stats.Requests.Load(),
		stats.Accepted.Load(),
		stats.Acked.Load(),
		stats.Rejected.Load(),
		stats.Failed.Load(),
		stats.BytesReceived.Load(),
		tracker.inflight(),
	)
}
