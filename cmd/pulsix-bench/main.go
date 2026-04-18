// Package main provides an end-to-end benchmark tool for Pulsix.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/segmentio/ksuid"
	"github.com/udhos/pulsix/inject"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
	"github.com/udhos/pulsix/sub"
)

type configFlags struct {
	backend string

	messages    int
	payloadSize int
	prefix      string
	timeout     time.Duration

	flushAge     time.Duration
	flushBytes   int64
	ackBuffer    int
	hardFail     time.Duration
	senderDebug  bool
	disableMsgID bool

	injectBuffer int

	consumerIdleSleep time.Duration
	milestones        bool

	awsBucket   string
	awsQueueURL string

	simBaseDir string
	simKeepDir bool
}

type runCounters struct {
	produced   atomic.Uint64
	acked      atomic.Uint64
	consumed   atomic.Uint64
	duplicates atomic.Uint64
	bytesSent  atomic.Uint64
	bytesWire  atomic.Uint64
	batches    atomic.Uint64
}

type latencyStats struct {
	values []int64
	sum    int64
	min    int64
	max    int64
}

func (l *latencyStats) add(ns int64) {
	if ns < 0 {
		ns = 0
	}
	if len(l.values) == 0 {
		l.min = ns
		l.max = ns
	} else {
		if ns < l.min {
			l.min = ns
		}
		if ns > l.max {
			l.max = ns
		}
	}
	l.values = append(l.values, ns)
	l.sum += ns
}

func (l *latencyStats) quantile(q float64) time.Duration {
	if len(l.values) == 0 {
		return 0
	}
	idx := max(int(float64(len(l.values)-1)*q), 0)
	if idx >= len(l.values) {
		idx = len(l.values) - 1
	}
	return time.Duration(l.values[idx])
}

type benchBackend struct {
	storage pulsix.Storage
	queue   sub.Queue
	cleanup func()
}

type milestoneLogger struct {
	enabled bool
}

func (m milestoneLogger) logf(format string, args ...any) {
	if !m.enabled {
		return
	}
	fmt.Printf("%s - %s\n", time.Now().Format(time.RFC3339Nano), fmt.Sprintf(format, args...))
}

type storageWithMilestones struct {
	inner pulsix.Storage
	ms    milestoneLogger
	track *downloadTracker
}

func (s storageWithMilestones) PutObject(ctx context.Context, key string, r io.Reader, contentLength int64) error {
	start := time.Now()
	err := s.inner.PutObject(ctx, key, r, contentLength)
	if err != nil {
		s.ms.logf("batch %s upload failed in %s: %v", key, time.Since(start), err)
		return err
	}
	s.ms.logf("batch %s upload completed in %s", key, time.Since(start))
	return nil
}

func (s storageWithMilestones) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.inner.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if s.track != nil {
		s.track.markGetStart(key)
	}
	return &timedReadCloser{inner: rc, key: key, track: s.track}, nil
}

type queueWithMilestones struct {
	inner sub.Queue
	ms    milestoneLogger
	track *downloadTracker
}

func (q queueWithMilestones) ReceiveNotifications(ctx context.Context) ([]sub.Notification, error) {
	notifications, err := q.inner.ReceiveNotifications(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range notifications {
		if q.track != nil {
			q.track.markNotified(n.GetKey())
		}
		q.ms.logf("recv SQS notification for batch %s", n.GetKey())
	}
	return notifications, nil
}

type batchIOStats struct {
	notifiedAt time.Time
	getStart   time.Time
	firstRead  time.Time
	eofAt      time.Time
	closedAt   time.Time
	bytesRead  int64
}

type downloadTracker struct {
	mu    sync.Mutex
	stats map[string]*batchIOStats
}

func newDownloadTracker() *downloadTracker {
	return &downloadTracker{stats: make(map[string]*batchIOStats)}
}

func (d *downloadTracker) getOrCreate(key string) *batchIOStats {
	s, ok := d.stats[key]
	if !ok {
		s = &batchIOStats{}
		d.stats[key] = s
	}
	return s
}

func (d *downloadTracker) markNotified(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.getOrCreate(key).notifiedAt = time.Now()
}

func (d *downloadTracker) markGetStart(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.getOrCreate(key).getStart = time.Now()
}

func (d *downloadTracker) markRead(key string, n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.getOrCreate(key)
	if n > 0 {
		s.bytesRead += int64(n)
		if s.firstRead.IsZero() {
			s.firstRead = time.Now()
		}
	}
	if errors.Is(err, io.EOF) && s.eofAt.IsZero() {
		s.eofAt = time.Now()
	}
}

func (d *downloadTracker) markClosed(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.getOrCreate(key)
	if s.closedAt.IsZero() {
		s.closedAt = time.Now()
	}
}

func (d *downloadTracker) pop(key string) (batchIOStats, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.stats[key]
	if !ok {
		return batchIOStats{}, false
	}
	delete(d.stats, key)
	return *s, true
}

type timedReadCloser struct {
	inner io.ReadCloser
	key   string
	track *downloadTracker
}

func (t *timedReadCloser) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if t.track != nil {
		t.track.markRead(t.key, n, err)
	}
	return n, err
}

func (t *timedReadCloser) Close() error {
	err := t.inner.Close()
	if t.track != nil {
		t.track.markClosed(t.key)
	}
	return err
}

func main() {
	cfg := parseFlags()
	if err := validateFlags(cfg); err != nil {
		log.Fatal(err)
	}

	ms := milestoneLogger{enabled: cfg.milestones}
	tracker := newDownloadTracker()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	backend, err := buildBackend(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	// Keep tracking always on for accurate wire-byte throughput.
	// Logging still depends on -milestones.
	backend.storage = storageWithMilestones{inner: backend.storage, ms: ms, track: tracker}
	backend.queue = queueWithMilestones{inner: backend.queue, ms: ms, track: tracker}
	if backend.cleanup != nil {
		defer backend.cleanup()
	}

	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: backend.storage,
			Prefix:  cfg.prefix,
			GenerateIDFunc: func() string {
				if cfg.disableMsgID {
					return ""
				}
				return pulsix.GenerateID()
			},
		},
		FlushThresholdAge:   cfg.flushAge,
		FlushThresholdBytes: cfg.flushBytes,
		AckChannelSize:      cfg.ackBuffer,
		HardFailDeadline:    cfg.hardFail,
		Debug:               cfg.senderDebug,
	})

	runID := ksuid.New().String()
	var counters runCounters
	lat := &latencyStats{values: make([]int64, 0, cfg.messages)}

	subscriber := sub.New(sub.Options{
		Storage: backend.storage,
		Queue:   backend.queue,
	})

	consumerErr := make(chan error, 1)
	start := time.Now()
	go func() {
		consumerErr <- consumeLoop(ctx, cfg, subscriber, runID, &counters, lat, ms, tracker)
	}()

	injector := inject.New(sender, func(_ string) {
		counters.acked.Add(1)
	}, cfg.injectBuffer)

	injectErr := make(chan error, 1)
	go func() {
		injectErr <- injector.Run()
	}()

	for i := range cfg.messages {
		payload := buildPayload(runID, uint64(i), time.Now().UnixNano(), cfg.payloadSize)
		select {
		case injector.C <- inject.InjectMessage{Receipt: strconv.Itoa(i), Data: payload}:
			counters.produced.Add(1)
			counters.bytesSent.Add(uint64(len(payload)))
		case <-ctx.Done():
			close(injector.C)
			sender.Close()
			log.Fatalf("producer canceled: %v", ctx.Err())
		}
	}
	close(injector.C)

	if err := <-injectErr; err != nil {
		sender.Close()
		log.Fatalf("injector failed: %v", err)
	}

	sender.Close()

	if err := <-consumerErr; err != nil {
		log.Fatalf("consumer failed: %v", err)
	}

	elapsed := time.Since(start)
	report(cfg, elapsed, &counters, lat)
}

func parseFlags() configFlags {
	cfg := configFlags{}

	flag.StringVar(&cfg.backend, "backend", "sim", "backend to use: sim or aws")

	flag.IntVar(&cfg.messages, "messages", 200000, "number of messages to publish and consume")
	flag.IntVar(&cfg.payloadSize, "payload-size", 10000, "message payload size in bytes")
	flag.StringVar(&cfg.prefix, "prefix", "events", "pulsix object key prefix")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "maximum total benchmark duration")

	flag.DurationVar(&cfg.flushAge, "flush-age", pub.DefaultFlushThresholdAge, "sender flush threshold by age")
	flag.Int64Var(&cfg.flushBytes, "flush-bytes", pub.DefaultFlushThresholdBytes, "sender flush threshold by bytes")
	flag.IntVar(&cfg.ackBuffer, "ack-buffer", pub.DefaultAckChannelSize, "sender ack channel size")
	flag.DurationVar(&cfg.hardFail, "hard-fail-deadline", pub.HardFailDeadline, "sender hard-fail deadline")
	flag.BoolVar(&cfg.senderDebug, "sender-debug", false, "enable sender debug logs")
	flag.BoolVar(&cfg.disableMsgID, "disable-message-id", false, "disable metadata message ID generation")

	flag.IntVar(&cfg.injectBuffer, "inject-buffer", inject.DefaultBufferSize, "injector channel buffer size")
	flag.DurationVar(&cfg.consumerIdleSleep, "consumer-idle-sleep", 100*time.Millisecond, "consumer sleep duration when no batches are available")
	flag.BoolVar(&cfg.milestones, "milestones", true, "print major end-to-end milestones")

	flag.StringVar(&cfg.awsBucket, "aws-bucket", "", "S3 bucket for backend=aws")
	flag.StringVar(&cfg.awsQueueURL, "aws-queue-url", "", "SQS queue URL for backend=aws")

	flag.StringVar(&cfg.simBaseDir, "sim-base-dir", "", "base directory for backend=sim (empty creates temporary directory)")
	flag.BoolVar(&cfg.simKeepDir, "sim-keep-dir", false, "keep simulated backend directory after run")

	flag.Parse()
	return cfg
}

func validateFlags(cfg configFlags) error {
	if cfg.messages <= 0 {
		return fmt.Errorf("-messages must be > 0")
	}
	if cfg.payloadSize < 0 {
		return fmt.Errorf("-payload-size must be >= 0")
	}
	if cfg.flushBytes <= 0 {
		return fmt.Errorf("-flush-bytes must be > 0")
	}
	if cfg.ackBuffer <= 0 {
		return fmt.Errorf("-ack-buffer must be > 0")
	}
	if cfg.injectBuffer <= 0 {
		return fmt.Errorf("-inject-buffer must be > 0")
	}
	if cfg.timeout <= 0 {
		return fmt.Errorf("-timeout must be > 0")
	}
	if cfg.backend != "sim" && cfg.backend != "aws" {
		return fmt.Errorf("-backend must be one of: sim, aws")
	}
	if cfg.backend == "aws" {
		if cfg.awsBucket == "" {
			return fmt.Errorf("-aws-bucket is required when -backend=aws")
		}
		if cfg.awsQueueURL == "" {
			return fmt.Errorf("-aws-queue-url is required when -backend=aws")
		}
	}
	return nil
}

func buildBackend(ctx context.Context, cfg configFlags) (benchBackend, error) {
	switch cfg.backend {
	case "aws":
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return benchBackend{}, fmt.Errorf("load aws config: %w", err)
		}
		s3Client := s3.NewFromConfig(awsCfg)
		sqsClient := sqs.NewFromConfig(awsCfg)
		return benchBackend{
			storage: pulsix.NewS3Storage(s3Client, cfg.awsBucket),
			queue: &sub.SQSQueue{
				Client:   sqsClient,
				QueueURL: cfg.awsQueueURL,
			},
		}, nil

	case "sim":
		baseDir := cfg.simBaseDir
		createdTemp := false
		if baseDir == "" {
			d, err := os.MkdirTemp("", "pulsix-bench-")
			if err != nil {
				return benchBackend{}, fmt.Errorf("create temp dir: %w", err)
			}
			baseDir = d
			createdTemp = true
		}

		dataDir := filepath.Join(baseDir, "s3_blobs")
		queueDir := filepath.Join(baseDir, "sqs_queue")
		if err := os.RemoveAll(baseDir); err != nil {
			return benchBackend{}, fmt.Errorf("cleanup sim base dir: %w", err)
		}
		if err := os.MkdirAll(queueDir, 0o755); err != nil {
			return benchBackend{}, fmt.Errorf("create sim queue dir: %w", err)
		}

		cleanup := func() {}
		if createdTemp && !cfg.simKeepDir {
			cleanup = func() {
				if err := os.RemoveAll(baseDir); err != nil {
					log.Printf("warning: could not remove temp dir %s: %v", baseDir, err)
				}
			}
		} else {
			log.Printf("sim backend dir kept at: %s", baseDir)
		}

		var f func(key string)
		if cfg.milestones {
			f = func(key string) {
				fmt.Printf("📣 SQS: Notifying new batch at %s\n", key)
			}
		}

		return benchBackend{
			storage: &pulsix.SimulatedStorage{
				BaseDir:     dataDir,
				QueueDir:    queueDir,
				LogNewBatch: f,
			},
			queue:   &sub.FileQueue{Dir: queueDir},
			cleanup: cleanup,
		}, nil
	}

	return benchBackend{}, fmt.Errorf("unsupported backend: %s", cfg.backend)
}

func buildPayload(runID string, id uint64, sentUnixNano int64, payloadSize int) []byte {
	header := fmt.Sprintf("%s|%d|%d|", runID, id, sentUnixNano)
	if payloadSize <= len(header) {
		return []byte(header)
	}
	pad := strings.Repeat("a", payloadSize-len(header))
	return []byte(header + pad)
}

func parsePayload(data []byte) (runID string, id uint64, sentUnixNano int64, err error) {
	parts := bytes.SplitN(data, []byte("|"), 4)
	if len(parts) < 4 {
		return "", 0, 0, errors.New("invalid payload: expected 4 parts")
	}
	runID = string(parts[0])
	id, err = strconv.ParseUint(string(parts[1]), 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid id: %w", err)
	}
	sentUnixNano, err = strconv.ParseInt(string(parts[2]), 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid timestamp: %w", err)
	}
	return runID, id, sentUnixNano, nil
}

func consumeLoop(ctx context.Context, cfg configFlags, subscriber *sub.Sub,
	runID string, counters *runCounters, lat *latencyStats, ms milestoneLogger, tracker *downloadTracker) error {

	seen := make(map[uint64]struct{}, cfg.messages)

	for int(counters.consumed.Load()) < cfg.messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batches, err := subscriber.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("receive error: %v", err)
			time.Sleep(cfg.consumerIdleSleep)
			continue
		}

		if len(batches) == 0 {
			time.Sleep(cfg.consumerIdleSleep)
			continue
		}

		counters.batches.Add(uint64(len(batches)))

		for _, batch := range batches {
			parseStart := time.Now()
			parsedInBatch := 0
			for batch.Next() {
				msg := batch.Message()
				rid, id, sentUnixNano, err := parsePayload(msg.Data)
				if err != nil {
					continue
				}
				if rid != runID {
					continue
				}
				if _, ok := seen[id]; ok {
					counters.duplicates.Add(1)
					continue
				}
				seen[id] = struct{}{}
				lat.add(time.Now().UnixNano() - sentUnixNano)
				counters.consumed.Add(1)
				parsedInBatch++
			}
			ms.logf("full batch parsing for %s took %s (%d matching messages)", batch.GetKey(), time.Since(parseStart), parsedInBatch)

			if err := batch.Error(); err != nil {
				log.Printf("batch decode error key=%s: %v", batch.GetKey(), err)
			}
			if err := batch.Done(); err != nil {
				log.Printf("batch done error key=%s: %v", batch.GetKey(), err)
			}

			if tracker != nil {
				if ioStats, ok := tracker.pop(batch.GetKey()); ok {
					if ioStats.bytesRead > 0 {
						counters.bytesWire.Add(uint64(ioStats.bytesRead))
					}
					if !ioStats.firstRead.IsZero() && !ioStats.getStart.IsZero() {
						ms.logf("batch %s stream first-byte delay: %s", batch.GetKey(), ioStats.firstRead.Sub(ioStats.getStart))
					}
					if !ioStats.eofAt.IsZero() && !ioStats.firstRead.IsZero() {
						ms.logf("batch %s stream download took %s (%d bytes)", batch.GetKey(), ioStats.eofAt.Sub(ioStats.firstRead), ioStats.bytesRead)
					}
					if !ioStats.notifiedAt.IsZero() && !ioStats.getStart.IsZero() {
						ms.logf("batch %s notify->get delay: %s", batch.GetKey(), ioStats.getStart.Sub(ioStats.notifiedAt))
					}
				}
			}
		}
	}

	return nil
}

func report(cfg configFlags, elapsed time.Duration, counters *runCounters, lat *latencyStats) {
	produced := counters.produced.Load()
	acked := counters.acked.Load()
	consumed := counters.consumed.Load()
	dups := counters.duplicates.Load()
	bytesSent := counters.bytesSent.Load()
	bytesWire := counters.bytesWire.Load()
	batches := counters.batches.Load()

	slices.Sort(lat.values)

	avgLatency := time.Duration(0)
	if len(lat.values) > 0 {
		avgLatency = time.Duration(lat.sum / int64(len(lat.values)))
	}

	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1e-9
	}

	throughputMsg := float64(consumed) / seconds
	throughputMB := (float64(bytesSent) / (1024 * 1024)) / seconds
	throughputWireMB := (float64(bytesWire) / (1024 * 1024)) / seconds

	fmt.Println("=== pulsix-bench report ===")
	fmt.Printf("backend:              %s\n", cfg.backend)
	fmt.Printf("messages target:      %d\n", cfg.messages)
	fmt.Printf("payload size:         %d bytes\n", cfg.payloadSize)
	fmt.Printf("elapsed:              %s\n", elapsed)
	fmt.Printf("produced:             %d\n", produced)
	fmt.Printf("acked:                %d\n", acked)
	fmt.Printf("consumed unique:      %d\n", consumed)
	fmt.Printf("duplicates seen:      %d\n", dups)
	fmt.Printf("batches received:     %d\n", batches)
	fmt.Printf("throughput:           %.2f msg/s\n", throughputMsg)
	fmt.Printf("throughput payload:   %.2f MiB/s\n", throughputMB)
	fmt.Printf("throughput wire:      %.2f MiB/s\n", throughputWireMB)

	if len(lat.values) == 0 {
		fmt.Println("latency:              no samples")
		return
	}

	fmt.Printf("latency min:          %s\n", time.Duration(lat.min))
	fmt.Printf("latency avg:          %s\n", avgLatency)
	fmt.Printf("latency p50:          %s\n", lat.quantile(0.50))
	fmt.Printf("latency p95:          %s\n", lat.quantile(0.95))
	fmt.Printf("latency p99:          %s\n", lat.quantile(0.99))
	fmt.Printf("latency max:          %s\n", time.Duration(lat.max))
}
