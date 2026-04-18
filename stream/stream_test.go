package stream

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pubpkg "github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

const fixedID = "FIXED_ID_FOR_TESTING_1234567"

type trackingStorage struct {
	mu            sync.Mutex
	objects       map[string]string
	firstRead     time.Time
	firstReadOnce sync.Once
	firstReadCh   chan struct{}
	notifyCh      chan struct{}
}

func newTrackingStorage() *trackingStorage {
	return &trackingStorage{
		objects:     make(map[string]string),
		firstReadCh: make(chan struct{}),
		notifyCh:    make(chan struct{}, 16),
	}
}

func (s *trackingStorage) PutObject(_ context.Context, key string, r io.Reader, _ int64) error {
	buf := new(strings.Builder)
	chunk := make([]byte, 64)

	for {
		n, err := r.Read(chunk)
		if n > 0 {
			s.firstReadOnce.Do(func() {
				s.mu.Lock()
				s.firstRead = time.Now()
				s.mu.Unlock()
				close(s.firstReadCh)
			})
			buf.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.objects[key] = buf.String()
	s.mu.Unlock()

	select {
	case s.notifyCh <- struct{}{}:
	default:
	}

	return nil
}

func (s *trackingStorage) GetObject(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return io.NopCloser(strings.NewReader(s.objects[key])), nil
}

func (s *trackingStorage) waitFirstRead(t *testing.T) time.Time {
	t.Helper()
	select {
	case <-s.firstReadCh:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.firstRead
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first storage read")
		return time.Time{}
	}
}

func (s *trackingStorage) waitCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		s.mu.Lock()
		count := len(s.objects)
		s.mu.Unlock()
		if count >= want {
			return
		}

		select {
		case <-s.notifyCh:
		case <-deadline:
			t.Fatalf("timed out waiting for %d objects", want)
		}
	}
}

func (s *trackingStorage) contents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for _, content := range s.objects {
		out = append(out, content)
	}
	sort.Strings(out)
	return out
}

type completionTrackingStorage struct {
	mu          sync.Mutex
	firstReadAt []time.Time
	completedAt []time.Time
	completeCh  chan struct{}
	chunkSize   int
	chunkDelay  time.Duration
	callLatency time.Duration // simulates per-call API latency (e.g. TLS handshake)
}

func newCompletionTrackingStorage(chunkSize int, chunkDelay time.Duration) *completionTrackingStorage {
	if chunkSize <= 0 {
		chunkSize = 32
	}
	return &completionTrackingStorage{
		completeCh: make(chan struct{}, 32),
		chunkSize:  chunkSize,
		chunkDelay: chunkDelay,
	}
}

func newCompletionTrackingStorageWithCallLatency(chunkSize int, chunkDelay, callLatency time.Duration) *completionTrackingStorage {
	s := newCompletionTrackingStorage(chunkSize, chunkDelay)
	s.callLatency = callLatency
	return s
}

func (s *completionTrackingStorage) PutObject(_ context.Context, _ string, r io.Reader, _ int64) error {
	if s.callLatency > 0 {
		time.Sleep(s.callLatency)
	}
	buf := make([]byte, s.chunkSize)
	var firstRead time.Time
	for {
		n, err := r.Read(buf)
		if n > 0 && firstRead.IsZero() {
			firstRead = time.Now()
		}
		if n > 0 && s.chunkDelay > 0 {
			time.Sleep(s.chunkDelay)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.firstReadAt = append(s.firstReadAt, firstRead)
	s.completedAt = append(s.completedAt, time.Now())
	s.mu.Unlock()

	select {
	case s.completeCh <- struct{}{}:
	default:
	}

	return nil
}

func (s *completionTrackingStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *completionTrackingStorage) waitCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		s.mu.Lock()
		count := len(s.completedAt)
		s.mu.Unlock()
		if count >= want {
			return
		}

		select {
		case <-s.completeCh:
		case <-deadline:
			t.Fatalf("timed out waiting for %d completed uploads", want)
		}
	}
}

func (s *completionTrackingStorage) completionTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.completedAt))
	copy(out, s.completedAt)
	return out
}

func (s *completionTrackingStorage) firstReadTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.firstReadAt))
	copy(out, s.firstReadAt)
	return out
}

func totalBatchCompletionLatency(arrivals [][]time.Time, completions []time.Time) time.Duration {
	var total time.Duration
	for batchIndex, batchArrivals := range arrivals {
		for _, arrival := range batchArrivals {
			total += completions[batchIndex].Sub(arrival)
		}
	}
	return total
}

func totalBatchFirstByteLatency(arrivals [][]time.Time, firstReads []time.Time) time.Duration {
	var total time.Duration
	for batchIndex, batchArrivals := range arrivals {
		if len(batchArrivals) == 0 {
			continue
		}
		total += firstReads[batchIndex].Sub(batchArrivals[0])
	}
	return total
}

type latencyMetrics struct {
	totalCompletion time.Duration
	totalFirstByte  time.Duration
}

func runStreamLatencyScenario(t *testing.T, store *completionTrackingStorage, batches [][][]pulsix.Message, intraBatchGap, interBatchGap, flushSilence time.Duration) latencyMetrics {
	t.Helper()

	publisher := New(Options{
		Storage:               store,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: flushSilence,
	})

	arrivalTimes := make([][]time.Time, len(batches))

	for batchIndex, batch := range batches {
		for groupIndex, group := range batch {
			now := time.Now()
			for range group {
				arrivalTimes[batchIndex] = append(arrivalTimes[batchIndex], now)
			}

			if _, err := publisher.SendBatch(context.Background(), group); err != nil {
				t.Fatalf("stream SendBatch failed for batch=%d group=%d: %v", batchIndex, groupIndex, err)
			}

			if groupIndex < len(batch)-1 {
				time.Sleep(intraBatchGap)
			}
		}

		if batchIndex < len(batches)-1 {
			time.Sleep(interBatchGap)
		}
	}

	if err := publisher.Close(); err != nil {
		t.Fatalf("stream Close failed: %v", err)
	}

	store.waitCount(t, len(batches))
	return latencyMetrics{
		totalCompletion: totalBatchCompletionLatency(arrivalTimes, store.completionTimes()),
		totalFirstByte:  totalBatchFirstByteLatency(arrivalTimes, store.firstReadTimes()),
	}
}

func runLegacyLatencyScenario(t *testing.T, store *completionTrackingStorage, batches [][][]pulsix.Message, intraBatchGap, interBatchGap time.Duration) latencyMetrics {
	t.Helper()

	publisher := pubpkg.New(pubpkg.Options{
		Storage:        store,
		Prefix:         "test",
		GenerateIDFunc: func() string { return fixedID },
	})

	arrivalTimes := make([][]time.Time, len(batches))

	for batchIndex, batch := range batches {
		messages := make([]pulsix.Message, 0)
		for groupIndex, group := range batch {
			now := time.Now()
			for range group {
				arrivalTimes[batchIndex] = append(arrivalTimes[batchIndex], now)
			}
			messages = append(messages, group...)

			if groupIndex < len(batch)-1 {
				time.Sleep(intraBatchGap)
			}
		}

		headerBuf := make([]byte, 0, 128)
		if err := publisher.SendBatch(context.Background(), messages, headerBuf); err != nil {
			t.Fatalf("pub SendBatch failed for batch=%d: %v", batchIndex, err)
		}

		if batchIndex < len(batches)-1 {
			time.Sleep(interBatchGap)
		}
	}

	store.waitCount(t, len(batches))
	return latencyMetrics{
		totalCompletion: totalBatchCompletionLatency(arrivalTimes, store.completionTimes()),
		totalFirstByte:  totalBatchFirstByteLatency(arrivalTimes, store.firstReadTimes()),
	}
}

func TestStreamSingleBatchEncodesP1(t *testing.T) {
	t.Parallel()

	store := newTrackingStorage()
	publisher := New(Options{
		Storage:               store,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: time.Hour,
	})

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{
		{Data: []byte("hello")},
		{Data: []byte("pulsix")},
	}); err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store.waitCount(t, 1)
	contents := store.contents()
	if len(contents) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(contents))
	}

	want := `p1:53:m:39:j:{"id":"FIXED_ID_FOR_TESTING_1234567"}d:5:hello54:m:39:j:{"id":"FIXED_ID_FOR_TESTING_1234567"}d:6:pulsix`
	if contents[0] != want {
		t.Fatalf("unexpected content:\nwant: %s\n got: %s", want, contents[0])
	}
}

func TestStreamStartsUploadingBeforeBatchComplete(t *testing.T) {
	t.Parallel()

	const gap = 50 * time.Millisecond

	streamStore := newTrackingStorage()
	streamPub := New(Options{
		Storage:               streamStore,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: time.Hour,
	})

	streamStart := time.Now()
	if _, err := streamPub.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("first")}}); err != nil {
		t.Fatalf("stream first SendBatch failed: %v", err)
	}
	streamFirstRead := streamStore.waitFirstRead(t).Sub(streamStart)

	time.Sleep(gap)

	if _, err := streamPub.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("second")}}); err != nil {
		t.Fatalf("stream second SendBatch failed: %v", err)
	}
	if err := streamPub.Close(); err != nil {
		t.Fatalf("stream close failed: %v", err)
	}

	batchStore := newTrackingStorage()
	legacyPub := pubpkg.New(pubpkg.Options{
		Storage:        batchStore,
		Prefix:         "test",
		GenerateIDFunc: func() string { return fixedID },
	})

	legacyStart := time.Now()
	messages := []pulsix.Message{{Data: []byte("first")}}
	time.Sleep(gap)
	messages = append(messages, pulsix.Message{Data: []byte("second")})

	headerBuf := make([]byte, 0, 128)
	if err := legacyPub.SendBatch(context.Background(), messages, headerBuf); err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}
	legacyFirstRead := batchStore.waitFirstRead(t).Sub(legacyStart)

	if streamFirstRead >= gap/2 {
		t.Fatalf("expected stream first read to happen well before full batch completion: %s", streamFirstRead)
	}
	if legacyFirstRead < gap/2 {
		t.Fatalf("expected SendBatch first read to wait for the accumulation gap: %s", legacyFirstRead)
	}
	if legacyFirstRead-streamFirstRead < gap/2 {
		t.Fatalf("expected stream upload to start materially earlier: stream=%s legacy=%s", streamFirstRead, legacyFirstRead)
	}
}

func TestStreamHasLowerTotalCompletionLatencyThanLegacySendBatch(t *testing.T) {
	t.Parallel()

	makePayload := func(label string) []byte {
		return []byte(strings.Repeat(label, 64))
	}

	batches := [][][]pulsix.Message{
		{{{Data: makePayload("a")}}, {{Data: makePayload("b")}}, {{Data: makePayload("c")}}},
		{{{Data: makePayload("d")}}, {{Data: makePayload("e")}}, {{Data: makePayload("f")}}},
		{{{Data: makePayload("g")}}, {{Data: makePayload("h")}}, {{Data: makePayload("i")}}},
	}

	const (
		intraBatchGap = 20 * time.Millisecond
		interBatchGap = 60 * time.Millisecond
		flushSilence  = 30 * time.Millisecond
		chunkDelay    = 3 * time.Millisecond
		chunkSize     = 32
	)

	streamStore := newCompletionTrackingStorage(chunkSize, chunkDelay)
	legacyStore := newCompletionTrackingStorage(chunkSize, chunkDelay)

	streamMetrics := runStreamLatencyScenario(t, streamStore, batches, intraBatchGap, interBatchGap, flushSilence)
	legacyMetrics := runLegacyLatencyScenario(t, legacyStore, batches, intraBatchGap, interBatchGap)

	t.Logf("total message completion latency: stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)

	if streamMetrics.totalCompletion >= legacyMetrics.totalCompletion {
		t.Fatalf("expected stream total completion latency to be lower: stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)
	}

	if legacyMetrics.totalCompletion-streamMetrics.totalCompletion < 50*time.Millisecond {
		t.Fatalf("expected a material latency improvement: stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)
	}
}

func TestStreamHasLowerTotalFirstByteLatencyThanLegacySendBatch(t *testing.T) {
	t.Parallel()

	makePayload := func(label string) []byte {
		return []byte(strings.Repeat(label, 64))
	}

	batches := [][][]pulsix.Message{
		{{{Data: makePayload("a")}}, {{Data: makePayload("b")}}, {{Data: makePayload("c")}}},
		{{{Data: makePayload("d")}}, {{Data: makePayload("e")}}, {{Data: makePayload("f")}}},
		{{{Data: makePayload("g")}}, {{Data: makePayload("h")}}, {{Data: makePayload("i")}}},
	}

	const (
		intraBatchGap = 20 * time.Millisecond
		interBatchGap = 60 * time.Millisecond
		flushSilence  = 30 * time.Millisecond
		chunkDelay    = 3 * time.Millisecond
		chunkSize     = 32
	)

	streamStore := newCompletionTrackingStorage(chunkSize, chunkDelay)
	legacyStore := newCompletionTrackingStorage(chunkSize, chunkDelay)

	streamMetrics := runStreamLatencyScenario(t, streamStore, batches, intraBatchGap, interBatchGap, flushSilence)
	legacyMetrics := runLegacyLatencyScenario(t, legacyStore, batches, intraBatchGap, interBatchGap)

	t.Logf("total batch first-byte latency: stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)

	if streamMetrics.totalFirstByte >= legacyMetrics.totalFirstByte {
		t.Fatalf("expected stream total first-byte latency to be lower: stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)
	}

	if legacyMetrics.totalFirstByte-streamMetrics.totalFirstByte < 50*time.Millisecond {
		t.Fatalf("expected a material first-byte improvement: stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)
	}
}

// The tests below repeat the latency comparisons with a 100ms per-call API
// latency injected into the mock storage, simulating real-world conditions such
// as TLS handshake time or S3 request round-trip overhead. The stream package
// starts the upload on the first message of each batch, so subsequent messages
// in the same batch pipeline their data while the connection is already open.
// The legacy SendBatch must wait until all messages are accumulated before
// opening the connection, so it pays the full call latency after every group.

func TestStreamHasLowerTotalCompletionLatencyThanLegacySendBatchWithAPILatency(t *testing.T) {
	t.Parallel()

	makePayload := func(label string) []byte {
		return []byte(strings.Repeat(label, 64))
	}

	batches := [][][]pulsix.Message{
		{{{Data: makePayload("a")}}, {{Data: makePayload("b")}}, {{Data: makePayload("c")}}},
		{{{Data: makePayload("d")}}, {{Data: makePayload("e")}}, {{Data: makePayload("f")}}},
		{{{Data: makePayload("g")}}, {{Data: makePayload("h")}}, {{Data: makePayload("i")}}},
	}

	const (
		intraBatchGap = 20 * time.Millisecond
		interBatchGap = 60 * time.Millisecond
		flushSilence  = 30 * time.Millisecond
		chunkDelay    = 3 * time.Millisecond
		chunkSize     = 32
		callLatency   = 100 * time.Millisecond
	)

	streamStore := newCompletionTrackingStorageWithCallLatency(chunkSize, chunkDelay, callLatency)
	legacyStore := newCompletionTrackingStorageWithCallLatency(chunkSize, chunkDelay, callLatency)

	streamMetrics := runStreamLatencyScenario(t, streamStore, batches, intraBatchGap, interBatchGap, flushSilence)
	legacyMetrics := runLegacyLatencyScenario(t, legacyStore, batches, intraBatchGap, interBatchGap)

	t.Logf("total message completion latency (100ms API latency): stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)

	if streamMetrics.totalCompletion >= legacyMetrics.totalCompletion {
		t.Fatalf("expected stream total completion latency to be lower: stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)
	}

	if legacyMetrics.totalCompletion-streamMetrics.totalCompletion < 200*time.Millisecond {
		t.Fatalf("expected a material latency improvement: stream=%s legacy=%s", streamMetrics.totalCompletion, legacyMetrics.totalCompletion)
	}
}

func TestStreamHasLowerTotalFirstByteLatencyThanLegacySendBatchWithAPILatency(t *testing.T) {
	t.Parallel()

	makePayload := func(label string) []byte {
		return []byte(strings.Repeat(label, 64))
	}

	batches := [][][]pulsix.Message{
		{{{Data: makePayload("a")}}, {{Data: makePayload("b")}}, {{Data: makePayload("c")}}},
		{{{Data: makePayload("d")}}, {{Data: makePayload("e")}}, {{Data: makePayload("f")}}},
		{{{Data: makePayload("g")}}, {{Data: makePayload("h")}}, {{Data: makePayload("i")}}},
	}

	const (
		intraBatchGap = 20 * time.Millisecond
		interBatchGap = 60 * time.Millisecond
		flushSilence  = 30 * time.Millisecond
		chunkDelay    = 3 * time.Millisecond
		chunkSize     = 32
		callLatency   = 100 * time.Millisecond
	)

	streamStore := newCompletionTrackingStorageWithCallLatency(chunkSize, chunkDelay, callLatency)
	legacyStore := newCompletionTrackingStorageWithCallLatency(chunkSize, chunkDelay, callLatency)

	streamMetrics := runStreamLatencyScenario(t, streamStore, batches, intraBatchGap, interBatchGap, flushSilence)
	legacyMetrics := runLegacyLatencyScenario(t, legacyStore, batches, intraBatchGap, interBatchGap)

	t.Logf("total batch first-byte latency (100ms API latency): stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)

	if streamMetrics.totalFirstByte >= legacyMetrics.totalFirstByte {
		t.Fatalf("expected stream total first-byte latency to be lower: stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)
	}

	if legacyMetrics.totalFirstByte-streamMetrics.totalFirstByte < 100*time.Millisecond {
		t.Fatalf("expected a material first-byte improvement: stream=%s legacy=%s", streamMetrics.totalFirstByte, legacyMetrics.totalFirstByte)
	}
}

func TestStreamClosesBatchOnBytes(t *testing.T) {
	t.Parallel()

	store := newTrackingStorage()
	publisher := New(Options{
		Storage:               store,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   5,
		FlushThresholdSilence: time.Hour,
	})

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("hello")}}); err != nil {
		t.Fatalf("first SendBatch failed: %v", err)
	}
	store.waitCount(t, 1)

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("world")}}); err != nil {
		t.Fatalf("second SendBatch failed: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store.waitCount(t, 2)
}

func TestStreamClosesBatchOnAge(t *testing.T) {
	t.Parallel()

	store := newTrackingStorage()
	publisher := New(Options{
		Storage:               store,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     20 * time.Millisecond,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: time.Hour,
	})

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("hello")}}); err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}
	store.waitCount(t, 1)

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("world")}}); err != nil {
		t.Fatalf("second SendBatch failed: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store.waitCount(t, 2)
}

func TestStreamClosesBatchOnSilence(t *testing.T) {
	t.Parallel()

	store := newTrackingStorage()
	publisher := New(Options{
		Storage:               store,
		Prefix:                "test",
		GenerateIDFunc:        func() string { return fixedID },
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: 20 * time.Millisecond,
	})

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("hello")}}); err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}
	store.waitCount(t, 1)

	if _, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("world")}}); err != nil {
		t.Fatalf("second SendBatch failed: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store.waitCount(t, 2)
}

func TestStreamSendBatchEmpty(t *testing.T) {
	t.Parallel()

	publisher := New(Options{Storage: newTrackingStorage(), Prefix: "test"})
	t.Cleanup(func() {
		_ = publisher.Close()
	})

	_, err := publisher.SendBatch(context.Background(), nil)
	if err != ErrEmptyMessages {
		t.Fatalf("expected ErrEmptyMessages, got %v", err)
	}
}

func TestStreamSendBatchReturnsOffsetsAndAckRanges(t *testing.T) {
	t.Parallel()

	ackCh := make(chan Ack, 4)
	publisher := New(Options{
		Storage:               newTrackingStorage(),
		Prefix:                "test",
		FlushThresholdAge:     time.Hour,
		FlushThresholdBytes:   1 << 20,
		FlushThresholdSilence: 20 * time.Millisecond,
		AckCh:                 ackCh,
	})

	offset0, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("a")}, {Data: []byte("b")}})
	if err != nil {
		t.Fatalf("first SendBatch failed: %v", err)
	}
	if offset0 != 0 {
		t.Fatalf("expected first offset 0, got %d", offset0)
	}

	offset1, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("c")}})
	if err != nil {
		t.Fatalf("second SendBatch failed: %v", err)
	}
	if offset1 != 2 {
		t.Fatalf("expected second offset 2, got %d", offset1)
	}

	time.Sleep(50 * time.Millisecond)

	offset2, err := publisher.SendBatch(context.Background(), []pulsix.Message{{Data: []byte("d")}, {Data: []byte("e")}})
	if err != nil {
		t.Fatalf("third SendBatch failed: %v", err)
	}
	if offset2 != 3 {
		t.Fatalf("expected third offset 3, got %d", offset2)
	}

	if err := publisher.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	ack1 := <-ackCh
	if ack1.Err != nil {
		t.Fatalf("unexpected first ack error: %v", ack1.Err)
	}
	if ack1.Offset != 0 || ack1.Amount != 3 {
		t.Fatalf("unexpected first ack range: offset=%d amount=%d", ack1.Offset, ack1.Amount)
	}

	ack2 := <-ackCh
	if ack2.Err != nil {
		t.Fatalf("unexpected second ack error: %v", ack2.Err)
	}
	if ack2.Offset != 3 || ack2.Amount != 2 {
		t.Fatalf("unexpected second ack range: offset=%d amount=%d", ack2.Offset, ack2.Amount)
	}

	if ack1.Key == "" || ack2.Key == "" {
		t.Fatalf("expected non-empty ack keys: first=%q second=%q", ack1.Key, ack2.Key)
	}
}
