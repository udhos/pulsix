// Package main implements a reference ingress model that generates random messages
// and injects them into Pulsix using the Sender API.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

type unsentMessage struct {
	IngressID uint64
	Msg       pulsix.Message
}

type unackedMessage struct {
	IngressID uint64
	Msg       pulsix.Message
}

type modelStats struct {
	Generated uint64
	Sent      uint64
	Acked     uint64
	Reverted  uint64
	AcksSeen  uint64
}

type modelState struct {
	mu      sync.Mutex
	nextID  uint64
	unsent  map[uint64]unsentMessage  // key: ingress ID
	unacked map[uint64]unackedMessage // key: sender ID
	acked   map[uint64]unackedMessage // key: sender ID
	stats   modelStats
}

func newModelState() *modelState {
	return &modelState{
		unsent:  make(map[uint64]unsentMessage),
		unacked: make(map[uint64]unackedMessage),
		acked:   make(map[uint64]unackedMessage),
	}
}

func (s *modelState) addUnsent(batch []pulsix.Message) []unsentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]unsentMessage, 0, len(batch))
	for _, msg := range batch {
		ingressID := s.nextID
		s.nextID++
		entry := unsentMessage{IngressID: ingressID, Msg: msg}
		s.unsent[ingressID] = entry
		entries = append(entries, entry)
		s.stats.Generated++
	}

	return entries
}

func (s *modelState) moveUnsentToUnacked(ingressID, senderID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.unsent[ingressID]
	if !ok {
		return
	}

	delete(s.unsent, ingressID)
	s.unacked[senderID] = unackedMessage{IngressID: ingressID, Msg: entry.Msg}
	s.stats.Sent++
}

func (s *modelState) moveUnackedToAcked(ackedUpTo uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	moved := 0
	for senderID, entry := range s.unacked {
		if senderID <= ackedUpTo {
			delete(s.unacked, senderID)
			s.acked[senderID] = entry
			moved++
		}
	}
	s.stats.Acked += uint64(moved)
	s.stats.AcksSeen++
	return moved
}

func (s *modelState) revertAllUnackedToUnsent() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	reverted := 0
	for senderID, entry := range s.unacked {
		delete(s.unacked, senderID)
		s.unsent[entry.IngressID] = unsentMessage(entry)
		reverted++
	}
	s.stats.Reverted += uint64(reverted)
	return reverted
}

func (s *modelState) snapshot() (int, int, int, modelStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.unsent), len(s.unacked), len(s.acked), s.stats
}

// simpleRandomRange returns a random integer in the range [low, high).
func simpleRandomRange(low, high int) int {
	return low + rand.Intn(high-low)
}

func buildRandomBatch(low, high, payloadSize int) []pulsix.Message {
	n := simpleRandomRange(low, high)
	batch := make([]pulsix.Message, 0, n)

	data := []byte(strings.Repeat("a", payloadSize))

	for range n {
		batch = append(batch, pulsix.Message{
			Data: data,
		})
	}

	return batch
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

// getSenderFlushInterval returns the sender batch flush interval.
// This controls Pulsix flushing behavior, not ingress generation rate.
func getSenderFlushInterval() time.Duration {
	return getenvDuration("BATCH_INTERVAL", pub.DefaultFlushThresholdAge)
}

// getIngressPaceSleep returns the delay between random ingress batches.
// This controls ingress pace, not Pulsix sender flush behavior.
func getIngressPaceSleep() time.Duration {
	return getenvDuration("INGRESS_PACE_SLEEP", 500*time.Millisecond)
}

func main() {
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		log.Fatal("BUCKET environment variable is required")
	}

	prefix := os.Getenv("PREFIX")
	if prefix == "" {
		prefix = "events"
	}

	flushInterval := getSenderFlushInterval()
	ingressPaceSleep := getIngressPaceSleep()

	randSeed := time.Now().UnixNano()
	if rawSeed := os.Getenv("RANDOM_SEED"); rawSeed != "" {
		parsedSeed, err := strconv.ParseInt(rawSeed, 10, 64)
		if err != nil {
			log.Fatalf("invalid RANDOM_SEED=%q: %v", rawSeed, err)
		}
		randSeed = parsedSeed
	}

	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsConfig)
	store := pulsix.NewS3Storage(s3Client, bucket)
	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: store,
			Prefix:  prefix,
		},
		FlushThresholdAge: flushInterval,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	state := newModelState()
	log.Printf("pulsix-ingress-random starting: bucket=%s prefix=%s flush_interval=%s ingress_pace_sleep=%s seed=%d", bucket, prefix, flushInterval, ingressPaceSleep, randSeed)

	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for ack := range sender.AckChan() {
			if ack.Err != nil {
				reverted := state.revertAllUnackedToUnsent()
				log.Printf("ack error: %v; reverted=%d from Unacked to Unsent", ack.Err, reverted)
				continue
			}

			moved := state.moveUnackedToAcked(ack.AckedUpTo)
			unsentCount, unackedCount, ackedCount, stats := state.snapshot()
			log.Printf("ack AckedUpTo=%d moved=%d state unsent=%d unacked=%d acked=%d stats generated=%d sent=%d acked=%d reverted=%d acks=%d",
				ack.AckedUpTo,
				moved,
				unsentCount,
				unackedCount,
				ackedCount,
				stats.Generated,
				stats.Sent,
				stats.Acked,
				stats.Reverted,
				stats.AcksSeen,
			)
		}
	}()

	batchCount := 0
	for {
		if ctx.Err() != nil {
			break
		}

		const (
			low         = 100
			high        = 200
			payloadSize = 1000
		)

		batch := buildRandomBatch(low, high, payloadSize)

		// Step 1 + 2: generate random batch and add all messages into Unsent.
		entries := state.addUnsent(batch)
		batchCount++

		// Step 3: Send API moves each message from Unsent to Unacked.
		for _, entry := range entries {
			senderID, err := sender.Send(ctx, entry.Msg)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Printf("send failed ingress_id=%d: %v", entry.IngressID, err)
				continue
			}
			state.moveUnsentToUnacked(entry.IngressID, senderID)
		}

		unsentCount, unackedCount, ackedCount, stats := state.snapshot()
		log.Printf("batch=%d generated=%d state unsent=%d unacked=%d acked=%d stats generated=%d sent=%d acked=%d reverted=%d acks=%d",
			batchCount,
			len(batch),
			unsentCount,
			unackedCount,
			ackedCount,
			stats.Generated,
			stats.Sent,
			stats.Acked,
			stats.Reverted,
			stats.AcksSeen,
		)

		if ingressPaceSleep > 0 {
			time.Sleep(ingressPaceSleep)
		}
	}

	// Flush remaining in-flight messages and close AckChan.
	sender.Close()
	<-ackDone

	unsentCount, unackedCount, ackedCount, stats := state.snapshot()
	remainingUnacked := make([]uint64, 0, unackedCount)

	state.mu.Lock()
	for id := range state.unacked {
		remainingUnacked = append(remainingUnacked, id)
	}
	state.mu.Unlock()

	slices.Sort(remainingUnacked)
	if len(remainingUnacked) > 10 {
		remainingUnacked = remainingUnacked[:10]
	}

	fmt.Printf("final state: unsent=%d unacked=%d acked=%d generated=%d sent=%d acked=%d reverted=%d acks=%d\n",
		unsentCount,
		unackedCount,
		ackedCount,
		stats.Generated,
		stats.Sent,
		stats.Acked,
		stats.Reverted,
		stats.AcksSeen,
	)
	if len(remainingUnacked) > 0 {
		fmt.Printf("sample remaining unacked sender IDs: %v\n", remainingUnacked)
	}
}
