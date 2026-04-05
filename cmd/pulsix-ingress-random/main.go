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

type modelStats struct {
	Generated uint64
	Sent      uint64
	Acked     uint64
	Reverted  uint64
	AcksSeen  uint64
}

type modelState struct {
	mu      sync.Mutex
	unsent  []pulsix.Message
	unacked map[uint64]pulsix.Message // key: sender ID
	stats   modelStats
}

func newModelState() *modelState {
	return &modelState{
		unsent:  make([]pulsix.Message, 0),
		unacked: make(map[uint64]pulsix.Message),
	}
}

func (s *modelState) addUnsent(batch []pulsix.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, msg := range batch {
		s.unsent = append(s.unsent, msg)
		s.stats.Generated++
	}
}

func (s *modelState) peekUnsent() (pulsix.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.unsent) == 0 {
		return pulsix.Message{}, false
	}

	return s.unsent[0], true
}

func (s *modelState) moveFrontUnsentToUnacked(senderID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.unsent) == 0 {
		return false
	}

	msg := s.unsent[0]
	s.unsent = s.unsent[1:]
	s.unacked[senderID] = msg
	s.stats.Sent++
	return true
}

func (s *modelState) moveUnackedToAcked(ackedUpTo uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	moved := 0
	for senderID := range s.unacked {
		if senderID <= ackedUpTo {
			delete(s.unacked, senderID)
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
		s.unsent = append(s.unsent, entry)
		reverted++
	}
	s.stats.Reverted += uint64(reverted)
	return reverted
}

func (s *modelState) snapshot() (int, int, modelStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.unsent), len(s.unacked), s.stats
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

	//
	// Spawn goroutine to consume acks and update model state accordingly.
	//
	// On Ack.Err, we revert all Unacked messages back to Unsent to be retried.
	//
	// On successful acks, we move messages from Unacked to Acked based on the
	// AckedUpTo sender ID.
	//
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for ack := range sender.AckChan() {
			if ack.Err != nil {
				// On error, revert all Unacked messages back to Unsent for retry.
				reverted := state.revertAllUnackedToUnsent()
				log.Printf("ack error: %v; reverted=%d from Unacked to Unsent", ack.Err, reverted)
				continue
			}

			// On success, move messages from Unacked to Acked based on AckedUpTo sender ID.
			moved := state.moveUnackedToAcked(ack.AckedUpTo)
			unsentCount, unackedCount, stats := state.snapshot()
			ackedCount := stats.Acked
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

		// Generate random batch and add all messages into Unsent.
		state.addUnsent(batch)
		batchCount++

		// Send API moves messages from Unsent to Unacked.
		// This includes both newly generated messages and any reverted ones.
		for {
			msg, ok := state.peekUnsent()
			if !ok {
				break
			}

			senderID, err := sender.Send(ctx, msg)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Printf("send failed: %v", err)
				break
			}
			state.moveFrontUnsentToUnacked(senderID)
		}

		unsentCount, unackedCount, stats := state.snapshot()
		ackedCount := stats.Acked
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

	unsentCount, unackedCount, stats := state.snapshot()
	ackedCount := stats.Acked
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
