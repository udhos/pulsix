// Package main implements a reference ingress model that generates random messages
// and injects them into Pulsix using the Sender API.
package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/pulsix/inject"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

type modelStats struct {
	Generated uint64
	Acked     uint64
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

	batchLimit := os.Getenv("BATCH_LIMIT")
	if batchLimit == "" {
		batchLimit = "3"
	}
	maxBatch, err := strconv.Atoi(batchLimit)
	if err != nil || maxBatch <= 0 {
		maxBatch = 3
	}

	flushInterval := getSenderFlushInterval()
	ingressPaceSleep := getIngressPaceSleep()

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

	var stats modelStats
	var receiptSeq atomic.Uint64

	inj := inject.New(sender, func(_ string) {
		atomic.AddUint64(&stats.Acked, 1)
	}, 20_000)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	log.Printf("pulsix-ingress-random starting: bucket=%s prefix=%s flush_interval=%s ingress_pace_sleep=%s",
		bucket, prefix, flushInterval, ingressPaceSleep)

loop:
	for batchCount := range maxBatch {
		if ctx.Err() != nil {
			break
		}

		const (
			low         = 100
			high        = 200
			payloadSize = 1000
		)

		batch := buildRandomBatch(low, high, payloadSize)

		for _, msg := range batch {
			receipt := strconv.FormatUint(receiptSeq.Add(1)-1, 10)
			select {
			case inj.C <- inject.InjectMessage{Receipt: receipt, Data: msg.Data}:
				atomic.AddUint64(&stats.Generated, 1)
			case <-ctx.Done():
				break loop
			}
		}

		generated := atomic.LoadUint64(&stats.Generated)
		acked := atomic.LoadUint64(&stats.Acked)
		log.Printf("batch=%d generated=%d stats generated=%d acked=%d outstanding=%d",
			batchCount,
			len(batch),
			generated,
			acked,
			generated-acked,
		)

		if ingressPaceSleep > 0 {
			time.Sleep(ingressPaceSleep)
		}
	}

	close(inj.C)
	if err := <-runErr; err != nil {
		log.Printf("inject run error: %v", err)
	}

	// Flush remaining in-flight messages and close AckChan.
	sender.Close()

	generated := atomic.LoadUint64(&stats.Generated)
	acked := atomic.LoadUint64(&stats.Acked)
	log.Printf("final stats: generated=%d acked=%d outstanding=%d", generated, acked, generated-acked)
}
