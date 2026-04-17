// Package main implements an ingress model that reads messages from SQS
// and injects them into Pulsix using the Sender API.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/udhos/pulsix/inject"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

type ingressStats struct {
	ReadFromSQS uint64
	Acked       uint64
	DeletedSQS  uint64
	DeleteError uint64
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

func getSenderFlushInterval() time.Duration {
	return getenvDuration("BATCH_INTERVAL", pub.DefaultFlushThresholdAge)
}

func clamp(v, low, high int) int {
	return min(max(v, low), high)
}

func main() {
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		log.Fatal("BUCKET environment variable is required")
	}

	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		log.Fatal("QUEUE_URL environment variable is required")
	}

	prefix := os.Getenv("PREFIX")
	if prefix == "" {
		prefix = "events"
	}

	flushInterval := getSenderFlushInterval()
	maxNumberOfMessages := clamp(getenvInt("MAX_NUMBER_OF_MESSAGES", 10), 1, 10)
	waitTimeSeconds := clamp(getenvInt("WAIT_TIME_SECONDS", 20), 0, 20)
	injectBufferSize := getenvInt("INJECT_BUFFER_SIZE", 20_000)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsConfig)
	sqsClient := sqs.NewFromConfig(awsConfig)

	store := pulsix.NewS3Storage(s3Client, bucket)
	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: store,
			Prefix:  prefix,
		},
		FlushThresholdAge: flushInterval,
	})

	const errorCooldown = 500 * time.Millisecond

	var stats ingressStats

	inj := inject.New(sender, func(receipt string) {
		atomic.AddUint64(&stats.Acked, 1)
		_, err := sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: &receipt,
		})
		if err != nil {
			atomic.AddUint64(&stats.DeleteError, 1)
			log.Printf("delete from source sqs failed: %v, sleeping %v",
				err, errorCooldown)
			time.Sleep(errorCooldown)
			return
		}
		atomic.AddUint64(&stats.DeletedSQS, 1)
	}, injectBufferSize)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	log.Printf("pulsix-ingress-sqs starting: bucket=%s prefix=%s queue=%s flush_interval=%s max_messages=%d wait_time_seconds=%d inject_buffer=%d",
		bucket, prefix, queueURL, flushInterval, maxNumberOfMessages, waitTimeSeconds, injectBufferSize)

pollLoop:
	for {
		if ctx.Err() != nil {
			break
		}

		// Grab messages from SQS.

		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: int32(maxNumberOfMessages),
			WaitTimeSeconds:     int32(waitTimeSeconds),
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("receive from source sqs failed: %v, sleeping %v",
				err, errorCooldown)
			time.Sleep(errorCooldown)
			continue
		}

		// Scan messages from SQS injecting them with the package inject API.

		for _, m := range out.Messages {
			if m.Body == nil || m.ReceiptHandle == nil {
				log.Printf("skipping message with missing body or receipt handle")
				continue
			}

			payload := []byte(aws.ToString(m.Body))
			receipt := aws.ToString(m.ReceiptHandle)

			select {
			case inj.C <- inject.InjectMessage{Receipt: receipt, Data: payload}:
				atomic.AddUint64(&stats.ReadFromSQS, 1)
			case <-ctx.Done():
				break pollLoop
			}
		}

		if len(out.Messages) > 0 {
			read := atomic.LoadUint64(&stats.ReadFromSQS)
			acked := atomic.LoadUint64(&stats.Acked)
			deleted := atomic.LoadUint64(&stats.DeletedSQS)
			deleteErr := atomic.LoadUint64(&stats.DeleteError)
			log.Printf("stats: read=%d acked=%d deleted=%d delete_error=%d",
				read,
				acked,
				deleted,
				deleteErr,
			)
		}
	}

	close(inj.C)
	if err := <-runErr; err != nil {
		log.Printf("inject run error: %v", err)
	}

	// Flush remaining in-flight messages and close AckChan.
	sender.Close()

	read := atomic.LoadUint64(&stats.ReadFromSQS)
	acked := atomic.LoadUint64(&stats.Acked)
	deleted := atomic.LoadUint64(&stats.DeletedSQS)
	deleteErr := atomic.LoadUint64(&stats.DeleteError)
	log.Printf("final stats: read=%d acked=%d deleted=%d delete_error=%d",
		read,
		acked,
		deleted,
		deleteErr,
	)
}
