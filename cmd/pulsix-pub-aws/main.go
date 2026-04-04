// Package main implements an example of a pulsix publisher using real AWS S3.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

func main() {

	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		log.Fatal("BUCKET environment variable is required")
	}

	ctx := context.Background()

	count := 1
	if rawCount := os.Getenv("COUNT"); rawCount != "" {
		parsedCount, err := strconv.Atoi(rawCount)
		if err != nil || parsedCount <= 0 {
			log.Fatalf("invalid COUNT=%q: expected positive integer", rawCount)
		}
		count = parsedCount
	}

	// 1. Load the default AWS Configuration
	// This looks for AWS_REGION, AWS_ACCESS_KEY_ID, etc.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// 2. Initialize the S3 Client
	s3Client := s3.NewFromConfig(cfg)

	// 3. Create the S3-backed storage
	store := pulsix.NewS3Storage(s3Client, bucketName)

	// 4. Initialize the Pulsix Sender (primary async API)
	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: store,
			Prefix:  "events",
		},
	})

	// 5. Define messages
	messages := []pulsix.Message{
		{Data: []byte(`{"event": "login", "user": "alice"}`), Attributes: map[string]string{"key1": "val1"}},
		{Data: []byte(`{"event": "click", "button": "buy"}`), Attributes: map[string]string{"key2": "val2"}},
	}

	if count <= len(messages) {
		for i, msg := range messages[:count] {
			fmt.Printf("📨 Message %d: %s\n", i+1, string(msg.Data))
		}
	} else {
		fmt.Printf("Sending sample messages %d times\n", count)
	}

	fmt.Printf("🚀 Pulsix Producer starting (Bucket: %s)...\n", bucketName)

	var lastID uint64
	var i int
	for range count {
		i = (i + 1) % len(messages)
		msg := messages[i]
		id, sendErr := sender.Send(ctx, msg)
		if sendErr != nil {
			log.Fatalf("❌ Failed to send message: %v", sendErr)
		}
		lastID = id
	}

	// Wait for durability watermark to cover all sent messages.
	ackWaitStart := time.Now()
	for ack := range sender.AckChan() {
		if ack.Err != nil {
			log.Fatalf("Ack error: %v", ack.Err)
		}
		fmt.Printf("Acked up to ID %d\n", ack.AckedUpTo)
		if ack.AckedUpTo >= lastID {
			break
		}
	}
	fmt.Printf("Ack wait time: %s\n", time.Since(ackWaitStart))

	sender.Close()

	fmt.Println("✅ Messages sent and durably acked.")
}
