// Package main implements an example of a pulsix publisher using real AWS S3.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

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

	// 4. Initialize the Pulsix Publisher
	publisher := pub.New(pub.Options{
		Storage: store,
		Prefix:  "events",
	})

	// 5. Define messages to "Pulse"
	messages := [][]byte{
		[]byte(`{"event": "login", "user": "alice"}`),
		[]byte(`{"event": "click", "button": "buy"}`),
	}

	for i, msg := range messages {
		fmt.Printf("📨 Message %d: %s\n", i+1, string(msg))
	}

	fmt.Printf("🚀 Pulsix Producer starting (Bucket: %s)...\n", bucketName)

	// 6. Send the Batch
	// This will stream directly to S3 and trigger the SQS notification
	// if S3 Event Notifications are configured.
	err = publisher.SendBatch(ctx, messages)
	if err != nil {
		log.Fatalf("❌ Failed to send batch: %v", err)
	}

	fmt.Println("✅ Batch fired! The Railgun has left the building.")
}
