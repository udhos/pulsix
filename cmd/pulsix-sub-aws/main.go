// Package main implements a simple Pulsix Subscriber using AWS S3 for storage and SQS for queuing.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/udhos/pulsix/pulsix"
	"github.com/udhos/pulsix/sub"
)

func main() {

	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		log.Fatal("BUCKET environment variable is required")
	}
	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		log.Fatal("QUEUE_URL environment variable is required")
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// 1. Initialize AWS Clients
	s3Client := s3.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)

	// 2. Setup Subscriber with AWS implementations
	subscriber := sub.New(sub.Options{
		Storage: pulsix.NewS3Storage(s3Client, bucketName),
		Queue:   &sub.SQSQueue{Client: sqsClient, QueueURL: queueURL},
	})

	fmt.Println("🎧 Pulsix AWS Subscriber listening...")

	for {
		batches, err := subscriber.Receive(ctx)
		if err != nil {
			log.Printf("Error receiving: %v", err)
			continue
		}

		for _, b := range batches {
			fmt.Printf("\n📦 Batch: %s\n", b.GetKey())

			for b.Next() {
				msg := b.Message()
				fmt.Printf("  📩 Msg: %s\n", string(msg.Data))

				// Show Metadata
				if msg.Metadata.MessageID != "" {
					fmt.Printf("     Meta ID: %s\n", msg.Metadata.MessageID)
				}

				// Show Attributes
				if len(msg.Attributes) > 0 {
					fmt.Printf("     Attrs: %v\n", msg.Attributes)
				}
			}

			if err := b.Done(); err != nil {
				log.Printf("  ❌ Failed to Ack: %v", err)
			} else {
				fmt.Println("  ✅ Processed & Deleted from SQS.")
			}
		}
	}
}
