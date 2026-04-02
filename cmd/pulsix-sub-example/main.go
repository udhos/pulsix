// Package main implements an example of a pulsix publisher.
package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/udhos/pulsix/pulsix"
	"github.com/udhos/pulsix/sub"
)

func main() {
	base := "/tmp/pulsix_demo"
	dataDir := filepath.Join(base, "s3_blobs")
	queueDir := filepath.Join(base, "sqs_queue")

	// 1. Setup the Subscriber
	subscriber := sub.New(sub.Options{
		Storage: &pulsix.SimulatedStorage{BaseDir: dataDir}, // Reuse the storage from previous step
		Queue:   &sub.FileQueue{Dir: queueDir},
	})

	fmt.Println("🎧 Pulsix Subscriber listening for batches...")

	for {
		ctx := context.Background()

		// 2. Poll for new batches
		batches, err := subscriber.Receive(ctx)
		if err != nil {
			log.Printf("Error receiving: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(batches) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, b := range batches {
			fmt.Printf("\n📦 Processing Batch: %s\n", b.GetKey())

			// 3. Stream through the messages
			for b.Next() {
				msg := b.Message()
				fmt.Printf("  📩 Msg: %s\n", string(msg.Data))
				if len(msg.Attributes) > 0 {
					fmt.Printf("     Attrs: %v\n", msg.Attributes)
				}
			}
			if err := b.Error(); err != nil {
				log.Printf("  ❌ Stream error: %v", err)
			}

			// 4. Acknowledge (Deletes the local .json file)
			if err := b.Done(); err != nil {
				log.Printf("  ❌ Failed to ack: %v", err)
			} else {
				fmt.Println("  ✅ Batch processed and Acked.")
			}
		}
	}
}
