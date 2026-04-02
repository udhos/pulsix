// Package main implements an example of a pulsix publisher.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/pulsix"
)

func main() {
	// Setup paths
	base := "/tmp/pulsix_demo"
	dataDir := filepath.Join(base, "s3_blobs")
	queueDir := filepath.Join(base, "sqs_queue")

	// Clean start
	os.RemoveAll(base)
	os.MkdirAll(queueDir, 0755)

	// 1. Initialize our Simulated Storage
	store := &pulsix.SimulatedStorage{
		BaseDir:  dataDir,
		QueueDir: queueDir,
	}

	// 2. Create the Publisher
	publisher := pub.New(pub.Options{
		Storage: store,
		Prefix:  "events",
	})

	// 3. Send a Batch
	ctx := context.Background()
	messages := [][]byte{
		[]byte(`{"event": "login", "user": "alice"}`),
		[]byte(`{"event": "click", "button": "buy"}`),
	}

	fmt.Println("🚀 Pulsix Producer starting...")

	err := publisher.SendBatch(ctx, messages)
	if err != nil {
		log.Fatalf("❌ Failed: %v", err)
	}

	fmt.Printf("\n--- Verification ---\n")
	fmt.Printf("Data stored in: %s\n", dataDir)
	fmt.Printf("Notifications in: %s\n", queueDir)

	// List the "SQS" folder to prove it worked
	files, _ := os.ReadDir(queueDir)
	for _, f := range files {
		fmt.Printf("Found SQS Message: %s\n", f.Name())
	}
}
