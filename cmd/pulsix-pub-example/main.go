// Package main implements an example of a pulsix publisher.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

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

	ctx := context.Background()

	count := 1
	if rawCount := os.Getenv("COUNT"); rawCount != "" {
		parsedCount, err := strconv.Atoi(rawCount)
		if err != nil || parsedCount <= 0 {
			log.Fatalf("invalid COUNT=%q: expected positive integer", rawCount)
		}
		count = parsedCount
	}

	// 2. Create the Sender (async, auto-batching API)
	sender := pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: store,
			Prefix:  "events",
		},
	})

	fmt.Println("Pulsix Producer starting...")

	// 3. Send messages and track IDs
	messages := []pulsix.Message{
		{Data: []byte(`{"event": "login", "user": "alice"}`), Attributes: map[string]string{"key1": "val1"}},
		{Data: []byte(`{"event": "click", "button": "buy"}`), Attributes: map[string]string{"key2": "val2"}},
	}

	if count <= len(messages) {
		for i, msg := range messages[:count] {
			fmt.Printf("Message %d: %s\n", i+1, string(msg.Data))
		}
	} else {
		fmt.Printf("Sending sample messages %d times\n", count)
	}

	var lastID uint64
	var i int
	for range count {
		i = (i + 1) % len(messages)
		msg := messages[i]
		id, err := sender.Send(ctx, msg)
		if err != nil {
			log.Fatalf("Send failed: %v", err)
		}
		lastID = id
	}

	// 4. Wait for ack confirming all messages are durable
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

	fmt.Printf("\n--- Verification ---\n")
	fmt.Printf("Data stored in: %s\n", dataDir)
	fmt.Printf("Notifications in: %s\n", queueDir)

	// List the "SQS" folder to prove it worked
	files, _ := os.ReadDir(queueDir)
	for _, f := range files {
		fmt.Printf("Found SQS Message: %s\n", f.Name())
	}
}
