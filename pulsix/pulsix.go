// Package pulsix provides a simple interface to publish messages to S3 in batches.
package pulsix

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// HeaderPrefix is used to identify the size of a message in the batch. The format is "PULSIX-SIZE:<size>\n".
const HeaderPrefix = "PULSIX-SIZE:"

// Storage abstracts the S3-specific calls.
// Using io.Reader allows for zero-copy streaming to S3.
type Storage interface {
	// PutObject for the Producer
	PutObject(ctx context.Context, key string, r io.Reader, contentLength int64) error
	// GetObject for the Consumer
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
}

// SimulatedStorage wraps local file writes with a "Notification" trigger.
type SimulatedStorage struct {
	BaseDir  string
	QueueDir string
}

// PutObject simulates writing to S3 and then creates a notification in the "SQS" folder.
func (s *SimulatedStorage) PutObject(_ context.Context, key string, r io.Reader, _ int64) error {
	// 1. Write to "S3" (The Blob Store)
	fullPath := filepath.Join(s.BaseDir, key)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}
	out, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, r); err != nil {
		return err
	}

	// 2. Write to "SQS" (The Notification Queue)
	// We create a tiny JSON file in the QueueDir to simulate a message.
	notificationKey := filepath.Base(key) + ".json"
	notifPath := filepath.Join(s.QueueDir, notificationKey)

	notifData, _ := json.Marshal(map[string]string{"s3_key": key})

	fmt.Printf("📣 SQS: Notifying new batch at %s\n", key)
	return os.WriteFile(notifPath, notifData, 0644)
}

// GetObject simulates reading from S3 by opening the corresponding file.
func (s *SimulatedStorage) GetObject(_ context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.BaseDir, key))
}
