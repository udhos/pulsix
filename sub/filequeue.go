package sub

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

// FileNotification implements Notification
type FileNotification struct {
	Key      string
	FilePath string
}

// GetKey returns the S3 key associated with the notification.
func (f *FileNotification) GetKey() string { return f.Key }

// Delete removes the notification from the queue after processing.
func (f *FileNotification) Delete(_ context.Context) error { return os.Remove(f.FilePath) }

// FileQueue implements Queue
type FileQueue struct {
	Dir string
}

// ReceiveNotifications reads the .json files in the directory and returns a list of Notifications.
func (f *FileQueue) ReceiveNotifications(_ context.Context) ([]Notification, error) {
	entries, err := os.ReadDir(f.Dir)
	if err != nil {
		return nil, err
	}

	var notifications []Notification
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			path := filepath.Join(f.Dir, entry.Name())
			data, _ := os.ReadFile(path)

			var m map[string]string
			json.Unmarshal(data, &m)

			notifications = append(notifications, &FileNotification{
				Key:      m["s3_key"],
				FilePath: path,
			})
		}
	}
	return notifications, nil
}
