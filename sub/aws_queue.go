package sub

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSQueue implements the Queue interface using AWS SQS.
type SQSQueue struct {
	Client   *sqs.Client
	QueueURL string
}

// S3Event represents the structure sent by S3 to SQS.
type S3Event struct {
	Records []struct {
		S3 struct {
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// ReceiveNotifications polls SQS for new S3 event messages.
func (q *SQSQueue) ReceiveNotifications(ctx context.Context) ([]Notification, error) {
	output, err := q.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.QueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20, // Long Polling for cost efficiency
	})
	if err != nil {
		return nil, err
	}

	var notifications []Notification
	for _, m := range output.Messages {
		var event S3Event
		if err := json.Unmarshal([]byte(*m.Body), &event); err != nil {
			// Skip malformed messages
			continue
		}

		// If Records is empty, check if this is an AWS S3 Test Event
		if len(event.Records) == 0 {
			if strings.Contains(*m.Body, "s3:TestEvent") {
				// Acknowledge/Delete the test event so it doesn't stay in the queue
				_, err := q.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(q.QueueURL),
					ReceiptHandle: m.ReceiptHandle,
				})
				if err != nil {
					log.Printf("Failed to delete test event message: %v", err)
				}
			}
			continue
		}

		for _, record := range event.Records {
			notifications = append(notifications, &SQSNotification{
				client:   q.Client,
				queueURL: q.QueueURL,
				key:      record.S3.Object.Key,
				handle:   m.ReceiptHandle,
			})
		}
	}

	return notifications, nil
}

// SQSNotification implements the Notification interface.
type SQSNotification struct {
	client   *sqs.Client
	queueURL string
	key      string
	handle   *string
}

// GetKey returns the S3 object key associated with this notification.
func (s *SQSNotification) GetKey() string { return s.key }

// Delete removes the message from the SQS queue, acknowledging it has been processed.
func (s *SQSNotification) Delete(ctx context.Context) error {
	_, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: s.handle,
	})
	return err
}
