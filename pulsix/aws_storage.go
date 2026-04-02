package pulsix

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Storage implements the Storage interface using the official AWS SDK v2.
type S3Storage struct {
	client *s3.Client
	bucket string
	tm     *transfermanager.Client
}

// NewS3Storage creates a new S3-backed storage layer.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{
		client: client,
		bucket: bucket,
		tm:     transfermanager.New(client),
	}
}

// PutObject streams data directly to S3 using the modern Transfer Manager.
// It handles non-seekable readers (like MultiReader) by buffering
// parts of the stream internally for retries and parallel uploads.
func (s *S3Storage) PutObject(ctx context.Context, key string, r io.Reader, _ int64) error {
	// In Transfer Manager v2, use UploadObject with UploadObjectInput
	_, err := s.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	return err
}

// GetObject returns a streaming ReadCloser from S3.
func (s *S3Storage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}
