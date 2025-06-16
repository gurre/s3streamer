// Package s3streamer implements the S3 streaming functionality. It handles streaming gzipped JSON lines from S3 data files.
package s3streamer

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client interface defines the required S3 operations for streaming data.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	streamer := s3streamer.NewS3Streamer(client, 1)
type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Streamer interface defines the contract for streaming data from S3.
// Example:
//
//	var streamer s3streamer.Streamer
//	err := streamer.Stream(ctx, "my-bucket", "data.json.gz", 0, func(line []byte) error {
//	    // Process each line
//	    return nil
//	})
type Streamer interface {
	Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte) error) error
}

// S3Streamer implements the Streamer interface for streaming data from S3.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	streamer := s3streamer.NewS3Streamer(client, 1)
//	err := streamer.Stream(ctx, "my-bucket", "data.json.gz", 0, processLine)
type S3Streamer struct {
	client    S3Client
	chunkSize int64 // Size of each chunk to download
}

// NewS3Streamer creates a new S3Streamer instance with configurable chunk size.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	streamer := s3streamer.NewS3Streamer(client, 1) // 1 concurrent download
func NewS3Streamer(client S3Client, concurrency int) *S3Streamer {
	return &S3Streamer{
		client:    client,
		chunkSize: 5 * 1024 * 1024, // 5MB chunks
	}
}

// Stream downloads data from S3 in chunks, decompresses it if needed, and processes each line.
// Example:
//
//	streamer := s3streamer.NewS3Streamer(client, 1)
//	err := streamer.Stream(ctx, "my-bucket", "data.json.gz", 0, func(line []byte) error {
//	    // Process each line
//	    return nil
//	})
func (s *S3Streamer) Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte) error) error {

	// Get the object size first
	headResp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}

	if headResp.ContentLength == nil {
		return fmt.Errorf("content length is missing from object metadata")
	}

	totalSize := *headResp.ContentLength
	if totalSize == 0 {
		return fmt.Errorf("object is empty")
	}

	if offset >= totalSize {
		return fmt.Errorf("offset %d exceeds object size %d", offset, totalSize)
	}

	// Get a small sample to detect compression type
	detectionChunkSize := int64(512) // 512 bytes should be enough to detect compression
	endOffset := offset + detectionChunkSize - 1
	if endOffset >= totalSize {
		endOffset = totalSize - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, endOffset)
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  &rangeHeader,
	})
	if err != nil {
		return fmt.Errorf("failed to download detection chunk (%s): %w", rangeHeader, err)
	}

	// Read just enough to detect compression
	sampleData := make([]byte, 512)
	n, err := io.ReadAtLeast(resp.Body, sampleData, 1)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		resp.Body.Close()
		return fmt.Errorf("failed to read sample data: %w", err)
	}
	sampleData = sampleData[:n]

	// Close the detection response as we'll create a new stream from the beginning
	resp.Body.Close()

	// Detect compression from the sample (for logging/debugging purposes)
	compression := DetectCompression(sampleData)
	compressionType := "none"
	if compression != Uncompressed {
		compressionType = compression.Extension()
	}

	// Start a fresh download from the offset
	remainingSize := totalSize - offset
	chunkStreamer := NewChunkStreamer(ctx, s.client, bucket, key, offset, remainingSize, s.chunkSize)

	// Decompress the stream if needed, or pass through as-is
	reader, err := Decompress(chunkStreamer)
	if err != nil {
		return fmt.Errorf("failed to process data stream (type: %s): %w", compressionType, err)
	}

	// Process the file line by line
	scanner := bufio.NewScanner(reader)
	// Use a larger buffer size for better performance with large lines
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line size

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if err := fn(scanner.Bytes()); err != nil {
			return fmt.Errorf("error processing line %d: %w", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning lines: %w", err)
	}

	return nil
}
