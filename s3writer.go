package s3streamer

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// min returns the smaller of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// S3Writer implements io.Writer for streaming data to S3 using multipart uploads.
// It automatically handles multipart upload creation, part uploads, and completion.
//
// Thread Safety: S3Writer is safe for concurrent use by multiple goroutines.
// All methods are protected by an internal mutex.
//
// Memory Usage: The writer maintains an internal buffer that grows up to the
// configured part size before uploading. Memory usage is proportional to the
// part size, not the total data size.
//
// Error Handling: Once an error occurs, the writer becomes unusable and all
// subsequent operations will return the same error. Use Abort() to clean up
// resources in error conditions.
//
// Context Cancellation: All operations respect the context passed to NewS3Writer.
// If the context is cancelled, operations will return context.Canceled.
//
// Example:
//
//	writer := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024)
//	defer writer.Close()
//	data := []byte("hello world\n")
//	n, err := writer.Write(data)
type S3Writer struct {
	client     S3Client
	bucket     string
	key        string
	partSize   int64
	ctx        context.Context
	uploadID   *string
	buffer     *bytes.Buffer
	partNumber int32
	parts      []types.CompletedPart
	mu         sync.Mutex
	closed     bool
	err        error
}

// NewS3Writer creates a new S3Writer for uploading data to S3 using multipart uploads.
// The writer will buffer data until it reaches partSize bytes, then upload a part.
//
// Parameters:
//   - ctx: Context for request lifecycle and cancellation
//   - client: S3 client interface for API calls
//   - bucket: S3 bucket name (must not be empty)
//   - key: S3 object key (must not be empty)
//   - partSize: Size of each part in bytes (minimum 5MiB, automatically adjusted)
//
// Returns an error if required parameters are invalid or if the initial
// multipart upload creation fails.
//
// Example:
//
//	writer := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024)
//	defer writer.Close()
func NewS3Writer(ctx context.Context, client S3Client, bucket, key string, partSize int64) (*S3Writer, error) {
	// Validate required parameters
	if ctx == nil {
		return nil, fmt.Errorf("context cannot be nil")
	}
	if client == nil {
		return nil, fmt.Errorf("S3 client cannot be nil")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}
	if key == "" {
		return nil, fmt.Errorf("object key cannot be empty")
	}

	// AWS S3 multipart upload minimum part size is 5MiB (except for the last part)
	if partSize < 5*1024*1024 {
		return nil, fmt.Errorf("part size must be at least 5MiB (5242880 bytes), got %d bytes", partSize)
	}

	writer := &S3Writer{
		client:     client,
		bucket:     bucket,
		key:        key,
		partSize:   partSize,
		ctx:        ctx,
		buffer:     bytes.NewBuffer(make([]byte, 0, min(partSize, 1024*1024))), // Cap initial buffer at 1MiB
		partNumber: 1,
		parts:      make([]types.CompletedPart, 0),
	}

	// Initiate multipart upload
	if err := writer.initMultipartUpload(); err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	return writer, nil
}

// Write implements io.Writer interface. It buffers data and uploads parts when the buffer
// reaches the configured part size. The write operation respects context cancellation.
// Example:
//
//	writer := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024)
//	data := []byte("hello world\n")
//	n, err := writer.Write(data)
func (w *S3Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if context is cancelled
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
	}

	if w.closed {
		return 0, fmt.Errorf("cannot write to closed S3Writer (bucket: %s, key: %s)", w.bucket, w.key)
	}

	if w.err != nil {
		return 0, w.err
	}

	totalWritten := 0
	for len(p) > 0 {
		// Calculate how much we can write to the current buffer
		remaining := w.partSize - int64(w.buffer.Len())
		if remaining <= 0 {
			// Buffer is full, upload the current part
			if err := w.uploadPart(); err != nil {
				w.err = err
				return totalWritten, err
			}
			remaining = w.partSize
		}

		// Write as much as we can to the buffer
		toWrite := int64(len(p))
		if toWrite > remaining {
			toWrite = remaining
		}

		n, err := w.buffer.Write(p[:toWrite])
		if err != nil {
			w.err = err
			return totalWritten, err
		}

		totalWritten += n
		p = p[toWrite:]

		// If buffer is now full, upload it
		if int64(w.buffer.Len()) >= w.partSize {
			if err := w.uploadPart(); err != nil {
				w.err = err
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

// Close finalizes the multipart upload by uploading any remaining buffered data
// and completing the multipart upload. This method must be called to ensure
// all data is written to S3. The operation respects context cancellation.
// Example:
//
//	writer := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024)
//	defer writer.Close()
//	// ... write data ...
//	if err := writer.Close(); err != nil {
//	    log.Fatal(err)
//	}
func (w *S3Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if context is cancelled
	select {
	case <-w.ctx.Done():
		if !w.closed {
			w.closed = true
			w.abortMultipartUpload() // Clean up on cancellation
		}
		return w.ctx.Err()
	default:
	}

	if w.closed {
		return w.err
	}

	w.closed = true

	// Upload any remaining data in the buffer
	if w.buffer.Len() > 0 {
		if err := w.uploadPart(); err != nil {
			w.err = err
			// Attempt to abort the multipart upload on error
			w.abortMultipartUpload() // Ignore abort errors during cleanup
			return err
		}
	}

	// Complete the multipart upload
	if err := w.completeMultipartUpload(); err != nil {
		w.err = err
		// Attempt to abort the multipart upload on error
		w.abortMultipartUpload() // Ignore abort errors during cleanup
		return err
	}

	return nil
}

// Abort cancels the multipart upload and cleans up any uploaded parts.
// This is useful for error handling or when you need to cancel an upload.
// Example:
//
//	writer := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024)
//	// ... write some data ...
//	if someError != nil {
//	    writer.Abort() // Clean up the partial upload
//	    return
//	}
func (w *S3Writer) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.closed {
		w.closed = true
	}

	return w.abortMultipartUpload()
}

// initMultipartUpload initiates a new multipart upload session
func (w *S3Writer) initMultipartUpload() error {
	resp, err := w.client.CreateMultipartUpload(w.ctx, &s3.CreateMultipartUploadInput{
		Bucket: &w.bucket,
		Key:    &w.key,
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	w.uploadID = resp.UploadId
	return nil
}

// uploadPart uploads the current buffer as a part and resets the buffer
func (w *S3Writer) uploadPart() error {
	if w.buffer.Len() == 0 {
		return nil
	}

	// AWS S3 has a maximum of 10,000 parts per multipart upload
	if w.partNumber > 10000 {
		return fmt.Errorf("exceeded maximum number of parts (10,000) for multipart upload")
	}

	// Create a copy of the buffer data
	data := make([]byte, w.buffer.Len())
	copy(data, w.buffer.Bytes())

	contentLength := int64(len(data))
	resp, err := w.client.UploadPart(w.ctx, &s3.UploadPartInput{
		Bucket:        &w.bucket,
		Key:           &w.key,
		PartNumber:    &w.partNumber,
		UploadId:      w.uploadID,
		Body:          bytes.NewReader(data),
		ContentLength: &contentLength,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part %d: %w", w.partNumber, err)
	}

	// Ensure we have a valid ETag
	if resp.ETag == nil || *resp.ETag == "" {
		return fmt.Errorf("received empty ETag for part %d", w.partNumber)
	}

	// Store the completed part info with current part number
	currentPartNumber := w.partNumber
	w.parts = append(w.parts, types.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: &currentPartNumber,
	})

	// Reset buffer and increment part number for next part
	w.buffer.Reset()
	w.partNumber++

	return nil
}

// completeMultipartUpload finalizes the multipart upload
func (w *S3Writer) completeMultipartUpload() error {
	if w.uploadID == nil {
		return fmt.Errorf("no multipart upload in progress")
	}

	if len(w.parts) == 0 {
		return fmt.Errorf("no parts uploaded for multipart upload")
	}

	// Sort parts by part number (should already be sorted, but ensure it)
	sort.Slice(w.parts, func(i, j int) bool {
		return *w.parts[i].PartNumber < *w.parts[j].PartNumber
	})

	// Validate all parts have valid ETags
	for i, part := range w.parts {
		if part.ETag == nil || *part.ETag == "" {
			return fmt.Errorf("part %d has empty ETag", i+1)
		}
		if part.PartNumber == nil {
			return fmt.Errorf("part %d has nil PartNumber", i+1)
		}
	}

	_, err := w.client.CompleteMultipartUpload(w.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: w.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.parts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload with %d parts: %w", len(w.parts), err)
	}

	return nil
}

// abortMultipartUpload cancels the multipart upload and cleans up
func (w *S3Writer) abortMultipartUpload() error {
	if w.uploadID == nil {
		return nil // No upload to abort
	}

	_, err := w.client.AbortMultipartUpload(w.ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: w.uploadID,
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	return nil
}
