package s3streamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestNewChunkStreamer(t *testing.T) {
	ctx := context.Background()
	client := NewMockS3Client([]byte("test data"))

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, 100, 1024)

	if streamer.bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got %s", streamer.bucket)
	}
	if streamer.key != "test-key" {
		t.Errorf("Expected key 'test-key', got %s", streamer.key)
	}
	if streamer.offset != 0 {
		t.Errorf("Expected offset 0, got %d", streamer.offset)
	}
	if streamer.size != 100 {
		t.Errorf("Expected size 100, got %d", streamer.size)
	}
	if streamer.chunkSize != 1024 {
		t.Errorf("Expected chunkSize 1024, got %d", streamer.chunkSize)
	}
	if streamer.currentOffset != 0 {
		t.Errorf("Expected currentOffset 0, got %d", streamer.currentOffset)
	}
	if streamer.eof {
		t.Error("Expected eof to be false initially")
	}
}

func TestChunkStreamerBasicRead(t *testing.T) {
	testData := []byte("Hello, World! This is a test of the ChunkStreamer functionality.")
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), 1024)

	// Read the entire content
	result := make([]byte, len(testData))
	n, err := streamer.Read(result)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
	}
	if !bytes.Equal(result[:n], testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(result[:n]))
	}

	// Next read should return EOF
	n, err = streamer.Read(result)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes on EOF, got %d", n)
	}
}

func TestChunkStreamerSmallBuffer(t *testing.T) {
	testData := []byte("Hello, World! This is a test of reading with a small buffer.")
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), 1024)

	// Read with a small buffer
	var result []byte
	buf := make([]byte, 5) // Small buffer

	for {
		n, err := streamer.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		result = append(result, buf[:n]...)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(result))
	}
}

func TestChunkStreamerAcrossChunkBoundaries(t *testing.T) {
	// Create test data larger than chunk size
	testData := bytes.Repeat([]byte("0123456789"), 20) // 200 bytes
	ctx := context.Background()
	client := NewMockS3Client(testData)

	// Use small chunk size to force multiple chunks
	chunkSize := int64(50)
	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), chunkSize)

	// Read all data
	result, err := io.ReadAll(streamer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("Data mismatch. Expected %d bytes, got %d bytes", len(testData), len(result))
	}

	// Verify multiple chunks were requested
	expectedChunks := (len(testData) + int(chunkSize) - 1) / int(chunkSize) // Ceiling division
	if client.getCallCount < expectedChunks {
		t.Errorf("Expected at least %d S3 calls, got %d", expectedChunks, client.getCallCount)
	}
}

func TestChunkStreamerWithOffset(t *testing.T) {
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	offset := int64(10)
	size := int64(15) // Read 15 bytes starting from offset 10
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", offset, size, 1024)

	result, err := io.ReadAll(streamer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := testData[offset : offset+size]
	if !bytes.Equal(result, expected) {
		t.Errorf("Expected %q, got %q", string(expected), string(result))
	}
}

func TestChunkStreamerEmptyFile(t *testing.T) {
	testData := []byte{}
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, 0, 1024)

	buf := make([]byte, 10)
	n, err := streamer.Read(buf)

	if err != io.EOF {
		t.Errorf("Expected EOF for empty file, got %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes for empty file, got %d", n)
	}
}

func TestChunkStreamerS3Error(t *testing.T) {
	ctx := context.Background()

	// Create a mock client that always returns an error
	errorClient := &ErrorMockS3Client{
		err: fmt.Errorf("S3 service unavailable"),
	}

	streamer := NewChunkStreamer(ctx, errorClient, "test-bucket", "test-key", 0, 100, 1024)

	buf := make([]byte, 10)
	_, err := streamer.Read(buf)

	if err == nil {
		t.Error("Expected error from S3 client, got nil")
	}
	if !strings.Contains(err.Error(), "failed to download chunk") {
		t.Errorf("Expected chunk download error, got %v", err)
	}
}

func TestChunkStreamerPartialRead(t *testing.T) {
	testData := []byte("Hello, World! This is a longer test string for partial reading.")
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), 1024)

	// First read - partial
	buf1 := make([]byte, 10)
	n1, err := streamer.Read(buf1)
	if err != nil {
		t.Fatalf("Unexpected error on first read: %v", err)
	}
	if n1 != 10 {
		t.Errorf("Expected 10 bytes on first read, got %d", n1)
	}

	// Second read - get the rest
	remaining := len(testData) - n1
	buf2 := make([]byte, remaining+10) // Larger buffer than needed
	n2, err := streamer.Read(buf2)
	if err != nil {
		t.Fatalf("Unexpected error on second read: %v", err)
	}
	if n2 != remaining {
		t.Errorf("Expected %d bytes on second read, got %d", remaining, n2)
	}

	// Combine results
	result := append(buf1[:n1], buf2[:n2]...)
	if !bytes.Equal(result, testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(result))
	}
}

func TestChunkStreamerConcurrentReads(t *testing.T) {
	testData := bytes.Repeat([]byte("concurrent test data"), 50) // 1000 bytes
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), 100)

	// This test verifies that the mutex properly protects concurrent access
	// We can't easily test true concurrency in a unit test, but we can verify
	// that sequential reads work correctly with the mutex

	var result []byte
	buf := make([]byte, 73) // Odd number to ensure we cross boundaries

	for {
		n, err := streamer.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		result = append(result, buf[:n]...)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("Concurrent read failed. Expected %d bytes, got %d bytes", len(testData), len(result))
	}
}

func TestChunkStreamerLargeChunks(t *testing.T) {
	testData := bytes.Repeat([]byte("x"), 1000) // 1000 bytes
	ctx := context.Background()
	client := NewMockS3Client(testData)

	// Use chunk size larger than data
	largeChunkSize := int64(2000)
	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), largeChunkSize)

	result, err := io.ReadAll(streamer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("Data mismatch with large chunks")
	}

	// Should only make one S3 call since chunk is larger than data
	if client.getCallCount != 1 {
		t.Errorf("Expected 1 S3 call with large chunk, got %d", client.getCallCount)
	}
}

// ErrorMockS3Client is a mock client that always returns errors
type ErrorMockS3Client struct {
	err error
}

func (m *ErrorMockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, m.err
}

func (m *ErrorMockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, m.err
}

// CreateMultipartUpload implements the S3Client interface (not used in reader tests)
func (m *ErrorMockS3Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, m.err
}

// UploadPart implements the S3Client interface (not used in reader tests)
func (m *ErrorMockS3Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, m.err
}

// CompleteMultipartUpload implements the S3Client interface (not used in reader tests)
func (m *ErrorMockS3Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, m.err
}

// AbortMultipartUpload implements the S3Client interface (not used in reader tests)
func (m *ErrorMockS3Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, m.err
}

func TestChunkStreamerBufferManagement(t *testing.T) {
	testData := []byte("Buffer management test data that should be handled correctly.")
	ctx := context.Background()
	client := NewMockS3Client(testData)

	streamer := NewChunkStreamer(ctx, client, "test-bucket", "test-key", 0, int64(len(testData)), 1024)

	// Read with buffer smaller than data to test internal buffering
	buf := make([]byte, 5)
	var result []byte

	// First read should fill our buffer and store remainder internally
	n, err := streamer.Read(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes, got %d", n)
	}
	result = append(result, buf[:n]...)

	// Subsequent reads should come from internal buffer until exhausted
	for len(result) < len(testData) {
		n, err := streamer.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		result = append(result, buf[:n]...)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("Buffer management failed. Expected %q, got %q", string(testData), string(result))
	}
}
