package s3streamer

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestData represents a simple test record structure
type TestData struct {
	ID    string            `json:"id"`
	Name  string            `json:"name"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

// prepareTestData creates JSON lines test data with optional compression
func prepareTestData(t testing.TB, count int, compression Compression) []byte {
	// Create test records
	records := make([]TestData, count)
	for i := 0; i < count; i++ {
		records[i] = TestData{
			ID:   fmt.Sprintf("id-%04d", i),
			Name: fmt.Sprintf("Test Record %d", i),
			Attrs: map[string]string{
				"created": "2023-05-01",
				"type":    "test",
				"index":   fmt.Sprintf("%d", i),
			},
		}
	}

	// Convert to JSON lines format
	var buf bytes.Buffer
	for _, r := range records {
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("Failed to marshal test data: %v", err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	// Return uncompressed data
	if compression == Uncompressed {
		return buf.Bytes()
	}

	// Compress with gzip
	if compression == Gzip {
		var compressedBuf bytes.Buffer
		gw := gzip.NewWriter(&compressedBuf)
		if _, err := gw.Write(buf.Bytes()); err != nil {
			t.Fatalf("Failed to compress with gzip: %v", err)
		}
		if err := gw.Close(); err != nil {
			t.Fatalf("Failed to close gzip writer: %v", err)
		}
		return compressedBuf.Bytes()
	}

	t.Fatalf("Unsupported compression type: %v", compression)
	return nil
}

// MockS3Client implements a mock S3 client that supports range requests
type MockS3Client struct {
	data          []byte
	contentLength int64
	getCallCount  int
	headCallCount int
}

// NewMockS3Client creates a new mock S3 client with the given data
func NewMockS3Client(data []byte) *MockS3Client {
	return &MockS3Client{
		data:          data,
		contentLength: int64(len(data)),
	}
}

// GetObject implements the S3Client interface
func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.getCallCount++

	// Parse range if specified
	var start, end int64 = 0, m.contentLength - 1
	if params.Range != nil {
		var parseErr error
		start, end, parseErr = parseRangeHeader(*params.Range, m.contentLength)
		if parseErr != nil {
			return nil, parseErr
		}
	}

	// Validate range
	if start < 0 || start >= m.contentLength || end < start || end >= m.contentLength {
		return nil, fmt.Errorf("invalid range: %d-%d (content length: %d)", start, end, m.contentLength)
	}

	// Extract data for this range
	rangeData := m.data[start : end+1]
	rangeLength := int64(len(rangeData))

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(rangeData)),
		ContentLength: &rangeLength,
	}, nil
}

// HeadObject implements the S3Client interface
func (m *MockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.headCallCount++
	return &s3.HeadObjectOutput{
		ContentLength: &m.contentLength,
	}, nil
}

// parseRangeHeader parses S3 range header formats like "bytes=0-499" or "bytes=500-"
func parseRangeHeader(rangeHeader string, contentLength int64) (int64, int64, error) {
	var start, end int64
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
		// Check if it's an open-ended range (e.g., "bytes=100-")
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err != nil {
			return 0, 0, fmt.Errorf("invalid range format: %s", rangeHeader)
		}
		end = contentLength - 1
	}

	// Validate and adjust ranges
	if start < 0 {
		start = 0
	}
	if end >= contentLength {
		end = contentLength - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	return start, end, nil
}

func TestS3StreamerUncompressed(t *testing.T) {
	// Prepare test data
	testData := prepareTestData(t, 50, Uncompressed)
	mockClient := NewMockS3Client(testData)

	// Create streamer with small chunks
	streamer := NewS3Streamer(mockClient, 1)
	streamer.chunkSize = 1024 // 1KB chunks

	// Process the stream
	var records []TestData
	err := streamer.Stream(context.Background(), "test-bucket", "test-key", 0, func(line []byte) error {
		var record TestData
		if err := json.Unmarshal(line, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if got, want := len(records), 50; got != want {
		t.Errorf("Record count = %d, want %d", got, want)
	}

	if got, want := mockClient.getCallCount, 1; got <= want {
		t.Errorf("GetObject call count = %d, want > %d", got, want)
	}

	if got, want := mockClient.headCallCount, 1; got != want {
		t.Errorf("HeadObject call count = %d, want %d", got, want)
	}

	// Verify first and last record
	if len(records) > 0 {
		if got, want := records[0].ID, "id-0000"; got != want {
			t.Errorf("First record ID = %s, want %s", got, want)
		}

		if got, want := records[49].ID, "id-0049"; got != want {
			t.Errorf("Last record ID = %s, want %s", got, want)
		}
	}
}

func TestS3StreamerGzipped(t *testing.T) {
	// Prepare test data
	testData := prepareTestData(t, 50, Gzip)
	mockClient := NewMockS3Client(testData)

	// Create streamer
	streamer := NewS3Streamer(mockClient, 1)

	// Process the stream
	var records []TestData
	err := streamer.Stream(context.Background(), "test-bucket", "test-key", 0, func(line []byte) error {
		var record TestData
		if err := json.Unmarshal(line, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if got, want := len(records), 50; got != want {
		t.Errorf("Record count = %d, want %d", got, want)
	}

	if got, want := mockClient.getCallCount, 1; got <= want {
		t.Errorf("GetObject call count = %d, want > %d", got, want)
	}

	if got, want := mockClient.headCallCount, 1; got != want {
		t.Errorf("HeadObject call count = %d, want %d", got, want)
	}

	// Verify first and last record
	if len(records) > 0 {
		if got, want := records[0].ID, "id-0000"; got != want {
			t.Errorf("First record ID = %s, want %s", got, want)
		}

		if got, want := records[49].ID, "id-0049"; got != want {
			t.Errorf("Last record ID = %s, want %s", got, want)
		}
	}
}

func TestS3StreamerWithOffset(t *testing.T) {
	// Prepare a larger set of test data
	testData := prepareTestData(t, 100, Uncompressed)
	mockClient := NewMockS3Client(testData)

	// Find the byte position of the 50th record (0-indexed)
	var offset int64
	lines := bytes.Split(testData, []byte{'\n'})
	for i := 0; i < 50; i++ {
		if i < len(lines) {
			offset += int64(len(lines[i])) + 1 // +1 for the newline
		}
	}

	// Create streamer
	streamer := NewS3Streamer(mockClient, 1)

	// Process the stream from offset
	var records []TestData
	err := streamer.Stream(context.Background(), "test-bucket", "test-key", offset, func(line []byte) error {
		var record TestData
		if err := json.Unmarshal(line, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if got, want := len(records), 50; got != want {
		t.Errorf("Record count = %d, want %d", got, want)
	}

	// Verify first record after offset is the 50th record
	if len(records) > 0 {
		if got, want := records[0].ID, "id-0050"; got != want {
			t.Errorf("First record ID = %s, want %s", got, want)
		}

		if got, want := records[49].ID, "id-0099"; got != want {
			t.Errorf("Last record ID = %s, want %s", got, want)
		}
	}
}

func TestS3StreamerProcessingError(t *testing.T) {
	// Prepare test data
	testData := prepareTestData(t, 50, Uncompressed)
	mockClient := NewMockS3Client(testData)

	// Create streamer
	streamer := NewS3Streamer(mockClient, 1)

	// Inject an error during processing of the 10th record
	expectedError := errors.New("test processing error")
	var processedCount int
	err := streamer.Stream(context.Background(), "test-bucket", "test-key", 0, func(line []byte) error {
		processedCount++
		if processedCount == 10 {
			return expectedError
		}
		return nil
	})

	// Verify results
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	if !strings.Contains(err.Error(), expectedError.Error()) {
		t.Errorf("Error message = %q, want to contain %q", err.Error(), expectedError.Error())
	}

	if got, want := processedCount, 10; got != want {
		t.Errorf("Processed record count = %d, want %d", got, want)
	}
}

func TestS3StreamerInvalidRange(t *testing.T) {
	// Prepare test data
	testData := prepareTestData(t, 50, Uncompressed)
	mockClient := NewMockS3Client(testData)

	// Create streamer
	streamer := NewS3Streamer(mockClient, 1)

	// Attempt to stream with an offset beyond the file size
	invalidOffset := int64(len(testData)) + 100
	err := streamer.Stream(context.Background(), "test-bucket", "test-key", invalidOffset, func(line []byte) error {
		return nil
	})

	// Verify results
	if err == nil {
		t.Fatal("Expected an error for invalid offset, got nil")
	}

	if !strings.Contains(err.Error(), "offset") || !strings.Contains(err.Error(), "exceeds object size") {
		t.Errorf("Error message = %q, want to contain 'offset' and 'exceeds object size'", err.Error())
	}
}

func BenchmarkS3StreamerPerformance(b *testing.B) {
	// Prepare a large dataset
	testData := prepareTestData(b, 1000, Gzip)

	// Test different chunk sizes
	chunkSizes := []int64{
		256 * 1024,      // 256KB
		512 * 1024,      // 512KB
		1 * 1024 * 1024, // 1MB
		5 * 1024 * 1024, // 5MB
	}

	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("ChunkSize_%dKB", chunkSize/1024), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mockClient := NewMockS3Client(testData)
				streamer := NewS3Streamer(mockClient, 1)
				streamer.chunkSize = chunkSize
				b.StartTimer()

				var count int
				err := streamer.Stream(context.Background(), "test-bucket", "test-key", 0, func(line []byte) error {
					count++
					return nil
				})

				if err != nil {
					b.Fatalf("Error streaming data: %v", err)
				}

				if count != 1000 {
					b.Fatalf("Expected 1000 records, got %d", count)
				}
			}
		})
	}
}
