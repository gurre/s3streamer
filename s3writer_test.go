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

// mockS3ClientWriter extends the existing mock to support writer operations
type mockS3ClientWriter struct {
	getObjectFunc               func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	headObjectFunc              func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	createMultipartUploadFunc   func(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	uploadPartFunc              func(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	completeMultipartUploadFunc func(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	abortMultipartUploadFunc    func(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)

	// Storage for uploaded data
	uploadedParts map[int32][]byte
	uploadID      string
	completed     bool
	aborted       bool
}

func (m *mockS3ClientWriter) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getObjectFunc != nil {
		return m.getObjectFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetObject not implemented")
}

func (m *mockS3ClientWriter) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headObjectFunc != nil {
		return m.headObjectFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("HeadObject not implemented")
}

func (m *mockS3ClientWriter) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if m.createMultipartUploadFunc != nil {
		return m.createMultipartUploadFunc(ctx, params, optFns...)
	}

	m.uploadID = "test-upload-id-12345"
	m.uploadedParts = make(map[int32][]byte)
	return &s3.CreateMultipartUploadOutput{
		UploadId: &m.uploadID,
	}, nil
}

func (m *mockS3ClientWriter) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if m.uploadPartFunc != nil {
		return m.uploadPartFunc(ctx, params, optFns...)
	}

	// Read the body and store it
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.uploadedParts[*params.PartNumber] = data
	etag := fmt.Sprintf("\"etag-part-%d\"", *params.PartNumber)

	return &s3.UploadPartOutput{
		ETag: &etag,
	}, nil
}

func (m *mockS3ClientWriter) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if m.completeMultipartUploadFunc != nil {
		return m.completeMultipartUploadFunc(ctx, params, optFns...)
	}

	m.completed = true
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3ClientWriter) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if m.abortMultipartUploadFunc != nil {
		return m.abortMultipartUploadFunc(ctx, params, optFns...)
	}

	m.aborted = true
	return &s3.AbortMultipartUploadOutput{}, nil
}

// GetUploadedData returns all uploaded data concatenated in order
func (m *mockS3ClientWriter) GetUploadedData() []byte {
	var result []byte
	for i := int32(1); i <= int32(len(m.uploadedParts)); i++ {
		if data, exists := m.uploadedParts[i]; exists {
			result = append(result, data...)
		}
	}
	return result
}

func TestS3Writer_BasicWrite(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	testData := []byte("Hello, World!\nThis is a test file.\n")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	if !mock.completed {
		t.Error("Multipart upload was not completed")
	}

	uploadedData := mock.GetUploadedData()
	if !bytes.Equal(uploadedData, testData) {
		t.Errorf("Uploaded data doesn't match. Expected: %q, Got: %q", testData, uploadedData)
	}
}

func TestS3Writer_LargeData(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	// Use the minimum part size but create larger test data
	partSize := int64(5 * 1024 * 1024) // 5MB minimum
	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", partSize)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	// Write data larger than one part (create ~7MB of data)
	testData := bytes.Repeat([]byte("Hello, World! This is a line of test data.\n"), 160000) // ~7MB
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	if !mock.completed {
		t.Error("Multipart upload was not completed")
	}

	// Should have uploaded multiple parts
	if len(mock.uploadedParts) < 2 {
		t.Errorf("Expected multiple parts, got %d", len(mock.uploadedParts))
	}

	uploadedData := mock.GetUploadedData()
	if !bytes.Equal(uploadedData, testData) {
		t.Errorf("Uploaded data doesn't match expected data")
	}
}

func TestS3Writer_MultipleWrites(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	var expectedData []byte

	// Write multiple chunks
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("Chunk %d: This is test data.\n", i))
		expectedData = append(expectedData, data...)

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Failed to write chunk %d: %v", i, err)
		}

		if n != len(data) {
			t.Errorf("Chunk %d: expected to write %d bytes, wrote %d", i, len(data), n)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	uploadedData := mock.GetUploadedData()
	if !bytes.Equal(uploadedData, expectedData) {
		t.Errorf("Uploaded data doesn't match expected data")
	}
}

func TestS3Writer_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{
		createMultipartUploadFunc: func(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			return nil, fmt.Errorf("simulated S3 error")
		},
	}

	_, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err == nil {
		t.Fatal("Expected error when creating S3Writer with failing S3 client")
	}

	if !strings.Contains(err.Error(), "failed to initiate multipart upload") {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

func TestS3Writer_UploadPartError(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{
		uploadPartFunc: func(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			return nil, fmt.Errorf("simulated upload part error")
		},
	}

	// Use minimum part size and write enough data to trigger part upload
	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	// Write enough data to trigger a part upload (more than 5MB)
	testData := bytes.Repeat([]byte("x"), 6*1024*1024) // 6MB
	_, err = writer.Write(testData)
	if err == nil {
		t.Fatal("Expected error when uploading part fails")
	}

	if !strings.Contains(err.Error(), "failed to upload part") {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

func TestS3Writer_CloseWithoutWrites(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}

	err = writer.Close()
	if err == nil {
		t.Fatal("Expected error when closing writer without writes, but got nil")
	}

	// Should get an error about no parts uploaded
	if err.Error() != "no parts uploaded for multipart upload" {
		t.Errorf("Expected 'no parts uploaded' error, got: %v", err)
	}

	if mock.completed {
		t.Error("Multipart upload should not be completed when no data was written")
	}
}

func TestS3Writer_Abort(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}

	// Write some data
	testData := []byte("Hello, World!")
	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Abort the upload
	err = writer.Abort()
	if err != nil {
		t.Fatalf("Failed to abort upload: %v", err)
	}

	if !mock.aborted {
		t.Error("Multipart upload was not aborted")
	}

	if mock.completed {
		t.Error("Multipart upload should not be completed after abort")
	}
}

func TestS3Writer_WriteAfterClose(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}

	// Write some data first so we can close successfully
	testData := []byte("test data")
	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Try to write after close
	_, err = writer.Write([]byte("test"))
	if err == nil {
		t.Fatal("Expected error when writing to closed writer")
	}

	if !strings.Contains(err.Error(), "closed S3Writer") {
		t.Errorf("Expected error message about closed writer, got: %v", err)
	}
}

func TestS3Writer_MinPartSize(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	// Test that part size is enforced to minimum 5MiB
	_, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 1024) // 1KB
	if err == nil {
		t.Fatal("Expected error for part size below 5MiB minimum")
	}

	// Verify error message
	expectedErrMsg := "part size must be at least 5MiB"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("Expected error message containing %q, got %q", expectedErrMsg, err.Error())
	}

	// Verify valid part size works
	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024) // 5MiB
	if err != nil {
		t.Fatalf("Failed to create S3Writer with valid part size: %v", err)
	}
	defer writer.Close()

	// The writer should have the correct part size
	if writer.partSize != 5*1024*1024 {
		t.Errorf("Expected part size to be 5MiB, got %d", writer.partSize)
	}
}

func TestS3Writer_IoWriter(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	// Test that it can be used as io.Writer
	var w io.Writer = writer

	testData := []byte("Testing io.Writer interface")
	n, err := w.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write via io.Writer interface: %v", err)
	}

	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}
}

func TestS3Writer_ParameterValidation(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	tests := []struct {
		name     string
		ctx      context.Context
		client   S3Client
		bucket   string
		key      string
		partSize int64
		wantErr  string
	}{
		{
			name:     "nil context",
			ctx:      nil,
			client:   mock,
			bucket:   "bucket",
			key:      "key",
			partSize: 5 * 1024 * 1024,
			wantErr:  "context cannot be nil",
		},
		{
			name:     "nil client",
			ctx:      ctx,
			client:   nil,
			bucket:   "bucket",
			key:      "key",
			partSize: 5 * 1024 * 1024,
			wantErr:  "S3 client cannot be nil",
		},
		{
			name:     "empty bucket",
			ctx:      ctx,
			client:   mock,
			bucket:   "",
			key:      "key",
			partSize: 5 * 1024 * 1024,
			wantErr:  "bucket name cannot be empty",
		},
		{
			name:     "empty key",
			ctx:      ctx,
			client:   mock,
			bucket:   "bucket",
			key:      "",
			partSize: 5 * 1024 * 1024,
			wantErr:  "object key cannot be empty",
		},
		{
			name:     "part size too small",
			ctx:      ctx,
			client:   mock,
			bucket:   "bucket",
			key:      "key",
			partSize: 1024 * 1024, // 1MiB
			wantErr:  "part size must be at least 5MiB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewS3Writer(tt.ctx, tt.client, tt.bucket, tt.key, tt.partSize)
			if err == nil {
				t.Fatalf("Expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestS3Writer_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mock := &mockS3ClientWriter{}

	writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create S3Writer: %v", err)
	}
	defer writer.Close()

	// Cancel the context
	cancel()

	// Try to write after cancellation
	testData := []byte("test data")
	_, err = writer.Write(testData)
	if err == nil {
		t.Fatal("Expected error due to context cancellation")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}

	// Try to close after cancellation
	err = writer.Close()
	if err == nil {
		t.Fatal("Expected error due to context cancellation")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// prepareWriterTestData creates test data similar to the reader tests
func prepareWriterTestData(b testing.TB, count int) []byte {
	// Create test records similar to reader tests
	var buf bytes.Buffer
	for i := 0; i < count; i++ {
		record := fmt.Sprintf(`{"id":"id-%04d","name":"Test Record %d","attrs":{"created":"2023-05-01","type":"test","index":"%d"}}`, i, i, i)
		buf.WriteString(record)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func BenchmarkS3WriterPerformance(b *testing.B) {
	// Prepare test data - 1000 records similar to reader benchmark
	testData := prepareWriterTestData(b, 1000)

	// Test different part sizes (AWS minimum is 5MB, but we can test larger sizes)
	partSizes := []int64{
		5 * 1024 * 1024,  // 5MB (minimum)
		10 * 1024 * 1024, // 10MB
		25 * 1024 * 1024, // 25MB
		50 * 1024 * 1024, // 50MB
	}

	for _, partSize := range partSizes {
		b.Run(fmt.Sprintf("PartSize_%dMB", partSize/(1024*1024)), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ctx := context.Background()
				mock := &mockS3ClientWriter{}

				writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", partSize)
				if err != nil {
					b.Fatalf("Failed to create S3Writer: %v", err)
				}
				b.StartTimer()

				// Write all test data
				_, err = writer.Write(testData)
				if err != nil {
					b.Fatalf("Error writing data: %v", err)
				}

				// Close to complete the upload
				err = writer.Close()
				if err != nil {
					b.Fatalf("Error closing writer: %v", err)
				}

				b.StopTimer()
				// Verify all data was written correctly
				uploadedData := mock.GetUploadedData()
				if !bytes.Equal(uploadedData, testData) {
					b.Fatalf("Uploaded data doesn't match original data")
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkCompressedS3WriterPerformance(b *testing.B) {
	// Prepare test data - 1000 records
	testData := prepareWriterTestData(b, 1000)

	// Test different compression types with fixed part size
	partSize := int64(10 * 1024 * 1024) // 10MB

	compressionTypes := []struct {
		name        string
		compression Compression
	}{
		{"Uncompressed", Uncompressed},
		{"Gzip", Gzip},
		{"Bzip2", Bzip2},
	}

	for _, ct := range compressionTypes {
		b.Run(fmt.Sprintf("Compression_%s", ct.name), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ctx := context.Background()
				mock := &mockS3ClientWriter{}

				writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-key", partSize, ct.compression)
				if err != nil {
					b.Fatalf("Failed to create CompressedS3Writer: %v", err)
				}
				b.StartTimer()

				// Write all test data
				_, err = writer.Write(testData)
				if err != nil {
					b.Fatalf("Error writing data: %v", err)
				}

				// Close to complete the upload
				err = writer.Close()
				if err != nil {
					b.Fatalf("Error closing writer: %v", err)
				}

				b.StopTimer()
				// For compressed data, we can't easily verify the content matches,
				// but we can verify that data was uploaded
				uploadedData := mock.GetUploadedData()
				if len(uploadedData) == 0 {
					b.Fatalf("No data was uploaded")
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkS3WriterChunkedWrites(b *testing.B) {
	// Test performance with many small writes vs fewer large writes
	testRecord := []byte(`{"id":"test-record","name":"Test Record","attrs":{"created":"2023-05-01","type":"test"}}` + "\n")
	totalRecords := 1000
	partSize := int64(10 * 1024 * 1024) // 10MB

	writePatterns := []struct {
		name      string
		chunkSize int
		numWrites int
	}{
		{"SingleWrite", totalRecords, 1},
		{"Write100Records", 100, 10},
		{"Write10Records", 10, 100},
		{"WritePerRecord", 1, 1000},
	}

	for _, pattern := range writePatterns {
		b.Run(pattern.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ctx := context.Background()
				mock := &mockS3ClientWriter{}

				writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", partSize)
				if err != nil {
					b.Fatalf("Failed to create S3Writer: %v", err)
				}
				b.StartTimer()

				// Write data according to pattern
				for j := 0; j < pattern.numWrites; j++ {
					var chunk []byte
					for k := 0; k < pattern.chunkSize; k++ {
						chunk = append(chunk, testRecord...)
					}

					_, err = writer.Write(chunk)
					if err != nil {
						b.Fatalf("Error writing chunk %d: %v", j, err)
					}
				}

				// Close to complete the upload
				err = writer.Close()
				if err != nil {
					b.Fatalf("Error closing writer: %v", err)
				}

				b.StopTimer()
				// Verify expected amount of data was written
				uploadedData := mock.GetUploadedData()
				expectedSize := len(testRecord) * totalRecords
				if len(uploadedData) != expectedSize {
					b.Fatalf("Expected %d bytes, got %d bytes", expectedSize, len(uploadedData))
				}
				b.StartTimer()
			}
		})
	}
}
