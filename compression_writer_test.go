package s3streamer

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/dsnet/compress/bzip2"
)

func TestCompressedS3Writer_GzipCompression(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-file.gz", 5*1024*1024, Gzip)
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
	}
	defer writer.Close()

	testData := []byte("Hello, World!\nThis is a test file.\nCompressed with gzip.\n")
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

	// Verify that data was compressed
	uploadedData := mock.GetUploadedData()
	if len(uploadedData) == 0 {
		t.Fatal("No data was uploaded")
	}

	// Decompress and verify
	reader, err := gzip.NewReader(bytes.NewReader(uploadedData))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	if !bytes.Equal(decompressed, testData) {
		t.Errorf("Decompressed data doesn't match original. Expected: %q, Got: %q", testData, decompressed)
	}
}

func TestCompressedS3Writer_Bzip2Compression(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-file.bz2", 5*1024*1024, Bzip2)
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
	}
	defer writer.Close()

	testData := []byte("Hello, World!\nThis is a test file.\nCompressed with bzip2.\n")
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

	// Verify that data was compressed
	uploadedData := mock.GetUploadedData()
	if len(uploadedData) == 0 {
		t.Fatal("No data was uploaded")
	}

	// Decompress and verify
	reader, err := bzip2.NewReader(bytes.NewReader(uploadedData), nil)
	if err != nil {
		t.Fatalf("Failed to create bzip2 reader: %v", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	if !bytes.Equal(decompressed, testData) {
		t.Errorf("Decompressed data doesn't match original. Expected: %q, Got: %q", testData, decompressed)
	}
}

func TestCompressedS3Writer_NoCompression(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-file.txt", 5*1024*1024, Uncompressed)
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
	}
	defer writer.Close()

	testData := []byte("Hello, World!\nThis is a test file.\nNo compression.\n")
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

	// Verify that data was not compressed
	uploadedData := mock.GetUploadedData()
	if !bytes.Equal(uploadedData, testData) {
		t.Errorf("Uploaded data doesn't match original. Expected: %q, Got: %q", testData, uploadedData)
	}
}

func TestCompressedS3Writer_LargeData(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "large-file.gz", 5*1024*1024, Gzip) // 5MiB minimum part size
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
	}
	defer writer.Close()

	// Create large test data that should compress well
	testData := bytes.Repeat([]byte("This is a repeated line for compression testing.\n"), 100)

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

	// Verify compression worked
	uploadedData := mock.GetUploadedData()
	if len(uploadedData) >= len(testData) {
		t.Errorf("Compressed data (%d bytes) should be smaller than original (%d bytes)", len(uploadedData), len(testData))
	}

	// Decompress and verify
	reader, err := gzip.NewReader(bytes.NewReader(uploadedData))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	if !bytes.Equal(decompressed, testData) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestCompressedS3Writer_Abort(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-file.gz", 5*1024*1024, Gzip)
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
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

func TestCompressedS3Writer_IoWriter(t *testing.T) {
	ctx := context.Background()
	mock := &mockS3ClientWriter{}

	writer, err := NewCompressedS3Writer(ctx, mock, "test-bucket", "test-file.gz", 5*1024*1024, Gzip)
	if err != nil {
		t.Fatalf("Failed to create CompressedS3Writer: %v", err)
	}
	defer writer.Close()

	// Test that it can be used as io.Writer
	var w io.Writer = writer

	testData := []byte("Testing io.Writer interface with compression")
	n, err := w.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write via io.Writer interface: %v", err)
	}

	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}
}
