package s3streamer

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestCompressionExtension(t *testing.T) {
	tests := []struct {
		compression Compression
		expected    string
	}{
		{Uncompressed, ""},
		{Gzip, ".gz"},
		{Bzip2, ".bz2"},
	}

	for _, test := range tests {
		result := test.compression.Extension()
		if result != test.expected {
			t.Errorf("Compression %d: expected extension %q, got %q", test.compression, test.expected, result)
		}
	}
}

func TestCompressionExtensionUnknown(t *testing.T) {
	// Test with an invalid compression value
	invalidCompression := Compression(999)
	result := invalidCompression.Extension()
	expected := "[unknown]"

	if result != expected {
		t.Errorf("Invalid compression: expected %q, got %q", expected, result)
	}
}

func TestDetectCompressionGzip(t *testing.T) {
	// Gzip magic bytes: 0x1F, 0x8B, 0x08
	gzipData := []byte{0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF}

	compression := DetectCompression(gzipData)
	if compression != Gzip {
		t.Errorf("Expected Gzip compression, got %d", compression)
	}
}

func TestDetectCompressionBzip2(t *testing.T) {
	// Bzip2 magic bytes: 0x42, 0x5A, 0x68
	bzip2Data := []byte{0x42, 0x5A, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59}

	compression := DetectCompression(bzip2Data)
	if compression != Bzip2 {
		t.Errorf("Expected Bzip2 compression, got %d", compression)
	}
}

func TestDetectCompressionUncompressed(t *testing.T) {
	// Regular text data
	textData := []byte("Hello, World! This is uncompressed text data.")

	compression := DetectCompression(textData)
	if compression != Uncompressed {
		t.Errorf("Expected Uncompressed, got %d", compression)
	}
}

func TestDetectCompressionEmptyData(t *testing.T) {
	emptyData := []byte{}

	compression := DetectCompression(emptyData)
	if compression != Uncompressed {
		t.Errorf("Expected Uncompressed for empty data, got %d", compression)
	}
}

func TestDetectCompressionShortData(t *testing.T) {
	// Data shorter than magic bytes
	shortData := []byte{0x1F, 0x8B} // Only 2 bytes, gzip needs 3

	compression := DetectCompression(shortData)
	if compression != Uncompressed {
		t.Errorf("Expected Uncompressed for short data, got %d", compression)
	}
}

func TestDetectCompressionPartialMatch(t *testing.T) {
	// Data that starts like gzip but isn't
	partialGzipData := []byte{0x1F, 0x8B, 0x09, 0x00, 0x00} // Wrong third byte

	compression := DetectCompression(partialGzipData)
	if compression != Uncompressed {
		t.Errorf("Expected Uncompressed for partial match, got %d", compression)
	}
}

func TestDecompressUncompressed(t *testing.T) {
	testData := "Hello, World! This is uncompressed test data."
	reader := strings.NewReader(testData)

	decompressed, err := Decompress(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	result, err := io.ReadAll(decompressed)
	if err != nil {
		t.Fatalf("Error reading decompressed data: %v", err)
	}

	if string(result) != testData {
		t.Errorf("Expected %q, got %q", testData, string(result))
	}
}

func TestDecompressBzip2SimpleDetection(t *testing.T) {
	// Create a simple test with just bzip2 magic bytes followed by data
	// This tests the detection logic even if we can't easily create valid bzip2 data
	bzip2Magic := []byte{0x42, 0x5A, 0x68} // bzip2 magic bytes
	additionalData := []byte("some data that might not decompress properly")
	testData := append(bzip2Magic, additionalData...)

	reader := bytes.NewReader(testData)
	decompressed, err := Decompress(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// The decompressed reader should be a bzip2.Reader
	// We can't easily verify the content without valid bzip2 data,
	// but we can verify that the function returns without error
	if decompressed == nil {
		t.Error("Expected decompressed reader, got nil")
	}
}

func TestDecompressEmptyStream(t *testing.T) {
	reader := bytes.NewReader([]byte{})

	decompressed, err := Decompress(reader)
	if err != nil && err != io.EOF {
		t.Fatalf("Unexpected error: %v", err)
	}

	result, err := io.ReadAll(decompressed)
	if err != nil && err != io.EOF {
		t.Fatalf("Error reading decompressed data: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(result))
	}
}
