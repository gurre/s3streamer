package s3streamer

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
)

// CompressedS3Writer wraps an S3Writer with compression support.
// It applies the specified compression type before writing to S3.
//
// Supported Formats:
//   - Uncompressed: No compression (passthrough to S3Writer)
//   - Gzip: Gzip compression
//   - Bzip2: Bzip2 compression
//
// Thread Safety: CompressedS3Writer is safe for concurrent use by multiple
// goroutines, as the underlying S3Writer is thread-safe and compression
// operations are serialized.
//
// Performance: Compression adds CPU overhead but reduces network transfer size.
// Gzip is faster but less effective than bzip2. Choose based on your CPU vs
// bandwidth constraints.
//
// Error Handling: Compression errors are propagated to the caller. If compression
// fails, the underlying S3 upload is automatically aborted.
//
// Example:
//
//	writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer writer.Close()
//	data := []byte("hello world\n")
//	n, err := writer.Write(data)
type CompressedS3Writer struct {
	s3Writer        *S3Writer
	compressor      io.WriteCloser
	compressionType Compression
}

// NewCompressedS3Writer creates a new CompressedS3Writer with the specified compression type.
//
// Supported compression types:
//   - Uncompressed: No compression
//   - Gzip: Gzip compression
//   - Bzip2: Bzip2 compression
//
// Parameters are validated by the underlying S3Writer constructor.
//
// Example:
//
//	// Create gzip compressed writer
//	writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer writer.Close()
func NewCompressedS3Writer(ctx context.Context, client S3Client, bucket, key string, partSize int64, compression Compression) (*CompressedS3Writer, error) {
	// Create the underlying S3Writer
	s3Writer, err := NewS3Writer(ctx, client, bucket, key, partSize)
	if err != nil {
		return nil, err
	}

	wrapper := &CompressedS3Writer{
		s3Writer:        s3Writer,
		compressionType: compression,
	}

	// Set up the appropriate compressor
	if err := wrapper.setupCompressor(); err != nil {
		s3Writer.Abort() // Clean up the S3 writer on error
		return nil, err
	}

	return wrapper, nil
}

// Write implements io.Writer interface. Data is compressed before being written to S3.
// Example:
//
//	writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
//	data := []byte("hello world\n")
//	n, err := writer.Write(data)
func (cw *CompressedS3Writer) Write(p []byte) (int, error) {
	if cw.compressor != nil {
		return cw.compressor.Write(p)
	}
	// No compression, write directly to S3Writer
	return cw.s3Writer.Write(p)
}

// Close finalizes the compression and the S3 upload. This method must be called
// to ensure all data is properly compressed and written to S3.
// Example:
//
//	writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
//	defer writer.Close()
//	// ... write data ...
//	if err := writer.Close(); err != nil {
//	    log.Fatal(err)
//	}
func (cw *CompressedS3Writer) Close() error {
	// First close the compressor to flush any remaining data
	if cw.compressor != nil {
		if err := cw.compressor.Close(); err != nil {
			// Still try to close the S3Writer even if compressor close fails
			cw.s3Writer.Abort()
			return fmt.Errorf("failed to close compressor: %w", err)
		}
	}

	// Then close the underlying S3Writer
	return cw.s3Writer.Close()
}

// Abort cancels the upload and cleans up resources.
// Example:
//
//	writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
//	// ... write some data ...
//	if someError != nil {
//	    writer.Abort() // Clean up the partial upload
//	    return
//	}
func (cw *CompressedS3Writer) Abort() error {
	// Close compressor if it exists (ignore errors since we're aborting)
	if cw.compressor != nil {
		cw.compressor.Close()
	}

	// Abort the underlying S3Writer
	return cw.s3Writer.Abort()
}

// setupCompressor initializes the appropriate compressor based on the detected compression type
func (cw *CompressedS3Writer) setupCompressor() error {
	switch cw.compressionType {
	case Gzip:
		gzipWriter := gzip.NewWriter(cw.s3Writer)
		cw.compressor = gzipWriter
		return nil
	case Bzip2:
		bzip2Writer, err := bzip2.NewWriter(cw.s3Writer, &bzip2.WriterConfig{
			Level: bzip2.DefaultCompression,
		})
		if err != nil {
			return fmt.Errorf("failed to create bzip2 writer: %w", err)
		}
		cw.compressor = bzip2Writer
		return nil
	case Uncompressed:
		// No compressor needed
		cw.compressor = nil
		return nil
	default:
		return fmt.Errorf("unsupported compression type: %v", cw.compressionType)
	}
}
