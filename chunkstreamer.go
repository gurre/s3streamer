package s3streamer

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ChunkStreamer is an io.Reader implementation that streams data in chunks from S3.
// Example:
//
//	streamer := s3streamer.NewChunkStreamer(ctx, client, "my-bucket", "data.json.gz", 0, 1024*1024, 5*1024*1024)
//	data := make([]byte, 1024)
//	n, err := streamer.Read(data)
type ChunkStreamer struct {
	client        S3Client
	bucket, key   string
	offset, size  int64
	chunkSize     int64
	ctx           context.Context
	currentOffset int64
	eof           bool
	buffer        []byte
	mu            sync.Mutex
}

// NewChunkStreamer creates a new ChunkStreamer for retrieving a file from S3 in chunks.
// Example:
//
//	streamer := s3streamer.NewChunkStreamer(ctx, client, "my-bucket", "data.json.gz", 0, 1024*1024, 5*1024*1024)
func NewChunkStreamer(ctx context.Context, client S3Client, bucket, key string, offset, size, chunkSize int64) *ChunkStreamer {
	return &ChunkStreamer{
		client:        client,
		bucket:        bucket,
		key:           key,
		offset:        offset,
		size:          size,
		chunkSize:     chunkSize,
		ctx:           ctx,
		currentOffset: offset,
		eof:           false,
		buffer:        []byte{},
	}
}

// Read implements io.Reader to fetch chunks from S3 as needed.
// Example:
//
//	streamer := s3streamer.NewChunkStreamer(ctx, client, "my-bucket", "data.json.gz", 0, 1024*1024, 5*1024*1024)
//	data := make([]byte, 1024)
//	n, err := streamer.Read(data)
func (c *ChunkStreamer) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.eof && len(c.buffer) == 0 {
		return 0, io.EOF
	}

	// If we have data in the buffer, return it
	if len(c.buffer) > 0 {
		n := copy(p, c.buffer)
		c.buffer = c.buffer[n:]
		return n, nil
	}

	// If we've reached the end of the file, return EOF
	if c.currentOffset >= c.offset+c.size {
		c.eof = true
		return 0, io.EOF
	}

	// Calculate the end of the range for this chunk
	endOffset := c.currentOffset + c.chunkSize - 1
	if endOffset >= c.offset+c.size {
		endOffset = c.offset + c.size - 1
	}

	// Set up range header
	rangeHeader := fmt.Sprintf("bytes=%d-%d", c.currentOffset, endOffset)

	// Get this chunk
	resp, err := c.client.GetObject(c.ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &c.key,
		Range:  &rangeHeader,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to download chunk (%s): %w", rangeHeader, err)
	}
	defer resp.Body.Close()

	// Read the chunk into our buffer
	chunkData, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk data: %w", err)
	}

	// Move to the next chunk for subsequent reads
	c.currentOffset = endOffset + 1

	// If we're at the end of the file, mark EOF
	if c.currentOffset >= c.offset+c.size {
		c.eof = true
	}

	// Copy as much as we can into p
	n := copy(p, chunkData)

	// If we couldn't copy everything, store the rest in our buffer
	if n < len(chunkData) {
		c.buffer = chunkData[n:]
	}

	return n, nil
}
