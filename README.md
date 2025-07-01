# s3streamer

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/s3streamer.svg)](https://pkg.go.dev/github.com/gurre/s3streamer)
[![Go Report Card](https://goreportcard.com/badge/github.com/gurre/s3streamer)](https://goreportcard.com/report/github.com/gurre/s3streamer)

A high-performance, memory-efficient Go library for streaming large objects from Amazon S3. Built on Go's standard `io.Reader` interface, s3streamer enables processing of multi-gigabyte files without loading them entirely into memory.

## Key Features

- **Memory Efficient**: Stream objects of any size with configurable chunk sizes
- **Standard Library Integration**: Implements `io.Reader` for seamless integration with Go's ecosystem
- **Automatic Compression Detection**: Supports gzip and bzip2 with automatic detection via magic bytes
- **Resume Capability**: Start streaming from any byte offset for resumable processing
- **Line-by-Line Processing**: Optimized for JSON Lines and other line-delimited formats with offset tracking
- **High Performance**: Configurable chunking with up to 10MB line buffer support
- **AWS SDK v2 Compatible**: Works with the latest AWS SDK for Go v2

## Installation

```bash
go get github.com/gurre/s3streamer
```

## Quick Start

```go
// Load AWS configuration
cfg, err := config.LoadDefaultConfig(context.TODO())
if err != nil {
    log.Fatal(err)
}

// Create S3 client and streamer
client := s3.NewFromConfig(cfg)
streamer := s3streamer.NewS3Streamer(client)

// Process a large JSON Lines file with offset tracking
err = streamer.Stream(context.Background(), "my-bucket", "large-file.jsonl.gz", 0, 
    func(line []byte, offset int64) error {
        var record map[string]interface{}
        if err := json.Unmarshal(line, &record); err != nil {
            return err
        }
        
        // Process your record here with access to its byte offset
        fmt.Printf("Processing record at offset %d: %v\n", offset, record["id"])
        return nil
    })

if err != nil {
    log.Fatal(err)
}
```

## Standard Library Integration

### io.Reader Implementation

The `ChunkStreamer` implements Go's standard `io.Reader` interface, making it compatible with any library that accepts readers:

```go
// Stream directly to any io.Writer
func copyToFile(ctx context.Context, client *s3.Client, bucket, key, filename string, fileSize int64) error {
    // Create chunk streamer
    streamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 5*1024*1024)
    
    // Use with standard library
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    // Standard io.Copy works seamlessly
    _, err = io.Copy(file, streamer)
    return err
}

// Use with compression libraries
func processCompressedStream(ctx context.Context, client *s3.Client, bucket, key string, fileSize int64) error {
    streamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 1024*1024)
    
    // Automatic decompression
    reader, err := s3streamer.Decompress(streamer)
    if err != nil {
        return err
    }
    
    // Use with bufio.Scanner for line processing
    scanner := bufio.NewScanner(reader)
    for scanner.Scan() {
        line := scanner.Text()
        // Process line
    }
    return scanner.Err()
}
```

### Pipe Integration

Combine with Go's `io.Pipe` for concurrent processing:

```go
func streamWithPipe(ctx context.Context, client *s3.Client, bucket, key string, fileSize int64) error {
    pr, pw := io.Pipe()
    
    // Stream in background goroutine
    go func() {
        defer pw.Close()
        streamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 1024*1024)
        io.Copy(pw, streamer)
    }()
    
    // Process in main goroutine
    reader, err := s3streamer.Decompress(pr)
    if err != nil {
        return err
    }
    
    scanner := bufio.NewScanner(reader)
    for scanner.Scan() {
        // Process each line as it arrives
        processLine(scanner.Bytes())
    }
    
    return scanner.Err()
}
```

## Advanced Usage

### Line Offset Tracking

Each line callback receives both the line data and its byte offset within the decompressed stream, enabling precise error reporting and resumable processing:

```go
func processWithOffsetTracking(ctx context.Context, client *s3.Client, bucket, key string) error {
    streamer := s3streamer.NewS3Streamer(client)
    
    var processedCount int
    return streamer.Stream(ctx, bucket, key, 0, func(line []byte, offset int64) error {
        processedCount++
        
        // Use offset for precise error reporting
        if err := validateRecord(line); err != nil {
            return fmt.Errorf("validation failed at byte offset %d (record %d): %w", 
                offset, processedCount, err)
        }
        
        // Save checkpoint with exact position for resumable processing
        if processedCount%1000 == 0 {
            if err := saveCheckpoint(offset); err != nil {
                return fmt.Errorf("failed to save checkpoint at offset %d: %w", offset, err)
            }
        }
        
        return processRecord(line)
    })
}
```

### Resume from Offset

Process large files in chunks or resume interrupted operations:

> [!NOTE]
> Resume capability with non-zero offsets is **only supported for uncompressed files**. Compressed files (gzip, bzip2) cannot be resumed from arbitrary byte offsets because compression streams require reading from the beginning to properly decompress. For compressed files, only use `offset = 0`.

```go
func resumableProcessing(ctx context.Context, client *s3.Client, bucket, key string) error {
    streamer := s3streamer.NewS3Streamer(client)
    
    // Calculate offset (e.g., from previous processing state)
    // NOTE: This only works for uncompressed files!
    offset := int64(1024 * 1024 * 100) // Skip first 100MB
    
    var recordCount int
    err := streamer.Stream(ctx, bucket, key, offset, func(line []byte, lineOffset int64) error {
        recordCount++
        
        // The lineOffset parameter gives you the exact position of this line
        // within the decompressed stream (starting from 0)
        actualFileOffset := offset + lineOffset
        
        // Save progress every 1000 records with precise positioning
        if recordCount%1000 == 0 {
            saveCheckpoint(actualFileOffset)
        }
        
        return processRecord(line)
    })
    
    return err
}
```

**Supported Resume Scenarios:**
- ✅ Uncompressed files with any offset
- ✅ Compressed files with offset = 0 only
- ❌ Compressed files with non-zero offsets (will cause decompression errors)

## Performance Characteristics

### Memory Usage

- **Constant Memory**: Memory usage remains constant regardless of file size
- **Configurable Buffer**: Default 5MB chunks with configurable sizes via ChunkStreamer
- **Line Buffer**: Up to 10MB for processing extremely long lines

### Throughput Optimization

```go
// Default configuration uses 5MB chunks, optimal for most cases
streamer := s3streamer.NewS3Streamer(client)

// For high-throughput or custom chunk sizes, use ChunkStreamer directly
highThroughputStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 10*1024*1024) // 10MB chunks

// For low-latency processing of many small files
lowLatencyStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 256*1024) // 256KB chunks
```

### Benchmark Results

Based on included benchmarks processing 1000 records with different chunk sizes (Apple M4 Pro):

| Chunk Size | Time/Operation | Memory/Operation | Allocations/Operation |
|------------|----------------|------------------|-----------------------|
| 256KB      | 152.4 μs       | 1.13 MB          | 66                    |
| 512KB      | 153.5 μs       | 1.13 MB          | 66                    |
| 1MB        | 155.2 μs       | 1.13 MB          | 66                    |
| 5MB        | 150.6 μs       | 1.13 MB          | 66                    |

*Results show consistent performance across chunk sizes with minimal overhead.*

## License

MIT License - see [LICENSE](LICENSE) file for details.
