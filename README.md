# s3streamer

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/s3streamer.svg)](https://pkg.go.dev/github.com/gurre/s3streamer)
[![Go Report Card](https://goreportcard.com/badge/github.com/gurre/s3streamer)](https://goreportcard.com/report/github.com/gurre/s3streamer)

A high-performance, memory-efficient Go library for streaming large objects from Amazon S3. Built on Go's standard `io.Reader` interface, s3streamer enables processing of multi-gigabyte files without loading them entirely into memory.

## Key Features

- **Memory Efficient**: Stream objects of any size with configurable chunk sizes
- **Standard Library Integration**: Implements `io.Reader` for seamless integration with Go's ecosystem
- **Automatic Compression Detection**: Supports gzip and bzip2 with automatic detection via magic bytes
- **Resume Capability**: Start streaming from any byte offset for resumable processing
- **Line-by-Line Processing**: Optimized for JSON Lines and other line-delimited formats
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
streamer := s3streamer.NewS3Streamer(client, 1)

// Process a large JSON Lines file
err = streamer.Stream(context.Background(), "my-bucket", "large-file.jsonl.gz", 0, 
    func(line []byte) error {
        var record map[string]interface{}
        if err := json.Unmarshal(line, &record); err != nil {
            return err
        }
        
        // Process your record here
        fmt.Printf("Processing record: %v\n", record["id"])
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

### Resume from Offset

Process large files in chunks or resume interrupted operations:

```go
func resumableProcessing(ctx context.Context, client *s3.Client, bucket, key string) error {
    streamer := s3streamer.NewS3Streamer(client, 1)
    
    // Calculate offset (e.g., from previous processing state)
    offset := int64(1024 * 1024 * 100) // Skip first 100MB
    
    var recordCount int
    err := streamer.Stream(ctx, bucket, key, offset, func(line []byte) error {
        recordCount++
        
        // Save progress every 1000 records
        if recordCount%1000 == 0 {
            saveCheckpoint(offset + int64(len(line)))
        }
        
        return processRecord(line)
    })
    
    return err
}
```

## Performance Characteristics

### Memory Usage

- **Constant Memory**: Memory usage remains constant regardless of file size
- **Configurable Buffer**: Default 5MB chunks with configurable sizes via ChunkStreamer
- **Line Buffer**: Up to 10MB for processing extremely long lines

### Throughput Optimization

```go
// Default configuration uses 5MB chunks, optimal for most cases
streamer := s3streamer.NewS3Streamer(client, 1)

// For high-throughput or custom chunk sizes, use ChunkStreamer directly
highThroughputStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 10*1024*1024) // 10MB chunks

// For low-latency processing of many small files
lowLatencyStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 256*1024) // 256KB chunks
```

### Benchmark Results

Based on included benchmarks processing 1000 records with different chunk sizes (Apple M4 Pro):

| Chunk Size | Time/Operation | Memory/Operation | Allocations/Operation |
|------------|----------------|------------------|-----------------------|
| 256KB      | 158.3 μs       | 1.13 MB          | 66                    |
| 512KB      | 158.5 μs       | 1.13 MB          | 66                    |
| 1MB        | 161.4 μs       | 1.13 MB          | 66                    |
| 5MB        | 166.5 μs       | 1.13 MB          | 66                    |

*Results show consistent performance across chunk sizes with minimal overhead.*

## API Reference

### Core Types

```go
type S3Client interface {
    GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
    HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type Streamer interface {
    Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte) error) error
}

type Compression int
const (
    Uncompressed Compression = iota
    Bzip2
    Gzip
)
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
