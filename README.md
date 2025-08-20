# s3streamer

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/s3streamer.svg)](https://pkg.go.dev/github.com/gurre/s3streamer)
[![Go Report Card](https://goreportcard.com/badge/github.com/gurre/s3streamer)](https://goreportcard.com/report/github.com/gurre/s3streamer)

A high-performance, memory-efficient Go library for streaming large objects to and from Amazon S3. Built on Go's standard `io.Reader` and `io.Writer` interfaces, s3streamer enables processing of multi-gigabyte files without loading them entirely into memory. Features both reading from S3 with chunked downloads and writing to S3 with multipart uploads.

## Key Features

- **Memory Efficient**: Stream objects of any size with configurable chunk sizes
- **Bidirectional Streaming**: Both `io.Reader` and `io.Writer` implementations for complete S3 integration
- **Automatic Compression**: Supports gzip and bzip2 with automatic detection/compression via magic bytes or file extensions
- **Resume Capability**: Start streaming from any byte offset for resumable processing
- **Line-by-Line Processing**: Optimized for JSON Lines and other line-delimited formats with offset tracking
- **Multipart Upload**: Efficient writing to S3 using multipart uploads with configurable part sizes (enforces 5MiB minimum)
- **High Performance**: Configurable chunking with up to 10MB line buffer support
- **AWS SDK v2 Compatible**: Works with the latest AWS SDK for Go v2

## Installation

```bash
go get github.com/gurre/s3streamer
```

## Quick Start

### Reading from S3

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

### Writing to S3

```go
// Load AWS configuration
cfg, err := config.LoadDefaultConfig(context.TODO())
if err != nil {
    log.Fatal(err)
}

// Create S3 client and writer
client := s3.NewFromConfig(cfg)
writer, err := s3streamer.NewCompressedS3Writer(context.Background(), client, "my-bucket", "output.jsonl.gz", 5*1024*1024, s3streamer.Gzip)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// Write JSON Lines data (automatically compressed)
for i := 0; i < 10000; i++ {
    record := map[string]interface{}{
        "id":      i,
        "message": fmt.Sprintf("Record number %d", i),
        "timestamp": time.Now().Unix(),
    }
    
    data, _ := json.Marshal(record)
    data = append(data, '\n') // Add newline for JSON Lines format
    
    if _, err := writer.Write(data); err != nil {
        log.Fatal(err)
    }
}

// Close to finalize the upload
if err := writer.Close(); err != nil {
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

## Writing to S3

### Basic S3 Writing

The `S3Writer` implements Go's standard `io.Writer` interface and uses AWS S3's multipart upload API for efficient streaming uploads:

```go
// Create an S3 writer with 5MiB part size
writer, err := s3streamer.NewS3Writer(ctx, client, "my-bucket", "output.json", 5*1024*1024)
if err != nil {
    log.Fatal(err)
}
defer writer.Close() // Important: always close to complete the upload

// Write data (can be called multiple times)
data := []byte("Hello, World!\nThis is streaming to S3.\n")
n, err := writer.Write(data)
if err != nil {
    log.Fatal(err)
}

// Close finalizes the multipart upload
if err := writer.Close(); err != nil {
    log.Fatal(err)
}
```

### Compressed Writing

The `CompressedS3Writer` applies the specified compression format before uploading:

```go
// Gzip compression
writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "output.json.gz", 5*1024*1024, s3streamer.Gzip)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// Data is compressed before upload
for i := 0; i < 1000; i++ {
    record := fmt.Sprintf(`{"id": %d, "message": "Hello, World!"}` + "\n", i)
    if _, err := writer.Write([]byte(record)); err != nil {
        log.Fatal(err)
    }
}
```

### Different Compression Types

You can specify any supported compression type:

```go
// Bzip2 compression (slower but better compression ratio)
writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "data.txt.bz2", 
    5*1024*1024, s3streamer.Bzip2)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// Uncompressed (no compression)
uncompressedWriter, err := s3streamer.NewCompressedS3Writer(ctx, client, "my-bucket", "data.txt", 
    5*1024*1024, s3streamer.Uncompressed)
if err != nil {
    log.Fatal(err)
}
defer uncompressedWriter.Close()

// Write data
largeData := bytes.Repeat([]byte("This data compresses well!\n"), 10000)
writer.Write(largeData)
```

### Standard Library Integration

Works seamlessly with any code that accepts `io.Writer`:

```go
// Copy from any reader to S3
func uploadFile(ctx context.Context, client *s3.Client, filename, bucket, key string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    writer, err := s3streamer.NewCompressedS3Writer(ctx, client, bucket, key+".gz", 5*1024*1024, s3streamer.Gzip)
    if err != nil {
        return err
    }
    defer writer.Close()
    
    // Standard io.Copy works seamlessly
    _, err = io.Copy(writer, file)
    if err != nil {
        return err
    }
    
    return writer.Close()
}

// Use with encoding libraries
func writeJSONLines(ctx context.Context, client *s3.Client, records []Record) error {
    writer, err := s3streamer.NewCompressedS3Writer(ctx, client, "data-bucket", "records.jsonl.gz", 10*1024*1024, s3streamer.Gzip)
    if err != nil {
        return err
    }
    defer writer.Close()
    
    encoder := json.NewEncoder(writer)
    for _, record := range records {
        if err := encoder.Encode(record); err != nil {
            return err
        }
    }
    
    return writer.Close()
}
```

### Error Handling and Cleanup

Always handle errors properly and use the abort functionality when needed:

```go
func safeCopyToS3(ctx context.Context, client *s3.Client, src io.Reader, bucket, key string) error {
    writer, err := s3streamer.NewCompressedS3Writer(ctx, client, bucket, key, 5*1024*1024, s3streamer.Gzip)
    if err != nil {
        return err
    }
    
    // Use defer to ensure cleanup happens
    defer func() {
        if err != nil {
            // Abort the upload on error to clean up partial uploads
            writer.Abort()
        } else {
            // Complete the upload on success
            writer.Close()
        }
    }()
    
    _, err = io.Copy(writer, src)
    return err
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

- **Constant Memory**: Memory usage remains constant regardless of file size for both reading and writing
- **Configurable Buffers**: Default 5MiB chunks/parts with configurable sizes
- **Line Buffer**: Up to 10MB for processing extremely long lines (reading)
- **Part Buffer**: Each writer part is buffered separately, with minimal memory overhead

### Reading Optimization

```go
// Default configuration uses 5MiB chunks, optimal for most cases
streamer := s3streamer.NewS3Streamer(client)

// For high-throughput or custom chunk sizes, use ChunkStreamer directly
highThroughputStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 10*1024*1024) // 10MiB chunks

// For low-latency processing of many small files
lowLatencyStreamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, fileSize, 256*1024) // 256KB chunks
```

### Writing Optimization

```go
// Default 5MiB parts balance performance and memory usage
writer, _ := s3streamer.NewS3Writer(ctx, client, bucket, key, 5*1024*1024)

// High-throughput uploads with larger parts (AWS supports up to 5GiB per part)
highThroughputWriter, _ := s3streamer.NewS3Writer(ctx, client, bucket, key, 100*1024*1024) // 100MiB parts

// Part size must be at least 5MiB (AWS requirement)
memoryEfficientWriter, _ := s3streamer.NewS3Writer(ctx, client, bucket, key, 5*1024*1024) // 5MiB minimum

// Compressed writing (data is compressed before applying part size limits)
compressedWriter, _ := s3streamer.NewCompressedS3Writer(ctx, client, bucket, key, 10*1024*1024, s3streamer.Gzip) // 10MiB parts
```

### Benchmark Results

Based on included benchmarks processing 1000 records (Apple M4 Pro):

#### Reading Performance (Different Chunk Sizes)

| Chunk Size | Time/Operation | Memory/Operation | Allocations/Operation |
|------------|----------------|------------------|-----------------------|
| 256KB      | 152.4 μs       | 1.13 MB          | 66                    |
| 512KB      | 153.5 μs       | 1.13 MB          | 66                    |
| 1MB        | 155.2 μs       | 1.13 MB          | 66                    |
| 5MiB       | 150.6 μs       | 1.13 MB          | 66                    |

*Results show consistent performance across chunk sizes with minimal overhead.*

#### Writing Performance (Different Part Sizes)

| Part Size | Time/Operation | Memory/Operation | Allocations/Operation |
|-----------|----------------|------------------|-----------------------|
| 5MiB      | 84.0 μs        | 609 KB           | 34                    |
| 10MiB     | 84.1 μs        | 609 KB           | 34                    |
| 25MiB     | 84.5 μs        | 609 KB           | 34                    |
| 50MiB     | 83.0 μs        | 609 KB           | 34                    |

*Performance remains consistent across different part sizes with minimal overhead.*

#### Compression Performance (10MiB Parts)

| Compression | Time/Operation | Memory/Operation | Allocations/Operation |
|-------------|----------------|------------------|-----------------------|
| None        | 76.2 μs        | 609 KB           | 33                    |
| Gzip        | 356.3 μs       | 840 KB           | 40                    |
| Bzip2       | 3,058.2 μs     | 2.08 MB          | 68                    |

*Compression adds CPU overhead but significantly reduces upload size for compressible data.*

#### Write Pattern Performance (10MiB Parts)

| Write Pattern    | Time/Operation | Memory/Operation | Allocations/Operation |
|------------------|----------------|------------------|-----------------------|
| Single Write     | 117.3 μs       | 838 KB           | 53                    |
| 100 Records/Write| 112.0 μs       | 785 KB           | 143                   |
| 10 Records/Write | 108.4 μs       | 712 KB           | 533                   |
| Per Record Write | 97.4 μs        | 559 KB           | 1033                  |

*Smaller writes show better performance due to reduced buffer management overhead.*

## License

MIT License - see [LICENSE](LICENSE) file for details.
