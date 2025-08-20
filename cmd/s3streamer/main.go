package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gurre/s3streamer"
)

const (
	defaultPartSize  = 5 * 1024 * 1024 // 5MiB
	defaultChunkSize = 5 * 1024 * 1024 // 5MiB
)

func main() {
	// Check for help or no arguments first
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	// Check for help flag anywhere in args
	for _, arg := range os.Args[1:] {
		if arg == "-help" || arg == "--help" || arg == "-h" {
			printUsage()
			return
		}
	}

	// Extract command from first argument
	command := os.Args[1]

	// Create a new flag set and parse remaining arguments
	flagSet := flag.NewFlagSet("s3streamer", flag.ExitOnError)
	bucket := flagSet.String("bucket", "", "S3 bucket name (required)")
	key := flagSet.String("key", "", "S3 object key (required)")
	filePath := flagSet.String("file", "", "Local file path (required)")
	compression := flagSet.String("compress", "", "Compression type for upload: 'gzip', 'bzip2', or 'none' (auto-detect from extension if not specified)")
	partSize := flagSet.Int64("part-size", defaultPartSize, "Part size for multipart uploads (minimum 5MiB)")
	chunkSize := flagSet.Int64("chunk-size", defaultChunkSize, "Chunk size for downloads")
	region := flagSet.String("region", "", "AWS region (optional, uses default from config/environment)")
	profile := flagSet.String("profile", "", "AWS profile to use (optional, uses default profile if not specified)")

	// Parse flags starting from the second argument
	if err := flagSet.Parse(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if *bucket == "" || *key == "" || *filePath == "" {
		fmt.Fprintf(os.Stderr, "Error: bucket, key, and file are required\n\n")
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()

	// Load AWS configuration
	var cfg aws.Config
	var err error

	// Build config options
	var configOpts []func(*config.LoadOptions) error

	if *profile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(*profile))
	}

	if *region != "" {
		configOpts = append(configOpts, config.WithRegion(*region))
	}

	cfg, err = config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)

	switch strings.ToLower(command) {
	case "upload", "up":
		if err := uploadFile(ctx, client, *bucket, *key, *filePath, *compression, *partSize); err != nil {
			log.Fatalf("Upload failed: %v", err)
		}
	case "download", "down":
		if err := downloadFile(ctx, client, *bucket, *key, *filePath, *chunkSize); err != nil {
			log.Fatalf("Download failed: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command '%s'\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`s3streamer - Stream files to/from S3 with automatic compression

USAGE:
    s3streamer <command> -bucket <bucket> -key <key> -file <file> [options]

COMMANDS:
    upload, up      Upload a file to S3 with optional compression
    download, down  Download a file from S3 with automatic decompression

REQUIRED FLAGS:
    -bucket <name>  S3 bucket name
    -key <key>      S3 object key (path)
    -file <path>    Local file path

OPTIONAL FLAGS:
    -compress <type>    Compression for upload: 'gzip', 'bzip2', 'none'
                       (auto-detects from file extension if not specified)
    -part-size <bytes>  Part size for uploads (default: 5MiB, minimum: 5MiB)
    -chunk-size <bytes> Chunk size for downloads (default: 5MiB)
    -region <region>    AWS region (uses default from config if not specified)
    -profile <name>     AWS profile to use (uses default profile if not specified)
    -help              Show this help message

EXAMPLES:
    # Upload a file with gzip compression
    s3streamer upload -bucket my-bucket -key data/file.json.gz -file local.json -compress gzip

    # Upload with auto-detected compression from extension
    s3streamer up -bucket my-bucket -key data/file.json.gz -file local.json

    # Download a compressed file (automatic decompression)
    s3streamer download -bucket my-bucket -key data/file.json.gz -file local.json

    # Upload uncompressed with custom part size
    s3streamer upload -bucket my-bucket -key data/file.txt -file local.txt -compress none -part-size 10485760

    # Download with custom chunk size
    s3streamer down -bucket my-bucket -key data/file.txt -file local.txt -chunk-size 1048576

    # Use a specific AWS profile
    s3streamer upload -bucket my-bucket -key data/file.json.gz -file local.json -profile production

    # Use profile with specific region
    s3streamer download -bucket my-bucket -key data/file.json.gz -file local.json -profile dev -region us-west-2

COMPRESSION TYPES:
    gzip    - Fast compression, good balance of speed and size
    bzip2   - Slower compression, better compression ratio
    none    - No compression (raw upload)
    auto    - Auto-detect from file extension (.gz, .bz2)

NOTES:
    - Upload uses S3 multipart uploads for efficient streaming
    - Download automatically detects and decompresses gzip/bzip2 files
    - Part size must be at least 5MiB (AWS requirement)
    - Large files are processed with constant memory usage
    - Supports resumable operations on network interruptions
`)
}

func uploadFile(ctx context.Context, client *s3.Client, bucket, key, filePath, compressionType string, partSize int64) error {
	// Validate part size
	if partSize < 5*1024*1024 {
		return fmt.Errorf("part size must be at least 5MiB (5242880 bytes), got %d", partSize)
	}

	// Open source file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Get file info for size tracking
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Determine compression type
	compression, err := determineCompression(compressionType, key, filePath)
	if err != nil {
		return fmt.Errorf("failed to determine compression: %w", err)
	}

	// Create appropriate writer
	var writer io.WriteCloser
	if compression == s3streamer.Uncompressed {
		w, err := s3streamer.NewS3Writer(ctx, client, bucket, key, partSize)
		if err != nil {
			return fmt.Errorf("failed to create S3 writer: %w", err)
		}
		writer = w
	} else {
		w, err := s3streamer.NewCompressedS3Writer(ctx, client, bucket, key, partSize, compression)
		if err != nil {
			return fmt.Errorf("failed to create compressed S3 writer: %w", err)
		}
		writer = w
	}

	// Progress tracking
	fmt.Printf("Uploading %s to s3://%s/%s\n", filePath, bucket, key)
	fmt.Printf("File size: %d bytes (%.2f MB)\n", info.Size(), float64(info.Size())/(1024*1024))
	fmt.Printf("Part size: %d bytes (%.2f MB)\n", partSize, float64(partSize)/(1024*1024))
	fmt.Printf("Compression: %s\n", compression.Extension())
	fmt.Printf("Expected parts: %d\n", (info.Size()+partSize-1)/partSize)

	start := time.Now()

	// Copy file content to S3 writer
	bytesWritten, err := io.Copy(writer, file)
	if err != nil {
		if abortErr := writer.(interface{ Abort() error }).Abort(); abortErr != nil {
			fmt.Printf("Warning: failed to abort upload: %v\n", abortErr)
		}
		return fmt.Errorf("failed to upload file: %w", err)
	}

	// Close the writer to finalize the upload
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to finalize upload: %w", err)
	}

	duration := time.Since(start)
	throughput := float64(bytesWritten) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Printf("Upload completed successfully!\n")
	fmt.Printf("Bytes written: %d (%.2f MB)\n", bytesWritten, float64(bytesWritten)/(1024*1024))
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Throughput: %.2f MB/s\n", throughput)

	return nil
}

func downloadFile(ctx context.Context, client *s3.Client, bucket, key, filePath string, chunkSize int64) error {
	// Get object metadata
	resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}

	if resp.ContentLength == nil {
		return fmt.Errorf("object content length is unknown")
	}

	objectSize := *resp.ContentLength

	// Create output file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Create chunk streamer
	streamer := s3streamer.NewChunkStreamer(ctx, client, bucket, key, 0, objectSize, chunkSize)

	// Decompress if needed
	reader, err := s3streamer.Decompress(streamer)
	if err != nil {
		return fmt.Errorf("failed to create decompressed reader: %w", err)
	}

	// Progress tracking
	fmt.Printf("Downloading s3://%s/%s to %s\n", bucket, key, filePath)
	fmt.Printf("Object size: %d bytes (%.2f MB)\n", objectSize, float64(objectSize)/(1024*1024))
	fmt.Printf("Chunk size: %d bytes (%.2f MB)\n", chunkSize, float64(chunkSize)/(1024*1024))

	start := time.Now()

	// Copy from S3 to local file
	bytesRead, err := io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	duration := time.Since(start)
	throughput := float64(objectSize) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Printf("Download completed successfully!\n")
	fmt.Printf("Bytes read: %d (%.2f MB)\n", bytesRead, float64(bytesRead)/(1024*1024))
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Throughput: %.2f MB/s\n", throughput)

	return nil
}

func determineCompression(compressionType, key, filePath string) (s3streamer.Compression, error) {
	if compressionType != "" {
		switch strings.ToLower(compressionType) {
		case "gzip", "gz":
			return s3streamer.Gzip, nil
		case "bzip2", "bz2":
			return s3streamer.Bzip2, nil
		case "none", "uncompressed":
			return s3streamer.Uncompressed, nil
		default:
			return s3streamer.Uncompressed, fmt.Errorf("unsupported compression type: %s", compressionType)
		}
	}

	// Auto-detect from key or file extension
	for _, path := range []string{key, filePath} {
		ext := strings.ToLower(filepath.Ext(path))
		switch ext {
		case ".gz":
			return s3streamer.Gzip, nil
		case ".bz2":
			return s3streamer.Bzip2, nil
		}
	}

	// Default to uncompressed
	return s3streamer.Uncompressed, nil
}
