package s3streamer

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"io"
)

// Compression represents the supported compression types for data files.
// Example:
//
//	compression := s3streamer.DetectCompression(data)
//	ext := compression.Extension() // Returns ".gz" for gzipped files
type Compression int

const (
	// Uncompressed indicates no compression is used
	Uncompressed Compression = iota
	// Bzip2 indicates bzip2 compression
	Bzip2
	// Gzip indicates gzip compression
	Gzip
)

// Extension returns the file extension for the detected compression type.
// Example:
//
//	compression := s3streamer.Gzip
//	ext := compression.Extension() // Returns ".gz"
func (compression *Compression) Extension() string {
	switch *compression {
	case Uncompressed:
		return ""
	case Bzip2:
		return ".bz2"
	case Gzip:
		return ".gz"
	}
	return "[unknown]"
}

// DetectCompression detects the compression type from the file's magic bytes.
// Example:
//
//	data := []byte{0x1F, 0x8B, ...} // Gzip magic bytes
//	compression := s3streamer.DetectCompression(data) // Returns s3streamer.Gzip
func DetectCompression(source []byte) Compression {
	for compression, m := range map[Compression][]byte{
		Bzip2: {0x42, 0x5A, 0x68},
		Gzip:  {0x1F, 0x8B}, // Only check first 2 bytes to support all gzip compression methods
	} {
		if len(source) >= len(m) && bytes.Equal(m, source[:len(m)]) {
			return compression
		}
	}
	return Uncompressed
}

// Decompress takes a reader and returns a decompressed reader based on the detected compression.
// Example:
//
//	reader := bytes.NewReader(compressedData)
//	decompressed, err := s3streamer.Decompress(reader)
//	if err != nil {
//	    log.Fatal(err)
//	}
func Decompress(stream io.Reader) (io.Reader, error) {
	buf := bufio.NewReader(stream)
	bs, err := buf.Peek(10)
	if err != nil && err != io.EOF {
		return nil, err
	}

	compression := DetectCompression(bs)
	switch compression {
	case Uncompressed:
		return buf, nil
	case Gzip:
		return gzip.NewReader(buf)
	case Bzip2:
		return bzip2.NewReader(buf), nil
	default:
		return stream, nil
	}
}
