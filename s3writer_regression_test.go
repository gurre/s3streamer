package s3streamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestS3Writer_PartNumberIncrement tests the regression fix for the part number increment bug.
// This test ensures that parts are uploaded with correct part numbers and stored with matching numbers.
//
// Bug scenario: Part number was incremented before storing in parts array, causing mismatch
// between uploaded part number and completed part number in CompleteMultipartUpload request.
//
// The bug manifested as:
// 1. Part uploaded with partNumber = 1
// 2. partNumber incremented to 2
// 3. Part stored in array with PartNumber = 2 (wrong!)
// 4. CompleteMultipartUpload called with part 2, but S3 only has part 1
// 5. Result: "InvalidPart: One or more of the specified parts could not be found"
func TestS3Writer_PartNumberIncrement(t *testing.T) {
	ctx := context.Background()

	// Track uploaded part numbers and completion part numbers to detect the bug
	var uploadedPartNumbers []int32
	var completionPartNumbers []int32

	mock := &mockS3ClientWriter{}
	mock.uploadPartFunc = func(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
		// Record the part number used for upload
		uploadedPartNumbers = append(uploadedPartNumbers, *params.PartNumber)

		// Store the data
		data, err := io.ReadAll(params.Body)
		if err != nil {
			return nil, err
		}

		if mock.uploadedParts == nil {
			mock.uploadedParts = make(map[int32][]byte)
		}
		mock.uploadedParts[*params.PartNumber] = data

		etag := fmt.Sprintf("\"etag-part-%d\"", *params.PartNumber)
		return &s3.UploadPartOutput{
			ETag: &etag,
		}, nil
	}
	mock.completeMultipartUploadFunc = func(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
		// Record the part numbers used for completion - this is where the bug was caught
		for _, part := range params.MultipartUpload.Parts {
			completionPartNumbers = append(completionPartNumbers, *part.PartNumber)
		}

		mock.completed = true
		return &s3.CompleteMultipartUploadOutput{}, nil
	}

	// Test multiple scenarios to ensure the bug is properly fixed in all cases
	testCases := []struct {
		name          string
		dataSize      int64
		partSize      int64
		expectedParts int
		description   string
	}{
		{
			name:          "single part smaller than part size",
			dataSize:      3 * 1024 * 1024, // 3MB
			partSize:      5 * 1024 * 1024, // 5MB
			expectedParts: 1,
			description:   "Tests the original bug scenario - single part upload",
		},
		{
			name:          "exactly part size",
			dataSize:      5 * 1024 * 1024, // 5MB
			partSize:      5 * 1024 * 1024, // 5MB
			expectedParts: 1,
			description:   "Edge case: data exactly matches part size",
		},
		{
			name:          "two parts",
			dataSize:      8 * 1024 * 1024, // 8MB
			partSize:      5 * 1024 * 1024, // 5MB
			expectedParts: 2,
			description:   "Multiple parts: ensures sequential part numbers are correct",
		},
		{
			name:          "three parts",
			dataSize:      12 * 1024 * 1024, // 12MB
			partSize:      5 * 1024 * 1024,  // 5MB
			expectedParts: 3,
			description:   "More parts: comprehensive test of part number tracking",
		},
		{
			name:          "minimum part size with small data",
			dataSize:      1024 * 1024,     // 1MB
			partSize:      5 * 1024 * 1024, // 5MB (minimum)
			expectedParts: 1,
			description:   "Small data with minimum part size",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.description)

			// Reset tracking arrays for each test
			uploadedPartNumbers = nil
			completionPartNumbers = nil
			mock.uploadedParts = make(map[int32][]byte)
			mock.completed = false

			// Create writer
			writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", tc.partSize)
			if err != nil {
				t.Fatalf("Failed to create S3Writer: %v", err)
			}

			// Create test data
			testData := bytes.Repeat([]byte("a"), int(tc.dataSize))

			// Write data
			n, err := writer.Write(testData)
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			if int64(n) != tc.dataSize {
				t.Fatalf("Expected to write %d bytes, wrote %d", tc.dataSize, n)
			}

			// Close to finalize upload
			err = writer.Close()
			if err != nil {
				t.Fatalf("Failed to close writer: %v", err)
			}

			// Verify the upload was completed
			if !mock.completed {
				t.Fatal("Upload was not completed")
			}

			// CRITICAL TEST: Verify part numbers match between upload and completion
			// This is the core test that would have caught the original bug
			if len(uploadedPartNumbers) != len(completionPartNumbers) {
				t.Fatalf("Mismatch in part count: uploaded %d parts, completed with %d parts",
					len(uploadedPartNumbers), len(completionPartNumbers))
			}

			if len(uploadedPartNumbers) != tc.expectedParts {
				t.Fatalf("Expected %d parts, got %d parts", tc.expectedParts, len(uploadedPartNumbers))
			}

			// Verify each uploaded part number matches its corresponding completion part number
			for i := 0; i < len(uploadedPartNumbers); i++ {
				uploadNum := uploadedPartNumbers[i]
				completionNum := completionPartNumbers[i]

				// This comparison would fail with the original bug:
				// uploadNum would be 1, but completionNum would be 2
				if uploadNum != completionNum {
					t.Errorf("Part number mismatch at index %d: uploaded as part %d, completed as part %d",
						i, uploadNum, completionNum)
					t.Error("This indicates the part number increment bug has returned!")
				}

				// Verify part numbers are sequential starting from 1
				expectedPartNum := int32(i + 1)
				if uploadNum != expectedPartNum {
					t.Errorf("Expected part number %d, got %d", expectedPartNum, uploadNum)
				}
			}

			// Verify all data was uploaded correctly and can be reconstructed
			var reconstructedData []byte
			for i := 1; i <= len(uploadedPartNumbers); i++ {
				partData, exists := mock.uploadedParts[int32(i)]
				if !exists {
					t.Errorf("Part %d data not found in uploaded parts", i)
					continue
				}
				reconstructedData = append(reconstructedData, partData...)
			}

			if !bytes.Equal(testData, reconstructedData) {
				t.Error("Reconstructed data does not match original data")
			}

			t.Logf("✓ Part numbers correctly match: %v", uploadedPartNumbers)
		})
	}
}

// TestS3Writer_PartNumberIncrementEdgeCases tests additional edge cases for the part number bug
func TestS3Writer_PartNumberIncrementEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("multiple writes creating multiple parts", func(t *testing.T) {
		var uploadedPartNumbers []int32
		var completionPartNumbers []int32

		mock := &mockS3ClientWriter{}
		mock.uploadPartFunc = func(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedPartNumbers = append(uploadedPartNumbers, *params.PartNumber)

			data, err := io.ReadAll(params.Body)
			if err != nil {
				return nil, err
			}

			if mock.uploadedParts == nil {
				mock.uploadedParts = make(map[int32][]byte)
			}
			mock.uploadedParts[*params.PartNumber] = data

			etag := fmt.Sprintf("\"etag-part-%d\"", *params.PartNumber)
			return &s3.UploadPartOutput{ETag: &etag}, nil
		}
		mock.completeMultipartUploadFunc = func(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			for _, part := range params.MultipartUpload.Parts {
				completionPartNumbers = append(completionPartNumbers, *part.PartNumber)
			}
			mock.completed = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		}

		writer, err := NewS3Writer(ctx, mock, "test-bucket", "test-key", 5*1024*1024)
		if err != nil {
			t.Fatalf("Failed to create S3Writer: %v", err)
		}

		// Write in multiple chunks to create multiple parts
		chunk1 := bytes.Repeat([]byte("1"), 3*1024*1024) // 3MB
		chunk2 := bytes.Repeat([]byte("2"), 3*1024*1024) // 3MB (will trigger first part upload)
		chunk3 := bytes.Repeat([]byte("3"), 2*1024*1024) // 2MB (will be part of second part)

		// First write (3MB) - should not trigger upload yet
		_, err = writer.Write(chunk1)
		if err != nil {
			t.Fatalf("Failed to write chunk1: %v", err)
		}

		// Second write (3MB) - should trigger upload of first part (6MB total)
		_, err = writer.Write(chunk2)
		if err != nil {
			t.Fatalf("Failed to write chunk2: %v", err)
		}

		// Third write (2MB) - should be buffered for second part
		_, err = writer.Write(chunk3)
		if err != nil {
			t.Fatalf("Failed to write chunk3: %v", err)
		}

		// Close should upload the remaining buffered data as second part
		err = writer.Close()
		if err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Should have 2 parts
		if len(uploadedPartNumbers) != 2 {
			t.Fatalf("Expected 2 parts, got %d", len(uploadedPartNumbers))
		}

		// Critical: part numbers must match
		for i, uploadNum := range uploadedPartNumbers {
			completionNum := completionPartNumbers[i]
			if uploadNum != completionNum {
				t.Errorf("Part number mismatch: uploaded part %d, completed part %d", uploadNum, completionNum)
			}
		}

		// Verify sequential numbering
		expectedParts := []int32{1, 2}
		for i, expected := range expectedParts {
			if uploadedPartNumbers[i] != expected {
				t.Errorf("Expected part %d, got %d", expected, uploadedPartNumbers[i])
			}
		}
	})
}
