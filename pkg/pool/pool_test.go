package pool

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestBufferSize(t *testing.T) {
	if BufferSize != 32*1024 {
		t.Fatalf("expected BufferSize to be 32768, got %d", BufferSize)
	}
}

func TestGetAndPutBuffer(t *testing.T) {
	bufPtr := GetBuffer()
	if bufPtr == nil {
		t.Fatal("expected non-nil buffer pointer from pool")
	}
	if len(*bufPtr) != BufferSize {
		t.Fatalf("expected buffer length %d, got %d", BufferSize, len(*bufPtr))
	}

	// Modify buffer to verify it can be reused safely
	(*bufPtr)[0] = 0xAA
	(*bufPtr)[BufferSize-1] = 0xBB

	PutBuffer(bufPtr)

	// Fetch another buffer (may or may not be the same one, but must have valid length)
	bufPtr2 := GetBuffer()
	if bufPtr2 == nil || len(*bufPtr2) != BufferSize {
		t.Fatal("failed to get valid buffer after Put")
	}
	PutBuffer(bufPtr2)
}

func TestStreamCopyVariedSizes(t *testing.T) {
	sizes := []int{
		0,                  // Empty payload
		1,                  // Single byte
		1024,               // 1 KB
		32 * 1024,          // Exact buffer size (32 KB)
		32*1024 + 1,        // Exact buffer size + 1 byte
		128 * 1024,         // 128 KB
		1024 * 1024,        // 1 MB
	}

	for _, sz := range sizes {
		t.Run(fmt.Sprintf("Size_%d", sz), func(t *testing.T) {
			data := make([]byte, sz)
			if sz > 0 {
				_, _ = rand.Read(data)
			}

			hasher := sha256.New()
			_, _ = hasher.Write(data)
			expectedHash := hex.EncodeToString(hasher.Sum(nil))

			src := bytes.NewReader(data)
			dst := &bytes.Buffer{}

			written, err := StreamCopy(dst, src)
			if err != nil {
				t.Fatalf("StreamCopy failed for size %d: %v", sz, err)
			}
			if written != int64(sz) {
				t.Fatalf("expected written %d, got %d", sz, written)
			}

			actualHash := hex.EncodeToString(sha256HexBytes(dst.Bytes()))
			if actualHash != expectedHash {
				t.Fatalf("data corrupted: expected hash %s, got %s", expectedHash, actualHash)
			}
		})
	}
}

func TestConcurrentStreamCopy(t *testing.T) {
	const goroutines = 50
	const dataSize = 64 * 1024 // 64 KB per stream

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			data := bytes.Repeat([]byte(fmt.Sprintf("thread-%03d-pool-concurrency-test-", id)), 1024)
			src := bytes.NewReader(data)
			dst := &bytes.Buffer{}

			written, err := StreamCopy(dst, src)
			if err != nil {
				errs <- fmt.Errorf("[%d] StreamCopy: %w", id, err)
				return
			}
			if written != int64(len(data)) {
				errs <- fmt.Errorf("[%d] expected %d bytes, got %d", id, len(data), written)
				return
			}
			if !bytes.Equal(dst.Bytes(), data) {
				errs <- fmt.Errorf("[%d] streamed data mismatch", id)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent test failure: %v", err)
	}
}

func BenchmarkStreamCopy(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		src := bytes.NewReader(data)
		_, err := StreamCopy(io.Discard, src)
		if err != nil {
			b.Fatalf("StreamCopy failed: %v", err)
		}
	}
}

func sha256HexBytes(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
