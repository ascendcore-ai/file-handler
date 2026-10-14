package pool

import (
	"io"
	"sync"
)

// BufferSize is 32KB (32 * 1024 bytes), per the requirement for stream copying.
const BufferSize = 32 * 1024

// bufferPool pools 32KB byte slices to minimize allocations and prevent GC spikes
// during high-concurrency file uploads and stream copying.
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, BufferSize)
		return &b
	},
}

// GetBuffer retrieves a 32KB buffer pointer from the pool.
func GetBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// PutBuffer returns a 32KB buffer pointer to the pool.
func PutBuffer(b *[]byte) {
	bufferPool.Put(b)
}

// StreamCopy copies from src to dst using a pooled 32KB buffer.
// It ensures constant memory usage regardless of file size and prevents
// garbage collection spikes by recycling buffers via sync.Pool.
func StreamCopy(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := GetBuffer()
	defer PutBuffer(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}
