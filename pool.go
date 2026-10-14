package main

import (
	"file-handler/pkg/pool"
	"io"
)

// BufferSize is re-exported from pkg/pool.
const BufferSize = pool.BufferSize

// GetBuffer retrieves a 32KB buffer pointer from the pool.
func GetBuffer() *[]byte {
	return pool.GetBuffer()
}

// PutBuffer returns a 32KB buffer pointer to the pool.
func PutBuffer(b *[]byte) {
	pool.PutBuffer(b)
}

// StreamCopy copies from src to dst using a pooled 32KB buffer.
func StreamCopy(dst io.Writer, src io.Reader) (int64, error) {
	return pool.StreamCopy(dst, src)
}
