package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var (
	// ErrNotFound is returned when a requested file or object does not exist.
	ErrNotFound = errors.New("file not found")

	// ErrInvalidPath is returned when a target path contains forbidden path traversal patterns.
	ErrInvalidPath = errors.New("invalid file path")
)

// PresignedUpload contains parameters for direct client-to-storage upload.
type PresignedUpload struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresIn int               `json:"expires_in"` // expiration in seconds
	IsDirect  bool              `json:"is_direct"`  // true if direct to cloud storage, false if proxied through server
}

// PresignedDownload contains parameters for direct client-to-storage download.
type PresignedDownload struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	FileID    string            `json:"file_id,omitempty"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresIn int               `json:"expires_in"` // expiration in seconds
	IsDirect  bool              `json:"is_direct"`  // true if direct to cloud storage, false if proxied through server
}

// Presigner is an optional interface implemented by providers that support direct pre-signed URLs.
type Presigner interface {
	PresignUpload(ctx context.Context, targetPath string, filename string, contentType string, expirySeconds int) (*PresignedUpload, error)
	PresignDownload(ctx context.Context, targetPath string, expirySeconds int) (*PresignedDownload, error)
}

// DownloadPresigner is an optional interface for providers that support pre-signing download URLs.
type DownloadPresigner interface {
	PresignDownload(ctx context.Context, targetPath string, expirySeconds int) (*PresignedDownload, error)
}

// Provider defines the contract that all pluggable storage backends must implement.
type Provider interface {
	// Name returns the provider identifier (e.g. "local", "s3", "gcs", "azure").
	Name() string

	// Save streams data directly from src into the target storage path
	// without buffering whole files into memory. Returns the number of bytes written.
	Save(ctx context.Context, targetPath string, filename string, src io.Reader) (bytesWritten int64, err error)

	// ServeDownload handles HTTP download requests.
	// For local files, it uses http.ServeContent to support automatic Range requests.
	// For remote streams, it pipes directly to http.ResponseWriter using pool.StreamCopy.
	ServeDownload(w http.ResponseWriter, r *http.Request, targetPath string) error

	// Open opens a stored file for streaming read operations.
	// The caller is responsible for closing the returned ReadCloser.
	Open(ctx context.Context, targetPath string) (io.ReadCloser, error)

	// Delete removes a stored file or object by its target path.
	Delete(ctx context.Context, targetPath string) error
}

// Factory constructs a Provider based on Config.
type Factory func(cfg Config) (Provider, error)

var (
	registryMu sync.RWMutex
	factories  = make(map[string]Factory)
)

// Register registers a new storage provider factory under a given name.
func Register(name string, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	factories[strings.ToLower(name)] = factory
}

// NewProvider creates and initializes a storage provider based on cfg.Type.
func NewProvider(cfg Config) (Provider, error) {
	providerType := strings.ToLower(strings.TrimSpace(cfg.Type))
	if providerType == "" {
		providerType = "local"
	}

	registryMu.RLock()
	factory, exists := factories[providerType]
	registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown storage provider %q (registered: %s)", providerType, strings.Join(SupportedProviders(), ", "))
	}
	return factory(cfg)
}

// SupportedProviders returns the list of registered provider names.
func SupportedProviders() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	var names []string
	for k := range factories {
		names = append(names, k)
	}
	return names
}

// SanitizeRelativePath validates and cleans multi-segment relative paths
// (e.g. "documents/2026/invoices/sample.pdf"), strictly forbidding path traversal (..),
// backslashes, leading slashes, and control characters.
func SanitizeRelativePath(rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", errors.New("path cannot be empty")
	}

	// Normalize Windows slashes to forward slashes for cross-platform consistency
	normalized := strings.ReplaceAll(rawPath, "\\", "/")

	// Reject directory traversal patterns
	if strings.Contains(normalized, "..") {
		return "", ErrInvalidPath
	}

	// Clean path using standard path package (POSIX/URL style)
	cleaned := path.Clean("/" + normalized)
	trimmed := strings.TrimPrefix(cleaned, "/")

	if trimmed == "" || trimmed == "." || strings.HasPrefix(trimmed, "../") || strings.Contains(trimmed, "/../") {
		return "", ErrInvalidPath
	}

	// Double check individual segments
	segments := strings.Split(trimmed, "/")
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" || seg == "." || seg == ".." {
			return "", ErrInvalidPath
		}
	}

	return trimmed, nil
}

// SanitizePathSegment validates that a given string is a safe single path element.
func SanitizePathSegment(val string) (string, error) {
	val = strings.TrimSpace(val)
	if val == "" {
		return "", errors.New("identifier cannot be empty")
	}
	if strings.Contains(val, "/") || strings.Contains(val, "\\") || strings.Contains(val, "..") || val == "." {
		return "", ErrInvalidPath
	}
	base := filepath.Base(filepath.Clean(val))
	if base == "." || base == "/" || base == "\\" || strings.Contains(base, "..") {
		return "", ErrInvalidPath
	}
	return base, nil
}
