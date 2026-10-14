package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"file-handler/pkg/pool"
)

func init() {
	Register("local", func(cfg Config) (Provider, error) {
		return NewLocalStorage(cfg.LocalDir, cfg.StoragePrefix)
	})
}

// LocalStorage implements Provider for the local filesystem.
type LocalStorage struct {
	baseDir string
	prefix  string
}

// NewLocalStorage creates and initializes a new LocalStorage provider.
func NewLocalStorage(baseDir, prefix string) (*LocalStorage, error) {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = "./uploads"
	}

	cleanBase, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("invalid local storage base directory: %w", err)
	}

	if err := os.MkdirAll(cleanBase, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local directory %s: %w", cleanBase, err)
	}

	cleanPrefix := ""
	if strings.TrimSpace(prefix) != "" {
		if p, err := SanitizeRelativePath(prefix); err == nil {
			cleanPrefix = p
		}
	}

	return &LocalStorage{
		baseDir: cleanBase,
		prefix:  cleanPrefix,
	}, nil
}

// Name returns the provider identifier.
func (l *LocalStorage) Name() string {
	return "local"
}

// resolvePath computes the safe local filesystem path for a given target path.
func (l *LocalStorage) resolvePath(targetPath string) (string, error) {
	cleanRel, err := SanitizeRelativePath(targetPath)
	if err != nil {
		return "", err
	}

	if l.prefix != "" {
		cleanRel = path.Join(l.prefix, cleanRel)
	}

	targetLocal := filepath.Join(l.baseDir, filepath.FromSlash(cleanRel))
	absTarget, err := filepath.Abs(targetLocal)
	if err != nil {
		return "", ErrInvalidPath
	}

	// Strict containment check
	if !strings.HasPrefix(absTarget, l.baseDir+string(filepath.Separator)) && absTarget != l.baseDir {
		return "", ErrInvalidPath
	}

	return absTarget, nil
}

// Save streams data directly from src into a local file using the 32KB sync.Pool buffer.
func (l *LocalStorage) Save(ctx context.Context, targetPath string, filename string, src io.Reader) (int64, error) {
	destPath, err := l.resolvePath(targetPath)
	if err != nil {
		return 0, err
	}

	// Create parent directories if nested
	parentDir := filepath.Dir(destPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory structure: %w", err)
	}

	dstFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to create destination file: %w", err)
	}

	var copyErr error
	written, copyErr := pool.StreamCopy(dstFile, src)
	closeErr := dstFile.Close()

	if copyErr != nil {
		_ = os.Remove(destPath)
		return 0, fmt.Errorf("streaming copy failed: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(destPath)
		return 0, fmt.Errorf("failed to close destination file: %w", closeErr)
	}

	// If a distinct filename was provided, create a relative symlink
	// in the same directory so it can also be accessed by its original filename.
	baseName := filepath.Base(destPath)
	if cleanName, err := SanitizePathSegment(filename); err == nil && cleanName != baseName {
		symlinkPath := filepath.Join(parentDir, cleanName)
		_ = os.Remove(symlinkPath)
		_ = os.Symlink(baseName, symlinkPath)
	}

	// If destPath is formatted as <id>_<filename>, also create a symlink for <id>
	// so the file can be downloaded directly using its unique identifier.
	if parts := strings.SplitN(baseName, "_", 2); len(parts) == 2 && len(parts[0]) >= 6 {
		idSymlink := filepath.Join(parentDir, parts[0])
		_ = os.Remove(idSymlink)
		_ = os.Symlink(baseName, idSymlink)
	}

	return written, nil
}

// ServeDownload handles GET requests using http.ServeContent, granting automatic
// Range request (206 Partial Content), caching headers, and zero RAM buffering.
func (l *LocalStorage) ServeDownload(w http.ResponseWriter, r *http.Request, targetPath string) error {
	fullPath, err := l.resolvePath(targetPath)
	if err != nil {
		return err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("error accessing file: %w", err)
	}

	if fi.IsDir() {
		return ErrNotFound
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Constraint 3: For local files, use http.ServeContent to provide automatic Range support.
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), file)
	return nil
}

// Open opens a stored local file for streaming read operations.
func (l *LocalStorage) Open(ctx context.Context, targetPath string) (io.ReadCloser, error) {
	fullPath, err := l.resolvePath(targetPath)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if fi.IsDir() {
		return nil, ErrNotFound
	}

	return os.Open(fullPath)
}

// Delete removes a file and any corresponding symlink from local disk.
func (l *LocalStorage) Delete(ctx context.Context, targetPath string) error {
	fullPath, err := l.resolvePath(targetPath)
	if err != nil {
		return err
	}

	if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
