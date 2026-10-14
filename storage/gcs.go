package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"file-handler/pkg/pool"
)

func init() {
	Register("gcs", func(cfg Config) (Provider, error) {
		return NewGCSStorage(cfg)
	})
}

// GCSStorage provides a streaming Google Cloud Storage provider.
type GCSStorage struct {
	bucket      string
	endpoint    string
	bearerToken string
	prefix      string
	httpClient  *http.Client
}

// NewGCSStorage initializes a GCSStorage instance.
func NewGCSStorage(cfg Config) (*GCSStorage, error) {
	if strings.TrimSpace(cfg.GCSBucket) == "" {
		return nil, errors.New("GCS bucket name is required")
	}

	endpoint := strings.TrimRight(strings.TrimSpace(cfg.GCSEndpoint), "/")
	if endpoint == "" {
		endpoint = "https://storage.googleapis.com"
	}

	cleanPrefix := ""
	if strings.TrimSpace(cfg.StoragePrefix) != "" {
		if p, err := SanitizeRelativePath(cfg.StoragePrefix); err == nil {
			cleanPrefix = p
		}
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &GCSStorage{
		bucket:      cfg.GCSBucket,
		endpoint:    endpoint,
		bearerToken: cfg.GCSBearerToken,
		prefix:      cleanPrefix,
		httpClient:  &http.Client{Transport: transport},
	}, nil
}

// Name returns the provider identifier.
func (g *GCSStorage) Name() string {
	return "gcs"
}

func (g *GCSStorage) resolveKey(targetPath string) (string, error) {
	cleanRel, err := SanitizeRelativePath(targetPath)
	if err != nil {
		return "", err
	}
	if g.prefix != "" {
		cleanRel = path.Join(g.prefix, cleanRel)
	}
	return cleanRel, nil
}

// Save streams data directly to GCS via the media upload API without buffering in memory.
func (g *GCSStorage) Save(ctx context.Context, targetPath string, filename string, src io.Reader) (int64, error) {
	key, err := g.resolveKey(targetPath)
	if err != nil {
		return 0, err
	}

	uploadURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
		g.endpoint, url.PathEscape(g.bucket), url.QueryEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, src)
	if err != nil {
		return 0, fmt.Errorf("failed to create GCS request: %w", err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)

	if g.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.bearerToken)
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("GCS upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("GCS upload returned status %d", resp.StatusCode)
	}

	return resp.ContentLength, nil
}

// ServeDownload streams the object directly from GCS to http.ResponseWriter.
// Constraint 3: For remote streams, pipe directly to ResponseWriter using pool.StreamCopy.
func (g *GCSStorage) ServeDownload(w http.ResponseWriter, r *http.Request, targetPath string) error {
	key, err := g.resolveKey(targetPath)
	if err != nil {
		return err
	}

	downloadURL := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media",
		g.endpoint, url.PathEscape(g.bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GCS GET request: %w", err)
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}

	if g.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.bearerToken)
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GCS GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("GCS GET returned status %d", resp.StatusCode)
	}

	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified"} {
		if val := resp.Header.Get(h); val != "" {
			w.Header().Set(h, val)
		}
	}
	w.WriteHeader(resp.StatusCode)

	_, err = pool.StreamCopy(w, resp.Body)
	return err
}

// Open opens a stored object in GCS for streaming read operations.
func (g *GCSStorage) Open(ctx context.Context, targetPath string) (io.ReadCloser, error) {
	key, err := g.resolveKey(targetPath)
	if err != nil {
		return nil, err
	}

	downloadURL := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media",
		g.endpoint, url.PathEscape(g.bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS GET request: %w", err)
	}

	if g.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.bearerToken)
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GCS GET request failed: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("GCS GET returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// Delete removes an object from GCS.
func (g *GCSStorage) Delete(ctx context.Context, targetPath string) error {
	key, err := g.resolveKey(targetPath)
	if err != nil {
		return err
	}

	deleteURL := fmt.Sprintf("%s/storage/v1/b/%s/o/%s",
		g.endpoint, url.PathEscape(g.bucket), url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL, nil)
	if err != nil {
		return err
	}

	if g.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.bearerToken)
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GCS DELETE returned status %d", resp.StatusCode)
	}
	return nil
}
