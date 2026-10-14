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
	Register("azure", func(cfg Config) (Provider, error) {
		return NewAzureStorage(cfg)
	})
}

// AzureStorage implements Provider for Azure Blob Storage.
type AzureStorage struct {
	account    string
	container  string
	endpoint   string
	sasToken   string
	prefix     string
	httpClient *http.Client
}

// NewAzureStorage initializes an AzureStorage instance.
func NewAzureStorage(cfg Config) (*AzureStorage, error) {
	if strings.TrimSpace(cfg.AzureContainer) == "" {
		return nil, errors.New("Azure container name is required")
	}

	account := strings.TrimSpace(cfg.AzureAccount)
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.AzureEndpoint), "/")

	if endpoint == "" {
		if account == "" {
			return nil, errors.New("either Azure account or Azure endpoint must be provided")
		}
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", account)
	}

	sasToken := strings.TrimPrefix(strings.TrimSpace(cfg.AzureSASToken), "?")

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

	return &AzureStorage{
		account:    account,
		container:  cfg.AzureContainer,
		endpoint:   endpoint,
		sasToken:   sasToken,
		prefix:     cleanPrefix,
		httpClient: &http.Client{Transport: transport},
	}, nil
}

// Name returns the provider identifier.
func (a *AzureStorage) Name() string {
	return "azure"
}

func (a *AzureStorage) resolveBlob(targetPath string) (string, error) {
	cleanRel, err := SanitizeRelativePath(targetPath)
	if err != nil {
		return "", err
	}
	if a.prefix != "" {
		cleanRel = path.Join(a.prefix, cleanRel)
	}
	return cleanRel, nil
}

func (a *AzureStorage) blobURL(blob string) string {
	baseURL := fmt.Sprintf("%s/%s/%s", a.endpoint, url.PathEscape(a.container), blob)
	if a.sasToken != "" {
		return baseURL + "?" + a.sasToken
	}
	return baseURL
}

// Save streams data directly to Azure Blob Storage as a BlockBlob without holding in memory.
func (a *AzureStorage) Save(ctx context.Context, targetPath string, filename string, src io.Reader) (int64, error) {
	blob, err := a.resolveBlob(targetPath)
	if err != nil {
		return 0, err
	}

	reqURL := a.blobURL(blob)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, src)
	if err != nil {
		return 0, fmt.Errorf("failed to create Azure request: %w", err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("x-ms-version", "2020-10-02")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Azure upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("Azure upload returned status %d", resp.StatusCode)
	}

	return resp.ContentLength, nil
}

// ServeDownload streams the blob directly to http.ResponseWriter.
// Constraint 3: For remote streams, pipe directly to http.ResponseWriter using pool.StreamCopy.
func (a *AzureStorage) ServeDownload(w http.ResponseWriter, r *http.Request, targetPath string) error {
	blob, err := a.resolveBlob(targetPath)
	if err != nil {
		return err
	}

	reqURL := a.blobURL(blob)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure GET request: %w", err)
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	req.Header.Set("x-ms-version", "2020-10-02")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Azure GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Azure GET returned status %d", resp.StatusCode)
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

// Open opens a stored blob in Azure Blob Storage for streaming read operations.
func (a *AzureStorage) Open(ctx context.Context, targetPath string) (io.ReadCloser, error) {
	blob, err := a.resolveBlob(targetPath)
	if err != nil {
		return nil, err
	}

	reqURL := a.blobURL(blob)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure GET request: %w", err)
	}
	req.Header.Set("x-ms-version", "2020-10-02")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Azure GET request failed: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("Azure GET returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// Delete removes a blob from Azure Blob Storage.
func (a *AzureStorage) Delete(ctx context.Context, targetPath string) error {
	blob, err := a.resolveBlob(targetPath)
	if err != nil {
		return err
	}

	reqURL := a.blobURL(blob)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-ms-version", "2020-10-02")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("Azure DELETE returned status %d", resp.StatusCode)
	}
	return nil
}
