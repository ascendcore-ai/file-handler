package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"file-handler/pkg/pool"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestLocalStorage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "local-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	provider, err := NewLocalStorage(tempDir, "tenant-a")
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	payload := bytes.Repeat([]byte("test-data-for-local-storage\n"), 500)
	targetPath := "reports/2026/quarter1/summary.txt"

	// 1. Save file
	written, err := provider.Save(context.Background(), targetPath, "summary.txt", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if written != int64(len(payload)) {
		t.Fatalf("expected written %d, got %d", len(payload), written)
	}

	// Verify file exists on disk in nested path
	expectedFile := filepath.Join(tempDir, "tenant-a", "reports", "2026", "quarter1", "summary.txt")
	if _, err := os.Stat(expectedFile); err != nil {
		t.Fatalf("expected file to exist at %s: %v", expectedFile, err)
	}

	// 2. Download file
	req := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rec := httptest.NewRecorder()
	if err := provider.ServeDownload(rec, req, targetPath); err != nil {
		t.Fatalf("ServeDownload failed: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	downloadedBuf := &bytes.Buffer{}
	if _, err := pool.StreamCopy(downloadedBuf, rec.Body); err != nil {
		t.Fatalf("stream copy failed: %v", err)
	}
	if !bytes.Equal(downloadedBuf.Bytes(), payload) {
		t.Fatal("downloaded content mismatch")
	}

	// 3. Range request
	rangeReq := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rangeReq.Header.Set("Range", "bytes=0-15")
	rangeRec := httptest.NewRecorder()
	if err := provider.ServeDownload(rangeRec, rangeReq, targetPath); err != nil {
		t.Fatalf("ServeDownload with Range failed: %v", err)
	}
	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d", rangeRec.Code)
	}
	if rangeRec.Body.Len() != 16 {
		t.Fatalf("expected 16 bytes for Range 0-15, got %d", rangeRec.Body.Len())
	}

	// 4. Delete file
	if err := provider.Delete(context.Background(), targetPath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err := os.Stat(expectedFile); !os.IsNotExist(err) {
		t.Fatal("file should have been deleted")
	}
}

func TestS3StorageMock(t *testing.T) {
	storedData := make(map[string][]byte)

	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")

		switch r.Method {
		case http.MethodPut:
			buf := &bytes.Buffer{}
			_, _ = pool.StreamCopy(buf, r.Body)
			storedData[key] = buf.Bytes()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil

		case http.MethodGet:
			data, exists := storedData[key]
			if !exists {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			h := make(http.Header)
			h.Set("Content-Type", "application/octet-stream")

			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "bytes=0-3" {
				h.Set("Content-Range", fmt.Sprintf("bytes 0-3/%d", len(data)))
				return &http.Response{
					StatusCode: http.StatusPartialContent,
					Header:     h,
					Body:       io.NopCloser(bytes.NewReader(data[0:4])),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     h,
				Body:       io.NopCloser(bytes.NewReader(data)),
			}, nil

		case http.MethodDelete:
			delete(storedData, key)
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil

		default:
			return &http.Response{
				StatusCode: http.StatusMethodNotAllowed,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
	})

	cfg := Config{
		Type:             "s3",
		S3Bucket:         "test-bucket",
		S3Endpoint:       "http://s3.example.com",
		S3AccessKey:      "test-key",
		S3SecretKey:      "test-secret",
		S3ForcePathStyle: true,
		StoragePrefix:    "app-data",
	}

	provider, err := NewS3Storage(cfg)
	if err != nil {
		t.Fatalf("NewS3Storage failed: %v", err)
	}
	provider.httpClient.Transport = mockTransport

	payload := []byte("hello-mock-s3-streaming-storage")
	targetPath := "uploads/2026/data.bin"

	// 1. Save
	if _, err := provider.Save(context.Background(), targetPath, "data.bin", bytes.NewReader(payload)); err != nil {
		t.Fatalf("S3 Save failed: %v", err)
	}

	// 2. Download
	req := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rec := httptest.NewRecorder()
	if err := provider.ServeDownload(rec, req, targetPath); err != nil {
		t.Fatalf("S3 ServeDownload failed: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	downloadedBuf := &bytes.Buffer{}
	_, _ = pool.StreamCopy(downloadedBuf, rec.Body)
	if !bytes.Equal(downloadedBuf.Bytes(), payload) {
		t.Fatal("S3 downloaded content mismatch")
	}

	// 3. Range request forwarding
	rangeReq := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rangeReq.Header.Set("Range", "bytes=0-3")
	rangeRec := httptest.NewRecorder()
	if err := provider.ServeDownload(rangeRec, rangeReq, targetPath); err != nil {
		t.Fatalf("S3 Range request failed: %v", err)
	}
	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d", rangeRec.Code)
	}
	if rangeRec.Body.String() != "hell" {
		t.Fatalf("expected 'hell', got %q", rangeRec.Body.String())
	}
}

func TestS3PresignUpload(t *testing.T) {
	cfg := Config{
		Type:             "s3",
		S3Bucket:         "presign-bucket",
		S3Region:         "us-east-1",
		S3AccessKey:      "AKIAIOSFODNN7EXAMPLE",
		S3SecretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		S3ForcePathStyle: true,
		StoragePrefix:    "uploads",
	}

	provider, err := NewS3Storage(cfg)
	if err != nil {
		t.Fatalf("NewS3Storage failed: %v", err)
	}

	presigned, err := provider.PresignUpload(context.Background(), "2026/documents/invoice.pdf", "invoice.pdf", "application/pdf", 900)
	if err != nil {
		t.Fatalf("PresignUpload failed: %v", err)
	}

	if presigned.Method != http.MethodPut {
		t.Fatalf("expected method PUT, got %s", presigned.Method)
	}
	if !presigned.IsDirect {
		t.Fatal("expected IsDirect to be true for S3 presigned upload")
	}
	if presigned.ExpiresIn != 900 {
		t.Fatalf("expected ExpiresIn 900, got %d", presigned.ExpiresIn)
	}
	if presigned.Headers["Content-Type"] != "application/pdf" {
		t.Fatalf("expected Content-Type header 'application/pdf', got %q", presigned.Headers["Content-Type"])
	}

	u, err := url.Parse(presigned.URL)
	if err != nil {
		t.Fatalf("failed to parse generated presigned URL: %v", err)
	}

	q := u.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		t.Fatalf("expected X-Amz-Algorithm AWS4-HMAC-SHA256, got %q", q.Get("X-Amz-Algorithm"))
	}
	if !strings.Contains(q.Get("X-Amz-Credential"), "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("credential does not contain access key: %q", q.Get("X-Amz-Credential"))
	}
	if q.Get("X-Amz-Expires") != "900" {
		t.Fatalf("expected X-Amz-Expires 900, got %q", q.Get("X-Amz-Expires"))
	}
	if q.Get("X-Amz-Signature") == "" {
		t.Fatal("expected non-empty X-Amz-Signature")
	}
}

func TestS3PresignDownload(t *testing.T) {
	cfg := Config{
		Type:             "s3",
		S3Bucket:         "presign-bucket",
		S3Region:         "us-east-1",
		S3AccessKey:      "AKIAIOSFODNN7EXAMPLE",
		S3SecretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		S3ForcePathStyle: true,
		StoragePrefix:    "uploads",
	}

	provider, err := NewS3Storage(cfg)
	if err != nil {
		t.Fatalf("NewS3Storage failed: %v", err)
	}

	presigned, err := provider.PresignDownload(context.Background(), "2026/documents/invoice.pdf", 600)
	if err != nil {
		t.Fatalf("PresignDownload failed: %v", err)
	}

	if presigned.Method != http.MethodGet {
		t.Fatalf("expected method GET, got %s", presigned.Method)
	}
	if !presigned.IsDirect {
		t.Fatal("expected IsDirect to be true for S3 presigned download")
	}
	if presigned.ExpiresIn != 600 {
		t.Fatalf("expected ExpiresIn 600, got %d", presigned.ExpiresIn)
	}

	u, err := url.Parse(presigned.URL)
	if err != nil {
		t.Fatalf("failed to parse generated presigned download URL: %v", err)
	}

	q := u.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		t.Fatalf("expected X-Amz-Algorithm AWS4-HMAC-SHA256, got %q", q.Get("X-Amz-Algorithm"))
	}
	if !strings.Contains(q.Get("X-Amz-Credential"), "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("credential does not contain access key: %q", q.Get("X-Amz-Credential"))
	}
	if q.Get("X-Amz-Expires") != "600" {
		t.Fatalf("expected X-Amz-Expires 600, got %q", q.Get("X-Amz-Expires"))
	}
	if q.Get("X-Amz-Signature") == "" {
		t.Fatal("expected non-empty X-Amz-Signature")
	}
}

func TestGCSStorageMock(t *testing.T) {
	storedData := make(map[string][]byte)

	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/upload/storage/v1/b/") {
			name := r.URL.Query().Get("name")
			buf := &bytes.Buffer{}
			_, _ = pool.StreamCopy(buf, r.Body)
			storedData[name] = buf.Bytes()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/storage/v1/b/") {
			parts := strings.Split(r.URL.Path, "/o/")
			if len(parts) < 2 {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			name := parts[1]
			data, exists := storedData[name]
			if !exists {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(data)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})

	cfg := Config{
		Type:        "gcs",
		GCSBucket:   "my-bucket",
		GCSEndpoint: "http://storage.googleapis.com",
	}

	provider, err := NewGCSStorage(cfg)
	if err != nil {
		t.Fatalf("NewGCSStorage failed: %v", err)
	}
	provider.httpClient.Transport = mockTransport

	payload := []byte("gcs-media-stream-content")
	targetPath := "gcs-folder/file.txt"

	if _, err := provider.Save(context.Background(), targetPath, "file.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("GCS Save failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rec := httptest.NewRecorder()
	if err := provider.ServeDownload(rec, req, targetPath); err != nil {
		t.Fatalf("GCS Download failed: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
}

func TestAzureStorageMock(t *testing.T) {
	storedData := make(map[string][]byte)

	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		key := strings.TrimPrefix(r.URL.Path, "/mycontainer/")
		if r.Method == http.MethodPut {
			buf := &bytes.Buffer{}
			_, _ = pool.StreamCopy(buf, r.Body)
			storedData[key] = buf.Bytes()
			return &http.Response{
				StatusCode: http.StatusCreated,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
		if r.Method == http.MethodGet {
			data, exists := storedData[key]
			if !exists {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(data)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})

	cfg := Config{
		Type:           "azure",
		AzureContainer: "mycontainer",
		AzureEndpoint:  "http://azure.example.com",
	}

	provider, err := NewAzureStorage(cfg)
	if err != nil {
		t.Fatalf("NewAzureStorage failed: %v", err)
	}
	provider.httpClient.Transport = mockTransport

	payload := []byte("azure-blockblob-stream")
	targetPath := "azure-dir/sample.bin"

	if _, err := provider.Save(context.Background(), targetPath, "sample.bin", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Azure Save failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/download/"+targetPath, nil)
	rec := httptest.NewRecorder()
	if err := provider.ServeDownload(rec, req, targetPath); err != nil {
		t.Fatalf("Azure Download failed: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
}

func TestLoadEnvFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-env-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := `# Test configuration
TEST_STORAGE_VAR=s3_custom_bucket
TEST_QUOTED_VAR="hello world"
TEST_SINGLE_QUOTE='single value'
# Empty line next

TEST_FLAG_BOOL=true
`
	_, _ = tempFile.WriteString(content)
	_ = tempFile.Close()

	if err := LoadEnvFile(tempFile.Name()); err != nil {
		t.Fatalf("LoadEnvFile failed: %v", err)
	}

	if os.Getenv("TEST_STORAGE_VAR") != "s3_custom_bucket" {
		t.Fatalf("expected s3_custom_bucket, got %q", os.Getenv("TEST_STORAGE_VAR"))
	}
	if os.Getenv("TEST_QUOTED_VAR") != "hello world" {
		t.Fatalf("expected 'hello world', got %q", os.Getenv("TEST_QUOTED_VAR"))
	}
	if os.Getenv("TEST_SINGLE_QUOTE") != "single value" {
		t.Fatalf("expected 'single value', got %q", os.Getenv("TEST_SINGLE_QUOTE"))
	}
}

func TestSanitizeRelativePath(t *testing.T) {
	validCases := map[string]string{
		"file.txt":                  "file.txt",
		"docs/2026/invoice.pdf":     "docs/2026/invoice.pdf",
		"a/b/c/d/e.bin":             "a/b/c/d/e.bin",
		"/leading/slash.txt":        "leading/slash.txt",
		"trailing/slash/":           "trailing/slash",
		"nested\\windows\\path.txt": "nested/windows/path.txt",
	}

	for input, expected := range validCases {
		clean, err := SanitizeRelativePath(input)
		if err != nil {
			t.Errorf("SanitizeRelativePath(%q) unexpected error: %v", input, err)
		}
		if clean != expected {
			t.Errorf("SanitizeRelativePath(%q) = %q; expected %q", input, clean, expected)
		}
	}

	invalidCases := []string{
		"",
		"..",
		"../etc/passwd",
		"docs/../../etc/passwd",
		"docs/../..",
		"/",
		"//",
	}

	for _, input := range invalidCases {
		_, err := SanitizeRelativePath(input)
		if err == nil {
			t.Errorf("SanitizeRelativePath(%q) expected error, got nil", input)
		}
	}
}

func TestDBConfigDefaults(t *testing.T) {
	_ = os.Setenv("DB_ENABLED", "true")
	_ = os.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/testdb?sslmode=disable")
	defer func() {
		_ = os.Unsetenv("DB_ENABLED")
		_ = os.Unsetenv("DB_DSN")
	}()

	cfg := NewDefaultConfig()
	if !cfg.DBEnabled {
		t.Fatal("expected DBEnabled to be true")
	}
	if cfg.DBDSN != "postgres://user:pass@localhost:5432/testdb?sslmode=disable" {
		t.Fatalf("expected postgres DSN, got %q", cfg.DBDSN)
	}
	if cfg.DBMaxOpenConns != 25 {
		t.Fatalf("expected default 25 max open conns, got %d", cfg.DBMaxOpenConns)
	}
}
