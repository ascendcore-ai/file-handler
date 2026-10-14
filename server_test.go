package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"file-handler/pkg/metadata"
	"file-handler/storage"
)

func TestBufferPool(t *testing.T) {
	bufPtr := GetBuffer()
	if bufPtr == nil {
		t.Fatal("expected non-nil buffer pointer from pool")
	}
	if len(*bufPtr) != BufferSize {
		t.Fatalf("expected buffer length %d, got %d", BufferSize, len(*bufPtr))
	}
	PutBuffer(bufPtr)

	// Test StreamCopy with pooled buffer
	sampleData := bytes.Repeat([]byte("High-Performance-Streaming-Buffer-Test\n"), 1000)
	src := bytes.NewReader(sampleData)
	dst := &bytes.Buffer{}

	written, err := StreamCopy(dst, src)
	if err != nil {
		t.Fatalf("StreamCopy failed: %v", err)
	}
	if written != int64(len(sampleData)) {
		t.Fatalf("expected %d bytes written, got %d", len(sampleData), written)
	}
	if !bytes.Equal(dst.Bytes(), sampleData) {
		t.Fatal("streamed data does not match original data")
	}
}

func TestUploadAndDownload(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "stream-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	// Prepare 128KB payload (larger than a single 32KB buffer)
	content := bytes.Repeat([]byte("0123456789abcdef"), 8*1024)
	hasher := sha256.New()
	_, _ = io.Copy(hasher, bytes.NewReader(content))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	// Create multipart request
	bodyBuf := &bytes.Buffer{}
	mpWriter := multipart.NewWriter(bodyBuf)
	part, err := mpWriter.CreateFormFile("file", "test-document.bin")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(content)); err != nil {
		t.Fatalf("failed to write part content: %v", err)
	}
	_ = mpWriter.Close()

	// 1. Test POST /upload
	req := httptest.NewRequest("POST", "/upload", bodyBuf)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var uploadResp UploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&uploadResp); err != nil {
		t.Fatalf("failed to decode upload response: %v", err)
	}

	if uploadResp.ID == "" {
		t.Fatal("expected non-empty file ID in upload response")
	}
	expectedFilename := uploadResp.ID + "_test-document.bin"
	if uploadResp.Filename != expectedFilename {
		t.Fatalf("expected stored filename %q, got %q", expectedFilename, uploadResp.Filename)
	}
	if uploadResp.OriginalFilename != "test-document.bin" {
		t.Fatalf("expected original_filename 'test-document.bin', got %q", uploadResp.OriginalFilename)
	}
	if uploadResp.Bytes != int64(len(content)) {
		t.Fatalf("expected bytes %d, got %d", len(content), uploadResp.Bytes)
	}

	// 2. Test GET /download/{id} with generated ID
	getReq := httptest.NewRequest("GET", "/download/"+uploadResp.ID, nil)
	getRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", getRec.Code)
	}
	if getRec.Header().Get("Accept-Ranges") != "bytes" {
		t.Fatalf("expected Accept-Ranges: bytes, got %q", getRec.Header().Get("Accept-Ranges"))
	}

	// Verify downloaded content hash using streaming copy
	downloadHasher := sha256.New()
	if _, err := StreamCopy(downloadHasher, getRec.Body); err != nil {
		t.Fatalf("failed to stream response body: %v", err)
	}
	actualHash := hex.EncodeToString(downloadHasher.Sum(nil))
	if actualHash != expectedHash {
		t.Fatalf("content hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	// 3. Test GET /download/{filename} via symlink
	linkReq := httptest.NewRequest("GET", "/download/test-document.bin", nil)
	linkRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(linkRec, linkReq)

	if linkRec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK when fetching by original filename, got %d", linkRec.Code)
	}
}

func TestDownloadRangeRequest(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "range-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	// Upload a 100-byte test file
	rawContent := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()_+-=[]{}|;:,.<>?0123456789")
	bodyBuf := &bytes.Buffer{}
	mpWriter := multipart.NewWriter(bodyBuf)
	part, _ := mpWriter.CreateFormFile("file", "range-sample.txt")
	_, _ = io.Copy(part, bytes.NewReader(rawContent))
	_ = mpWriter.Close()

	uploadReq := httptest.NewRequest("POST", "/upload?id=range-sample.txt", bodyBuf)
	uploadReq.Header.Set("Content-Type", mpWriter.FormDataContentType())
	uploadRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(uploadRec, uploadReq)

	if uploadRec.Code != http.StatusCreated {
		t.Fatalf("upload failed: code %d", uploadRec.Code)
	}

	// Request Range: bytes=10-19 (10 bytes: "KLMNOPQRST")
	rangeReq := httptest.NewRequest("GET", "/download/range-sample.txt", nil)
	rangeReq.Header.Set("Range", "bytes=10-19")
	rangeRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rangeRec, rangeReq)

	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("expected status 206 Partial Content, got %d", rangeRec.Code)
	}

	contentRange := rangeRec.Header().Get("Content-Range")
	expectedContentRange := fmt.Sprintf("bytes 10-19/%d", len(rawContent))
	if contentRange != expectedContentRange {
		t.Fatalf("expected Content-Range %q, got %q", expectedContentRange, contentRange)
	}

	rangeBody := &bytes.Buffer{}
	if _, err := StreamCopy(rangeBody, rangeRec.Body); err != nil {
		t.Fatalf("failed to read range body: %v", err)
	}

	expectedRangeData := rawContent[10:20]
	if !bytes.Equal(rangeBody.Bytes(), expectedRangeData) {
		t.Fatalf("expected range data %q, got %q", string(expectedRangeData), rangeBody.String())
	}
}

func TestPathTraversalProtection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "traversal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	testCases := []string{
		"../../etc/passwd",
		"..%2F..%2Fetc%2Fpasswd",
		"../secret.txt",
		"/etc/passwd",
		".",
		"..",
	}

	for _, tc := range testCases {
		req := httptest.NewRequest("GET", "/download/"+tc, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code == http.StatusOK {
			t.Fatalf("vulnerability: path traversal payload %q returned 200 OK", tc)
		}
	}
}

func TestUploadNonMultipart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "non-mp-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	req := httptest.NewRequest("POST", "/upload", bytes.NewBufferString("plain text body"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 Bad Request for non-multipart upload, got %d", rec.Code)
	}
}

func TestDownloadNotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "notfound-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	req := httptest.NewRequest("GET", "/download/non-existent-uuid-1234", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404 Not Found, got %d", rec.Code)
	}
}

func TestConcurrentUploadAndDownload(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "concurrent-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			data := bytes.Repeat([]byte(fmt.Sprintf("thread-%d-streaming-data-payload-", idx)), 1024)
			bodyBuf := &bytes.Buffer{}
			mpWriter := multipart.NewWriter(bodyBuf)
			part, err := mpWriter.CreateFormFile("file", fmt.Sprintf("concurrent-%d.bin", idx))
			if err != nil {
				errs <- fmt.Errorf("[%d] create form file: %w", idx, err)
				return
			}
			if _, err := io.Copy(part, bytes.NewReader(data)); err != nil {
				errs <- fmt.Errorf("[%d] write part: %w", idx, err)
				return
			}
			_ = mpWriter.Close()

			uploadReq := httptest.NewRequest("POST", "/upload", bodyBuf)
			uploadReq.Header.Set("Content-Type", mpWriter.FormDataContentType())
			uploadRec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(uploadRec, uploadReq)

			if uploadRec.Code != http.StatusCreated {
				errs <- fmt.Errorf("[%d] upload status: %d", idx, uploadRec.Code)
				return
			}

			var resp UploadResponse
			if err := json.NewDecoder(uploadRec.Body).Decode(&resp); err != nil {
				errs <- fmt.Errorf("[%d] decode json: %w", idx, err)
				return
			}

			// Download back
			getReq := httptest.NewRequest("GET", "/download/"+resp.ID, nil)
			getRec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(getRec, getReq)

			if getRec.Code != http.StatusOK {
				errs <- fmt.Errorf("[%d] download status: %d", idx, getRec.Code)
				return
			}

			downloadBuf := &bytes.Buffer{}
			if _, err := StreamCopy(downloadBuf, getRec.Body); err != nil {
				errs <- fmt.Errorf("[%d] stream copy: %w", idx, err)
				return
			}

			if !bytes.Equal(downloadBuf.Bytes(), data) {
				errs <- fmt.Errorf("[%d] data corrupted during concurrent streaming", idx)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrency error: %v", err)
	}
}

// TestUploadWithDynamicPath verifies that target paths can be specified dynamically
// via query parameter, HTTP request header, or multipart form field.
func TestUploadWithDynamicPath(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dynamic-path-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	payload := []byte("dynamic-path-content-payload-stream")

	// Case 1: Path via query parameter (?path=...)
	body1 := &bytes.Buffer{}
	mp1 := multipart.NewWriter(body1)
	part1, _ := mp1.CreateFormFile("file", "invoice.pdf")
	_, _ = io.Copy(part1, bytes.NewReader(payload))
	_ = mp1.Close()

	req1 := httptest.NewRequest("POST", "/upload?path=finance/2026/q1&id=invoice.pdf", body1)
	req1.Header.Set("Content-Type", mp1.FormDataContentType())
	rec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("case 1 failed: status %d, body: %s", rec1.Code, rec1.Body.String())
	}

	getReq1 := httptest.NewRequest("GET", "/download/finance/2026/q1/invoice.pdf", nil)
	getRec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec1, getReq1)
	if getRec1.Code != http.StatusOK {
		t.Fatalf("case 1 download failed: status %d", getRec1.Code)
	}

	// Case 2: Path via HTTP header (X-Upload-Path)
	body2 := &bytes.Buffer{}
	mp2 := multipart.NewWriter(body2)
	part2, _ := mp2.CreateFormFile("file", "avatar.png")
	_, _ = io.Copy(part2, bytes.NewReader(payload))
	_ = mp2.Close()

	req2 := httptest.NewRequest("POST", "/upload?id=avatar.png", body2)
	req2.Header.Set("Content-Type", mp2.FormDataContentType())
	req2.Header.Set("X-Upload-Path", "users/42/profile")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated {
		t.Fatalf("case 2 failed: status %d", rec2.Code)
	}

	getReq2 := httptest.NewRequest("GET", "/download/users/42/profile/avatar.png", nil)
	getRec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec2, getReq2)
	if getRec2.Code != http.StatusOK {
		t.Fatalf("case 2 download failed: status %d", getRec2.Code)
	}

	// Case 3: Path via multipart form field "path"
	body3 := &bytes.Buffer{}
	mp3 := multipart.NewWriter(body3)
	_ = mp3.WriteField("path", "archive/records")
	_ = mp3.WriteField("id", "doc1.txt")
	part3, _ := mp3.CreateFormFile("file", "doc1.txt")
	_, _ = io.Copy(part3, bytes.NewReader(payload))
	_ = mp3.Close()

	req3 := httptest.NewRequest("POST", "/upload", body3)
	req3.Header.Set("Content-Type", mp3.FormDataContentType())
	rec3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusCreated {
		t.Fatalf("case 3 failed: status %d", rec3.Code)
	}

	getReq3 := httptest.NewRequest("GET", "/download/archive/records/doc1.txt", nil)
	getRec3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec3, getReq3)
	if getRec3.Code != http.StatusOK {
		t.Fatalf("case 3 download failed: status %d", getRec3.Code)
	}
}

// TestV1HealthCheck verifies that /v1/health returns 200 OK with the version and storage backend.
func TestV1HealthCheck(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "health-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	for _, endpoint := range []string{"/v1/health", "/health"} {
		req := httptest.NewRequest("GET", endpoint, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK for %s, got %d", endpoint, rec.Code)
		}
		if rec.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", rec.Header().Get("Content-Type"))
		}

		var body map[string]any
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("failed to parse JSON response for %s: %v", endpoint, err)
		}
		if body["status"] != "ok" {
			t.Fatalf("expected status 'ok', got %v", body["status"])
		}
		if body["version"] != "v1" {
			t.Fatalf("expected version 'v1', got %v", body["version"])
		}
		if body["storage"] != "local" {
			t.Fatalf("expected storage 'local', got %v", body["storage"])
		}
	}
}

// TestV1UploadAndDownloadIntegration verifies end-to-end streaming file upload
// and download using the versioned /v1/upload and /v1/download/{path...} endpoints.
func TestV1UploadAndDownloadIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "v1-integration-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	payload := bytes.Repeat([]byte("0123456789abcdef-v1-stream-test\n"), 2000) // ~64 KB
	hasher := sha256.New()
	_, _ = io.Copy(hasher, bytes.NewReader(payload))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	bodyBuf := &bytes.Buffer{}
	mpWriter := multipart.NewWriter(bodyBuf)
	part, err := mpWriter.CreateFormFile("file", "v1-report.pdf")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	_, _ = io.Copy(part, bytes.NewReader(payload))
	_ = mpWriter.Close()

	// 1. Upload via POST /v1/upload with dynamic subpath
	uploadReq := httptest.NewRequest("POST", "/v1/upload?path=reports/2026/annual&id=v1-report.pdf", bodyBuf)
	uploadReq.Header.Set("Content-Type", mpWriter.FormDataContentType())
	uploadRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(uploadRec, uploadReq)

	if uploadRec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d. Body: %s", uploadRec.Code, uploadRec.Body.String())
	}

	var uploadResp UploadResponse
	if err := json.NewDecoder(uploadRec.Body).Decode(&uploadResp); err != nil {
		t.Fatalf("failed to decode upload response: %v", err)
	}

	expectedPath := "reports/2026/annual/" + uploadResp.Filename
	if uploadResp.Path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, uploadResp.Path)
	}
	if uploadResp.OriginalFilename != "v1-report.pdf" {
		t.Fatalf("expected original_filename 'v1-report.pdf', got %q", uploadResp.OriginalFilename)
	}
	expectedLocation := "/v1/download/" + expectedPath
	if uploadRec.Header().Get("Location") != expectedLocation {
		t.Fatalf("expected Location %q, got %q", expectedLocation, uploadRec.Header().Get("Location"))
	}

	// 2. Download via GET /v1/download/{path...}
	getReq := httptest.NewRequest("GET", "/v1/download/"+expectedPath, nil)
	getRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("download failed: status %d", getRec.Code)
	}

	downloadHasher := sha256.New()
	if _, err := StreamCopy(downloadHasher, getRec.Body); err != nil {
		t.Fatalf("failed to stream response: %v", err)
	}
	actualHash := hex.EncodeToString(downloadHasher.Sum(nil))
	if actualHash != expectedHash {
		t.Fatalf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	// 3. Range request via GET /v1/download/{path...}
	rangeReq := httptest.NewRequest("GET", "/v1/download/"+expectedPath, nil)
	rangeReq.Header.Set("Range", "bytes=0-15")
	rangeRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rangeRec, rangeReq)

	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d", rangeRec.Code)
	}
	if rangeRec.Body.Len() != 16 {
		t.Fatalf("expected 16 bytes for range 0-15, got %d", rangeRec.Body.Len())
	}
}

// TestV1PresignUpload verifies the POST /v1/upload/presign endpoint
// with both local storage (fallback) and S3 storage (direct SigV4).
func TestV1PresignUpload(t *testing.T) {
	// 1. Test with Local storage provider (fallback)
	tempDir, _ := os.MkdirTemp("", "presign-test-*")
	defer os.RemoveAll(tempDir)
	localSrv, _ := NewServer(tempDir)

	presignReqJSON := `{"filename":"document.pdf","path":"finance/2026","content_type":"application/pdf","expires_in":600}`
	req1 := httptest.NewRequest("POST", "/v1/upload/presign", bytes.NewBufferString(presignReqJSON))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	localSrv.Handler().ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rec1.Code)
	}

	var fallbackResp storage.PresignedUpload
	if err := json.NewDecoder(rec1.Body).Decode(&fallbackResp); err != nil {
		t.Fatalf("failed to decode presign response: %v", err)
	}
	if fallbackResp.IsDirect {
		t.Fatal("expected IsDirect to be false for local storage fallback")
	}
	if !strings.Contains(fallbackResp.URL, "/v1/upload") {
		t.Fatalf("expected URL pointing to /v1/upload, got %q", fallbackResp.URL)
	}

	// 2. Test with S3 storage provider (direct presigned URL)
	s3Cfg := storage.Config{
		Type:             "s3",
		S3Bucket:         "cloud-bucket",
		S3Region:         "us-east-1",
		S3AccessKey:      "AKIAIOSFODNN7EXAMPLE",
		S3SecretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		S3ForcePathStyle: true,
	}
	s3Provider, err := storage.NewS3Storage(s3Cfg)
	if err != nil {
		t.Fatalf("failed to create S3 storage: %v", err)
	}
	s3Srv := NewServerWithStorage(s3Provider)

	req2 := httptest.NewRequest("POST", "/v1/upload/presign?filename=avatar.png&path=users/1&expires_in=900", nil)
	rec2 := httptest.NewRecorder()
	s3Srv.Handler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for S3 presign, got %d", rec2.Code)
	}

	var s3Resp storage.PresignedUpload
	if err := json.NewDecoder(rec2.Body).Decode(&s3Resp); err != nil {
		t.Fatalf("failed to decode S3 presign response: %v", err)
	}
	if !s3Resp.IsDirect {
		t.Fatal("expected IsDirect to be true for S3 storage")
	}
	if s3Resp.Method != http.MethodPut {
		t.Fatalf("expected method PUT, got %s", s3Resp.Method)
	}
	if !strings.Contains(s3Resp.URL, "X-Amz-Signature") {
		t.Fatalf("expected SigV4 signature in presigned URL, got %q", s3Resp.URL)
	}
}

// TestV1PresignDownload verifies POST & GET /v1/download/presign and /v1/download/presign/{file_id}
// taking file_id as the primary input with both local storage (fallback) and S3 storage (direct SigV4).
func TestV1PresignDownload(t *testing.T) {
	// 1. Test with Local storage provider (fallback)
	tempDir, _ := os.MkdirTemp("", "presign-dl-test-*")
	defer os.RemoveAll(tempDir)
	localSrv, _ := NewServer(tempDir)

	// Test via POST JSON with file_id
	dlJSON := `{"file_id":"photos/vacation.jpg","expires_in":600}`
	req1 := httptest.NewRequest("POST", "/v1/download/presign", bytes.NewBufferString(dlJSON))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	localSrv.Handler().ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d: %s", rec1.Code, rec1.Body.String())
	}

	var fallbackResp storage.PresignedDownload
	if err := json.NewDecoder(rec1.Body).Decode(&fallbackResp); err != nil {
		t.Fatalf("failed to decode presign download response: %v", err)
	}
	if fallbackResp.IsDirect {
		t.Fatal("expected IsDirect to be false for local storage fallback")
	}
	if fallbackResp.Method != http.MethodGet {
		t.Fatalf("expected method GET, got %s", fallbackResp.Method)
	}
	if fallbackResp.FileID != "photos/vacation.jpg" {
		t.Fatalf("expected FileID 'photos/vacation.jpg', got %q", fallbackResp.FileID)
	}
	if fallbackResp.URL != "/v1/download/photos/vacation.jpg" {
		t.Fatalf("expected fallback URL '/v1/download/photos/vacation.jpg', got %q", fallbackResp.URL)
	}

	// Test via GET query parameters with file_id
	reqGet := httptest.NewRequest("GET", "/v1/download/presign?file_id=documents/report.pdf", nil)
	recGet := httptest.NewRecorder()
	localSrv.Handler().ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for GET presign, got %d", recGet.Code)
	}

	// Test via GET path parameter: /v1/download/presign/{file_id...}
	reqPath := httptest.NewRequest("GET", "/v1/download/presign/documents/contract.pdf", nil)
	recPath := httptest.NewRecorder()
	localSrv.Handler().ServeHTTP(recPath, reqPath)
	if recPath.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for GET path presign, got %d", recPath.Code)
	}

	// 2. Test with S3 storage provider (direct SigV4 presigned download URL using file_id)
	s3Cfg := storage.Config{
		Type:             "s3",
		S3Bucket:         "cloud-bucket",
		S3Region:         "us-east-1",
		S3AccessKey:      "AKIAIOSFODNN7EXAMPLE",
		S3SecretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		S3ForcePathStyle: true,
	}
	s3Provider, err := storage.NewS3Storage(s3Cfg)
	if err != nil {
		t.Fatalf("failed to create S3 storage: %v", err)
	}
	s3Srv := NewServerWithStorage(s3Provider)

	reqS3 := httptest.NewRequest("GET", "/v1/download/presign?file_id=media/video.mp4&expires_in=300", nil)
	recS3 := httptest.NewRecorder()
	s3Srv.Handler().ServeHTTP(recS3, reqS3)

	if recS3.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for S3 download presign, got %d: %s", recS3.Code, recS3.Body.String())
	}

	var s3Resp storage.PresignedDownload
	if err := json.NewDecoder(recS3.Body).Decode(&s3Resp); err != nil {
		t.Fatalf("failed to decode S3 presign download response: %v", err)
	}
	if !s3Resp.IsDirect {
		t.Fatal("expected IsDirect to be true for S3 storage")
	}
	if s3Resp.FileID != "media/video.mp4" {
		t.Fatalf("expected FileID 'media/video.mp4', got %q", s3Resp.FileID)
	}
	if s3Resp.Method != http.MethodGet {
		t.Fatalf("expected method GET, got %s", s3Resp.Method)
	}
	if s3Resp.ExpiresIn != 300 {
		t.Fatalf("expected ExpiresIn 300, got %d", s3Resp.ExpiresIn)
	}
	if !strings.Contains(s3Resp.URL, "X-Amz-Signature") {
		t.Fatalf("expected SigV4 signature in presigned download URL, got %q", s3Resp.URL)
	}
	if !strings.Contains(s3Resp.URL, "X-Amz-Expires=300") {
		t.Fatalf("expected X-Amz-Expires=300 in URL, got %q", s3Resp.URL)
	}

	// 3. Test with Metadata Database resolution (file_id -> path resolution)
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	repo := metadata.NewGORMRepository(db)
	_ = repo.AutoMigrate()

	_ = repo.SaveFile(context.Background(), &metadata.FileRecord{
		ID:       "id_abc123",
		Filename: "id_abc123_photo.jpg",
		Path:     "uploads/2026/id_abc123_photo.jpg",
	})

	dbSrv := NewServerWithStorageAndMetadata(localSrv.Storage, repo)
	reqMeta := httptest.NewRequest("GET", "/v1/download/presign/id_abc123", nil)
	recMeta := httptest.NewRecorder()
	dbSrv.Handler().ServeHTTP(recMeta, reqMeta)

	if recMeta.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for metadata ID presign, got %d: %s", recMeta.Code, recMeta.Body.String())
	}
	var metaResp storage.PresignedDownload
	_ = json.NewDecoder(recMeta.Body).Decode(&metaResp)
	if metaResp.FileID != "id_abc123" {
		t.Fatalf("expected FileID 'id_abc123', got %q", metaResp.FileID)
	}
	if metaResp.Path != "uploads/2026/id_abc123_photo.jpg" {
		t.Fatalf("expected resolved path 'uploads/2026/id_abc123_photo.jpg', got %q", metaResp.Path)
	}
}

// TestV1UploadCompleteCallback verifies POST /v1/upload/complete for client confirmations.
func TestV1UploadCompleteCallback(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "complete-test-*")
	defer os.RemoveAll(tempDir)
	srv, _ := NewServer(tempDir)

	validPayload := `{"path":"documents/invoice.pdf","filename":"invoice.pdf","size":1048576,"etag":"\"d41d8cd98f00b204e9800998ecf8427e\""}`
	req := httptest.NewRequest("POST", "/v1/upload/complete", bytes.NewBufferString(validPayload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "recorded" {
		t.Fatalf("expected status 'recorded', got %v", resp["status"])
	}
	if resp["path"] != "documents/invoice.pdf" {
		t.Fatalf("expected path 'documents/invoice.pdf', got %v", resp["path"])
	}

	// Test invalid path traversal payload rejection
	invalidPayload := `{"path":"../../etc/passwd","filename":"passwd","size":12}`
	invalidReq := httptest.NewRequest("POST", "/v1/upload/complete", bytes.NewBufferString(invalidPayload))
	invalidReq.Header.Set("Content-Type", "application/json")
	invalidRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(invalidRec, invalidReq)

	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 Bad Request for path traversal, got %d", invalidRec.Code)
	}
}

// TestV1StorageEventsWebhook verifies POST /v1/webhooks/storage-events.
func TestV1StorageEventsWebhook(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "webhook-test-*")
	defer os.RemoveAll(tempDir)
	srv, _ := NewServer(tempDir)

	webhookPayload := `{"EventName":"s3:ObjectCreated:Put","Key":"documents/file.txt","Records":[{"eventSource":"aws:s3"}]}`
	req := httptest.NewRequest("POST", "/v1/webhooks/storage-events", bytes.NewBufferString(webhookPayload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode webhook response: %v", err)
	}
	if resp["status"] != "acknowledged" {
		t.Fatalf("expected status 'acknowledged', got %v", resp["status"])
	}
}

// TestUploadWithThumbnail verifies that setting ?thumbnail=true on image uploads
// generates a thumbnail in the path/to/folder/thumbnails/image.jpg path.
func TestUploadWithThumbnail(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "thumb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	// Create 800x600 test image in memory
	srcImg := image.NewRGBA(image.Rect(0, 0, 800, 600))
	for y := 0; y < 600; y++ {
		for x := 0; x < 800; x++ {
			srcImg.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		}
	}
	imgBuf := &bytes.Buffer{}
	if err := jpeg.Encode(imgBuf, srcImg, nil); err != nil {
		t.Fatalf("failed to encode test jpeg: %v", err)
	}

	// Prepare multipart upload request
	bodyBuf := &bytes.Buffer{}
	mp := multipart.NewWriter(bodyBuf)
	part, _ := mp.CreateFormFile("file", "vacation.jpg")
	_, _ = io.Copy(part, imgBuf)
	_ = mp.Close()

	req := httptest.NewRequest("POST", "/v1/upload?path=photos/2026&id=vacation.jpg&thumbnail=true", bodyBuf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var uploadResp UploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&uploadResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if uploadResp.Thumbnail == nil {
		t.Fatal("expected thumbnail to be generated, got nil")
	}

	expectedThumbPath := "photos/2026/thumbnails/" + uploadResp.Filename
	if uploadResp.Thumbnail.Path != expectedThumbPath {
		t.Fatalf("expected thumbnail path %q, got %q", expectedThumbPath, uploadResp.Thumbnail.Path)
	}
	if uploadResp.OriginalFilename != "vacation.jpg" {
		t.Fatalf("expected original_filename 'vacation.jpg', got %q", uploadResp.OriginalFilename)
	}
	if uploadResp.Thumbnail.Width != 200 {
		t.Fatalf("expected thumbnail width 200, got %d", uploadResp.Thumbnail.Width)
	}
	if uploadResp.Thumbnail.Height != 150 {
		t.Fatalf("expected thumbnail height 150, got %d", uploadResp.Thumbnail.Height)
	}

	// Verify thumbnail is downloadable via GET /v1/download/{path...}
	thumbReq := httptest.NewRequest("GET", "/v1/download/"+expectedThumbPath, nil)
	thumbRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(thumbRec, thumbReq)

	if thumbRec.Code != http.StatusOK {
		t.Fatalf("downloading thumbnail failed with status %d", thumbRec.Code)
	}

	decodedThumb, _, err := image.Decode(thumbRec.Body)
	if err != nil {
		t.Fatalf("failed to decode downloaded thumbnail: %v", err)
	}
	if decodedThumb.Bounds().Dx() != 200 || decodedThumb.Bounds().Dy() != 150 {
		t.Fatalf("thumbnail dimensions mismatch: got %dx%d", decodedThumb.Bounds().Dx(), decodedThumb.Bounds().Dy())
	}
}

// TestUploadWithCustomThumbnailFolderAndSize verifies custom thumb_dir and thumb_size overrides.
func TestUploadWithCustomThumbnailFolderAndSize(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "thumb-custom-*")
	defer os.RemoveAll(tempDir)
	srv, _ := NewServer(tempDir)

	srcImg := image.NewRGBA(image.Rect(0, 0, 400, 400))
	imgBuf := &bytes.Buffer{}
	_ = jpeg.Encode(imgBuf, srcImg, nil)

	bodyBuf := &bytes.Buffer{}
	mp := multipart.NewWriter(bodyBuf)
	part, _ := mp.CreateFormFile("file", "avatar.jpg")
	_, _ = io.Copy(part, imgBuf)
	_ = mp.Close()

	req := httptest.NewRequest("POST", "/v1/upload?path=users/12&id=avatar.jpg&thumbnail=true&thumb_dir=thumbs&thumb_size=120", bodyBuf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("upload failed: status %d", rec.Code)
	}

	var uploadResp UploadResponse
	_ = json.NewDecoder(rec.Body).Decode(&uploadResp)

	if uploadResp.Thumbnail == nil {
		t.Fatal("expected thumbnail result")
	}

	expectedThumbPath := "users/12/thumbs/" + uploadResp.Filename
	if uploadResp.Thumbnail.Path != expectedThumbPath {
		t.Fatalf("expected custom thumb path %q, got %q", expectedThumbPath, uploadResp.Thumbnail.Path)
	}
	if uploadResp.OriginalFilename != "avatar.jpg" {
		t.Fatalf("expected original_filename 'avatar.jpg', got %q", uploadResp.OriginalFilename)
	}
	if uploadResp.Thumbnail.Width != 120 || uploadResp.Thumbnail.Height != 120 {
		t.Fatalf("expected 120x120, got %dx%d", uploadResp.Thumbnail.Width, uploadResp.Thumbnail.Height)
	}
}

// TestUploadThumbnailIgnoredForNonImage ensures non-image files gracefully skip thumbnail generation.
func TestUploadThumbnailIgnoredForNonImage(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "thumb-nonimage-*")
	defer os.RemoveAll(tempDir)
	srv, _ := NewServer(tempDir)

	bodyBuf := &bytes.Buffer{}
	mp := multipart.NewWriter(bodyBuf)
	part, _ := mp.CreateFormFile("file", "notes.txt")
	_, _ = part.Write([]byte("plain text content"))
	_ = mp.Close()

	req := httptest.NewRequest("POST", "/v1/upload?id=notes.txt&thumbnail=true", bodyBuf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("upload failed: status %d", rec.Code)
	}

	var uploadResp UploadResponse
	_ = json.NewDecoder(rec.Body).Decode(&uploadResp)

	if uploadResp.Thumbnail != nil {
		t.Fatal("expected thumbnail to be nil for non-image files")
	}
}

func TestAPIDocumentationEndpoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "docs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	testCases := []struct {
		url          string
		expectedCode int
		contains     string
	}{
		{"/openapi.yaml", http.StatusOK, "openapi: 3.1.0"},
		{"/v1/openapi.yaml", http.StatusOK, "openapi: 3.1.0"},
		{"/docs", http.StatusOK, "@scalar/api-reference"},
		{"/v1/docs", http.StatusOK, "@scalar/api-reference"},
		{"/docs/swagger", http.StatusOK, "swagger-ui"},
		{"/v1/docs/swagger", http.StatusOK, "swagger-ui"},
	}

	for _, tc := range testCases {
		req := httptest.NewRequest("GET", tc.url, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code != tc.expectedCode {
			t.Errorf("GET %s expected status %d, got %d", tc.url, tc.expectedCode, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), tc.contains) {
			t.Errorf("GET %s body does not contain expected snippet %q", tc.url, tc.contains)
		}
	}
}

func TestV1MetadataEndpointsWhenDisabled(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "meta-disabled-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	srv, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize server: %v", err)
	}

	// 1. Health check reports database disabled
	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"database":"disabled"`) {
		t.Fatalf("expected database disabled in health, got %s", rec.Body.String())
	}

	// 2. Querying files returns 503 Service Unavailable
	req = httptest.NewRequest("GET", "/v1/files", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
	}

	// 3. Querying single file metadata returns 503
	req = httptest.NewRequest("GET", "/v1/files/some-id", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	// 4. Deleting file metadata returns 503
	req = httptest.NewRequest("DELETE", "/v1/files/some-id", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestV1MetadataEndpointsWhenEnabled(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "meta-enabled-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	provider, err := storage.NewLocalStorage(tempDir, "")
	if err != nil {
		t.Fatalf("failed to initialize local storage: %v", err)
	}

	// In-memory SQLite for fast testing
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to initialize sqlite: %v", err)
	}
	repo := metadata.NewGORMRepository(db)
	if err := repo.AutoMigrate(); err != nil {
		t.Fatalf("failed to automigrate: %v", err)
	}
	defer repo.Close()

	srv := NewServerWithStorageAndMetadata(provider, repo)

	// 1. Health check reports database enabled
	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"database":"enabled"`) {
		t.Fatalf("expected database enabled in health, got %s", rec.Body.String())
	}

	// 2. Upload file with collection, user_id, description, custom metadata, and thumbnail
	imgBuf := &bytes.Buffer{}
	img := image.NewRGBA(image.Rect(0, 0, 400, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 400; x++ {
			img.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		}
	}
	_ = jpeg.Encode(imgBuf, img, nil)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("collection", "invoices")
	_ = writer.WriteField("user_id", "usr_99")
	_ = writer.WriteField("description", "Q1 Expense Invoice")
	_ = writer.WriteField("metadata", `{"vendor":"ACME Corp","amount":1500}`)
	_ = writer.WriteField("thumbnail", "true")

	part, _ := writer.CreateFormFile("file", "invoice.jpg")
	_, _ = io.Copy(part, imgBuf)
	_ = writer.Close()

	uploadReq := httptest.NewRequest("POST", "/v1/upload?path=finance/2026&id=inv-2026.jpg", body)
	uploadReq.Header.Set("Content-Type", writer.FormDataContentType())
	uploadRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(uploadRec, uploadReq)

	if uploadRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", uploadRec.Code, uploadRec.Body.String())
	}

	var uploadResp UploadResponse
	_ = json.NewDecoder(uploadRec.Body).Decode(&uploadResp)
	if uploadResp.Collection != "invoices" {
		t.Errorf("expected collection 'invoices', got %q", uploadResp.Collection)
	}
	if uploadResp.UserID != "usr_99" {
		t.Errorf("expected user_id 'usr_99', got %q", uploadResp.UserID)
	}
	if uploadResp.Thumbnail == nil {
		t.Fatal("expected thumbnail to be generated")
	}

	// 3. Query files list: GET /v1/files?collection=invoices
	listReq := httptest.NewRequest("GET", "/v1/files?collection=invoices", nil)
	listRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", listRec.Code, listRec.Body.String())
	}

	var listResult metadata.QueryResult
	_ = json.NewDecoder(listRec.Body).Decode(&listResult)
	if listResult.Total != 1 || len(listResult.Files) != 1 {
		t.Fatalf("expected 1 file in list, got total %d, files %d", listResult.Total, len(listResult.Files))
	}
	fileRec := listResult.Files[0]
	if fileRec.ID != "inv-2026.jpg" {
		t.Errorf("expected ID 'inv-2026.jpg', got %q", fileRec.ID)
	}
	if fileRec.OriginalFilename != "invoice.jpg" {
		t.Errorf("expected original_filename 'invoice.jpg', got %q", fileRec.OriginalFilename)
	}
	if fileRec.Thumbnail == nil {
		t.Fatal("expected thumbnail attached to file in list")
	}

	// 4. Query files list with non-matching filter: GET /v1/files?collection=receipts
	listReq = httptest.NewRequest("GET", "/v1/files?collection=receipts", nil)
	listRec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(listRec, listReq)
	var emptyResult metadata.QueryResult
	_ = json.NewDecoder(listRec.Body).Decode(&emptyResult)
	if emptyResult.Total != 0 || len(emptyResult.Files) != 0 {
		t.Fatalf("expected 0 files, got %d", emptyResult.Total)
	}

	// 5. Query single file metadata: GET /v1/files/inv-2026.jpg
	getReq := httptest.NewRequest("GET", "/v1/files/inv-2026.jpg", nil)
	getRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}
	var singleFile metadata.FileRecord
	_ = json.NewDecoder(getRec.Body).Decode(&singleFile)
	if singleFile.Description != "Q1 Expense Invoice" {
		t.Errorf("expected description 'Q1 Expense Invoice', got %q", singleFile.Description)
	}

	// 6. Direct browser upload completion metadata recording: POST /v1/upload/complete
	completePayload := `{"path":"media/upload.pdf","filename":"upload.pdf","size":5000,"collection":"contracts","user_id":"usr_42"}`
	completeReq := httptest.NewRequest("POST", "/v1/upload/complete", strings.NewReader(completePayload))
	completeReq.Header.Set("Content-Type", "application/json")
	completeRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(completeRec, completeReq)
	if completeRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", completeRec.Code)
	}

	// Verify complete recorded file
	getContractReq := httptest.NewRequest("GET", "/v1/files/upload.pdf", nil)
	getContractRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getContractRec, getContractReq)
	if getContractRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getContractRec.Code)
	}
	var contractFile metadata.FileRecord
	_ = json.NewDecoder(getContractRec.Body).Decode(&contractFile)
	if contractFile.Collection != "contracts" {
		t.Errorf("expected collection contracts, got %s", contractFile.Collection)
	}

	// 7. Delete file metadata: DELETE /v1/files/inv-2026.jpg
	delReq := httptest.NewRequest("DELETE", "/v1/files/inv-2026.jpg", nil)
	delRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on delete, got %d", delRec.Code)
	}

	// Verify it's no longer found
	getAfterDelReq := httptest.NewRequest("GET", "/v1/files/inv-2026.jpg", nil)
	getAfterDelRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getAfterDelRec, getAfterDelReq)
	if getAfterDelRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after delete, got %d", getAfterDelRec.Code)
	}
}

// TestZeroMemoryBufferingRuleCompliance parses all Go source files into AST
// and verifies that none of the forbidden buffering functions are ever called.
func TestZeroMemoryBufferingRuleCompliance(t *testing.T) {
	fset := token.NewFileSet()
	dirs := []string{".", "storage", "pkg/pool", "pkg/thumbnail", "pkg/metadata"}

	forbiddenCalls := map[string]bool{
		"io." + "ReadAll":          true,
		"ioutil." + "ReadAll":      true,
		"Parse" + "MultipartForm": true,
	}

	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
			return strings.HasSuffix(fi.Name(), ".go")
		}, 0)
		if err != nil {
			t.Fatalf("failed to parse directory %s: %v", dir, err)
		}

		for _, pkg := range pkgs {
			for filename, file := range pkg.Files {
				ast.Inspect(file, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}

					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}

					fnName := sel.Sel.Name
					if ident, ok := sel.X.(*ast.Ident); ok {
						fullCall := ident.Name + "." + fnName
						if forbiddenCalls[fullCall] {
							t.Errorf("RULE VIOLATION in %s at %v: forbidden function call %s",
								filename, fset.Position(call.Pos()), fullCall)
						}
					}

					if forbiddenCalls[fnName] {
						t.Errorf("RULE VIOLATION in %s at %v: forbidden method call %s",
							filename, fset.Position(call.Pos()), fnName)
					}

					return true
				})
			}
		}
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

func TestGenerateShortID(t *testing.T) {
	id, err := generateShortID(0)
	if err != nil {
		t.Fatalf("failed to generate default short ID: %v", err)
	}
	if len(id) != defaultShortIDLength {
		t.Fatalf("expected default length %d, got %d (%s)", defaultShortIDLength, len(id), id)
	}

	for _, ch := range id {
		if !strings.ContainsRune(shortIDAlphabet, ch) {
			t.Fatalf("invalid character %q in generated short ID %q", ch, id)
		}
	}

	customID, err := generateShortID(8)
	if err != nil {
		t.Fatalf("failed to generate 8-char short ID: %v", err)
	}
	if len(customID) != 8 {
		t.Fatalf("expected length 8, got %d (%s)", len(customID), customID)
	}

	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id, err := generateShortID(12)
		if err != nil {
			t.Fatalf("iteration %d failed: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("collision detected in 1000 samples: %s", id)
		}
		seen[id] = true
	}
}

// TestUploadCollisionAvoidanceWithShortID verifies that uploading files with the same original filename
// prefixes them with distinct Base62 short IDs, avoiding any file or thumbnail overwrites, while preserving
// the original filename in the database and response.
func TestUploadCollisionAvoidanceWithShortID(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "collision-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	provider, err := storage.NewLocalStorage(tempDir, "")
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to initialize sqlite: %v", err)
	}
	dbRepo := metadata.NewGORMRepository(db)
	if err := dbRepo.AutoMigrate(); err != nil {
		t.Fatalf("failed to automigrate: %v", err)
	}
	defer dbRepo.Close()

	srv := NewServerWithStorageAndMetadata(provider, dbRepo)

	// Helper to generate a solid-color JPEG
	generateTestJPEG := func(c color.RGBA) []byte {
		buf := &bytes.Buffer{}
		img := image.NewRGBA(image.Rect(0, 0, 300, 300))
		for y := 0; y < 300; y++ {
			for x := 0; x < 300; x++ {
				img.Set(x, y, c)
			}
		}
		_ = jpeg.Encode(buf, img, nil)
		return buf.Bytes()
	}

	redJPEG := generateTestJPEG(color.RGBA{R: 255, G: 0, B: 0, A: 255})
	blueJPEG := generateTestJPEG(color.RGBA{R: 0, G: 0, B: 255, A: 255})

	// 1. Upload first file named "sunset.jpg"
	body1 := &bytes.Buffer{}
	mp1 := multipart.NewWriter(body1)
	part1, _ := mp1.CreateFormFile("file", "sunset.jpg")
	_, _ = io.Copy(part1, bytes.NewReader(redJPEG))
	_ = mp1.Close()

	req1 := httptest.NewRequest("POST", "/v1/upload?path=gallery&thumbnail=true", body1)
	req1.Header.Set("Content-Type", mp1.FormDataContentType())
	rec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("upload 1 failed: status %d", rec1.Code)
	}
	var resp1 UploadResponse
	_ = json.NewDecoder(rec1.Body).Decode(&resp1)

	// 2. Upload second file named "sunset.jpg" (same original name, different content)
	body2 := &bytes.Buffer{}
	mp2 := multipart.NewWriter(body2)
	part2, _ := mp2.CreateFormFile("file", "sunset.jpg")
	_, _ = io.Copy(part2, bytes.NewReader(blueJPEG))
	_ = mp2.Close()

	req2 := httptest.NewRequest("POST", "/v1/upload?path=gallery&thumbnail=true", body2)
	req2.Header.Set("Content-Type", mp2.FormDataContentType())
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusCreated {
		t.Fatalf("upload 2 failed: status %d", rec2.Code)
	}
	var resp2 UploadResponse
	_ = json.NewDecoder(rec2.Body).Decode(&resp2)

	// Verify both uploads preserved the original filename
	if resp1.OriginalFilename != "sunset.jpg" {
		t.Fatalf("expected resp1 original filename 'sunset.jpg', got %q", resp1.OriginalFilename)
	}
	if resp2.OriginalFilename != "sunset.jpg" {
		t.Fatalf("expected resp2 original filename 'sunset.jpg', got %q", resp2.OriginalFilename)
	}

	// Verify unique IDs and non-colliding paths
	if resp1.ID == resp2.ID {
		t.Fatalf("expected distinct IDs, got identical %q", resp1.ID)
	}
	if resp1.Path == resp2.Path {
		t.Fatalf("collision detected: both files have path %q", resp1.Path)
	}
	if resp1.Filename == resp2.Filename {
		t.Fatalf("collision detected: both files have filename %q", resp1.Filename)
	}

	// Verify thumbnail paths did NOT overwrite
	if resp1.Thumbnail == nil || resp2.Thumbnail == nil {
		t.Fatal("expected both thumbnails to be generated")
	}
	if resp1.Thumbnail.Path == resp2.Thumbnail.Path {
		t.Fatalf("collision detected: both thumbnails have path %q", resp1.Thumbnail.Path)
	}

	// Verify both original files are downloadable and distinct
	get1 := httptest.NewRequest("GET", "/v1/download/"+resp1.Path, nil)
	recGet1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recGet1, get1)
	if recGet1.Code != http.StatusOK {
		t.Fatalf("failed to download file 1: %d", recGet1.Code)
	}

	get2 := httptest.NewRequest("GET", "/v1/download/"+resp2.Path, nil)
	recGet2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recGet2, get2)
	if recGet2.Code != http.StatusOK {
		t.Fatalf("failed to download file 2: %d", recGet2.Code)
	}

	// Verify both thumbnails are downloadable
	getThumb1 := httptest.NewRequest("GET", "/v1/download/"+resp1.Thumbnail.Path, nil)
	recGetThumb1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recGetThumb1, getThumb1)
	if recGetThumb1.Code != http.StatusOK {
		t.Fatalf("failed to download thumbnail 1: %d", recGetThumb1.Code)
	}

	getThumb2 := httptest.NewRequest("GET", "/v1/download/"+resp2.Thumbnail.Path, nil)
	recGetThumb2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recGetThumb2, getThumb2)
	if recGetThumb2.Code != http.StatusOK {
		t.Fatalf("failed to download thumbnail 2: %d", recGetThumb2.Code)
	}

	// Verify database records contain original_filename
	file1Rec, err := dbRepo.GetFileByID(context.Background(), resp1.ID)
	if err != nil {
		t.Fatalf("failed to get file 1 record from DB: %v", err)
	}
	if file1Rec.OriginalFilename != "sunset.jpg" {
		t.Fatalf("expected DB record 1 original_filename 'sunset.jpg', got %q", file1Rec.OriginalFilename)
	}
	if file1Rec.Filename != resp1.Filename {
		t.Fatalf("expected DB record 1 filename %q, got %q", resp1.Filename, file1Rec.Filename)
	}

	file2Rec, err := dbRepo.GetFileByID(context.Background(), resp2.ID)
	if err != nil {
		t.Fatalf("failed to get file 2 record from DB: %v", err)
	}
	if file2Rec.OriginalFilename != "sunset.jpg" {
		t.Fatalf("expected DB record 2 original_filename 'sunset.jpg', got %q", file2Rec.OriginalFilename)
	}
	if file2Rec.Filename != resp2.Filename {
		t.Fatalf("expected DB record 2 filename %q, got %q", resp2.Filename, file2Rec.Filename)
	}
}

