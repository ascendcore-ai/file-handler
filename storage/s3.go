package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	Register("s3", func(cfg Config) (Provider, error) {
		return NewS3Storage(cfg)
	})
}

// S3Storage provides a zero-dependency, streaming S3 and S3-compatible storage backend
// (AWS S3, MinIO, Cloudflare R2, Wasabi, DigitalOcean Spaces, etc.).
type S3Storage struct {
	bucket         string
	region         string
	endpoint       string
	accessKey      string
	secretKey      string
	forcePathStyle bool
	prefix         string
	httpClient     *http.Client
}

// NewS3Storage initializes an S3Storage instance.
func NewS3Storage(cfg Config) (*S3Storage, error) {
	if strings.TrimSpace(cfg.S3Bucket) == "" {
		return nil, errors.New("S3 bucket name is required")
	}

	region := cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	endpoint := strings.TrimRight(strings.TrimSpace(cfg.S3Endpoint), "/")
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
	}

	cleanPrefix := ""
	if strings.TrimSpace(cfg.StoragePrefix) != "" {
		if p, err := SanitizeRelativePath(cfg.StoragePrefix); err == nil {
			cleanPrefix = p
		}
	}

	// Optimized transport for high concurrency connection reuse
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &S3Storage{
		bucket:         cfg.S3Bucket,
		region:         region,
		endpoint:       endpoint,
		accessKey:      cfg.S3AccessKey,
		secretKey:      cfg.S3SecretKey,
		forcePathStyle: cfg.S3ForcePathStyle || strings.Contains(endpoint, "localhost") || strings.Contains(endpoint, "127.0.0.1"),
		prefix:         cleanPrefix,
		httpClient:     &http.Client{Transport: transport},
	}, nil
}

// Name returns the provider identifier.
func (s *S3Storage) Name() string {
	return "s3"
}

// objectURL constructs the S3 URL for a given object key.
func (s *S3Storage) objectURL(key string) string {
	cleanKey := strings.TrimPrefix(key, "/")
	if s.forcePathStyle {
		return fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, cleanKey)
	}

	// Virtual-hosted style
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, cleanKey)
	}
	return fmt.Sprintf("%s://%s.%s/%s", u.Scheme, s.bucket, u.Host, cleanKey)
}

// resolveKey computes the final S3 key for a target path.
func (s *S3Storage) resolveKey(targetPath string) (string, error) {
	cleanRel, err := SanitizeRelativePath(targetPath)
	if err != nil {
		return "", err
	}
	if s.prefix != "" {
		cleanRel = path.Join(s.prefix, cleanRel)
	}
	return cleanRel, nil
}

// Save streams data directly from src into S3 using an HTTP PUT request.
// Adheres strictly to Zero Memory Buffering via UNSIGNED-PAYLOAD SigV4 streaming.
func (s *S3Storage) Save(ctx context.Context, targetPath string, filename string, src io.Reader) (int64, error) {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return 0, err
	}

	reqURL := s.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, src)
	if err != nil {
		return 0, fmt.Errorf("failed to create S3 request: %w", err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)

	if filename != "" {
		req.Header.Set("x-amz-meta-original-filename", filename)
	}

	// Sign with AWS SigV4 using UNSIGNED-PAYLOAD for streaming uploads without memory buffering
	if s.accessKey != "" && s.secretKey != "" {
		s.signV4(req, "UNSIGNED-PAYLOAD")
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("S3 upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("S3 upload returned status %d", resp.StatusCode)
	}

	// In chunked/streamed PUTs without known size, return 0 if unstated, or read content length
	return resp.ContentLength, nil
}

// ServeDownload streams the object from S3 directly to http.ResponseWriter.
// Constraint 3: For remote streams, pipe directly to http.ResponseWriter using io.Copy/StreamCopy.
func (s *S3Storage) ServeDownload(w http.ResponseWriter, r *http.Request, targetPath string) error {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return err
	}

	reqURL := s.objectURL(key)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create S3 GET request: %w", err)
	}

	// Forward Range header if client requested partial content
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}

	if s.accessKey != "" && s.secretKey != "" {
		s.signV4(req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") // empty body hash
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("S3 GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("S3 GET returned status %d", resp.StatusCode)
	}

	// Forward metadata headers
	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified"} {
		if val := resp.Header.Get(h); val != "" {
			w.Header().Set(h, val)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Constraint 3 & 4: Pipe directly to ResponseWriter using pooled 32KB buffer
	_, err = pool.StreamCopy(w, resp.Body)
	return err
}

// Open opens a stored object in S3 for streaming read operations.
func (s *S3Storage) Open(ctx context.Context, targetPath string) (io.ReadCloser, error) {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return nil, err
	}

	reqURL := s.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 GET request: %w", err)
	}

	if s.accessKey != "" && s.secretKey != "" {
		s.signV4(req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("S3 GET request failed: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("S3 GET returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// Delete removes an object from S3.
func (s *S3Storage) Delete(ctx context.Context, targetPath string) error {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return err
	}

	reqURL := s.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}

	if s.accessKey != "" && s.secretKey != "" {
		s.signV4(req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("S3 DELETE returned status %d", resp.StatusCode)
	}
	return nil
}

// PresignUpload generates an AWS SigV4 pre-signed URL for direct browser PUT uploads.
func (s *S3Storage) PresignUpload(ctx context.Context, targetPath string, filename string, contentType string, expirySeconds int) (*PresignedUpload, error) {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return nil, err
	}

	if expirySeconds <= 0 {
		expirySeconds = 900 // 15 minutes default
	}

	rawURL := s.objectURL(key)
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3 URL: %w", err)
	}

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)
	credential := fmt.Sprintf("%s/%s", s.accessKey, credentialScope)

	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", credential)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", expirySeconds))
	q.Set("X-Amz-SignedHeaders", "host")

	// Canonical query string must be sorted alphabetically by key
	canonicalQuery := q.Encode()

	canonicalHeaders := fmt.Sprintf("host:%s\n", u.Host)
	signedHeaders := "host"
	canonicalURI := u.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalRequest := fmt.Sprintf("PUT\n%s\n%s\n%s\n%s\n%s",
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	canonHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		canonHash,
	)

	signingKey := getSignatureKey(s.secretKey, dateStamp, s.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	q.Set("X-Amz-Signature", signature)
	u.RawQuery = q.Encode()

	headers := make(map[string]string)
	if contentType != "" {
		headers["Content-Type"] = contentType
	}

	return &PresignedUpload{
		Method:    http.MethodPut,
		URL:       u.String(),
		Path:      targetPath,
		Headers:   headers,
		ExpiresIn: expirySeconds,
		IsDirect:  true,
	}, nil
}

// PresignDownload generates an AWS SigV4 pre-signed URL for direct browser GET downloads.
func (s *S3Storage) PresignDownload(ctx context.Context, targetPath string, expirySeconds int) (*PresignedDownload, error) {
	key, err := s.resolveKey(targetPath)
	if err != nil {
		return nil, err
	}

	if expirySeconds <= 0 {
		expirySeconds = 900 // 15 minutes default
	}

	rawURL := s.objectURL(key)
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3 URL: %w", err)
	}

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)
	credential := fmt.Sprintf("%s/%s", s.accessKey, credentialScope)

	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", credential)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", expirySeconds))
	q.Set("X-Amz-SignedHeaders", "host")

	canonicalQuery := q.Encode()

	canonicalHeaders := fmt.Sprintf("host:%s\n", u.Host)
	signedHeaders := "host"
	canonicalURI := u.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\n%s\n%s\n%s",
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	canonHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		canonHash,
	)

	signingKey := getSignatureKey(s.secretKey, dateStamp, s.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	q.Set("X-Amz-Signature", signature)
	u.RawQuery = q.Encode()

	return &PresignedDownload{
		Method:    http.MethodGet,
		URL:       u.String(),
		Path:      targetPath,
		ExpiresIn: expirySeconds,
		IsDirect:  true,
	}, nil
}

// signV4 signs an HTTP request with AWS Signature Version 4.
func (s *S3Storage) signV4(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("host", req.URL.Host)

	// Canonical headers
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)
	canonHash := sha256Hex([]byte(canonicalRequest))

	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		canonHash,
	)

	signingKey := getSignatureKey(s.secretKey, dateStamp, s.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func getSignatureKey(key, dateStamp, regionName, serviceName string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+key), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(regionName))
	kService := hmacSHA256(kRegion, []byte(serviceName))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}
