package main

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"file-handler/pkg/logger"
	"file-handler/pkg/metadata"
	"file-handler/pkg/thumbnail"
	"file-handler/storage"
)

//go:embed docs/openapi.yaml
var openAPISpecYAML []byte

// Server encapsulates the HTTP multiplexer, pluggable storage configuration,
// and optional PostgreSQL metadata repository.
type Server struct {
	Storage  storage.Provider
	Metadata metadata.Repository
	Logger   *slog.Logger
	mux      *http.ServeMux
}

// FileInfoResult represents metadata for an uploaded file.
type FileInfoResult struct {
	ID               string            `json:"id"`
	Path             string            `json:"path"`
	Filename         string            `json:"filename"`
	OriginalFilename string            `json:"original_filename"`
	Bytes            int64             `json:"bytes"`
	MimeType         string            `json:"mime_type,omitempty"`
	Collection       string            `json:"collection,omitempty"`
	UserID           string            `json:"user_id,omitempty"`
	Description      string            `json:"description,omitempty"`
	Metadata         metadata.JSONMap  `json:"metadata,omitempty"`
	Thumbnail        *thumbnail.Result `json:"thumbnail,omitempty"`
}

// UploadResponse represents the JSON response returned after a successful upload.
type UploadResponse struct {
	ID               string            `json:"id"`
	Path             string            `json:"path"`
	Filename         string            `json:"filename"`
	OriginalFilename string            `json:"original_filename"`
	Bytes            int64             `json:"bytes"`
	MimeType         string            `json:"mime_type,omitempty"`
	Collection       string            `json:"collection,omitempty"`
	UserID           string            `json:"user_id,omitempty"`
	Description      string            `json:"description,omitempty"`
	Metadata         metadata.JSONMap  `json:"metadata,omitempty"`
	Thumbnail        *thumbnail.Result `json:"thumbnail,omitempty"`
	Files            []FileInfoResult  `json:"files,omitempty"`
}

// PresignRequest represents the parameters when requesting a pre-signed upload URL.
type PresignRequest struct {
	Filename    string `json:"filename"`
	Path        string `json:"path"`
	ContentType string `json:"content_type"`
	ExpiresIn   int    `json:"expires_in"` // expiration in seconds
}

// PresignDownloadRequest represents the parameters when requesting a pre-signed download URL.
type PresignDownloadRequest struct {
	FileID    string `json:"file_id,omitempty"`
	ID        string `json:"id,omitempty"`
	Path      string `json:"path,omitempty"`
	ExpiresIn int    `json:"expires_in"` // expiration in seconds
}

// UploadCompleteRequest represents the payload when a client confirms completion of direct upload.
type UploadCompleteRequest struct {
	Path             string           `json:"path"`
	Filename         string           `json:"filename"`
	OriginalFilename string           `json:"original_filename,omitempty"`
	Size             int64            `json:"size"`
	ETag             string           `json:"etag,omitempty"`
	MimeType         string           `json:"mime_type,omitempty"`
	Collection       string           `json:"collection,omitempty"`
	UserID           string           `json:"user_id,omitempty"`
	Description      string           `json:"description,omitempty"`
	Metadata         metadata.JSONMap `json:"metadata,omitempty"`
}

// NewServer initializes a Server using local filesystem storage at uploadDir.
// Maintained for backward compatibility.
func NewServer(uploadDir string) (*Server, error) {
	provider, err := storage.NewLocalStorage(uploadDir, "")
	if err != nil {
		return nil, err
	}
	return NewServerWithStorage(provider), nil
}

// NewServerWithStorage initializes a Server with any configured storage.Provider and nil metadata repository.
func NewServerWithStorage(provider storage.Provider) *Server {
	return NewServerWithStorageAndMetadata(provider, nil)
}

// NewServerWithAll initializes a Server with a storage.Provider, optional metadata.Repository, and structured logger.
func NewServerWithAll(provider storage.Provider, metaRepo metadata.Repository, appLogger *slog.Logger) *Server {
	if appLogger == nil {
		appLogger = slog.Default()
	}
	s := &Server{
		Storage:  provider,
		Metadata: metaRepo,
		Logger:   appLogger,
		mux:      http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// NewServerWithStorageAndMetadata initializes a Server with a storage.Provider and an optional metadata.Repository.
func NewServerWithStorageAndMetadata(provider storage.Provider, metaRepo metadata.Repository) *Server {
	return NewServerWithAll(provider, metaRepo, slog.Default())
}

// Handler returns the configured http.Handler for the server wrapped with structured logging middleware.
func (s *Server) Handler() http.Handler {
	return logger.HTTPMiddleware(s.Logger)(s.mux)
}

// registerRoutes sets up the endpoint handlers using Go 1.22+ method-based route patterns.
// Supports versioned (/v1/...) canonical routes and unversioned aliases for backward compatibility.
func (s *Server) registerRoutes() {
	// Canonical Version 1 (v1) routes
	s.mux.HandleFunc("POST /v1/upload", s.handleUpload)
	s.mux.HandleFunc("GET /v1/download/{path...}", s.handleDownload)
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("POST /v1/upload/presign", s.handlePresign)
	s.mux.HandleFunc("POST /v1/download/presign", s.handlePresignDownload)
	s.mux.HandleFunc("GET /v1/download/presign", s.handlePresignDownload)
	s.mux.HandleFunc("GET /v1/download/presign/{file_id...}", s.handlePresignDownload)
	s.mux.HandleFunc("POST /v1/upload/complete", s.handleUploadComplete)
	s.mux.HandleFunc("POST /v1/webhooks/storage-events", s.handleStorageWebhook)

	// Metadata Query & Management routes
	s.mux.HandleFunc("GET /v1/files", s.handleListFiles)
	s.mux.HandleFunc("GET /v1/files/{id...}", s.handleGetFileMetadata)
	s.mux.HandleFunc("DELETE /v1/files/{id...}", s.handleDeleteFileMetadata)

	// Unversioned aliases for backward compatibility
	s.mux.HandleFunc("POST /upload", s.handleUpload)
	s.mux.HandleFunc("GET /download/{path...}", s.handleDownload)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /upload/presign", s.handlePresign)
	s.mux.HandleFunc("POST /download/presign", s.handlePresignDownload)
	s.mux.HandleFunc("GET /download/presign", s.handlePresignDownload)
	s.mux.HandleFunc("GET /download/presign/{file_id...}", s.handlePresignDownload)
	s.mux.HandleFunc("POST /upload/complete", s.handleUploadComplete)
	s.mux.HandleFunc("POST /webhooks/storage-events", s.handleStorageWebhook)
	s.mux.HandleFunc("GET /files", s.handleListFiles)
	s.mux.HandleFunc("GET /files/{id...}", s.handleGetFileMetadata)
	s.mux.HandleFunc("DELETE /files/{id...}", s.handleDeleteFileMetadata)

	// API Documentation & Specification
	s.mux.HandleFunc("GET /v1/docs", s.handleDocsScalar)
	s.mux.HandleFunc("GET /v1/docs/swagger", s.handleDocsSwagger)
	s.mux.HandleFunc("GET /docs", s.handleDocsScalar)
	s.mux.HandleFunc("GET /docs/swagger", s.handleDocsSwagger)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPISpec)
	s.mux.HandleFunc("GET /v1/openapi.yaml", s.handleOpenAPISpec)
}

// handleOpenAPISpec returns the embedded OpenAPI 3.1 YAML definition.
func (s *Server) handleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPISpecYAML)
}

// handleDocsScalar serves the interactive Scalar API documentation UI.
func (s *Server) handleDocsScalar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	html := `<!doctype html>
<html lang="en">
  <head>
    <title>File Handler API Documentation</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>">
    <style>
      body { margin: 0; padding: 0; }
    </style>
  </head>
  <body>
    <script
      id="api-reference"
      data-url="/openapi.yaml"
      data-configuration='{"theme":"purple","layout":"modern","showSidebar":true}'></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>`
	_, _ = w.Write([]byte(html))
}

// handleDocsSwagger serves the interactive Swagger UI.
func (s *Server) handleDocsSwagger(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Swagger UI - File Handler API</title>
  <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📄</text></svg>">
  <style>
    html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
    *, *:before, *:after { box-sizing: inherit; }
    body { margin: 0; background: #fafafa; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>`
	_, _ = w.Write([]byte(html))
}

// handleHealth responds with a JSON health check, API version info, and database status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	dbState := "disabled"
	dbConn := "not_configured"
	if s.Metadata != nil {
		dbState = "enabled"
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Metadata.Ping(ctx); err != nil {
			dbConn = "error: " + err.Error()
		} else {
			dbConn = "connected"
		}
	}
	_, _ = w.Write([]byte(`{"status":"ok","version":"v1","storage":"` + s.Storage.Name() + `","database":"` + dbState + `","database_connection":"` + dbConn + `"}`))
}

// handleListFiles handles GET /v1/files.
// Returns a paginated list of file records filtered by collection, MIME type, user ID, or keyword search.
func (s *Server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	if s.Metadata == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "metadata database is disabled",
		})
		return
	}

	filter := metadata.QueryFilter{
		Collection: r.URL.Query().Get("collection"),
		MimeType:   r.URL.Query().Get("mime_type"),
		UserID:     r.URL.Query().Get("user_id"),
		Search:     r.URL.Query().Get("search"),
		Sort:       r.URL.Query().Get("sort"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			filter.Limit = limit
		}
	}
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil {
			filter.Offset = offset
		}
	}

	result, err := s.Metadata.ListFiles(r.Context(), filter)
	if err != nil {
		http.Error(w, "failed to query files: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

// handleGetFileMetadata handles GET /v1/files/{id...}.
// Returns the metadata and thumbnail details for a single file by ID or path.
func (s *Server) handleGetFileMetadata(w http.ResponseWriter, r *http.Request) {
	if s.Metadata == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "metadata database is disabled",
		})
		return
	}

	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing file identifier", http.StatusBadRequest)
		return
	}

	record, err := s.Metadata.GetFileByID(r.Context(), id)
	if err != nil {
		// Fallback to query by path
		record, err = s.Metadata.GetFileByPath(r.Context(), id)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "file metadata not found",
			})
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(record)
}

// handleDeleteFileMetadata handles DELETE /v1/files/{id...}.
// Deletes a file's metadata record and cascades to linked thumbnails.
func (s *Server) handleDeleteFileMetadata(w http.ResponseWriter, r *http.Request) {
	if s.Metadata == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "metadata database is disabled",
		})
		return
	}

	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing file identifier", http.StatusBadRequest)
		return
	}

	if err := s.Metadata.DeleteFile(r.Context(), id); err != nil {
		http.Error(w, "failed to delete file metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "deleted",
		"id":     id,
	})
}

// handlePresign handles POST /v1/upload/presign.
// Generates a pre-signed URL for direct browser-to-cloud upload.
// If the backend does not support pre-signing (e.g. local storage), returns
// a standard endpoint pointing to the streaming /v1/upload API.
func (s *Server) handlePresign(w http.ResponseWriter, r *http.Request) {
	var req PresignRequest

	// Support JSON body or query parameters
	if r.Header.Get("Content-Type") == "application/json" {
		_ = json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req)
	}

	if req.Filename == "" {
		req.Filename = r.URL.Query().Get("filename")
	}
	if req.Path == "" {
		req.Path = r.URL.Query().Get("path")
	}
	if req.ContentType == "" {
		req.ContentType = r.URL.Query().Get("content_type")
	}
	if req.ExpiresIn <= 0 {
		if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
			req.ExpiresIn, _ = strconv.Atoi(expStr)
		}
	}

	// Default filename / short ID if empty
	if req.Filename == "" {
		shortID, err := generateShortID(defaultShortIDLength)
		if err != nil {
			http.Error(w, "failed to generate unique identifier: "+err.Error(), http.StatusInternalServerError)
			return
		}
		req.Filename = shortID
	}

	cleanFilename, err := storage.SanitizePathSegment(req.Filename)
	if err != nil {
		http.Error(w, "invalid filename: "+err.Error(), http.StatusBadRequest)
		return
	}

	shortID, err := generateShortID(defaultShortIDLength)
	if err != nil {
		http.Error(w, "failed to generate unique identifier: "+err.Error(), http.StatusInternalServerError)
		return
	}
	storedFilename := fmt.Sprintf("%s_%s", shortID, cleanFilename)

	targetPath := storedFilename
	if req.Path != "" {
		cleanSub, err := storage.SanitizeRelativePath(req.Path)
		if err != nil {
			http.Error(w, "invalid path: "+err.Error(), http.StatusBadRequest)
			return
		}
		targetPath = path.Join(cleanSub, cleanFilename)
	}

	// Check if the current storage provider implements Presigner
	if presigner, ok := s.Storage.(storage.Presigner); ok {
		presigned, err := presigner.PresignUpload(r.Context(), targetPath, cleanFilename, req.ContentType, req.ExpiresIn)
		if err != nil {
			http.Error(w, "failed to generate presigned upload url: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(presigned)
		return
	}

	// Fallback for providers that don't support pre-signing (e.g. local disk)
	fallback := storage.PresignedUpload{
		Method:    http.MethodPost,
		URL:       "/v1/upload?path=" + req.Path,
		Path:      targetPath,
		ExpiresIn: 0,
		IsDirect:  false,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(fallback)
}

// handlePresignDownload handles POST & GET /v1/download/presign and /v1/download/presign/{file_id...}.
// Takes a file_id (or storage path) and generates a pre-signed URL for direct client-to-cloud download.
// If the backend does not support pre-signing (e.g. local storage), returns
// a fallback endpoint pointing to the streaming /v1/download/{path} API.
func (s *Server) handlePresignDownload(w http.ResponseWriter, r *http.Request) {
	var req PresignDownloadRequest

	if r.Method == http.MethodPost && strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		_ = json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req)
	}

	// 1. Resolve file_id from route path, query params, or JSON body
	fileID := r.PathValue("file_id")
	if fileID == "" {
		fileID = r.PathValue("id")
	}
	if fileID == "" {
		fileID = req.FileID
	}
	if fileID == "" {
		fileID = req.ID
	}
	if fileID == "" {
		fileID = r.URL.Query().Get("file_id")
	}
	if fileID == "" {
		fileID = r.URL.Query().Get("fileId")
	}
	if fileID == "" {
		fileID = r.URL.Query().Get("id")
	}

	// 2. Parse expiration
	if req.ExpiresIn <= 0 {
		if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
			req.ExpiresIn, _ = strconv.Atoi(expStr)
		}
	}
	if req.ExpiresIn <= 0 {
		req.ExpiresIn = 900 // 15 minutes default
	}

	// 3. Resolve target storage path
	var resolvedPath string

	if fileID != "" {
		if s.Metadata != nil {
			record, err := s.Metadata.GetFileByID(r.Context(), fileID)
			if err != nil {
				// Fallback: try lookup by stored path or filename
				record, err = s.Metadata.GetFileByPath(r.Context(), fileID)
			}
			if err != nil || record == nil {
				http.Error(w, fmt.Sprintf("file not found in metadata database for ID %q", fileID), http.StatusNotFound)
				return
			}
			resolvedPath = record.Path
		} else {
			// Without database, use fileID directly as path
			resolvedPath = fileID
		}
	} else if req.Path != "" || r.URL.Query().Get("path") != "" {
		if req.Path != "" {
			resolvedPath = req.Path
		} else {
			resolvedPath = r.URL.Query().Get("path")
		}
	} else {
		http.Error(w, "missing required 'file_id' parameter", http.StatusBadRequest)
		return
	}

	cleanPath, err := storage.SanitizeRelativePath(resolvedPath)
	if err != nil {
		http.Error(w, "invalid file path: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check if current storage provider implements DownloadPresigner or Presigner
	if presigner, ok := s.Storage.(storage.DownloadPresigner); ok {
		presigned, err := presigner.PresignDownload(r.Context(), cleanPath, req.ExpiresIn)
		if err != nil {
			http.Error(w, "failed to generate presigned download url: "+err.Error(), http.StatusInternalServerError)
			return
		}
		presigned.FileID = fileID

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(presigned)
		return
	}

	// Fallback for providers that don't support pre-signing (e.g. local disk)
	fallback := storage.PresignedDownload{
		Method:    http.MethodGet,
		URL:       "/v1/download/" + cleanPath,
		FileID:    fileID,
		Path:      cleanPath,
		ExpiresIn: 0,
		IsDirect:  false,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(fallback)
}

// handleUploadComplete handles POST /v1/upload/complete.
// Confirms that a direct client-to-storage upload has finished.
func (s *Server) handleUploadComplete(w http.ResponseWriter, r *http.Request) {
	var req UploadCompleteRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid request payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	cleanPath, err := storage.SanitizeRelativePath(req.Path)
	if err != nil {
		http.Error(w, "invalid path: "+err.Error(), http.StatusBadRequest)
		return
	}

	if s.Metadata != nil {
		id := req.Filename
		if id == "" {
			id = path.Base(cleanPath)
		}
		origName := req.OriginalFilename
		if origName == "" {
			base := path.Base(cleanPath)
			if parts := strings.SplitN(base, "_", 2); len(parts) == 2 && len(parts[0]) >= 6 {
				origName = parts[1]
			} else {
				origName = base
			}
		}
		mimeType := req.MimeType
		if mimeType == "" {
			if ext := path.Ext(cleanPath); ext != "" {
				mimeType = mime.TypeByExtension(ext)
			}
		}
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		fileRecord := &metadata.FileRecord{
			ID:               id,
			Filename:         req.Filename,
			OriginalFilename: origName,
			Path:             cleanPath,
			StorageType:      s.Storage.Name(),
			MimeType:         mimeType,
			Bytes:            req.Size,
			Collection:       req.Collection,
			UserID:           req.UserID,
			Description:      req.Description,
			Metadata:         req.Metadata,
		}
		if err := s.Metadata.SaveFile(r.Context(), fileRecord); err != nil {
			s.Logger.ErrorContext(r.Context(), "failed to save file metadata on upload complete",
				slog.String("file_id", fileRecord.ID),
				slog.String("path", fileRecord.Path),
				slog.String("error", err.Error()),
			)
		} else {
			s.Logger.InfoContext(r.Context(), "stored file metadata on upload complete",
				slog.String("file_id", fileRecord.ID),
				slog.String("path", fileRecord.Path),
			)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      "recorded",
		"path":        cleanPath,
		"filename":    req.Filename,
		"size":        req.Size,
		"etag":        req.ETag,
		"collection":  req.Collection,
		"user_id":     req.UserID,
	})
}

// handleStorageWebhook handles POST /v1/webhooks/storage-events.
// Acknowledges asynchronous cloud notifications (e.g. MinIO, S3 SNS, GCS Pub/Sub, Azure Event Grid).
func (s *Server) handleStorageWebhook(w http.ResponseWriter, r *http.Request) {
	var payload map[string]any
	_ = json.NewDecoder(io.LimitReader(r.Body, 128*1024)).Decode(&payload)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"acknowledged"}`))
}

// handleUpload streams files from incoming multipart/form-data directly to storage
// without loading whole files into memory, adhering strictly to streaming constraints.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Constraint 2: Strictly use r.MultipartReader() and avoid buffering parsers.
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "request must be multipart/form-data: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Extract target path from query parameter (?path=...) or request header
	targetSubpath := r.URL.Query().Get("path")
	if targetSubpath == "" {
		targetSubpath = r.Header.Get("X-Upload-Path")
	}

	// Extract thumbnail generation options
	genThumbStr := r.URL.Query().Get("thumbnail")
	if genThumbStr == "" {
		genThumbStr = r.URL.Query().Get("thumb")
	}
	if genThumbStr == "" {
		genThumbStr = r.Header.Get("X-Generate-Thumbnail")
	}
	generateThumbnail := false
	if genThumbStr != "" {
		generateThumbnail, _ = strconv.ParseBool(genThumbStr)
	}

	thumbDir := r.URL.Query().Get("thumb_dir")
	if thumbDir == "" {
		thumbDir = r.Header.Get("X-Thumbnail-Dir")
	}
	if thumbDir == "" {
		thumbDir = thumbnail.DefaultThumbnailFolder
	}

	thumbSize := thumbnail.DefaultMaxDimension
	if sizeStr := r.URL.Query().Get("thumb_size"); sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
			thumbSize = s
		}
	} else if sizeStr := r.Header.Get("X-Thumbnail-Size"); sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
			thumbSize = s
		}
	}

	// Extract custom file ID from query parameter (?id=...)
	customID := r.URL.Query().Get("id")

	// Extract metadata options
	collection := r.URL.Query().Get("collection")
	if collection == "" {
		collection = r.Header.Get("X-Collection")
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("X-User-ID")
	}
	description := r.URL.Query().Get("description")
	customMetaStr := r.URL.Query().Get("metadata")

	var results []FileInfoResult

	// Iterate through multipart parts using mr.NextPart()
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			http.Error(w, "failed reading multipart part: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Handle non-file form fields (e.g. form fields "path", "dir", "id", "thumbnail", "thumb_dir", "thumb_size", "collection", "user_id", "description", "metadata")
		if part.FileName() == "" {
			formField := strings.ToLower(part.FormName())
			if (formField == "path" || formField == "dir") && targetSubpath == "" {
				var pathBuf [512]byte
				n, _ := io.ReadFull(part, pathBuf[:])
				candidate := strings.TrimSpace(string(pathBuf[:n]))
				if candidate != "" {
					targetSubpath = candidate
				}
			} else if formField == "id" && customID == "" {
				var idBuf [256]byte
				n, _ := io.ReadFull(part, idBuf[:])
				candidate := strings.TrimSpace(string(idBuf[:n]))
				if candidate != "" {
					customID = candidate
				}
			} else if (formField == "thumbnail" || formField == "thumb") && !generateThumbnail {
				var boolBuf [16]byte
				n, _ := io.ReadFull(part, boolBuf[:])
				generateThumbnail, _ = strconv.ParseBool(strings.TrimSpace(string(boolBuf[:n])))
			} else if formField == "thumb_dir" && thumbDir == thumbnail.DefaultThumbnailFolder {
				var dirBuf [128]byte
				n, _ := io.ReadFull(part, dirBuf[:])
				candidate := strings.TrimSpace(string(dirBuf[:n]))
				if candidate != "" {
					thumbDir = candidate
				}
			} else if formField == "thumb_size" && thumbSize == thumbnail.DefaultMaxDimension {
				var sizeBuf [16]byte
				n, _ := io.ReadFull(part, sizeBuf[:])
				if s, err := strconv.Atoi(strings.TrimSpace(string(sizeBuf[:n]))); err == nil && s > 0 {
					thumbSize = s
				}
			} else if (formField == "collection" || formField == "col") && collection == "" {
				var collBuf [256]byte
				n, _ := io.ReadFull(part, collBuf[:])
				candidate := strings.TrimSpace(string(collBuf[:n]))
				if candidate != "" {
					collection = candidate
				}
			} else if (formField == "user_id" || formField == "userid") && userID == "" {
				var userBuf [256]byte
				n, _ := io.ReadFull(part, userBuf[:])
				candidate := strings.TrimSpace(string(userBuf[:n]))
				if candidate != "" {
					userID = candidate
				}
			} else if formField == "description" && description == "" {
				var descBuf [1024]byte
				n, _ := io.ReadFull(part, descBuf[:])
				candidate := strings.TrimSpace(string(descBuf[:n]))
				if candidate != "" {
					description = candidate
				}
			} else if formField == "metadata" && customMetaStr == "" {
				var metaBuf [4096]byte
				n, _ := io.ReadFull(part, metaBuf[:])
				candidate := strings.TrimSpace(string(metaBuf[:n]))
				if candidate != "" {
					customMetaStr = candidate
				}
			}
			part.Close()
			continue
		}

		// Determine the unique short identifier (Base62)
		fileID := customID
		if fileID == "" {
			generated, err := generateShortID(defaultShortIDLength)
			if err != nil {
				part.Close()
				http.Error(w, "failed to generate unique identifier: "+err.Error(), http.StatusInternalServerError)
				return
			}
			fileID = generated
		}

		rawName := part.FileName()
		if rawName == "" {
			if customID != "" {
				rawName = customID
			} else {
				rawName = "file"
			}
		}

		cleanOriginal, _ := storage.SanitizePathSegment(rawName)
		if cleanOriginal == "" {
			cleanOriginal = "file"
		}

		// Attach short ID in front of the filename to avoid collisions/overrides
		storedFilename := fmt.Sprintf("%s_%s", fileID, cleanOriginal)

		// Determine MIME type
		contentType := part.Header.Get("Content-Type")
		if contentType == "" || contentType == "application/octet-stream" {
			if ext := path.Ext(cleanOriginal); ext != "" {
				if detected := mime.TypeByExtension(ext); detected != "" {
					contentType = detected
				}
			}
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		// Compose target path combining subpath (if any) and storedFilename
		finalRelPath := storedFilename
		if targetSubpath != "" {
			cleanSub, err := storage.SanitizeRelativePath(targetSubpath)
			if err != nil {
				part.Close()
				http.Error(w, "invalid destination path: "+err.Error(), http.StatusBadRequest)
				return
			}
			finalRelPath = path.Join(cleanSub, storedFilename)
		}

		// Stream directly into the storage backend using the 32KB sync.Pool buffer
		written, saveErr := s.Storage.Save(r.Context(), finalRelPath, cleanOriginal, part)
		part.Close()

		if saveErr != nil {
			http.Error(w, "streaming upload failed: "+saveErr.Error(), http.StatusInternalServerError)
			return
		}

		// Generate thumbnail if requested and file is a supported image
		var thumbResult *thumbnail.Result
		if generateThumbnail && thumbnail.IsSupportedImage(cleanOriginal) {
			thumbPath := thumbnail.ComputeThumbnailPath(finalRelPath, thumbDir)

			fileReader, openErr := s.Storage.Open(r.Context(), finalRelPath)
			if openErr == nil {
				thumbStream, w, h, format, genErr := thumbnail.Generate(r.Context(), fileReader, thumbSize)
				_ = fileReader.Close()
				if genErr == nil {
					thumbBytes, saveThumbErr := s.Storage.Save(r.Context(), thumbPath, storedFilename, thumbStream)
					if saveThumbErr == nil {
						thumbResult = &thumbnail.Result{
							Path:     thumbPath,
							Filename: storedFilename,
							Bytes:    thumbBytes,
							Width:    w,
							Height:   h,
							Format:   format,
						}
					}
				}
			}
		}

		var customMeta metadata.JSONMap
		if customMetaStr != "" {
			_ = json.Unmarshal([]byte(customMetaStr), &customMeta)
		}

		// Persist metadata to database if enabled
		if s.Metadata != nil {
			fileRecord := &metadata.FileRecord{
				ID:               fileID,
				Filename:         storedFilename,
				OriginalFilename: cleanOriginal,
				Path:             finalRelPath,
				StorageType:      s.Storage.Name(),
				MimeType:         contentType,
				Bytes:            written,
				Collection:       collection,
				UserID:           userID,
				Description:      description,
				Metadata:         customMeta,
			}
			if thumbResult != nil {
				fileRecord.Thumbnail = &metadata.ThumbnailRecord{
					FileRecordID: fileID,
					Path:         thumbResult.Path,
					Filename:     thumbResult.Filename,
					Bytes:        thumbResult.Bytes,
					Width:        thumbResult.Width,
					Height:       thumbResult.Height,
					Format:       thumbResult.Format,
				}
			}
			if err := s.Metadata.SaveFile(r.Context(), fileRecord); err != nil {
				s.Logger.ErrorContext(r.Context(), "failed to save file metadata to database",
					slog.String("file_id", fileID),
					slog.String("filename", storedFilename),
					slog.String("error", err.Error()),
				)
			} else {
				s.Logger.InfoContext(r.Context(), "stored file metadata in database",
					slog.String("file_id", fileID),
					slog.String("filename", storedFilename),
					slog.Int64("size_bytes", written),
				)
			}
		}

		results = append(results, FileInfoResult{
			ID:               fileID,
			Path:             finalRelPath,
			Filename:         storedFilename,
			OriginalFilename: cleanOriginal,
			Bytes:            written,
			MimeType:         contentType,
			Collection:       collection,
			UserID:           userID,
			Description:      description,
			Metadata:         customMeta,
			Thumbnail:        thumbResult,
		})

		// Reset customID so subsequent files in multi-file uploads receive unique paths
		customID = ""
	}

	if len(results) == 0 {
		http.Error(w, "no file part found in request", http.StatusBadRequest)
		return
	}

	resp := UploadResponse{
		ID:               results[0].ID,
		Path:             results[0].Path,
		Filename:         results[0].Filename,
		OriginalFilename: results[0].OriginalFilename,
		Bytes:            results[0].Bytes,
		MimeType:         results[0].MimeType,
		Collection:       results[0].Collection,
		UserID:           results[0].UserID,
		Description:      results[0].Description,
		Metadata:         results[0].Metadata,
		Thumbnail:        results[0].Thumbnail,
		Files:            results,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/v1/download/"+resp.Path)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleDownload handles GET /v1/download/{path...}.
// Supports both simple IDs and nested subpaths.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	requestedPath := r.PathValue("path")
	cleanPath, err := storage.SanitizeRelativePath(requestedPath)
	if err != nil {
		http.Error(w, "invalid file path: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Storage.ServeDownload(w, r, cleanPath)
	if err != nil && errors.Is(err, storage.ErrNotFound) && s.Metadata != nil {
		if rec, metaErr := s.Metadata.GetFileByID(r.Context(), cleanPath); metaErr == nil && rec != nil {
			err = s.Storage.ServeDownload(w, r, rec.Path)
		}
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}
		http.Error(w, "error retrieving file: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

const shortIDAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const defaultShortIDLength = 12

// generateShortID returns a cryptographically secure, unbiased random Base62 alphanumeric string.
func generateShortID(length int) (string, error) {
	if length <= 0 {
		length = defaultShortIDLength
	}
	result := make([]byte, length)
	buf := make([]byte, length*2)
	idx := 0
	maxVal := byte(256 - (256 % len(shortIDAlphabet))) // 248 for unbiased sampling

	for idx < length {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b < maxVal {
				result[idx] = shortIDAlphabet[b%byte(len(shortIDAlphabet))]
				idx++
				if idx == length {
					break
				}
			}
		}
	}
	return string(result), nil
}

// generateUUID returns a cryptographically secure random UUID v4 string.
func generateUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
