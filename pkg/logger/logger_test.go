package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"invalid", slog.LevelInfo},
	}

	for _, tt := range tests {
		got := ParseLevel(tt.input)
		if got != tt.expected {
			t.Errorf("ParseLevel(%q) = %v; want %v", tt.input, got, tt.expected)
		}
	}
}

func TestJSONLoggerOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("json", "info", buf)

	l.Info("file uploaded",
		slog.String("file_id", "test12345678"),
		slog.Int64("bytes", 2048),
	)

	var logged map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logged); err != nil {
		t.Fatalf("failed to parse JSON log line: %v. Raw: %s", err, buf.String())
	}

	if logged["msg"] != "file uploaded" {
		t.Errorf("expected msg 'file uploaded', got %v", logged["msg"])
	}
	if logged["level"] != "INFO" {
		t.Errorf("expected level 'INFO', got %v", logged["level"])
	}
	if logged["file_id"] != "test12345678" {
		t.Errorf("expected file_id 'test12345678', got %v", logged["file_id"])
	}
	if logged["time"] == nil {
		t.Errorf("expected timestamp field 'time'")
	}
}

func TestTextLoggerOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("text", "info", buf)

	l.Info("server started", slog.Int("port", 8080))
	line := buf.String()

	if !strings.Contains(line, "level=INFO") {
		t.Errorf("expected level=INFO in text output, got %q", line)
	}
	if !strings.Contains(line, "msg=\"server started\"") {
		t.Errorf("expected msg='server started' in text output, got %q", line)
	}
	if !strings.Contains(line, "port=8080") {
		t.Errorf("expected port=8080 in text output, got %q", line)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("json", "info", buf)
	mw := HTTPMiddleware(l)

	// Test a normal 200 OK endpoint
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))

	req := httptest.NewRequest("GET", "/v1/download/test.pdf", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to decode log line: %v", err)
	}

	if parsed["method"] != "GET" {
		t.Errorf("expected method GET, got %v", parsed["method"])
	}
	if parsed["path"] != "/v1/download/test.pdf" {
		t.Errorf("expected path /v1/download/test.pdf, got %v", parsed["path"])
	}
	if parsed["status"] != float64(200) {
		t.Errorf("expected status 200, got %v", parsed["status"])
	}
	if parsed["bytes"] != float64(11) {
		t.Errorf("expected bytes 11, got %v", parsed["bytes"])
	}
}

func TestHTTPMiddlewareHealthCheckLevel(t *testing.T) {
	// With level "info", health check (logged as debug) should NOT output anything
	buf := &bytes.Buffer{}
	l := New("json", "info", buf)
	mw := HTTPMiddleware(l)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if buf.Len() != 0 {
		t.Fatalf("expected health check to be filtered out at INFO level, got: %s", buf.String())
	}
}
