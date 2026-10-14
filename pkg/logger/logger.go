package logger

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Config holds configuration parameters for the structured logger.
type Config struct {
	Format string // "json" or "text"
	Level  string // "debug", "info", "warn", "error"
	Output io.Writer
}

// ParseLevel converts a string representation of a log level into an slog.Level.
func ParseLevel(lvl string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(lvl)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New creates a new configured slog.Logger compatible with Kibana, Cloud Logging, and Datadog.
func New(format string, level string, out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}

	slogLevel := ParseLevel(level)

	// Custom attribute replacer for seamless cloud logging (GCP, Kibana, Datadog)
	replaceAttr := func(groups []string, a slog.Attr) slog.Attr {
		switch a.Key {
		case slog.TimeKey:
			if t, ok := a.Value.Any().(time.Time); ok {
				a.Value = slog.StringValue(t.UTC().Format(time.RFC3339Nano))
			}
		}
		return a
	}

	opts := &slog.HandlerOptions{
		Level:       slogLevel,
		ReplaceAttr: replaceAttr,
	}

	var handler slog.Handler
	if strings.ToLower(strings.TrimSpace(format)) == "json" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}

	return slog.New(handler)
}

// SetupDefault configures the global slog default logger.
func SetupDefault(format string, level string) *slog.Logger {
	l := New(format, level, os.Stdout)
	slog.SetDefault(l)
	return l
}

// responseCapture wraps http.ResponseWriter to intercept HTTP status codes and written byte counts
// without buffering or interfering with streaming or Range requests.
type responseCapture struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (rc *responseCapture) WriteHeader(code int) {
	if !rc.wroteHeader {
		rc.statusCode = code
		rc.wroteHeader = true
		rc.ResponseWriter.WriteHeader(code)
	}
}

func (rc *responseCapture) Write(b []byte) (int, error) {
	if !rc.wroteHeader {
		rc.WriteHeader(http.StatusOK)
	}
	n, err := rc.ResponseWriter.Write(b)
	rc.bytesWritten += int64(n)
	return n, err
}

// Flush supports streaming HTTP responses if the underlying ResponseWriter is a Flusher.
func (rc *responseCapture) Flush() {
	if f, ok := rc.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// extractClientIP extracts client IP address accounting for proxies.
func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return strings.TrimSpace(xrip)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// HTTPMiddleware creates an HTTP middleware that logs request execution,
// duration, response codes, and data sizes in structured format.
func HTTPMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			clientIP := extractClientIP(r)

			capture := &responseCapture{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Execute handler chain
			next.ServeHTTP(capture, r)

			duration := time.Since(start)
			durationMs := float64(duration.Microseconds()) / 1000.0

			// Common attributes for log search and dashboards
			attrs := []any{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", capture.statusCode),
				slog.Float64("duration_ms", durationMs),
				slog.Int64("bytes", capture.bytesWritten),
				slog.String("client_ip", clientIP),
			}

			if r.URL.RawQuery != "" {
				attrs = append(attrs, slog.String("query", r.URL.RawQuery))
			}

			// Downgrade health check polls to Debug level to prevent log clutter in Kibana/CloudWatch
			if r.URL.Path == "/v1/health" || r.URL.Path == "/health" {
				log.DebugContext(r.Context(), "HTTP health check", attrs...)
				return
			}

			ctx := r.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			if capture.statusCode >= 500 {
				log.ErrorContext(ctx, "HTTP request failed", attrs...)
			} else if capture.statusCode >= 400 {
				log.WarnContext(ctx, "HTTP request error", attrs...)
			} else {
				log.InfoContext(ctx, "HTTP request completed", attrs...)
			}
		})
	}
}
