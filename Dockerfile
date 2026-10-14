# syntax=docker/dockerfile:1

# ------------------------------------------------------------------------------
# Stage 1: Build static binary
# ------------------------------------------------------------------------------
FROM golang:alpine AS builder

WORKDIR /src

# Install ca-certificates and tzdata for SSL/TLS and timezone support
RUN apk add --no-cache ca-certificates tzdata

# Copy module definition files
COPY go.mod ./
# Copy go.sum if present (ignored if not present via wildcard)
COPY go.su* ./

# Copy source code
COPY . .

# Build a fully static, stripped binary with zero external runtime dependencies
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /app/server .

# ------------------------------------------------------------------------------
# Stage 2: Minimal, secure runtime container
# ------------------------------------------------------------------------------
FROM alpine:3.21

# Install ca-certificates for outbound HTTPS calls (AWS S3, GCS, Azure)
# and wget for healthchecks
RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -S appgroup -g 10001 \
    && adduser -S appuser -G appgroup -u 10001 -h /app

WORKDIR /app

# Create upload directory and grant permissions to non-root user
RUN mkdir -p /app/uploads \
    && chown -R appuser:appgroup /app

# Copy compiled binary from builder
COPY --from=builder --chown=appuser:appgroup /app/server /app/server
COPY --chown=appuser:appgroup .env.example /app/.env.example

# Drop root privileges
USER appuser:appgroup

# Default environment configuration
ENV PORT=8080 \
    STORAGE_TYPE=local \
    UPLOAD_DIR=/app/uploads

EXPOSE 8080

# Persist uploaded files across container recreation
VOLUME ["/app/uploads"]

# Container healthcheck querying the versioned /v1/health API
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/v1/health || exit 1

ENTRYPOINT ["/app/server"]
