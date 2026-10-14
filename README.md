# High-Performance Streaming File Server (Go)

A high-performance HTTP file upload and download service in Go built in strict compliance with zero-memory-buffering and streaming constraints. Supports pluggable storage backends: **Local Filesystem**, **AWS S3 / S3-Compatible (MinIO, Cloudflare R2, Wasabi)**, **Google Cloud Storage (GCS)**, and **Azure Blob Storage**, with **PostgreSQL Metadata ORM (GORM)**, **Automatic Image Thumbnails**, **API Versioning (v1)**, **Direct Browser Pre-signed Uploads**, **Event Webhooks**, **Dynamic Upload Paths**, **Docker Containerization**, and **Environment Variable (.env) Files**.

---

## Architecture & Core Features

- **Strict Zero Memory Buffering**:
  - Never buffers entire files into RAM. Files are streamed directly between network sockets and storage.
  - Zero calls to `io.ReadAll` or `ioutil.ReadAll` across the entire codebase (enforced and verified via AST testing).
- **PostgreSQL Metadata ORM (Optional, Disabled by Default)**:
  - Backed by **GORM** (`gorm.io/gorm`, `gorm.io/driver/postgres`) with connection pooling and auto-migrations.
  - Controlled by feature toggle: `DB_ENABLED=false` (or `-db-enabled=false`).
  - When disabled: service runs 100% statelessly with zero database overhead.
  - When enabled: records file sizes, MIME types, collection tags, owner IDs, descriptions, custom JSON metadata (`JSONB`), and thumbnail details.
  - Provides rich query endpoints (`GET /v1/files`) with collection filters, MIME search, and pagination.
- **API Versioning (v1)**:
  - Canonical endpoints are versioned with `/v1/` (`/v1/upload`, `/v1/download/{path...}`, `/v1/files`, `/v1/health`, etc.).
  - Unversioned endpoints (`/upload`, `/download/{path...}`, `/files`, `/health`) are maintained for 100% backward compatibility.
- **Direct Browser Uploads (Pre-signed URLs)**:
  - `POST /v1/upload/presign` generates temporary AWS SigV4 pre-signed PUT URLs.
  - Allows browsers to stream large files directly to cloud storage, bypassing server CPU and bandwidth.
  - Fallback support: When local storage is active, returns the streaming server endpoint.
- **Automatic Image Thumbnails**:
  - Bilinear image resizing (`?thumbnail=true`) stored in configurable subfolders (`thumbnails/`).
- **Post-Upload Event Callbacks & Webhooks**:
  - `POST /v1/upload/complete`: Client confirmation callback for immediate UI state transitions.
  - `POST /v1/webhooks/storage-events`: Webhook receiver for MinIO, AWS S3 (SNS/EventBridge), GCS (Pub/Sub), and Azure Event Grid.
- **Multipart Streaming Upload (`POST /v1/upload`)**:
  - Strictly uses `r.MultipartReader()` with `NextPart()`, avoiding `r.ParseMultipartForm()` which buffers to disk/memory.
  - Streams directly into storage via `pool.StreamCopy` using pooled 32KB buffers.
- **Dynamic Upload Paths (Payload & Config)**:
  - Global base path via configuration: `-dir`, `-prefix`, or `STORAGE_PREFIX`.
  - Per-request dynamic path via query parameter, header (`X-Upload-Path`), or leading multipart form field.
- **Pooled 32KB Buffers (`sync.Pool`)**:
  - Allocates 32KB byte slices using `sync.Pool` to eliminate garbage collection spikes under high concurrency.
- **Optimized Download with Range Support (`GET /v1/download/{path...}`)**:
  - **Local Storage**: Uses `http.ServeContent` with `*os.File` for automatic HTTP `Range` request support (`206 Partial Content`), caching headers, and zero RAM buffering.
  - **Remote Streams (S3 / GCS / Azure)**: Forwards `Range` headers to upstream cloud storage and pipes the stream directly to `http.ResponseWriter` via `pool.StreamCopy`.
- **Docker Containerization**:
  - Multi-stage build with minimal runtime footprint.
  - Rootless non-root container user (`appuser:10001`), persistent volume for uploads, and integrated health checks.
- **Structured Logging & Observability (`log/slog`)**:
  - Built on Go standard library `log/slog` (zero external dependencies).
  - Dual format output: `json` (production NDJSON for Kibana, AWS CloudWatch, GCP Cloud Logging, Datadog) and `text` (human-readable for local development).
  - Configurable log levels (`debug`, `info`, `warn`, `error`).
  - Streaming-safe HTTP access logging middleware with `http.Flusher` delegation, tracking `method`, `path`, `status`, `duration_ms`, `bytes`, and `client_ip`.
  - Automatic noise filtering: health checks (`/v1/health`) are logged at `DEBUG` level to prevent log pollution in production dashboards.

---

## API Endpoints (Version 1)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/docs` / `/v1/docs` | Interactive **Scalar** API Documentation & Reference console |
| `GET` | `/docs/swagger` | Interactive **Swagger UI** Documentation console |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 Specification definition file |
| `POST` | `/v1/upload` | Stream multipart file directly into storage (supports collections, user metadata & thumbnails) |
| `GET` | `/v1/download/{path...}` | Stream file from storage (supports HTTP `Range` requests) |
| `GET` | `/v1/files` | Query stored file metadata with filters (`collection`, `mime_type`, `search`), pagination, and sorting |
| `GET` | `/v1/files/{id...}` | Retrieve metadata and linked thumbnail details for a single file |
| `DELETE` | `/v1/files/{id...}` | Delete file metadata record |
| `POST` | `/v1/upload/presign` | Generate pre-signed URL for direct browser-to-cloud upload |
| `POST` / `GET` | `/v1/download/presign` | Generate pre-signed URL for direct client-to-cloud download (bypasses server bandwidth) |
| `POST` | `/v1/upload/complete` | Client confirmation callback after direct upload |
| `POST` | `/v1/webhooks/storage-events` | Cloud storage event webhook notification handler |
| `GET` | `/v1/health` | Service liveness, active storage backend, and metadata DB status |

*(Legacy unversioned endpoints `/upload`, `/download/{path...}`, `/files`, and `/health` remain active for backward compatibility).*

---

## API Usage Examples

### 1. Streaming Upload (`POST /v1/upload`)

#### Basic Upload
```bash
curl -X POST -F "file=@document.pdf" http://localhost:8080/v1/upload
```
**Response (`201 Created`)**:
```json
{
  "id": "k8F2mP9xL3vW",
  "path": "k8F2mP9xL3vW_document.pdf",
  "filename": "k8F2mP9xL3vW_document.pdf",
  "original_filename": "document.pdf",
  "bytes": 1048576
}
```

> **Collision-Free Storage**: To prevent overwriting when multiple clients upload files with the same name, a compact 12-character Base62 unique ID is automatically prefixed to the stored filename (`<id>_<filename>`). The client's original filename is always preserved in `original_filename` and stored in the PostgreSQL metadata repository.

#### Upload to Dynamic Nested Path
```bash
curl -X POST -F "file=@invoice.pdf" "http://localhost:8080/v1/upload?path=finance/2026/q1"
```

#### Upload with Automatic Thumbnail Generation (`?thumbnail=true`)
Generates an aspect-ratio-preserved thumbnail and saves it to the same file path in a `thumbnails` subfolder, also prefixed with the unique short ID to avoid overwriting:
```bash
curl -X POST -F "file=@photo.jpg" "http://localhost:8080/v1/upload?path=photos/2026&thumbnail=true"
```
**Response (`201 Created`)**:
```json
{
  "id": "k8F2mP9xL3vW",
  "path": "photos/2026/k8F2mP9xL3vW_photo.jpg",
  "filename": "k8F2mP9xL3vW_photo.jpg",
  "original_filename": "photo.jpg",
  "bytes": 2048576,
  "thumbnail": {
    "path": "photos/2026/thumbnails/k8F2mP9xL3vW_photo.jpg",
    "filename": "k8F2mP9xL3vW_photo.jpg",
    "bytes": 18450,
    "width": 200,
    "height": 150,
    "format": "jpeg"
  }
}
```

#### Upload with Custom Thumbnail Folder and Max Dimension
```bash
curl -X POST -F "file=@avatar.png" \
  "http://localhost:8080/v1/upload?path=users/42&thumbnail=true&thumb_dir=thumbs&thumb_size=300"
```
- Saved to: `users/42/thumbs/<id>_avatar.png` (max dimension 300px)

#### Upload with Custom Header Path
```bash
curl -X POST -H "X-Upload-Path: users/42/profile" -F "file=@avatar.png" http://localhost:8080/v1/upload
```

#### Upload with Collection, User ID, and Custom JSON Metadata
When PostgreSQL metadata storage is enabled (`DB_ENABLED=true`), attach collections, descriptions, user IDs, and custom metadata:
```bash
curl -X POST \
  -F "file=@invoice.pdf" \
  -F "collection=invoices" \
  -F "user_id=usr_99" \
  -F "description=Q1 Corporate Expense Report" \
  -F 'metadata={"vendor":"ACME Corp","amount":1500,"tax_deductible":true}' \
  "http://localhost:8080/v1/upload?path=finance/2026&id=invoice-2026.pdf"
```

---

### 2. Streaming Download (`GET /v1/download/{path...}`)

#### Download by Path or ID
```bash
# Download root file
curl -O http://localhost:8080/v1/download/e4d909c2-9e12-47e0-81f0-29c8942b0fe8

# Download nested path
curl -O http://localhost:8080/v1/download/finance/2026/q1/invoice.pdf
```

#### HTTP Range Request (Partial Content)
```bash
curl -H "Range: bytes=0-1023" -i http://localhost:8080/v1/download/finance/2026/q1/invoice.pdf
```
Returns `206 Partial Content` with header `Content-Range: bytes 0-1023/1048576`.

---

### 3. Metadata Queries & Management (`GET /v1/files`, `GET /v1/files/{id}`, `DELETE /v1/files/{id}`)
*(Requires `DB_ENABLED=true`)*

#### Query Stored Files with Filters and Pagination
```bash
# Filter by collection
curl "http://localhost:8080/v1/files?collection=invoices"

# Filter by MIME type and search keyword
curl "http://localhost:8080/v1/files?mime_type=image/&search=vacation"

# Paginate (limit & offset) and custom sort
curl "http://localhost:8080/v1/files?limit=10&offset=0&sort=created_at%20desc"
```

**Response (`200 OK`)**:
```json
{
  "total": 1,
  "limit": 10,
  "offset": 0,
  "files": [
    {
      "id": "invoice-2026.pdf",
      "filename": "invoice-2026.pdf",
      "path": "finance/2026/invoice-2026.pdf",
      "storage_type": "local",
      "mime_type": "application/pdf",
      "bytes": 1048576,
      "collection": "invoices",
      "user_id": "usr_99",
      "description": "Q1 Corporate Expense Report",
      "metadata": {
        "vendor": "ACME Corp",
        "amount": 1500,
        "tax_deductible": true
      },
      "created_at": "2026-10-13T10:00:00Z",
      "updated_at": "2026-10-13T10:00:00Z"
    }
  ]
}
```

#### Retrieve Single File Metadata
```bash
curl http://localhost:8080/v1/files/invoice-2026.pdf
```

#### Delete File Metadata
```bash
curl -X DELETE http://localhost:8080/v1/files/invoice-2026.pdf
```

---

### 4. Request Pre-Signed Upload URL (`POST /v1/upload/presign`)

```bash
curl -X POST http://localhost:8080/v1/upload/presign \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "video.mp4",
    "path": "media/uploads",
    "content_type": "video/mp4",
    "expires_in": 900
  }'
```

**Response (Cloud S3 / R2 / MinIO)**:
```json
{
  "method": "PUT",
  "url": "https://my-bucket.s3.us-east-1.amazonaws.com/media/uploads/video.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
  "path": "media/uploads/video.mp4",
  "headers": {
    "Content-Type": "video/mp4"
  },
  "expires_in": 900,
  "is_direct": true
}
```

The browser can then stream the `File` object directly to the cloud:
```javascript
await fetch(presigned.url, {
  method: presigned.method,
  headers: presigned.headers,
  body: file
});
```

---

### 5. Direct Upload Confirmation Callback (`POST /v1/upload/complete`)

```bash
curl -X POST http://localhost:8080/v1/upload/complete \
  -H "Content-Type: application/json" \
  -d '{
    "path": "media/uploads/video.mp4",
    "filename": "video.mp4",
    "size": 52428800,
    "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
    "collection": "videos",
    "user_id": "usr_42"
  }'
```
**Response**:
```json
{
  "status": "recorded",
  "path": "media/uploads/video.mp4",
  "filename": "video.mp4",
  "size": 52428800,
  "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "collection": "videos",
  "user_id": "usr_42"
}
```

---

### 6. Pre-signed Download URL (`/v1/download/presign/{file_id}`)

Generates a temporary pre-signed SigV4 GET URL for direct download from AWS S3, Cloudflare R2, or MinIO by passing the unique `file_id`. Bypasses the Go server's bandwidth entirely. When local storage is active, returns a fallback pointing to `/v1/download/{path}`.

#### 1. In Path (Simplest):
```bash
curl http://localhost:8080/v1/download/presign/O0GR6jnlUZ6L
```

#### 2. Query Parameter:
```bash
curl "http://localhost:8080/v1/download/presign?file_id=O0GR6jnlUZ6L&expires_in=900"
```

#### 3. POST JSON:
```bash
curl -X POST http://localhost:8080/v1/download/presign \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "O0GR6jnlUZ6L",
    "expires_in": 900
  }'
```

**Response (Cloud S3)**:
```json
{
  "file_id": "O0GR6jnlUZ6L",
  "method": "GET",
  "url": "https://my-bucket.s3.us-east-1.amazonaws.com/photos/2026/O0GR6jnlUZ6L_profile0img.jpeg?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Date=...&X-Amz-Expires=900&X-Amz-Signature=...",
  "path": "photos/2026/O0GR6jnlUZ6L_profile0img.jpeg",
  "expires_in": 900,
  "is_direct": true
}
```

**Response (Local Storage Fallback)**:
```json
{
  "file_id": "O0GR6jnlUZ6L",
  "method": "GET",
  "url": "/v1/download/photos/2026/O0GR6jnlUZ6L_profile0img.jpeg",
  "path": "photos/2026/O0GR6jnlUZ6L_profile0img.jpeg",
  "expires_in": 0,
  "is_direct": false
}
```

---

### 7. Storage Event Webhook (`POST /v1/webhooks/storage-events`)

Accepts incoming webhook notifications from MinIO, S3 SNS, GCS Pub/Sub, or Azure Event Grid:
```bash
curl -X POST http://localhost:8080/v1/webhooks/storage-events \
  -H "Content-Type: application/json" \
  -d '{"EventName":"s3:ObjectCreated:Put","Key":"media/uploads/video.mp4"}'
```
**Response**:
```json
{
  "status": "acknowledged"
}
```

---

### 7. Health Check (`GET /v1/health`)
```bash
curl http://localhost:8080/v1/health
```
```json
{
  "status": "ok",
  "version": "v1",
  "storage": "local",
  "database": "enabled"
}
```

---

### 8. Interactive API Documentation & OpenAPI Specification

The service comes with self-hosted interactive documentation:

- **Scalar API Reference** (Recommended): [http://localhost:8080/docs](http://localhost:8080/docs) (or `/v1/docs`)
  - Modern, responsive documentation interface with dark/light themes, instant search, copyable code samples for multiple languages, and a full interactive request tester.
- **Swagger UI**: [http://localhost:8080/docs/swagger](http://localhost:8080/docs/swagger)
  - Classic OpenAPI interactive console with "Try it out" multipart and download streaming testing.
- **OpenAPI 3.1 YAML Specification**: [http://localhost:8080/openapi.yaml](http://localhost:8080/openapi.yaml) (or file [docs/openapi.yaml](file:///Users/ajanthabandara/Documents/projects/personal/file-handler/docs/openapi.yaml))
  - Standardized OpenAPI 3.1 contract compatible with Postman, Insomnia, Bruno, and code generators.

---

## Development & Live Reload

### Standard Run
By default, Go compiles and runs the application once. Changes made to source files do **not** trigger an automatic restart:
```bash
go run .
```
*(Press `Ctrl+C` and re-run `go run .` to apply code changes).*

### Live Reload (Auto-Restart on Code Changes)
To automatically recompile and restart the server whenever `.go` files or `.env` change, use [Air](https://github.com/air-verse/air):

1. **Install Air**:
   ```bash
   go install github.com/air-verse/air@latest
   # Or on macOS with Homebrew:
   # brew install air
   ```

2. **Run with Air**:
   ```bash
   air
   ```
   *(A pre-configured [.air.toml](file:///Users/ajanthabandara/Documents/projects/personal/file-handler/.air.toml) is included in the project root. It automatically monitors `.go` files while excluding `uploads/`, `tmp/`, `.agent/`, and `docs/` so file uploads don't cause accidental server restarts).*

---

## Structured Logging & Observability

The service uses Go's standard library `log/slog` (introduced in Go 1.21) for high-performance, zero-allocation structured logging with no third-party dependencies.

### 1. Configuration

Logging can be configured via environment variables (in `.env`) or command-line flags:

| Config Option | Flag | Environment Variable | Default | Allowed Values |
|---|---|---|---|---|
| **Log Format** | `-log-format` | `LOG_FORMAT` | `text` | `text`, `json` |
| **Log Level** | `-log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

#### In `.env`:
```env
# Switch between human-readable text and production JSON
LOG_FORMAT=json

# Minimum severity level to log
LOG_LEVEL=info
```

#### Via CLI Flags:
```bash
go run . -log-format=json -log-level=debug
```

---

### 2. Output Formats

#### Local Development (`LOG_FORMAT=text`):
Human-friendly format with key-value pairs:
```text
2026/10/14 20:30:00 INFO HTTP request completed method=POST path=/v1/upload status=201 duration_ms=18.42 bytes=640 client_ip=127.0.0.1 query="thumbnail=true"
2026/10/14 20:30:05 WARN HTTP request error method=GET path=/v1/files/non-existent status=404 duration_ms=0.52 bytes=36 client_ip=127.0.0.1
```

#### Production / Cloud Aggregators (`LOG_FORMAT=json`):
Strict RFC3339-compliant newline-delimited JSON (NDJSON) compatible with all major observability tools:
```json
{"time":"2026-10-14T15:00:00.123456Z","level":"INFO","msg":"HTTP request completed","method":"POST","path":"/v1/upload","status":201,"duration_ms":18.42,"bytes":640,"client_ip":"192.168.1.50","query":"thumbnail=true"}
{"time":"2026-10-14T15:00:05.987654Z","level":"WARN","msg":"HTTP request error","method":"GET","path":"/v1/files/missing","status":404,"duration_ms":0.52,"bytes":36,"client_ip":"192.168.1.50"}
```

---

### 3. HTTP Access Logging Middleware

All incoming HTTP requests pass through the integrated logging middleware:

- **Metrics Tracked**: HTTP method, URL path, HTTP status code, request duration in milliseconds (`duration_ms`), bytes sent, client IP address, and query string.
- **Smart Level Mapping**:
  - `5xx` responses &rarr; logged as `ERROR`
  - `4xx` responses &rarr; logged as `WARN`
  - `2xx` / `3xx` responses &rarr; logged as `INFO`
- **Noise Reduction**: Liveness and readiness probes (`/v1/health` and `/health`) are automatically downgraded to `DEBUG` level. This prevents ALB/Kubernetes health checks from flooding your production dashboards and driving up log ingestion costs.
- **Streaming & Flusher Safety**: The response interceptor fully delegates `http.Flusher`, guaranteeing that zero-memory buffering and real-time streaming downloads are never interrupted or buffered in RAM.

---

### 4. Integration with Cloud Observability Tools

| Platform | Recommended Config | Integration Method |
|---|---|---|
| **Kibana / Elasticsearch (ELK)** | `LOG_FORMAT=json` | Ingest stdout using **Filebeat**, **Logstash**, or **Fluentbit**. The JSON keys (`time`, `level`, `msg`, `method`, `status`, `duration_ms`) are automatically parsed and indexed into Elasticsearch fields for instant filtering and Kibana dashboards. |
| **AWS CloudWatch Logs** | `LOG_FORMAT=json` | Direct container stdout via AWS ECS / EKS / Fargate awslogs driver. CloudWatch Logs Insights can immediately query `fields @timestamp, method, path, status, duration_ms | filter status >= 400`. |
| **Google Cloud Logging (GCP)** | `LOG_FORMAT=json` | In GKE, Cloud Run, or Compute Engine, the Google Cloud logging agent parses stdout JSON automatically. Maps `level` to severity and indexes all attributes for Log Explorer queries. |
| **Datadog / Grafana Loki** | `LOG_FORMAT=json` | Standard container stdout collection via Datadog Agent or Promtail/Vector for Loki. JSON attributes become searchable facets. |

---

## Docker Deployment

### 1. Build the Docker Image
```bash
docker build -t file-handler:latest .
```

### 2. Run the Container
```bash
# Run with local volume mount
docker run -d \
  -p 8080:8080 \
  -v file_uploads:/app/uploads \
  --name file-handler \
  file-handler:latest

# Run with AWS S3 / MinIO backend
docker run -d \
  -p 8080:8080 \
  -e STORAGE_TYPE=s3 \
  -e AWS_S3_BUCKET=my-bucket \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=AKIA... \
  -e AWS_SECRET_ACCESS_KEY=... \
  --name file-handler \
  file-handler:latest
```

### 3. Run with Docker Compose
```bash
docker compose up -d
```

---

## Testing & Verification

Run all unit, integration, and AST compliance tests with the Go race detector:
```bash
go test -v -race ./...
```

Run streaming throughput benchmarks:
```bash
go test -bench=BenchmarkStreamCopy -benchmem ./...
```
