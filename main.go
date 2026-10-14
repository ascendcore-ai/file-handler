package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"file-handler/pkg/logger"
	"file-handler/pkg/metadata"
	"file-handler/storage"
)

func main() {
	// Attempt to load .env file before initializing defaults
	envFile := os.Getenv("ENV_FILE")
	if envFile == "" {
		envFile = ".env"
	}
	_ = storage.LoadEnvFile(envFile)

	cfg := storage.NewDefaultConfig()

	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = "8080"
	}

	port := flag.String("port", defaultPort, "HTTP server port")
	storage.RegisterFlags(flag.CommandLine, &cfg)
	flag.Parse()

	// If a custom env file was specified via CLI flag, reload it
	if cfg.EnvFile != "" && cfg.EnvFile != envFile {
		_ = storage.LoadEnvFile(cfg.EnvFile)
		// Re-apply defaults after loading custom env file
		cfg = storage.NewDefaultConfig()
		flag.Parse()
	}

	// Initialize structured logger (compatible with Kibana, Cloud Logging, CloudWatch, Datadog)
	appLogger := logger.SetupDefault(cfg.LogFormat, cfg.LogLevel)

	addr := *port
	if len(addr) > 0 && addr[0] != ':' {
		addr = ":" + addr
	}

	provider, err := storage.NewProvider(cfg)
	if err != nil {
		appLogger.Error("failed to initialize storage provider", slog.String("error", err.Error()))
		os.Exit(1)
	}

	var metaRepo metadata.Repository
	if cfg.DBEnabled {
		appLogger.Info("connecting to PostgreSQL metadata database...")
		lifetime, err := time.ParseDuration(cfg.DBConnMaxLifetime)
		if err != nil {
			lifetime = 5 * time.Minute
		}
		repo, err := metadata.OpenPostgres(cfg.DBDSN, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, lifetime)
		if err != nil {
			appLogger.Error("failed to connect to PostgreSQL metadata database", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer repo.Close()
		metaRepo = repo
		appLogger.Info("connected successfully to PostgreSQL database (auto-migration complete)")
	} else {
		appLogger.Info("PostgreSQL metadata database is disabled (stateless mode)")
	}

	srvInstance := NewServerWithAll(provider, metaRepo, appLogger)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srvInstance.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Printf("🚀 File streaming server listening on %s\n", addr)
		fmt.Printf("📦 Storage backend: %s\n", provider.Name())
		if cfg.Type == "local" {
			fmt.Printf("📂 Local directory: %s\n", cfg.LocalDir)
		} else if cfg.Type == "s3" {
			fmt.Printf("☁️  S3 Bucket: %s (Region: %s)\n", cfg.S3Bucket, cfg.S3Region)
		} else if cfg.Type == "gcs" {
			fmt.Printf("☁️  GCS Bucket: %s\n", cfg.GCSBucket)
		} else if cfg.Type == "azure" {
			fmt.Printf("☁️  Azure Container: %s (Account: %s)\n", cfg.AzureContainer, cfg.AzureAccount)
		}
		if cfg.StoragePrefix != "" {
			fmt.Printf("🏷️  Storage prefix: %s\n", cfg.StoragePrefix)
		}
		if cfg.DBEnabled {
			fmt.Printf("🗄️  PostgreSQL metadata ORM: ENABLED & CONNECTED (auto-migrated)\n")
		} else {
			fmt.Printf("🗄️  PostgreSQL metadata ORM: DISABLED (stateless mode - set DB_ENABLED=true in .env to activate)\n")
		}
		fmt.Printf("⚡ 32KB buffer sync.Pool initialized\n")
		fmt.Println("Available endpoints:")
		fmt.Println("  GET  /v1/docs                    (Interactive Scalar API documentation)")
		fmt.Println("  GET  /v1/docs/swagger            (Interactive Swagger UI)")
		fmt.Println("  GET  /v1/openapi.yaml            (Raw OpenAPI 3.1 specification)")
		fmt.Println("  POST /v1/upload                  (Streaming multipart upload, ?path=..., ?thumbnail=true, ?collection=...)")
		fmt.Println("  GET  /v1/download/{path...}      (Streaming download with Range requests support)")
		fmt.Println("  GET  /v1/files                   (Query stored file metadata with filters and pagination)")
		fmt.Println("  GET  /v1/files/{id...}           (Retrieve single file metadata and thumbnail details)")
		fmt.Println("  DELETE /v1/files/{id...}         (Delete file metadata record)")
		fmt.Println("  POST /v1/upload/presign          (Direct browser upload pre-signed URL)")
		fmt.Println("  POST /v1/upload/complete         (Direct upload client confirmation callback)")
		fmt.Println("  POST /v1/webhooks/storage-events (Cloud storage webhook event notification callback)")
		fmt.Println("  GET  /v1/health                  (Service health, storage backend & DB status)")
		fmt.Println("  [Aliases: /upload, /download/{path...}, /files, /health, /docs]")

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			appLogger.Error("server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-stopChan
	appLogger.Info("initiating graceful shutdown...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		appLogger.Error("graceful shutdown failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	appLogger.Info("server stopped cleanly")
}
