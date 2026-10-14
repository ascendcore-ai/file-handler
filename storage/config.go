package storage

import (
	"bufio"
	"flag"
	"os"
	"strconv"
	"strings"
)

// Config encapsulates configuration parameters for all supported storage providers.
type Config struct {
	// EnvFile specifies the optional path to a .env configuration file.
	EnvFile string

	// Type specifies the backend provider ("local", "s3", "gcs", "azure").
	Type string

	// StoragePrefix is an optional global prefix/folder applied to all stored objects
	// (e.g. "tenant-1/" or "media/").
	StoragePrefix string

	// Local filesystem settings
	LocalDir string

	// S3 settings (AWS S3, Cloudflare R2, MinIO, Wasabi, DigitalOcean Spaces)
	S3Bucket         string
	S3Region         string
	S3Endpoint       string
	S3AccessKey      string
	S3SecretKey      string
	S3ForcePathStyle bool
	S3UseSSL         bool

	// Google Cloud Storage settings
	GCSBucket      string
	GCSEndpoint    string
	GCSBearerToken string

	// Azure Blob Storage settings
	AzureAccount   string
	AzureContainer string
	AzureEndpoint  string
	AzureSASToken  string

	// Database / ORM settings (PostgreSQL metadata persistence)
	DBEnabled         bool
	DBDSN             string
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime string

	// Logging settings (structured logging for Kibana, CloudWatch, GCP, Datadog)
	LogFormat string
	LogLevel  string
}

// LoadEnvFile reads a .env file line by line without buffering whole files in memory,
// populating os.Environ() for any variables not already defined.
func LoadEnvFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // .env is optional
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if idx := strings.Index(line, "="); idx != -1 {
			key := strings.TrimSpace(line[:idx])
			val := strings.TrimSpace(line[idx+1:])

			// Remove surrounding double or single quotes
			if len(val) >= 2 && ((val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'')) {
				val = val[1 : len(val)-1]
			}

			// Only set if not already set in the process environment
			if os.Getenv(key) == "" {
				_ = os.Setenv(key, val)
			}
		}
	}
	return scanner.Err()
}

// NewDefaultConfig initializes Config with sensible defaults and environment variables.
func NewDefaultConfig() Config {
	storageType := getEnvFirst("STORAGE_TYPE", "STORAGE_PROVIDER")
	if storageType == "" {
		storageType = "local"
	}

	localDir := getEnvFirst("UPLOAD_DIR", "LOCAL_DIR")
	if localDir == "" {
		localDir = "./uploads"
	}

	storagePrefix := getEnvFirst("STORAGE_PREFIX", "UPLOAD_PREFIX")

	s3Region := getEnvFirst("AWS_REGION", "S3_REGION")
	if s3Region == "" {
		s3Region = "us-east-1"
	}

	gcsEndpoint := getEnvFirst("GCS_ENDPOINT")
	if gcsEndpoint == "" {
		gcsEndpoint = "https://storage.googleapis.com"
	}

	forcePathStyle := false
	if v := getEnvFirst("S3_FORCE_PATH_STYLE"); v != "" {
		forcePathStyle, _ = strconv.ParseBool(v)
	}

	useSSL := true
	if v := getEnvFirst("S3_USE_SSL"); v != "" {
		useSSL, _ = strconv.ParseBool(v)
	}

	dbEnabled := false
	if v := getEnvFirst("DB_ENABLED", "DATABASE_ENABLED"); v != "" {
		dbEnabled, _ = strconv.ParseBool(v)
	}

	dbDSN := getEnvFirst("DB_DSN", "DATABASE_URL")
	if dbDSN == "" && dbEnabled {
		host := getEnvFirst("DB_HOST")
		if host == "" {
			host = "localhost"
		}
		port := getEnvFirst("DB_PORT")
		if port == "" {
			port = "5432"
		}
		user := getEnvFirst("DB_USER")
		if user == "" {
			user = "postgres"
		}
		pass := getEnvFirst("DB_PASSWORD", "DB_PASS")
		name := getEnvFirst("DB_NAME")
		if name == "" {
			name = "file_handler"
		}
		sslmode := getEnvFirst("DB_SSLMODE")
		if sslmode == "" {
			sslmode = "disable"
		}
		dbDSN = "postgres://" + user + ":" + pass + "@" + host + ":" + port + "/" + name + "?sslmode=" + sslmode
	}

	maxOpen := 25
	if v := getEnvFirst("DB_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}

	maxIdle := 5
	if v := getEnvFirst("DB_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxIdle = n
		}
	}

	maxLifetime := getEnvFirst("DB_CONN_MAX_LIFETIME")
	if maxLifetime == "" {
		maxLifetime = "5m"
	}

	return Config{
		EnvFile:          getEnvFirst("ENV_FILE"),
		Type:             storageType,
		StoragePrefix:    storagePrefix,
		LocalDir:         localDir,
		S3Bucket:         getEnvFirst("AWS_S3_BUCKET", "S3_BUCKET"),
		S3Region:         s3Region,
		S3Endpoint:       getEnvFirst("AWS_ENDPOINT_URL", "S3_ENDPOINT"),
		S3AccessKey:      getEnvFirst("AWS_ACCESS_KEY_ID", "S3_ACCESS_KEY"),
		S3SecretKey:      getEnvFirst("AWS_SECRET_ACCESS_KEY", "S3_SECRET_KEY"),
		S3ForcePathStyle: forcePathStyle,
		S3UseSSL:         useSSL,
		GCSBucket:        getEnvFirst("GCS_BUCKET", "GOOGLE_CLOUD_BUCKET"),
		GCSEndpoint:      gcsEndpoint,
		GCSBearerToken:   getEnvFirst("GCS_BEARER_TOKEN", "GOOGLE_OAUTH_TOKEN"),
		AzureAccount:     getEnvFirst("AZURE_STORAGE_ACCOUNT", "AZURE_ACCOUNT"),
		AzureContainer:   getEnvFirst("AZURE_CONTAINER", "AZURE_STORAGE_CONTAINER"),
		AzureEndpoint:    getEnvFirst("AZURE_ENDPOINT"),
		AzureSASToken:    getEnvFirst("AZURE_SAS_TOKEN"),
		DBEnabled:        dbEnabled,
		DBDSN:            dbDSN,
		DBMaxOpenConns:   maxOpen,
		DBMaxIdleConns:   maxIdle,
		DBConnMaxLifetime: maxLifetime,
		LogFormat:        getEnvFirst("LOG_FORMAT"),
		LogLevel:         getEnvFirst("LOG_LEVEL"),
	}
}

// RegisterFlags binds command line flags to the Config fields.
func RegisterFlags(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.EnvFile, "env", ".env", "Path to .env configuration file")
	fs.StringVar(&cfg.Type, "storage", cfg.Type, "Storage provider backend: local, s3, gcs, azure")
	fs.StringVar(&cfg.StoragePrefix, "prefix", cfg.StoragePrefix, "Global storage path prefix (e.g. tenant-1/)")
	fs.StringVar(&cfg.LocalDir, "dir", cfg.LocalDir, "Local directory path for local storage")

	// S3 flags
	fs.StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "AWS S3 / S3-compatible bucket name")
	fs.StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS S3 region (default: us-east-1)")
	fs.StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint URL (for MinIO, R2, Wasabi, etc.)")
	fs.StringVar(&cfg.S3AccessKey, "s3-access-key", cfg.S3AccessKey, "S3 access key ID")
	fs.StringVar(&cfg.S3SecretKey, "s3-secret-key", cfg.S3SecretKey, "S3 secret access key")
	fs.BoolVar(&cfg.S3ForcePathStyle, "s3-path-style", cfg.S3ForcePathStyle, "Force path-style S3 URLs (required for MinIO)")

	// GCS flags
	fs.StringVar(&cfg.GCSBucket, "gcs-bucket", cfg.GCSBucket, "Google Cloud Storage bucket name")
	fs.StringVar(&cfg.GCSEndpoint, "gcs-endpoint", cfg.GCSEndpoint, "Google Cloud Storage endpoint URL")
	fs.StringVar(&cfg.GCSBearerToken, "gcs-token", cfg.GCSBearerToken, "Google Cloud Storage bearer token")

	// Azure flags
	fs.StringVar(&cfg.AzureAccount, "azure-account", cfg.AzureAccount, "Azure storage account name")
	fs.StringVar(&cfg.AzureContainer, "azure-container", cfg.AzureContainer, "Azure blob storage container name")
	fs.StringVar(&cfg.AzureEndpoint, "azure-endpoint", cfg.AzureEndpoint, "Azure blob storage endpoint URL")
	fs.StringVar(&cfg.AzureSASToken, "azure-sas-token", cfg.AzureSASToken, "Azure shared access signature (SAS) token")

	// Database flags
	fs.BoolVar(&cfg.DBEnabled, "db-enabled", cfg.DBEnabled, "Enable PostgreSQL ORM metadata storage (default: false)")
	fs.StringVar(&cfg.DBDSN, "db-dsn", cfg.DBDSN, "PostgreSQL connection DSN (e.g. postgres://user:pass@host:5432/dbname?sslmode=disable)")

	// Logger flags
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log output format: text, json (default: text)")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error (default: info)")
}

func getEnvFirst(keys ...string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return ""
}
