package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Repository defines the contract for persisting and retrieving file metadata.
type Repository interface {
	AutoMigrate() error
	Ping(ctx context.Context) error
	SaveFile(ctx context.Context, record *FileRecord) error
	GetFileByID(ctx context.Context, id string) (*FileRecord, error)
	GetFileByPath(ctx context.Context, path string) (*FileRecord, error)
	ListFiles(ctx context.Context, filter QueryFilter) (*QueryResult, error)
	DeleteFile(ctx context.Context, id string) error
	Close() error
}

// GORMRepository implements Repository backed by GORM.
type GORMRepository struct {
	db *gorm.DB
}

// NewGORMRepository wraps an existing *gorm.DB.
func NewGORMRepository(db *gorm.DB) *GORMRepository {
	return &GORMRepository{db: db}
}

// Ping verifies that the underlying database connection is alive and responsive.
func (r *GORMRepository) Ping(ctx context.Context) error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// OpenPostgres initializes a PostgreSQL connection with pooling, ping verification, and auto-migration.
func OpenPostgres(dsn string, maxOpen, maxIdle int, maxLifetime time.Duration) (*GORMRepository, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve sql.DB: %w", err)
	}

	if maxOpen > 0 {
		sqlDB.SetMaxOpenConns(maxOpen)
	}
	if maxIdle > 0 {
		sqlDB.SetMaxIdleConns(maxIdle)
	}
	if maxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(maxLifetime)
	}

	// Verify active connectivity with Ping
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("database ping check failed: %w", err)
	}

	repo := &GORMRepository{db: db}
	if err := repo.AutoMigrate(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("failed to auto-migrate database schema: %w", err)
	}

	return repo, nil
}

// AutoMigrate applies schema migrations for file and thumbnail models.
func (r *GORMRepository) AutoMigrate() error {
	return r.db.AutoMigrate(&FileRecord{}, &ThumbnailRecord{})
}

// SaveFile inserts or updates a file record and its linked thumbnail.
func (r *GORMRepository) SaveFile(ctx context.Context, record *FileRecord) error {
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.UpdatedAt = time.Now().UTC()

	return r.db.WithContext(ctx).Save(record).Error
}

// GetFileByID retrieves a file by ID with its thumbnail preloaded.
func (r *GORMRepository) GetFileByID(ctx context.Context, id string) (*FileRecord, error) {
	var record FileRecord
	err := r.db.WithContext(ctx).Preload("Thumbnail").Where("id = ?", id).First(&record).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetFileByPath retrieves a file by its storage path with its thumbnail preloaded.
func (r *GORMRepository) GetFileByPath(ctx context.Context, path string) (*FileRecord, error) {
	var record FileRecord
	err := r.db.WithContext(ctx).Preload("Thumbnail").Where("path = ?", path).First(&record).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// ListFiles executes filtered, paginated queries on stored file records.
func (r *GORMRepository) ListFiles(ctx context.Context, filter QueryFilter) (*QueryResult, error) {
	query := r.db.WithContext(ctx).Model(&FileRecord{}).Preload("Thumbnail")

	if filter.Collection != "" {
		query = query.Where("collection = ?", filter.Collection)
	}
	if filter.MimeType != "" {
		query = query.Where("mime_type LIKE ?", "%"+filter.MimeType+"%")
	}
	if filter.UserID != "" {
		query = query.Where("user_id = ?", filter.UserID)
	}
	if filter.Search != "" {
		pattern := "%" + strings.ToLower(filter.Search) + "%"
		query = query.Where("LOWER(filename) LIKE ? OR LOWER(original_filename) LIKE ? OR LOWER(description) LIKE ?", pattern, pattern, pattern)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	sort := filter.Sort
	if sort == "" {
		sort = "created_at desc"
	}

	var files []FileRecord
	if err := query.Order(sort).Limit(limit).Offset(offset).Find(&files).Error; err != nil {
		return nil, err
	}

	return &QueryResult{
		Total:  total,
		Limit:  limit,
		Offset: offset,
		Files:  files,
	}, nil
}

// DeleteFile deletes a file record by ID or path.
func (r *GORMRepository) DeleteFile(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ? OR path = ?", id, id).Delete(&FileRecord{}).Error
}

// Close closes the underlying database connection pool.
func (r *GORMRepository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
