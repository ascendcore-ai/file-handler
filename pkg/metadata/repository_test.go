package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestDB(t *testing.T) Repository {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}

	repo := NewGORMRepository(db)
	if err := repo.AutoMigrate(); err != nil {
		t.Fatalf("failed to automigrate: %v", err)
	}
	return repo
}

func TestRepositorySaveAndGet(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()

	ctx := context.Background()

	file := &FileRecord{
		ID:               "inv-001",
		Filename:         "uuid_invoice-2026.pdf",
		OriginalFilename: "invoice-2026.pdf",
		Path:             "documents/2026/uuid_invoice-2026.pdf",
		StorageType:      "local",
		MimeType:         "application/pdf",
		Bytes:            102400,
		Collection:       "invoices",
		UserID:           "usr-99",
		Description:      "Q1 Corporate invoice",
		Metadata: JSONMap{
			"department": "accounting",
			"fiscal_year": float64(2026),
		},
		Thumbnail: &ThumbnailRecord{
			Path:     "documents/2026/thumbnails/uuid_invoice-2026.pdf",
			Filename: "uuid_invoice-2026.pdf",
			Bytes:    12400,
			Width:    200,
			Height:   280,
			Format:   "png",
		},
	}

	if err := repo.SaveFile(ctx, file); err != nil {
		t.Fatalf("failed to save file: %v", err)
	}

	// Retrieve by ID
	retrieved, err := repo.GetFileByID(ctx, "inv-001")
	if err != nil {
		t.Fatalf("failed to get file by ID: %v", err)
	}
	if retrieved.Filename != "uuid_invoice-2026.pdf" {
		t.Errorf("expected filename uuid_invoice-2026.pdf, got %s", retrieved.Filename)
	}
	if retrieved.OriginalFilename != "invoice-2026.pdf" {
		t.Errorf("expected original_filename invoice-2026.pdf, got %s", retrieved.OriginalFilename)
	}
	if retrieved.Collection != "invoices" {
		t.Errorf("expected collection invoices, got %s", retrieved.Collection)
	}
	if retrieved.Thumbnail == nil {
		t.Fatal("expected linked thumbnail record")
	}
	if retrieved.Thumbnail.Width != 200 {
		t.Errorf("expected thumbnail width 200, got %d", retrieved.Thumbnail.Width)
	}
	if retrieved.Metadata["department"] != "accounting" {
		t.Errorf("expected department accounting, got %v", retrieved.Metadata["department"])
	}

	// Retrieve by Path
	retrievedByPath, err := repo.GetFileByPath(ctx, "documents/2026/uuid_invoice-2026.pdf")
	if err != nil {
		t.Fatalf("failed to get file by path: %v", err)
	}
	if retrievedByPath.ID != "inv-001" {
		t.Errorf("expected ID inv-001, got %s", retrievedByPath.ID)
	}
}

func TestRepositoryListFilterAndPagination(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()

	ctx := context.Background()

	files := []FileRecord{
		{
			ID:               "f1",
			Filename:         "uuid1_vacation.jpg",
			OriginalFilename: "vacation.jpg",
			Path:             "photos/uuid1_vacation.jpg",
			StorageType:      "s3",
			MimeType:         "image/jpeg",
			Bytes:            200000,
			Collection:       "gallery",
			UserID:           "alice",
			Description:      "Summer trip photo",
		},
		{
			ID:               "f2",
			Filename:         "uuid2_avatar.png",
			OriginalFilename: "avatar.png",
			Path:             "profiles/uuid2_avatar.png",
			StorageType:      "s3",
			MimeType:         "image/png",
			Bytes:            50000,
			Collection:       "avatars",
			UserID:           "alice",
			Description:      "User profile picture",
		},
		{
			ID:               "f3",
			Filename:         "uuid3_tax_return.pdf",
			OriginalFilename: "tax_return.pdf",
			Path:             "docs/uuid3_tax_return.pdf",
			StorageType:      "local",
			MimeType:         "application/pdf",
			Bytes:            450000,
			Collection:       "taxes",
			UserID:           "bob",
			Description:      "2025 Tax statement",
		},
	}

	for i := range files {
		files[i].CreatedAt = time.Now().Add(time.Duration(i) * time.Minute)
		if err := repo.SaveFile(ctx, &files[i]); err != nil {
			t.Fatalf("failed to save file %s: %v", files[i].ID, err)
		}
	}

	// Test filter by collection
	res, err := repo.ListFiles(ctx, QueryFilter{Collection: "gallery"})
	if err != nil {
		t.Fatalf("failed to list by collection: %v", err)
	}
	if res.Total != 1 || len(res.Files) != 1 || res.Files[0].ID != "f1" {
		t.Fatalf("expected 1 file with ID f1, got %d files", len(res.Files))
	}

	// Test filter by mime_type
	res, err = repo.ListFiles(ctx, QueryFilter{MimeType: "image/"})
	if err != nil {
		t.Fatalf("failed to list by mime_type: %v", err)
	}
	if res.Total != 2 || len(res.Files) != 2 {
		t.Fatalf("expected 2 image files, got %d", len(res.Files))
	}

	// Test filter by user_id
	res, err = repo.ListFiles(ctx, QueryFilter{UserID: "alice"})
	if err != nil {
		t.Fatalf("failed to list by user_id: %v", err)
	}
	if res.Total != 2 {
		t.Fatalf("expected 2 files for alice, got %d", res.Total)
	}

	// Test search in description
	res, err = repo.ListFiles(ctx, QueryFilter{Search: "trip"})
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if res.Total != 1 || res.Files[0].ID != "f1" {
		t.Fatalf("expected 1 match for search 'trip', got %d", res.Total)
	}

	// Test search in original_filename
	res, err = repo.ListFiles(ctx, QueryFilter{Search: "tax_return"})
	if err != nil {
		t.Fatalf("failed to search original_filename: %v", err)
	}
	if res.Total != 1 || res.Files[0].ID != "f3" {
		t.Fatalf("expected 1 match for search 'tax_return', got %d", res.Total)
	}

	// Test pagination
	res, err = repo.ListFiles(ctx, QueryFilter{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("failed to paginate: %v", err)
	}
	if res.Total != 3 {
		t.Fatalf("expected total 3, got %d", res.Total)
	}
	if len(res.Files) != 2 {
		t.Fatalf("expected 2 files in page, got %d", len(res.Files))
	}
}

func TestRepositoryDelete(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()

	ctx := context.Background()

	file := &FileRecord{
		ID:          "del-01",
		Filename:    "to-delete.txt",
		Path:        "trash/to-delete.txt",
		StorageType: "local",
		MimeType:    "text/plain",
		Bytes:       100,
		Thumbnail: &ThumbnailRecord{
			Path:     "trash/thumbnails/to-delete.txt",
			Filename: "to-delete.txt",
			Bytes:    50,
			Width:    50,
			Height:   50,
			Format:   "png",
		},
	}

	if err := repo.SaveFile(ctx, file); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	if err := repo.DeleteFile(ctx, "del-01"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err := repo.GetFileByID(ctx, "del-01")
	if err == nil {
		t.Fatal("expected error getting deleted file, got nil")
	}
}
