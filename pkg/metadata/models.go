package metadata

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// JSONMap represents arbitrary user-defined metadata stored as JSON/JSONB.
type JSONMap map[string]interface{}

// Value implements driver.Valuer for database serialization.
func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return "{}", nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner for database deserialization.
func (j *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*j = make(JSONMap)
		return nil
	}
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return errors.New("unsupported type for JSONMap")
	}
	if len(bytes) == 0 {
		*j = make(JSONMap)
		return nil
	}
	return json.Unmarshal(bytes, j)
}

// FileRecord represents persistent metadata of an uploaded file.
type FileRecord struct {
	ID               string           `json:"id" gorm:"primaryKey;size:64"`
	Filename         string           `json:"filename" gorm:"size:255;index"`
	OriginalFilename string           `json:"original_filename" gorm:"size:255;index"`
	Path             string           `json:"path" gorm:"size:512;uniqueIndex;not null"`
	StorageType      string           `json:"storage_type" gorm:"size:32;not null"`
	MimeType         string           `json:"mime_type" gorm:"size:128;index"`
	Bytes            int64            `json:"bytes" gorm:"not null"`
	Collection       string           `json:"collection" gorm:"size:128;index"`
	UserID           string           `json:"user_id,omitempty" gorm:"size:128;index"`
	Description      string           `json:"description,omitempty" gorm:"type:text"`
	Metadata         JSONMap          `json:"metadata,omitempty" gorm:"type:jsonb"`
	Thumbnail        *ThumbnailRecord `json:"thumbnail,omitempty" gorm:"foreignKey:FileRecordID;constraint:OnDelete:CASCADE"`
	CreatedAt        time.Time        `json:"created_at" gorm:"index"`
	UpdatedAt        time.Time        `json:"updated_at"`
}

// ThumbnailRecord represents metadata for a generated image thumbnail linked to a FileRecord.
type ThumbnailRecord struct {
	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	FileRecordID string    `json:"file_record_id" gorm:"size:64;index;not null"`
	Path         string    `json:"path" gorm:"size:512;not null"`
	Filename     string    `json:"filename" gorm:"size:255;not null"`
	Bytes        int64     `json:"bytes" gorm:"not null"`
	Width        int       `json:"width" gorm:"not null"`
	Height       int       `json:"height" gorm:"not null"`
	Format       string    `json:"format" gorm:"size:32;not null"`
	CreatedAt    time.Time `json:"created_at"`
}

// QueryFilter defines filtering, search, and pagination criteria.
type QueryFilter struct {
	Collection string `json:"collection"`
	MimeType   string `json:"mime_type"`
	UserID     string `json:"user_id"`
	Search     string `json:"search"` // matches filename or description
	Limit      int    `json:"limit"`
	Offset     int    `json:"offset"`
	Sort       string `json:"sort"`   // e.g. "created_at desc"
}

// QueryResult represents a paginated list of file records.
type QueryResult struct {
	Total  int64        `json:"total"`
	Limit  int          `json:"limit"`
	Offset int          `json:"offset"`
	Files  []FileRecord `json:"files"`
}
