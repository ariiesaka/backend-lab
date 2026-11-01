package featurecontrol

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// PostgresStore is a Store backed by this Postgres table:
//
//	CREATE TABLE features (
//		key     TEXT PRIMARY KEY,
//		enabled BOOLEAN NOT NULL
//	);
//
// It works with any database/sql Postgres driver, which the application
// chooses and registers.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore returns a PostgresStore that reads through db.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// GetFeature reads a feature, returning ErrNotFound if it has no row.
func (s *PostgresStore) GetFeature(ctx context.Context, key string) (Feature, error) {
	feature := Feature{Key: key}

	err := s.db.QueryRowContext(ctx, `SELECT enabled FROM features WHERE key = $1`, key).Scan(&feature.Enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return Feature{}, ErrNotFound
	}
	if err != nil {
		return Feature{}, fmt.Errorf("featurecontrol: read feature %q: %w", key, err)
	}
	return feature, nil
}
