package featurecontrol

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// postgresURLEnv names the database used by these tests. They are skipped when
// it is unset, and they drop and recreate the features table when it is set.
const postgresURLEnv = "FEATURECONTROL_TEST_POSTGRES_URL"

func newTestPostgres(t *testing.T) *sql.DB {
	t.Helper()

	url := os.Getenv(postgresURLEnv)
	if url == "" {
		t.Skipf("%s is not set", postgresURLEnv)
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		execSQL(t, db, `DROP TABLE IF EXISTS features`)
		db.Close()
	})

	execSQL(t, db, `DROP TABLE IF EXISTS features`)
	execSQL(t, db, `CREATE TABLE features (key TEXT PRIMARY KEY, enabled BOOLEAN NOT NULL)`)
	execSQL(t, db, `INSERT INTO features (key, enabled) VALUES ('on', TRUE), ('off', FALSE)`)
	return db
}

func execSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// lockFeatures makes reads of the features table hang until the test ends, like
// a database that has stopped responding.
func lockFeatures(t *testing.T, db *sql.DB) {
	t.Helper()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback() })

	if _, err := tx.Exec(`LOCK TABLE features IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock: %v", err)
	}
}

func TestPostgresStoreGetFeature(t *testing.T) {
	store := NewPostgresStore(newTestPostgres(t))
	ctx := context.Background()

	for key, want := range map[string]bool{"on": true, "off": false} {
		got, err := store.GetFeature(ctx, key)
		if err != nil {
			t.Fatalf("GetFeature(%q): %v", key, err)
		}
		if got != (Feature{Key: key, Enabled: want}) {
			t.Errorf("GetFeature(%q) = %+v, want enabled %v", key, got, want)
		}
	}

	if _, err := store.GetFeature(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing feature: err = %v, want %v", err, ErrNotFound)
	}
}

func TestPostgresStoreReturnsDatabaseErrors(t *testing.T) {
	db := newTestPostgres(t)
	execSQL(t, db, `DROP TABLE features`)

	_, err := NewPostgresStore(db).GetFeature(context.Background(), "on")
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, a database failure must not look like %v", err, ErrNotFound)
	}
}

func TestPostgresStoreStopsWhenContextEnds(t *testing.T) {
	db := newTestPostgres(t)
	lockFeatures(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := NewPostgresStore(db).GetFeature(ctx, "on")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestCacheWithPostgres(t *testing.T) {
	db := newTestPostgres(t)
	cache, now := newTestCache(t, NewPostgresStore(db), time.Minute)
	cache.maxStale = time.Minute
	ctx := context.Background()

	assertEnabled := func(step string, want bool) {
		t.Helper()
		got, err := cache.IsEnabled(ctx, "on")
		if err != nil {
			t.Fatalf("%s: IsEnabled: %v", step, err)
		}
		if got != want {
			t.Errorf("%s: IsEnabled = %v, want %v", step, got, want)
		}
	}

	assertEnabled("first load", true)

	execSQL(t, db, `UPDATE features SET enabled = FALSE WHERE key = 'on'`)
	assertEnabled("still valid, so the database is not read", true)

	*now = now.Add(time.Minute)
	assertEnabled("expired, so the new row is read", false)

	execSQL(t, db, `DROP TABLE features`)
	*now = now.Add(time.Minute + time.Second)
	assertEnabled("reload failed within max stale", false)

	*now = now.Add(time.Minute)
	if _, err := cache.IsEnabled(ctx, "on"); err == nil {
		t.Error("reload failed past max stale: expected error")
	}
}

func TestCacheServesExpiredValueWhenPostgresHangs(t *testing.T) {
	db := newTestPostgres(t)
	cache, now := newTestCache(t, NewPostgresStore(db), time.Minute)
	cache.maxStale = time.Minute
	cache.loadTimeout = 200 * time.Millisecond

	var staleErr error
	cache.onStale = func(_ string, err error) { staleErr = err }

	ctx := context.Background()
	if _, err := cache.GetFeature(ctx, "on"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	lockFeatures(t, db)
	*now = now.Add(time.Minute)

	enabled, err := cache.IsEnabled(ctx, "on")
	if err != nil || !enabled {
		t.Errorf("IsEnabled = %v, %v, want the expired value", enabled, err)
	}
	if !errors.Is(staleErr, context.DeadlineExceeded) {
		t.Errorf("onStale err = %v, want %v", staleErr, context.DeadlineExceeded)
	}
}
