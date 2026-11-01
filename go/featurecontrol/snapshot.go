package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// SnapshotStore loads every feature from the source of truth.
type SnapshotStore interface {
	ListFeatures(ctx context.Context) ([]Feature, error)
}

// SnapshotConfig holds the Snapshot settings. RefreshInterval and LoadTimeout
// are required.
type SnapshotConfig struct {
	// RefreshInterval is how often every feature is reloaded.
	RefreshInterval time.Duration
	// LoadTimeout limits a single call to the SnapshotStore.
	LoadTimeout time.Duration
	// OnRefreshError, if set, is called when a background refresh fails, so the
	// failure can be logged or counted. It must not panic.
	OnRefreshError func(err error)
}

// SnapshotStats describes what the Snapshot has done since it was created.
type SnapshotStats struct {
	// Refreshes is successful loads, including the first.
	Refreshes uint64
	// RefreshErrors is background refreshes that failed.
	RefreshErrors uint64
	// Features is how many features are being served.
	Features int
	// LastRefresh is when the features being served were loaded.
	LastRefresh time.Time
}

type snapshotData struct {
	features map[string]Feature
	loadedAt time.Time
}

// Snapshot is a Store that keeps every feature in memory and reloads them all
// on a timer. Reads never touch the SnapshotStore, and a failed refresh keeps
// serving the last good set for as long as it takes to recover. It is safe for
// concurrent use.
type Snapshot struct {
	store          SnapshotStore
	loadTimeout    time.Duration
	onRefreshError func(err error)

	data          atomic.Pointer[snapshotData]
	refreshes     atomic.Uint64
	refreshErrors atomic.Uint64

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// NewSnapshot loads every feature, returning an error if that fails, and then
// refreshes them in the background until Close is called.
func NewSnapshot(ctx context.Context, store SnapshotStore, cfg SnapshotConfig) (*Snapshot, error) {
	if store == nil {
		return nil, errors.New("featurecontrol: store is required")
	}
	if cfg.RefreshInterval <= 0 {
		return nil, fmt.Errorf("featurecontrol: refresh interval must be positive, got %s", cfg.RefreshInterval)
	}
	if cfg.LoadTimeout <= 0 {
		return nil, fmt.Errorf("featurecontrol: load timeout must be positive, got %s", cfg.LoadTimeout)
	}

	s := &Snapshot{
		store:          store,
		loadTimeout:    cfg.LoadTimeout,
		onRefreshError: cfg.OnRefreshError,
		done:           make(chan struct{}),
	}
	if err := s.load(ctx); err != nil {
		return nil, fmt.Errorf("featurecontrol: first load: %w", err)
	}

	refreshCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.refreshLoop(refreshCtx, cfg.RefreshInterval)

	return s, nil
}

// GetFeature returns the feature from memory, or ErrNotFound if the last good
// set does not have it.
func (s *Snapshot) GetFeature(_ context.Context, key string) (Feature, error) {
	feature, ok := s.data.Load().features[key]
	if !ok {
		return Feature{}, ErrNotFound
	}
	return feature, nil
}

// IsEnabled reports whether the feature is switched on.
func (s *Snapshot) IsEnabled(ctx context.Context, key string) (bool, error) {
	feature, err := s.GetFeature(ctx, key)
	if err != nil {
		return false, err
	}
	return feature.Enabled, nil
}

// Stats returns the current counters.
func (s *Snapshot) Stats() SnapshotStats {
	data := s.data.Load()
	return SnapshotStats{
		Refreshes:     s.refreshes.Load(),
		RefreshErrors: s.refreshErrors.Load(),
		Features:      len(data.features),
		LastRefresh:   data.loadedAt,
	}
}

// Close stops refreshing and waits for the background work to end. The last
// good set can still be read afterwards.
func (s *Snapshot) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
	})
}

func (s *Snapshot) refreshLoop(ctx context.Context, interval time.Duration) {
	defer close(s.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		err := s.load(ctx)
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return // Close interrupted the refresh, so it did not fail.
		}
		s.refreshErrors.Add(1)
		if s.onRefreshError != nil {
			s.onRefreshError(err)
		}
	}
}

// load replaces the features being served. A panic becomes an error, because
// the refresh runs in its own goroutine where it would crash the process.
func (s *Snapshot) load(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("featurecontrol: store panicked: %v\n%s", r, debug.Stack())
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, s.loadTimeout)
	defer cancel()

	list, err := s.store.ListFeatures(ctx)
	if err != nil {
		return err
	}

	features := make(map[string]Feature, len(list))
	for _, feature := range list {
		features[feature.Key] = feature
	}

	s.data.Store(&snapshotData{features: features, loadedAt: time.Now()})
	s.refreshes.Add(1)
	return nil
}
