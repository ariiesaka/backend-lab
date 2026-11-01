package featurecontrol

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var _ Store = (*Snapshot)(nil)

const (
	testRefreshInterval = time.Minute
	testSnapshotTimeout = 10 * time.Second
)

// fakeSnapshotStore stands in for the database and counts how often it is
// called. If release is set, calls block until it is closed or the context is
// done.
type fakeSnapshotStore struct {
	mu       sync.Mutex
	features []Feature
	err      error
	panicVal any
	release  chan struct{}
	calls    int
}

func (s *fakeSnapshotStore) ListFeatures(ctx context.Context) ([]Feature, error) {
	s.mu.Lock()
	s.calls++
	release := s.release
	s.mu.Unlock()

	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.panicVal != nil {
		panic(s.panicVal)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.features, nil
}

func (s *fakeSnapshotStore) update(change func(*fakeSnapshotStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
}

func (s *fakeSnapshotStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// newTestSnapshot returns a Snapshot serving "on" and "off", and a channel that
// receives each refresh error. It must run inside a synctest bubble.
func newTestSnapshot(t *testing.T) (*Snapshot, *fakeSnapshotStore, chan error) {
	t.Helper()

	store := &fakeSnapshotStore{features: []Feature{{Key: "on", Enabled: true}, {Key: "off"}}}
	refreshErrs := make(chan error, 10)
	snapshot, err := NewSnapshot(context.Background(), store, SnapshotConfig{
		RefreshInterval: testRefreshInterval,
		LoadTimeout:     testSnapshotTimeout,
		OnRefreshError:  func(err error) { refreshErrs <- err },
	})
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	t.Cleanup(snapshot.Close)
	return snapshot, store, refreshErrs
}

func assertSnapshotEnabled(t *testing.T, snapshot *Snapshot, key string, want bool) {
	t.Helper()

	got, err := snapshot.IsEnabled(context.Background(), key)
	if err != nil {
		t.Fatalf("IsEnabled(%q): %v", key, err)
	}
	if got != want {
		t.Errorf("IsEnabled(%q) = %v, want %v", key, got, want)
	}
}

// advance moves the bubble's clock and waits for the refresh loop to settle.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func TestNewSnapshotRejectsInvalidConfig(t *testing.T) {
	ctx := context.Background()

	if _, err := NewSnapshot(ctx, nil, SnapshotConfig{RefreshInterval: time.Minute, LoadTimeout: time.Second}); err == nil {
		t.Error("nil store: expected error")
	}
	for _, invalid := range []time.Duration{0, -time.Second} {
		if _, err := NewSnapshot(ctx, &fakeSnapshotStore{}, SnapshotConfig{RefreshInterval: invalid, LoadTimeout: time.Second}); err == nil {
			t.Errorf("refresh interval %s: expected error", invalid)
		}
		if _, err := NewSnapshot(ctx, &fakeSnapshotStore{}, SnapshotConfig{RefreshInterval: time.Minute, LoadTimeout: invalid}); err == nil {
			t.Errorf("load timeout %s: expected error", invalid)
		}
	}
}

func TestNewSnapshotFailsIfFirstLoadFails(t *testing.T) {
	errDB := errors.New("db down")
	store := &fakeSnapshotStore{err: errDB}

	_, err := NewSnapshot(context.Background(), store, SnapshotConfig{RefreshInterval: time.Minute, LoadTimeout: time.Second})
	if !errors.Is(err, errDB) {
		t.Errorf("err = %v, want %v", err, errDB)
	}
}

func TestNewSnapshotTimesOutOnHungStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeSnapshotStore{release: make(chan struct{})}
		start := time.Now()

		_, err := NewSnapshot(context.Background(), store, SnapshotConfig{RefreshInterval: time.Hour, LoadTimeout: testSnapshotTimeout})

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want %v", err, context.DeadlineExceeded)
		}
		if elapsed := time.Since(start); elapsed != testSnapshotTimeout {
			t.Errorf("elapsed = %s, want %s", elapsed, testSnapshotTimeout)
		}
	})
}

func TestNewSnapshotHonoursCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := &fakeSnapshotStore{release: make(chan struct{})}
	_, err := NewSnapshot(ctx, store, SnapshotConfig{RefreshInterval: time.Hour, LoadTimeout: time.Hour})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want %v", err, context.Canceled)
	}
}

func TestSnapshotReadsDoNotCallStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, store, _ := newTestSnapshot(t)

		for i := 0; i < 3; i++ {
			assertSnapshotEnabled(t, snapshot, "on", true)
			assertSnapshotEnabled(t, snapshot, "off", false)
			if _, err := snapshot.GetFeature(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
				t.Errorf("missing feature: err = %v, want %v", err, ErrNotFound)
			}
		}

		if calls := store.callCount(); calls != 1 {
			t.Errorf("store calls = %d, want 1", calls)
		}
	})
}

func TestSnapshotRefreshesOnInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, store, _ := newTestSnapshot(t)
		loadedAt := snapshot.Stats().LastRefresh

		// One feature changes, one is removed and one is added.
		store.update(func(s *fakeSnapshotStore) {
			s.features = []Feature{{Key: "on"}, {Key: "new", Enabled: true}}
		})

		advance(testRefreshInterval - time.Nanosecond)
		assertSnapshotEnabled(t, snapshot, "on", true)

		advance(time.Nanosecond)
		assertSnapshotEnabled(t, snapshot, "on", false)
		assertSnapshotEnabled(t, snapshot, "new", true)
		if _, err := snapshot.GetFeature(context.Background(), "off"); !errors.Is(err, ErrNotFound) {
			t.Errorf("removed feature: err = %v, want %v", err, ErrNotFound)
		}

		stats := snapshot.Stats()
		want := SnapshotStats{Refreshes: 2, Features: 2, LastRefresh: loadedAt.Add(testRefreshInterval)}
		if stats != want {
			t.Errorf("Stats = %+v, want %+v", stats, want)
		}
	})
}

func TestSnapshotKeepsLastGoodSetWhenRefreshFails(t *testing.T) {
	failures := []struct {
		name   string
		change func(*fakeSnapshotStore)
		want   string
	}{
		{name: "store error", change: func(s *fakeSnapshotStore) { s.err = errors.New("db down") }, want: "db down"},
		{name: "store panic", change: func(s *fakeSnapshotStore) { s.panicVal = "boom" }, want: "store panicked: boom"},
	}

	for _, tt := range failures {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				snapshot, store, refreshErrs := newTestSnapshot(t)
				loadedAt := snapshot.Stats().LastRefresh

				store.update(tt.change)
				advance(10 * testRefreshInterval)

				// There is no age limit, however long the failure lasts.
				assertSnapshotEnabled(t, snapshot, "on", true)
				stats := snapshot.Stats()
				want := SnapshotStats{Refreshes: 1, RefreshErrors: 10, Features: 2, LastRefresh: loadedAt}
				if stats != want {
					t.Errorf("Stats = %+v, want %+v", stats, want)
				}
				if err := <-refreshErrs; !strings.Contains(err.Error(), tt.want) {
					t.Errorf("OnRefreshError err = %v, want it to contain %q", err, tt.want)
				}

				// The database recovered, so the next refresh picks up changes.
				store.update(func(s *fakeSnapshotStore) {
					s.err, s.panicVal = nil, nil
					s.features = []Feature{{Key: "on"}}
				})
				advance(testRefreshInterval)
				assertSnapshotEnabled(t, snapshot, "on", false)
			})
		})
	}
}

func TestSnapshotServesReadsDuringHungRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, store, refreshErrs := newTestSnapshot(t)

		store.update(func(s *fakeSnapshotStore) { s.release = make(chan struct{}) })
		advance(testRefreshInterval)

		// The refresh is stuck, and reads are answered without waiting.
		start := time.Now()
		assertSnapshotEnabled(t, snapshot, "on", true)
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("read waited %s, want no wait", elapsed)
		}

		advance(testSnapshotTimeout)
		if err := <-refreshErrs; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("OnRefreshError err = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func TestSnapshotCloseStopsRefreshing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, store, _ := newTestSnapshot(t)

		snapshot.Close()
		snapshot.Close() // Closing twice is safe.

		advance(10 * testRefreshInterval)
		if calls := store.callCount(); calls != 1 {
			t.Errorf("store calls = %d, want 1", calls)
		}
		assertSnapshotEnabled(t, snapshot, "on", true)
	})
}

func TestSnapshotCloseInterruptsRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, store, refreshErrs := newTestSnapshot(t)

		store.update(func(s *fakeSnapshotStore) { s.release = make(chan struct{}) })
		advance(testRefreshInterval)

		start := time.Now()
		snapshot.Close()
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Close waited %s, want no wait", elapsed)
		}

		if got := snapshot.Stats().RefreshErrors; got != 0 {
			t.Errorf("RefreshErrors = %d, want 0", got)
		}
		select {
		case err := <-refreshErrs:
			t.Errorf("OnRefreshError err = %v, want no call", err)
		default:
		}
	})
}

func TestSnapshotConcurrentAccess(t *testing.T) {
	store := &fakeSnapshotStore{features: []Feature{{Key: "on", Enabled: true}}}
	snapshot, err := NewSnapshot(context.Background(), store, SnapshotConfig{
		RefreshInterval: 100 * time.Microsecond,
		LoadTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	deadline := time.Now().Add(100 * time.Millisecond)

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				if _, err := snapshot.IsEnabled(context.Background(), "on"); err != nil {
					t.Errorf("IsEnabled: %v", err)
				}
				snapshot.Stats()
			}
		})
	}

	// The store fails on and off, so both refresh paths run during the reads.
	for time.Now().Before(deadline) {
		store.update(func(s *fakeSnapshotStore) {
			if s.err == nil {
				s.err = errors.New("db down")
			} else {
				s.err = nil
			}
		})
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}

func BenchmarkCacheRead(b *testing.B) {
	store := &fakeStore{features: map[string]Feature{"on": {Key: "on", Enabled: true}}}
	cache, err := NewCache(store, Config{TTL: time.Hour, LoadTimeout: time.Second})
	if err != nil {
		b.Fatalf("NewCache: %v", err)
	}

	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.IsEnabled(ctx, "on")
		}
	})
}

func BenchmarkSnapshotRead(b *testing.B) {
	store := &fakeSnapshotStore{features: []Feature{{Key: "on", Enabled: true}}}
	snapshot, err := NewSnapshot(context.Background(), store, SnapshotConfig{RefreshInterval: time.Hour, LoadTimeout: time.Second})
	if err != nil {
		b.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			snapshot.IsEnabled(ctx, "on")
		}
	})
}
