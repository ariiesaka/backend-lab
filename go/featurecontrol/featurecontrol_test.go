package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

const testLoadTimeout = time.Minute

// fakeStore stands in for the database and counts how often it is called.
// If release is set, calls block until it is closed or the context is done.
type fakeStore struct {
	mu       sync.Mutex
	features map[string]Feature
	err      error
	release  chan struct{}
	calls    int
}

func (s *fakeStore) GetFeature(ctx context.Context, key string) (Feature, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return Feature{}, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return Feature{}, s.err
	}
	feature, ok := s.features[key]
	if !ok {
		return Feature{}, ErrNotFound
	}
	return feature, nil
}

func newTestCache(t *testing.T, store Store, ttl time.Duration) (*Cache, *time.Time) {
	t.Helper()

	cache, err := NewCache(store, Config{TTL: ttl, LoadTimeout: testLoadTimeout})
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	return cache, &now
}

func TestNewCacheRejectsInvalidConfig(t *testing.T) {
	if _, err := NewCache(nil, Config{TTL: time.Minute, LoadTimeout: time.Second}); err == nil {
		t.Error("nil store: expected error")
	}
	for _, invalid := range []time.Duration{0, -time.Second} {
		if _, err := NewCache(&fakeStore{}, Config{TTL: invalid, LoadTimeout: time.Second}); err == nil {
			t.Errorf("ttl %s: expected error", invalid)
		}
		if _, err := NewCache(&fakeStore{}, Config{TTL: time.Minute, LoadTimeout: invalid}); err == nil {
			t.Errorf("load timeout %s: expected error", invalid)
		}
	}
	if _, err := NewCache(&fakeStore{}, Config{TTL: time.Minute, LoadTimeout: time.Second, MaxStale: -time.Second}); err == nil {
		t.Error("negative max stale: expected error")
	}
}

func TestValidValueSkipsStore(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)

	for i := 0; i < 3; i++ {
		enabled, err := cache.IsEnabled(context.Background(), "checkout")
		if err != nil {
			t.Fatalf("IsEnabled: %v", err)
		}
		if !enabled {
			t.Fatal("expected feature to be enabled")
		}
		*now = now.Add(20 * time.Second)
	}

	if store.calls != 1 {
		t.Errorf("store calls = %d, want 1", store.calls)
	}
}

func TestExpiredValueReloadsFromStore(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	// The cache keeps serving the old value until the TTL has passed.
	store.features["checkout"] = Feature{Key: "checkout", Enabled: false}
	*now = now.Add(time.Minute - time.Nanosecond)
	if enabled, _ := cache.IsEnabled(ctx, "checkout"); !enabled {
		t.Error("just before ttl: expected the cached value")
	}

	*now = now.Add(time.Nanosecond)
	if enabled, _ := cache.IsEnabled(ctx, "checkout"); enabled {
		t.Error("at ttl: expected the reloaded value")
	}

	if store.calls != 2 {
		t.Errorf("store calls = %d, want 2", store.calls)
	}
}

func TestKeysAreCachedIndependently(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{
		"a": {Key: "a", Enabled: true},
		"b": {Key: "b", Enabled: false},
	}}
	cache, _ := newTestCache(t, store, time.Minute)
	ctx := context.Background()

	for _, want := range []Feature{store.features["a"], store.features["b"], store.features["a"]} {
		got, err := cache.GetFeature(ctx, want.Key)
		if err != nil {
			t.Fatalf("GetFeature(%q): %v", want.Key, err)
		}
		if got != want {
			t.Errorf("GetFeature(%q) = %+v, want %+v", want.Key, got, want)
		}
	}

	if store.calls != 2 {
		t.Errorf("store calls = %d, want 2", store.calls)
	}
}

func TestErrorsAreNotCached(t *testing.T) {
	errDB := errors.New("db down")

	tests := []struct {
		name  string
		store *fakeStore
		want  error
	}{
		{name: "not found", store: &fakeStore{}, want: ErrNotFound},
		{name: "store failure", store: &fakeStore{err: errDB}, want: errDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, _ := newTestCache(t, tt.store, time.Minute)

			for i := 0; i < 2; i++ {
				if _, err := cache.IsEnabled(context.Background(), "missing"); !errors.Is(err, tt.want) {
					t.Fatalf("err = %v, want %v", err, tt.want)
				}
			}
			if tt.store.calls != 2 {
				t.Errorf("store calls = %d, want 2", tt.store.calls)
			}
		})
	}
}

func TestConcurrentMissesShareOneLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{
			features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}},
			release:  make(chan struct{}),
		}
		cache, _ := newTestCache(t, store, time.Minute)

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Go(func() {
				enabled, err := cache.IsEnabled(context.Background(), "checkout")
				if err != nil {
					t.Errorf("IsEnabled: %v", err)
				}
				if !enabled {
					t.Error("expected feature to be enabled")
				}
			})
		}

		// Every caller is now waiting on the load.
		synctest.Wait()
		close(store.release)
		wg.Wait()

		if store.calls != 1 {
			t.Errorf("store calls = %d, want 1", store.calls)
		}
	})
}

func TestConcurrentMissesShareOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errDB := errors.New("db down")
		store := &fakeStore{err: errDB, release: make(chan struct{})}
		cache, _ := newTestCache(t, store, time.Minute)

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Go(func() {
				if _, err := cache.GetFeature(context.Background(), "checkout"); !errors.Is(err, errDB) {
					t.Errorf("err = %v, want %v", err, errDB)
				}
			})
		}

		synctest.Wait()
		close(store.release)
		wg.Wait()

		if store.calls != 1 {
			t.Errorf("store calls = %d, want 1", store.calls)
		}
	})
}

func TestCancelledCallerDoesNotFailOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{
			features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}},
			release:  make(chan struct{}),
		}
		cache, _ := newTestCache(t, store, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		first := make(chan error, 1)
		go func() {
			_, err := cache.GetFeature(ctx, "checkout")
			first <- err
		}()
		synctest.Wait() // The first caller has started the load.

		second := make(chan error, 1)
		go func() {
			_, err := cache.GetFeature(context.Background(), "checkout")
			second <- err
		}()
		synctest.Wait()

		cancel()
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Errorf("first caller err = %v, want %v", err, context.Canceled)
		}

		close(store.release)
		if err := <-second; err != nil {
			t.Errorf("second caller err = %v, want nil", err)
		}

		// The load finished, so its result is cached.
		if _, err := cache.GetFeature(context.Background(), "checkout"); err != nil {
			t.Errorf("GetFeature: %v", err)
		}
		if store.calls != 1 {
			t.Errorf("store calls = %d, want 1", store.calls)
		}
	})
}

func TestLoadTimeoutStopsStuckStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{release: make(chan struct{})}
		cache, _ := newTestCache(t, store, time.Minute)

		start := time.Now()
		_, err := cache.GetFeature(context.Background(), "checkout")

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want %v", err, context.DeadlineExceeded)
		}
		if elapsed := time.Since(start); elapsed != testLoadTimeout {
			t.Errorf("elapsed = %s, want %s", elapsed, testLoadTimeout)
		}
	})
}

func TestLoadRechecksCache(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, _ := newTestCache(t, store, time.Minute)
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	// A caller that missed just before the previous load finished gets here.
	if _, err := cache.load(ctx, "checkout"); err != nil {
		t.Fatalf("load: %v", err)
	}

	if store.calls != 1 {
		t.Errorf("store calls = %d, want 1", store.calls)
	}
}

func TestFailedReloadServesExpiredValue(t *testing.T) {
	errDB := errors.New("db down")

	tests := []struct {
		name      string
		maxStale  time.Duration
		pastTTL   time.Duration
		wantStale bool
	}{
		{name: "within max stale", maxStale: time.Minute, pastTTL: time.Minute - time.Nanosecond, wantStale: true},
		{name: "at max stale", maxStale: time.Minute, pastTTL: time.Minute, wantStale: false},
		{name: "past max stale", maxStale: time.Minute, pastTTL: time.Hour, wantStale: false},
		{name: "turned off", maxStale: 0, pastTTL: 0, wantStale: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
			cache, now := newTestCache(t, store, time.Minute)
			cache.maxStale = tt.maxStale

			var staleErrs []error
			cache.onStale = func(_ string, err error) { staleErrs = append(staleErrs, err) }

			ctx := context.Background()
			if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
				t.Fatalf("GetFeature: %v", err)
			}

			store.err = errDB
			*now = now.Add(time.Minute + tt.pastTTL)
			got, err := cache.GetFeature(ctx, "checkout")

			if store.calls != 2 {
				t.Errorf("store calls = %d, want 2", store.calls)
			}
			if tt.wantStale {
				if err != nil || !got.Enabled {
					t.Errorf("got %+v, %v, want the expired value", got, err)
				}
				if len(staleErrs) != 1 || !errors.Is(staleErrs[0], errDB) {
					t.Errorf("onStale errors = %v, want [%v]", staleErrs, errDB)
				}
				return
			}
			if !errors.Is(err, errDB) {
				t.Errorf("err = %v, want %v", err, errDB)
			}
			if len(staleErrs) != 0 {
				t.Errorf("onStale errors = %v, want none", staleErrs)
			}
		})
	}
}

func TestServingExpiredValueDoesNotRenewIt(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.maxStale = time.Minute
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	store.err = errors.New("db down")
	*now = now.Add(time.Minute + time.Second)
	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature during outage: %v", err)
	}

	// The database recovered, so the next call must read the new value.
	store.err = nil
	store.features["checkout"] = Feature{Key: "checkout", Enabled: false}
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || enabled {
		t.Errorf("IsEnabled = %v, %v, want the reloaded value", enabled, err)
	}
	if store.calls != 3 {
		t.Errorf("store calls = %d, want 3", store.calls)
	}
}

func TestLoadTimeoutServesExpiredValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
		cache, now := newTestCache(t, store, time.Minute)
		cache.maxStale = time.Minute
		ctx := context.Background()

		if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
			t.Fatalf("GetFeature: %v", err)
		}

		store.release = make(chan struct{})
		*now = now.Add(time.Minute)
		if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
			t.Errorf("IsEnabled = %v, %v, want the expired value", enabled, err)
		}
	})
}

func TestDeletedFeatureIsNotServedExpired(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.maxStale = time.Hour
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	delete(store.features, "checkout")
	*now = now.Add(time.Minute)
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after delete: err = %v, want %v", err, ErrNotFound)
	}

	// A later outage must not bring the deleted feature back.
	errDB := errors.New("db down")
	store.err = errDB
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, errDB) {
		t.Errorf("during outage: err = %v, want %v", err, errDB)
	}
}

func TestFailedFirstLoadReturnsError(t *testing.T) {
	errDB := errors.New("db down")
	cache, _ := newTestCache(t, &fakeStore{err: errDB}, time.Minute)
	cache.maxStale = time.Hour

	if _, err := cache.GetFeature(context.Background(), "checkout"); !errors.Is(err, errDB) {
		t.Errorf("err = %v, want %v", err, errDB)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{}}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("feature-%d", i)
		store.features[key] = Feature{Key: key, Enabled: true}
	}

	cache, err := NewCache(store, Config{TTL: time.Millisecond, LoadTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("feature-%d", (g+i)%10)
				if _, err := cache.GetFeature(context.Background(), key); err != nil {
					t.Errorf("GetFeature(%q): %v", key, err)
				}
			}
		}(g)
	}
	wg.Wait()
}
