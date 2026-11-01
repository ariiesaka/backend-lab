package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	panicVal any
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

	if s.panicVal != nil {
		panic(s.panicVal)
	}
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
	if _, err := NewCache(&fakeStore{}, Config{TTL: time.Minute, LoadTimeout: time.Second, ErrorBackoff: -time.Second}); err == nil {
		t.Error("negative error backoff: expected error")
	}
	if _, err := NewCache(&fakeStore{}, Config{TTL: time.Minute, LoadTimeout: time.Second, NotFoundTTL: -time.Second}); err == nil {
		t.Error("negative not found ttl: expected error")
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
	if r := cache.load(ctx, "checkout"); r.err != nil {
		t.Fatalf("load: %v", r.err)
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

func TestConcurrentMissesShareOneExpiredValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
		cache, now := newTestCache(t, store, time.Minute)
		cache.maxStale = time.Minute

		staleCalls := 0
		cache.onStale = func(string, error) { staleCalls++ }

		if _, err := cache.GetFeature(context.Background(), "checkout"); err != nil {
			t.Fatalf("GetFeature: %v", err)
		}

		store.err = errors.New("db down")
		store.release = make(chan struct{})
		*now = now.Add(time.Minute)

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Go(func() {
				enabled, err := cache.IsEnabled(context.Background(), "checkout")
				if err != nil || !enabled {
					t.Errorf("IsEnabled = %v, %v, want the expired value", enabled, err)
				}
			})
		}

		synctest.Wait()
		close(store.release)
		wg.Wait()

		if store.calls != 2 {
			t.Errorf("store calls = %d, want 2", store.calls)
		}
		if staleCalls != 1 {
			t.Errorf("onStale calls = %d, want 1", staleCalls)
		}
	})
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

func TestBackoffSkipsStoreAfterFailure(t *testing.T) {
	errDB := errors.New("db down")
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}, err: errDB}
	cache, now := newTestCache(t, store, time.Minute)
	cache.errorBackoff = 10 * time.Second
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, errDB) {
			t.Fatalf("err = %v, want %v", err, errDB)
		}
	}
	if store.calls != 1 {
		t.Errorf("during backoff: store calls = %d, want 1", store.calls)
	}

	*now = now.Add(10*time.Second - time.Nanosecond)
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, errDB) {
		t.Errorf("just before retry: err = %v, want %v", err, errDB)
	}
	if store.calls != 1 {
		t.Errorf("just before retry: store calls = %d, want 1", store.calls)
	}

	// The database recovered, so the retry succeeds and clears the failure.
	store.err = nil
	*now = now.Add(time.Nanosecond)
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
		t.Errorf("at retry: IsEnabled = %v, %v, want the loaded value", enabled, err)
	}
	if store.calls != 2 {
		t.Errorf("at retry: store calls = %d, want 2", store.calls)
	}

	store.err = errDB
	*now = now.Add(time.Minute - time.Nanosecond)
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
		t.Errorf("after recovery: IsEnabled = %v, %v, want the cached value", enabled, err)
	}
	if store.calls != 2 {
		t.Errorf("after recovery: store calls = %d, want 2", store.calls)
	}
}

func TestBackoffServesExpiredValue(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.maxStale = 15 * time.Second
	cache.errorBackoff = 10 * time.Second

	staleCalls := 0
	cache.onStale = func(string, error) { staleCalls++ }

	ctx := context.Background()
	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	errDB := errors.New("db down")
	store.err = errDB
	*now = now.Add(time.Minute)
	for i := 0; i < 3; i++ {
		if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
			t.Fatalf("IsEnabled = %v, %v, want the expired value", enabled, err)
		}
	}
	if store.calls != 2 {
		t.Errorf("store calls = %d, want 2", store.calls)
	}
	if staleCalls != 1 {
		t.Errorf("onStale calls = %d, want 1", staleCalls)
	}

	// The next retry fails at 10s and starts a new backoff, which outlasts
	// MaxStale, so the cap still applies during it.
	*now = now.Add(10 * time.Second)
	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("second failed load: %v", err)
	}
	*now = now.Add(5 * time.Second)
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, errDB) {
		t.Errorf("past max stale during backoff: err = %v, want %v", err, errDB)
	}
	if store.calls != 3 {
		t.Errorf("store calls = %d, want 3", store.calls)
	}
}

func TestBackoffAnswersWithoutWaitingOnHungStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{release: make(chan struct{})}
		cache, _ := newTestCache(t, store, time.Minute)
		cache.errorBackoff = time.Hour
		ctx := context.Background()

		if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first call: err = %v, want %v", err, context.DeadlineExceeded)
		}

		start := time.Now()
		if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("second call: err = %v, want %v", err, context.DeadlineExceeded)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("second call waited %s, want no wait", elapsed)
		}
		if store.calls != 1 {
			t.Errorf("store calls = %d, want 1", store.calls)
		}
	})
}

func TestStorePanicBecomesError(t *testing.T) {
	store := &fakeStore{panicVal: "boom"}
	cache, _ := newTestCache(t, store, time.Minute)
	ctx := context.Background()

	_, err := cache.GetFeature(ctx, "checkout")
	if err == nil || !strings.Contains(err.Error(), "store panicked: boom") {
		t.Fatalf("err = %v, want it to report the panic", err)
	}

	// The key is not stuck, and the store is tried again.
	store.panicVal = nil
	store.features = map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
		t.Errorf("IsEnabled = %v, %v, want the loaded value", enabled, err)
	}
}

func TestStorePanicServesExpiredValue(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.maxStale = time.Minute
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); err != nil {
		t.Fatalf("GetFeature: %v", err)
	}

	store.panicVal = "boom"
	*now = now.Add(time.Minute)
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
		t.Errorf("IsEnabled = %v, %v, want the expired value", enabled, err)
	}
}

func TestNotFoundIsRemembered(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.notFoundTTL = 30 * time.Second
	cache.maxStale = time.Hour
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want %v", err, ErrNotFound)
		}
	}
	if store.calls != 1 {
		t.Errorf("store calls = %d, want 1", store.calls)
	}

	// The feature is created, and shows up once the answer expires.
	store.features["checkout"] = Feature{Key: "checkout", Enabled: true}
	*now = now.Add(30*time.Second - time.Nanosecond)
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, ErrNotFound) {
		t.Errorf("just before expiry: err = %v, want %v", err, ErrNotFound)
	}
	*now = now.Add(time.Nanosecond)
	if enabled, err := cache.IsEnabled(ctx, "checkout"); err != nil || !enabled {
		t.Errorf("at expiry: IsEnabled = %v, %v, want the new feature", enabled, err)
	}
}

func TestExpiredNotFoundIsNotServedOnFailure(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.notFoundTTL = 30 * time.Second
	cache.maxStale = time.Hour
	ctx := context.Background()

	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want %v", err, ErrNotFound)
	}

	errDB := errors.New("db down")
	store.err = errDB
	*now = now.Add(30 * time.Second)
	if _, err := cache.GetFeature(ctx, "checkout"); !errors.Is(err, errDB) {
		t.Errorf("err = %v, want %v", err, errDB)
	}
}

func TestStats(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{"checkout": {Key: "checkout", Enabled: true}}}
	cache, now := newTestCache(t, store, time.Minute)
	cache.maxStale = time.Minute
	cache.errorBackoff = 10 * time.Second
	cache.notFoundTTL = 30 * time.Second
	ctx := context.Background()

	cache.GetFeature(ctx, "checkout") // Miss and load.
	cache.GetFeature(ctx, "checkout") // Hit.
	cache.GetFeature(ctx, "missing")  // Miss and load, not a load error.
	cache.GetFeature(ctx, "missing")  // Hit.

	store.err = errors.New("db down")
	*now = now.Add(time.Minute)
	cache.GetFeature(ctx, "checkout") // Miss, failed load, stale.
	cache.GetFeature(ctx, "checkout") // Backed off, stale.

	want := Stats{Hits: 2, Misses: 3, BackedOff: 1, StaleServed: 2, Loads: 3, LoadErrors: 1}
	if got := cache.Stats(); got != want {
		t.Errorf("Stats = %+v, want %+v", got, want)
	}
}

// flakyStore fails every other call, and has every feature except "missing".
type flakyStore struct {
	calls atomic.Uint64
}

var errFlaky = errors.New("flaky store")

func (s *flakyStore) GetFeature(_ context.Context, key string) (Feature, error) {
	if s.calls.Add(1)%2 == 0 {
		return Feature{}, errFlaky
	}
	if key == "missing" {
		return Feature{}, ErrNotFound
	}
	return Feature{Key: key, Enabled: true}, nil
}

func TestConcurrentAccessWithFailures(t *testing.T) {
	var staleCalls atomic.Uint64
	cache, err := NewCache(&flakyStore{}, Config{
		TTL:          100 * time.Microsecond,
		LoadTimeout:  time.Second,
		MaxStale:     10 * time.Millisecond,
		ErrorBackoff: 100 * time.Microsecond,
		NotFoundTTL:  100 * time.Microsecond,
		OnStale:      func(string, error) { staleCalls.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	keys := []string{"a", "b", "c", "missing"}
	deadline := time.Now().Add(100 * time.Millisecond)

	var requests atomic.Uint64
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Go(func() {
			for i := g; time.Now().Before(deadline); i++ {
				key := keys[i%len(keys)]
				_, err := cache.GetFeature(context.Background(), key)
				requests.Add(1)
				if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, errFlaky) {
					t.Errorf("GetFeature(%q): %v", key, err)
				}
				cache.Stats()
			}
		})
	}
	wg.Wait()

	stats := cache.Stats()
	if answered := stats.Hits + stats.Misses + stats.BackedOff; answered != requests.Load() {
		t.Errorf("hits, misses and backed off = %d, want %d requests", answered, requests.Load())
	}

	// Every path must have run, or this test proves nothing about it.
	if stats.Hits == 0 || stats.BackedOff == 0 || stats.StaleServed == 0 || stats.LoadErrors == 0 || staleCalls.Load() == 0 {
		t.Errorf("a path was not exercised: %+v, onStale calls = %d", stats, staleCalls.Load())
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
