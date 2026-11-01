package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeStore stands in for the database and counts how often it is called.
type fakeStore struct {
	mu       sync.Mutex
	features map[string]Feature
	err      error
	calls    int
}

func (s *fakeStore) GetFeature(_ context.Context, key string) (Feature, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
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

	cache, err := NewCache(store, ttl)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	return cache, &now
}

func TestNewCacheRejectsInvalidConfig(t *testing.T) {
	if _, err := NewCache(nil, time.Minute); err == nil {
		t.Error("nil store: expected error")
	}
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := NewCache(&fakeStore{}, ttl); err == nil {
			t.Errorf("ttl %s: expected error", ttl)
		}
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

func TestConcurrentAccess(t *testing.T) {
	store := &fakeStore{features: map[string]Feature{}}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("feature-%d", i)
		store.features[key] = Feature{Key: key, Enabled: true}
	}

	cache, err := NewCache(store, time.Millisecond)
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
