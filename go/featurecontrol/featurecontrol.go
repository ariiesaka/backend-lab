// Package featurecontrol serves feature flags from an in-memory TTL cache
// placed in front of a slower Store such as a database.
package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrNotFound is returned by a Store when the feature does not exist.
var ErrNotFound = errors.New("featurecontrol: feature not found")

// Feature is a single feature flag.
type Feature struct {
	Key     string
	Enabled bool
}

// Store loads features from the source of truth, usually the database.
// Implementations must return once ctx is done. Requests for a key wait on
// the load in progress, so a call that never returns blocks that key.
type Store interface {
	GetFeature(ctx context.Context, key string) (Feature, error)
}

// Config holds the Cache settings. TTL and LoadTimeout are required.
type Config struct {
	// TTL is how long a loaded feature stays valid.
	TTL time.Duration
	// LoadTimeout limits a single call to the Store. Loads are shared between
	// requests, so they do not run under any one caller's deadline.
	LoadTimeout time.Duration
	// MaxStale is how long past its TTL a feature may still be served when
	// reloading it fails. Zero turns this off.
	MaxStale time.Duration
	// OnStale, if set, is called when a failed reload is hidden by serving an
	// expired feature, so the failure can be logged or counted. It is called
	// once per failed load, not once per waiting caller.
	OnStale func(key string, err error)
}

type entry struct {
	feature   Feature
	expiresAt time.Time
}

// Cache is a Store that answers from memory while a value is still valid and
// falls through to the wrapped Store otherwise. Concurrent misses on a key
// share one call to the Store. It is safe for concurrent use.
type Cache struct {
	store       Store
	ttl         time.Duration
	loadTimeout time.Duration
	maxStale    time.Duration
	onStale     func(key string, err error)
	now         func() time.Time
	loads       singleflight.Group

	mu      sync.RWMutex
	entries map[string]entry
}

// NewCache returns a Cache in front of store using the given config.
func NewCache(store Store, cfg Config) (*Cache, error) {
	if store == nil {
		return nil, errors.New("featurecontrol: store is required")
	}
	if cfg.TTL <= 0 {
		return nil, fmt.Errorf("featurecontrol: ttl must be positive, got %s", cfg.TTL)
	}
	if cfg.LoadTimeout <= 0 {
		return nil, fmt.Errorf("featurecontrol: load timeout must be positive, got %s", cfg.LoadTimeout)
	}
	if cfg.MaxStale < 0 {
		return nil, fmt.Errorf("featurecontrol: max stale must not be negative, got %s", cfg.MaxStale)
	}
	return &Cache{
		store:       store,
		ttl:         cfg.TTL,
		loadTimeout: cfg.LoadTimeout,
		maxStale:    cfg.MaxStale,
		onStale:     cfg.OnStale,
		now:         time.Now,
		entries:     make(map[string]entry),
	}, nil
}

// GetFeature returns the cached feature if it exists and has not expired.
// Otherwise it waits for a single shared load from the Store, or until ctx is
// done. If the load fails, every waiting caller gets the expired feature when
// it is within MaxStale, and otherwise the error as is. Errors are never
// cached, and ErrNotFound is always returned because the feature is gone.
func (c *Cache) GetFeature(ctx context.Context, key string) (Feature, error) {
	if feature, ok := c.lookup(key, 0); ok {
		return feature, nil
	}

	result := c.loads.DoChan(key, func() (any, error) {
		return c.load(ctx, key)
	})

	select {
	case res := <-result:
		if res.Err != nil {
			return Feature{}, res.Err
		}
		return res.Val.(Feature), nil
	case <-ctx.Done():
		return Feature{}, ctx.Err()
	}
}

// IsEnabled reports whether the feature is switched on.
func (c *Cache) IsEnabled(ctx context.Context, key string) (bool, error) {
	feature, err := c.GetFeature(ctx, key)
	if err != nil {
		return false, err
	}
	return feature.Enabled, nil
}

// load calls the Store and caches the result. Other callers may be waiting on
// it, so cancelling the caller that started it must not stop it.
func (c *Cache) load(ctx context.Context, key string) (Feature, error) {
	// The previous load may have finished after this caller's cache miss.
	if feature, ok := c.lookup(key, 0); ok {
		return feature, nil
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.loadTimeout)
	defer cancel()

	feature, err := c.store.GetFeature(ctx, key)
	if errors.Is(err, ErrNotFound) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return Feature{}, err
	}
	if err != nil {
		stale, ok := c.lookup(key, c.maxStale)
		if !ok {
			return Feature{}, err
		}
		if c.onStale != nil {
			c.onStale(key, err)
		}
		return stale, nil
	}

	c.mu.Lock()
	c.entries[key] = entry{feature: feature, expiresAt: c.now().Add(c.ttl)}
	c.mu.Unlock()

	return feature, nil
}

// lookup returns the cached feature if it expired less than grace ago, or has
// not expired at all.
func (c *Cache) lookup(key string, grace time.Duration) (Feature, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok || !c.now().Before(e.expiresAt.Add(grace)) {
		return Feature{}, false
	}
	return e.feature, true
}
