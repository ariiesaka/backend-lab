// Package featurecontrol serves feature flags from an in-memory TTL cache
// placed in front of a slower Store such as a database.
package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotFound is returned by a Store when the feature does not exist.
var ErrNotFound = errors.New("featurecontrol: feature not found")

// Feature is a single feature flag.
type Feature struct {
	Key     string
	Enabled bool
}

// Store loads features from the source of truth, usually the database.
type Store interface {
	GetFeature(ctx context.Context, key string) (Feature, error)
}

type entry struct {
	feature   Feature
	expiresAt time.Time
}

// Cache is a Store that answers from memory while a value is still valid and
// falls through to the wrapped Store otherwise. It is safe for concurrent use.
type Cache struct {
	store Store
	ttl   time.Duration
	now   func() time.Time

	mu      sync.RWMutex
	entries map[string]entry
}

// NewCache returns a Cache that keeps each loaded feature for ttl.
func NewCache(store Store, ttl time.Duration) (*Cache, error) {
	if store == nil {
		return nil, errors.New("featurecontrol: store is required")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("featurecontrol: ttl must be positive, got %s", ttl)
	}
	return &Cache{
		store:   store,
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[string]entry),
	}, nil
}

// GetFeature returns the cached feature if it exists and has not expired.
// Otherwise it loads the feature from the Store and caches the result.
// Errors, including ErrNotFound, are returned as is and never cached.
func (c *Cache) GetFeature(ctx context.Context, key string) (Feature, error) {
	if feature, ok := c.lookup(key); ok {
		return feature, nil
	}

	feature, err := c.store.GetFeature(ctx, key)
	if err != nil {
		return Feature{}, err
	}

	c.mu.Lock()
	c.entries[key] = entry{feature: feature, expiresAt: c.now().Add(c.ttl)}
	c.mu.Unlock()

	return feature, nil
}

// IsEnabled reports whether the feature is switched on.
func (c *Cache) IsEnabled(ctx context.Context, key string) (bool, error) {
	feature, err := c.GetFeature(ctx, key)
	if err != nil {
		return false, err
	}
	return feature.Enabled, nil
}

func (c *Cache) lookup(key string) (Feature, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok || !c.now().Before(e.expiresAt) {
		return Feature{}, false
	}
	return e.feature, true
}
