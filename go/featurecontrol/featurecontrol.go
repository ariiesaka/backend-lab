// Package featurecontrol serves feature flags from an in-memory TTL cache
// placed in front of a slower Store such as a database.
package featurecontrol

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	// ErrorBackoff is how long a key is not retried after a failed load.
	// Callers get the expired feature within MaxStale, or the last error,
	// without waiting on the Store. Zero turns this off.
	ErrorBackoff time.Duration
	// NotFoundTTL is how long ErrNotFound is remembered for a key. Zero turns
	// this off.
	NotFoundTTL time.Duration
	// OnStale, if set, is called when a failed reload is hidden by serving an
	// expired feature, so the failure can be logged or counted. It is called
	// once per failed load, not once per waiting caller, and must not panic.
	OnStale func(key string, err error)
}

// Stats counts what the Cache has done since it was created.
type Stats struct {
	// Hits is requests answered with a valid feature or a remembered
	// ErrNotFound.
	Hits uint64
	// Misses is requests that had to wait for a load.
	Misses uint64
	// BackedOff is requests answered without a load because one failed recently.
	BackedOff uint64
	// StaleServed is requests answered with an expired feature.
	StaleServed uint64
	// Loads is calls to the Store.
	Loads uint64
	// LoadErrors is calls to the Store that failed. ErrNotFound is not a failure.
	LoadErrors uint64
}

type entry struct {
	feature   Feature
	missing   bool // The Store reported ErrNotFound, so feature is not set.
	expiresAt time.Time
	failure   error // The last load failed, so do not retry before retryAt.
	retryAt   time.Time
}

// result is the answer to one request for a feature.
type result struct {
	feature   Feature
	err       error
	stale     bool
	backedOff bool
}

// Cache is a Store that answers from memory while a value is still valid and
// falls through to the wrapped Store otherwise. Concurrent misses on a key
// share one call to the Store. It is safe for concurrent use.
type Cache struct {
	store        Store
	ttl          time.Duration
	loadTimeout  time.Duration
	maxStale     time.Duration
	errorBackoff time.Duration
	notFoundTTL  time.Duration
	onStale      func(key string, err error)
	now          func() time.Time
	loads        singleflight.Group

	mu      sync.RWMutex
	entries map[string]entry

	stats struct {
		hits, misses, backedOff, staleServed, loads, loadErrors atomic.Uint64
	}
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
	if cfg.ErrorBackoff < 0 {
		return nil, fmt.Errorf("featurecontrol: error backoff must not be negative, got %s", cfg.ErrorBackoff)
	}
	if cfg.NotFoundTTL < 0 {
		return nil, fmt.Errorf("featurecontrol: not found ttl must not be negative, got %s", cfg.NotFoundTTL)
	}
	return &Cache{
		store:        store,
		ttl:          cfg.TTL,
		loadTimeout:  cfg.LoadTimeout,
		maxStale:     cfg.MaxStale,
		errorBackoff: cfg.ErrorBackoff,
		notFoundTTL:  cfg.NotFoundTTL,
		onStale:      cfg.OnStale,
		now:          time.Now,
		entries:      make(map[string]entry),
	}, nil
}

// GetFeature returns the cached feature if it exists and has not expired.
// Otherwise it waits for a single shared load from the Store, or until ctx is
// done. If the load fails, every waiting caller gets the expired feature when
// it is within MaxStale, and otherwise the error as is. ErrNotFound is always
// returned because the feature is gone.
func (c *Cache) GetFeature(ctx context.Context, key string) (Feature, error) {
	r, ok := c.lookup(key)
	switch {
	case !ok:
		c.stats.misses.Add(1)
		shared := c.loads.DoChan(key, func() (any, error) {
			return c.load(ctx, key), nil
		})
		select {
		case res := <-shared:
			r = res.Val.(result)
		case <-ctx.Done():
			return Feature{}, ctx.Err()
		}
	case r.backedOff:
		c.stats.backedOff.Add(1)
	default:
		c.stats.hits.Add(1)
	}

	if r.stale {
		c.stats.staleServed.Add(1)
	}
	return r.feature, r.err
}

// IsEnabled reports whether the feature is switched on.
func (c *Cache) IsEnabled(ctx context.Context, key string) (bool, error) {
	feature, err := c.GetFeature(ctx, key)
	if err != nil {
		return false, err
	}
	return feature.Enabled, nil
}

// Stats returns the current counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:        c.stats.hits.Load(),
		Misses:      c.stats.misses.Load(),
		BackedOff:   c.stats.backedOff.Load(),
		StaleServed: c.stats.staleServed.Load(),
		Loads:       c.stats.loads.Load(),
		LoadErrors:  c.stats.loadErrors.Load(),
	}
}

// load calls the Store and records the outcome. Other callers may be waiting
// on it, so cancelling the caller that started it must not stop it.
func (c *Cache) load(ctx context.Context, key string) result {
	// The previous load may have finished after this caller's cache miss.
	if r, ok := c.lookup(key); ok {
		return r
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.loadTimeout)
	defer cancel()

	c.stats.loads.Add(1)
	feature, err := c.readStore(ctx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		c.stats.loadErrors.Add(1)
	}

	r := c.record(key, feature, err)
	if r.stale && c.onStale != nil {
		c.onStale(key, err)
	}
	return r
}

// readStore calls the Store, turning a panic into an error. Loads run in their
// own goroutine, where a panic would otherwise crash the process.
func (c *Cache) readStore(ctx context.Context, key string) (feature Feature, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("featurecontrol: store panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return c.store.GetFeature(ctx, key)
}

// record stores the outcome of a load and returns the answer to give callers.
func (c *Cache) record(key string, feature Feature, err error) result {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	switch {
	case err == nil:
		c.entries[key] = entry{feature: feature, expiresAt: now.Add(c.ttl)}
		return result{feature: feature}
	case errors.Is(err, ErrNotFound):
		delete(c.entries, key)
		if c.notFoundTTL > 0 {
			c.entries[key] = entry{missing: true, expiresAt: now.Add(c.notFoundTTL)}
		}
		return result{err: err}
	}

	e := c.entries[key]
	if c.errorBackoff > 0 {
		e.failure, e.retryAt = err, now.Add(c.errorBackoff)
		c.entries[key] = e
	}
	return c.failureResult(e, err, now)
}

// lookup returns the answer held in memory, if there is one: a valid feature, a
// remembered ErrNotFound, or the fallback during a backoff.
func (c *Cache) lookup(key string) (result, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok {
		return result{}, false
	}

	now := c.now()
	switch {
	case now.Before(e.expiresAt) && e.missing:
		return result{err: ErrNotFound}, true
	case now.Before(e.expiresAt):
		return result{feature: e.feature}, true
	case e.failure != nil && now.Before(e.retryAt):
		r := c.failureResult(e, e.failure, now)
		r.backedOff = true
		return r, true
	}
	return result{}, false
}

// failureResult returns the expired feature if it is within MaxStale, and
// otherwise err.
func (c *Cache) failureResult(e entry, err error, now time.Time) result {
	if !e.missing && now.Before(e.expiresAt.Add(c.maxStale)) {
		return result{feature: e.feature, stale: true}
	}
	return result{err: err}
}
