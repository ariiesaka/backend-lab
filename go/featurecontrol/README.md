# Feature control cache

Feature flags are read on nearly every request but rarely change, so reading them from the database each time is wasted load. `Cache` wraps the database behind a `Store` interface and only calls it when needed.

```mermaid
flowchart LR
    A[GetFeature] --> B{In cache and<br>not expired?}
    B -- yes --> C[Return cached value]
    B -- no --> H[Join the load for this key,<br>or start it if there is none]
    H --> D[One call to the Store]
    D -- ok --> E[Cache with new expiry] --> F[Return value to all waiters]
    D -- error --> I{Expired value within MaxStale,<br>and not ErrNotFound?}
    I -- yes --> J[Return it to all waiters,<br>call OnStale]
    I -- no --> G[Return error to all waiters,<br>cache nothing]
```

## Usage

`PostgresStore` reads from this table, through any `database/sql` Postgres driver your application registers. For another database, implement `Store`.

```sql
CREATE TABLE features (
	key     TEXT PRIMARY KEY,
	enabled BOOLEAN NOT NULL
);
```

Wrap the store in a cache, with the settings taken from your config:

```go
cache, err := featurecontrol.NewCache(featurecontrol.NewPostgresStore(db), featurecontrol.Config{
	TTL:         30 * time.Second,
	LoadTimeout: 2 * time.Second,
	MaxStale:    10 * time.Minute,
	OnStale: func(key string, err error) {
		log.Printf("serving expired feature %q: %v", key, err)
	},
})
if err != nil {
	return err
}
```

A flag lookup can still fail, for example on the first load after startup, so choose a safe default instead of failing the request:

```go
enabled, err := cache.IsEnabled(ctx, "new-checkout")
if err != nil {
	log.Printf("feature lookup failed: %v", err)
	enabled = false
}
```

`Cache` also implements `Store`, so callers can depend on the interface without knowing a cache is there.

## Choosing a TTL

The TTL is the longest a flag change can take to reach a running instance. A shorter TTL means faster rollouts and kill switches, and more database reads: at most one successful read per key, per instance, per TTL. Errors and missing keys are not cached, so those lookups can read more often.

## Behaviour

- **Concurrent misses share one load.** When a key is missing or expired, one request calls the database and the others wait for its result. This avoids a cache stampede, where every request in flight at expiry hits the database.
- **Loads outlive the request that started them.** Other requests may be waiting, so a load runs on `context.WithoutCancel` and is limited by `LoadTimeout` instead. A cancelled caller stops waiting right away, and the load carries on for the rest. When `LoadTimeout` expires, callers get `context.DeadlineExceeded`, the same error as their own deadline expiring, so they cannot tell which one fired.
- **Errors are not cached.** The next call after a failure tries the database again.
- **Expired values are served only when a reload fails.** If the database call fails and the value expired less than `MaxStale` ago, every waiting caller gets that value instead of the error. Otherwise they get the error. This hides the failure from callers, so use `OnStale` to log or count it.
- **Serving an expired value does not renew it.** Its age keeps counting toward `MaxStale`, and the next call tries the database again.
- **`ErrNotFound` is never hidden.** The database has answered that the feature is gone, so its cached value is removed and the error is returned.

## Known limitations

- **Per-instance.** Each instance has its own cache, so instances can disagree for up to one TTL, and merging only applies within an instance. Ten instances can still make ten reads per key per TTL.
- **Unknown keys always reach the database.** Repeated lookups of a missing flag are not absorbed, though concurrent ones share a load.
- **Requests still wait during an outage.** There is no backoff, so if the database hangs, each round of requests waits up to `LoadTimeout` before getting the expired value or the error.
- **A caller's own deadline wins.** A caller whose context ends before the load finishes gets its context error, even if an expired value is available.
- **A Store that ignores its context blocks its key.** Requests for that key wait until their own context ends, and no new load starts.
- **A panic in the Store crashes the process.** Loads run in their own goroutine, and `singleflight` re-panics there, so HTTP recovery middleware cannot catch it.
- **No manual invalidation.** A change takes effect only when the TTL runs out.
- **Expired entries stay in memory until reloaded.** This is fine for a small, fixed set of flags, but keys should not come from user input.

## Tests

```bash
cd go && go test ./featurecontrol/
```

The Postgres tests are skipped unless `FEATURECONTROL_TEST_POSTGRES_URL` is set. They drop and recreate the `features` table, so point it at a throwaway database. GitHub Actions runs them against a Postgres container, along with the race detector:

```bash
cd go && FEATURECONTROL_TEST_POSTGRES_URL=postgres://postgres:postgres@localhost:5432/postgres go test -race ./featurecontrol/
```
