# Feature control cache

Feature flags are read on nearly every request but rarely change, so reading them from the database each time is wasted load. `Cache` wraps the database behind a `Store` interface and only calls it when needed.

```mermaid
flowchart LR
    A[GetFeature] --> B{Valid value or remembered<br>not-found in cache?}
    B -- yes --> C[Return it]
    B -- no --> K{Load failed within<br>ErrorBackoff?}
    K -- yes --> I
    K -- no --> H[Join the load for this key,<br>or start it if there is none]
    H --> D[One call to the Store]
    D -- ok --> E[Cache with new expiry] --> F[Return value to all waiters]
    D -- error --> I{Expired value<br>within MaxStale?}
    D -- ErrNotFound --> L[Remove the value, remember for<br>NotFoundTTL, return ErrNotFound]
    I -- yes --> J[Return the expired value]
    I -- no --> G[Return the error]
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
	TTL:          30 * time.Second,
	LoadTimeout:  2 * time.Second,
	MaxStale:     10 * time.Minute,
	ErrorBackoff: 5 * time.Second,
	NotFoundTTL:  30 * time.Second,
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

The TTL is the longest a flag change can take to reach a running instance. A shorter TTL means faster rollouts and kill switches, and more database reads: at most one successful read per key, per instance, per TTL. Failures and missing keys can read more often, limited by `ErrorBackoff` and `NotFoundTTL` when those are set.

## Behaviour

- **Concurrent misses share one load.** When a key is missing or expired, one request calls the database and the others wait for its result. This avoids a cache stampede, where every request in flight at expiry hits the database.
- **Loads outlive the request that started them.** Other requests may be waiting, so a load runs on `context.WithoutCancel` and is limited by `LoadTimeout` instead. A cancelled caller stops waiting right away, and the load carries on for the rest. When `LoadTimeout` expires, callers get `context.DeadlineExceeded`, the same error as their own deadline expiring, so they cannot tell which one fired.
- **Failures back off.** After a failed load, a key is not retried for `ErrorBackoff`. Callers get an answer straight away instead of each round waiting on a struggling database. Without an expired value to serve, they get the last error, so a brief failure on a cold key lasts up to `ErrorBackoff`. With it set to zero, the next call tries the database again.
- **Expired values are served only when a load fails.** If the load fails or is backing off, and the value expired less than `MaxStale` ago, callers get that value instead of the error. Otherwise they get the error. This hides the failure from callers, so use `OnStale` or `Stats` to see it.
- **Serving an expired value does not renew it.** Its age keeps counting toward `MaxStale`, even during a backoff.
- **`ErrNotFound` is never hidden.** The database has answered that the feature is gone, so its cached value is removed and the error is returned. It is remembered for `NotFoundTTL`, so a new feature can take that long to appear.
- **A panic in the Store becomes an error.** It is treated like any failed load, and the error message includes the stack.

## Metrics

`cache.Stats()` returns running totals to export to your metrics system:

| Counter | Counts | What to watch |
| --- | --- | --- |
| `Hits` | Requests answered with a valid value or a remembered not-found | Hit rate is `Hits / (Hits + Misses)` |
| `Misses` | Requests that waited for a load | |
| `BackedOff` | Requests answered without a load because one failed recently | Should be zero when healthy |
| `StaleServed` | Requests answered with an expired value | Should be zero when healthy |
| `Loads` | Calls to the database | Compare with `Misses` to see how many requests merging saved |
| `LoadErrors` | Database calls that failed, not counting `ErrNotFound` | Should be zero when healthy |

## Known limitations

- **Per-instance.** Each instance has its own cache, so instances can disagree for up to one TTL, and merging only applies within an instance. Ten instances can still make ten reads per key per TTL.
- **The first request after a backoff still waits.** If the database hangs, the requests that join each retry wait up to `LoadTimeout`. The backoff is a fixed length and does not grow during a long outage.
- **A caller's own deadline wins.** A caller whose context ends before the load finishes gets its context error, even if an expired value is available.
- **A Store that ignores its context blocks its key.** Requests for that key wait until their own context ends, and no new load starts.
- **A panic in `OnStale` crashes the process.** It runs in the load's own goroutine, so HTTP recovery middleware cannot catch it.
- **No manual invalidation.** A change takes effect only when the TTL runs out.
- **Entries are never evicted.** Expired values, remembered not-founds and failures stay in memory until replaced. This is fine for a small, fixed set of flags, but keys must not come from user input, or memory can grow without limit.

## Snapshot: the other design

`Cache` loads each key on demand, so a request can still wait on the database. `Snapshot` instead keeps every flag in memory and reloads them all on a timer, so reads never touch the database.

```go
snapshot, err := featurecontrol.NewSnapshot(ctx, featurecontrol.NewPostgresStore(db), featurecontrol.SnapshotConfig{
	RefreshInterval: 30 * time.Second,
	LoadTimeout:     2 * time.Second,
	OnRefreshError: func(err error) {
		log.Printf("feature refresh failed: %v", err)
	},
})
if err != nil {
	return err // Could not load the flags, so do not start serving.
}
defer snapshot.Close()

enabled, err := snapshot.IsEnabled(ctx, "new-checkout")
```

`Snapshot` also implements `Store`, so callers can switch between the two without changing.

- **The first load happens at startup.** If it fails, `NewSnapshot` returns the error, so an instance never serves without flags.
- **A failed refresh keeps the last good set**, with no age limit. Watch `Stats().LastRefresh` and alert if it gets old.
- **A panic in the store becomes a refresh error.** A panic in `OnRefreshError` crashes the process.
- **`Close` waits for the refresh to end.** It blocks for as long as a store ignores its context, and forever if called from `OnRefreshError`.
- **An empty result is taken as real.** If the table is emptied, every flag becomes `ErrNotFound` on the next refresh.
- **Every refresh reads the whole table**, on every instance, whether or not anything changed. Instances started together also refresh at about the same time.

### Which to use

| | `Cache` | `Snapshot` |
| --- | --- | --- |
| Reads touch the database | Yes, on a miss | Never |
| Slowest read | Up to `LoadTimeout` | A map lookup |
| Database load | Only for keys in use, when requested | Whole table, every interval, even when idle |
| First request after startup | Can fail or wait | Already loaded |
| Database outage | Old value up to `MaxStale`, then errors | Last good set, with no limit |
| Unknown keys | Reach the database, and can stay in memory | Answered from memory, and nothing stored |
| Change takes effect within | `TTL` | `RefreshInterval` |
| Background goroutine, needs `Close` | No | Yes |
| Good fit | Many keys with few in use, or per-key values too large to load up front | A small set of flags read on hot paths |

For flags in production, prefer `Snapshot`. It has fewer failure modes, and it is safe if keys ever come from user input. `Cache` shows how to solve the harder general problem, and it fits data that is too large to hold in memory.

## Tests

```bash
cd go && go test ./featurecontrol/
```

The Postgres tests are skipped unless `FEATURECONTROL_TEST_POSTGRES_URL` is set. They drop and recreate the `features` table, so point it at a throwaway database. GitHub Actions runs them against a Postgres container, along with the race detector:

```bash
cd go && FEATURECONTROL_TEST_POSTGRES_URL=postgres://postgres:postgres@localhost:5432/postgres go test -race ./featurecontrol/
```
