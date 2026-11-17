# Template-publish fan-out worker

When a template is published, engagements whose indexed version is **unknown or stale** must be verified
by a downstream call that **takes ~1 minute** and is **owned by another team with a fixed quota**
(assumed 20 concurrent calls per region). This worker queues that work and drains it within the quota.

Java 21, no runtime dependencies, 15 tests.

## Scope (agreed before coding)

| Question | Decision |
| --- | --- |
| What the call does | Loads the engagement and reports its real template version. **Read-only.** |
| Who is called | Only engagements not verified in the index. Lifecycle hooks keep the rest current. |
| Engagement open by a user | Downstream returns `Busy`; dropped, because the user's open verifies it via the `Loaded` hook. |
| Capacity | Fixed quota shared by all worker instances (permit table) + region-wide pause on Retry-After. |
| Fairness | Round-robin across firms + max live calls per firm. |
| Outage | Circuit breaker; the backlog is kept and no retry budget is burned. |

## Flow

```
TemplatePublished ─> PublishHandler ─enqueue─> WorkQueue <─claim── FanoutWorker ─permit─> PermitPool (quota)
                     (reads index,              (one item per          │
                      idempotent)                engagement)           ├─ CircuitBreaker
                                                                       ├─> VerificationClient (~1 min, other team)
                                                                       └─> VersionIndex (monotonic write)
```

## Classes

| Class | Role |
| --- | --- |
| `PublishHandler` | Publish event → one work item per unverified engagement |
| `WorkQueue` / `InMemoryWorkQueue` | Durable work with leases, fencing, per-firm cap and round-robin (Aurora in production) |
| `PermitPool` / `InMemoryPermitPool` | The downstream quota as N expiring slots, plus the region-wide pause |
| `CircuitBreaker` | CLOSED → OPEN after N consecutive failures → HALF_OPEN single probe |
| `FanoutWorker` | `pollOnce()`: permit + lease → call → handle the result |
| `VerificationClient`, `VerificationResult` | Downstream contract; the sealed result forces every case to be handled |
| `VersionIndex`, `EngagementDirectory` | Ports to the regional index |
| `WorkerSettings` | Timeouts, retries, per-firm cap; validates `lease > call timeout` |

## How each requirement is met

**Idempotency**
- One work item per engagement: a redelivered event, or v5 then v6, is a no-op if work is already queued.
- Verified engagements are not returned by the directory, so a later event creates no work.
- The index write is **monotonic by engagement revision**: a slow verification (read v3) cannot overwrite a
  newer hook (user applied v5 meanwhile).

**Concurrency**
- A call needs a **permit** (global quota, shared by every instance) and a **lease** (exclusive item ownership).
- The queue grants a lease only while the firm holds fewer than `maxPerTenant` live leases, so the cap holds
  across instances too.
- Every state change presents the lease token; a result from an expired, reclaimed lease is rejected.

**Downstream capacity**
- The permit is held for the whole call, never released early on a local timeout.
- `Overloaded(retryAfter)` pauses the whole region, and the item's attempt is refunded.
- Firms are served round-robin: a 40k-engagement backlog doesn't delay a firm with 3.

**Failure**

| Result | Handling |
| --- | --- |
| `Verified` | Write the index, complete the item |
| `NotFound`, `Busy` | Drop, no retry |
| `Overloaded` | Pause the region until Retry-After; attempt refunded |
| `Failure` / exception | Backoff with jitter; dead-letter after `maxAttempts` |
| Failure while the circuit trips | Systemic: retried later, attempt refunded |
| Worker crash | Lease and permit expire; another instance reclaims. Repeating the call is harmless (read-only). |
| Index write fails | Item stays leased, retried after the lease expires (at-least-once) |
| Item that keeps crashing workers | Crashed attempts count; dead-lettered without another call |

## Capacity math

20 slots × 60 per hour = **1,200 verifications/hour per region (28,800/day)**.
- Steady state (stale rows from lost hooks, a few thousand): well inside a day.
- Initial backfill (~480k unknown in the largest region): ~400 h ≈ 17 days. The 24 h freshness target applies
  after backfill.

## Production notes

- **Queue in Aurora:** claim with `SELECT … FOR UPDATE SKIP LOCKED`; transitions are
  `UPDATE … WHERE engagement_id = ? AND lease_token = ?` (0 rows = lease lost).
- **Permit pool in Aurora:** 20 rows `(slot, holder_token, expires_at)`; acquire with
  `UPDATE … WHERE expires_at < now() … LIMIT 1 SKIP LOCKED`; one row holds `paused_until`.
- The circuit breaker is per instance on purpose: each instance sees the same downstream.
- Run `pollOnce()` every second on a scheduler, with calls on virtual threads (each blocks ~1 minute).

## Tests

```bash
mvn test
```

| Test | Covers |
| --- | --- |
| `FanoutWorkerTest` (14) | Redelivery and coalescing, monotonic index write, firm fairness, overload pause, overload doesn't burn retries, backoff, dead-letter, Busy/NotFound, client exceptions, circuit breaker open/probe/recover, crashed worker, poison item, index outage, settings validation |
| `FanoutWorkerConcurrencyTest` (1) | 3 instances, virtual threads, shared quota of 4, max 2 per firm: limits never exceeded, each engagement called once |

## Out of scope

Aurora implementations (SQL sketched above), the HTTP adapter for the downstream, metrics export,
the backfill seeding job. Tests use JUnit 4.13.2 (only version available offline when this was built).
