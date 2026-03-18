# SQL CPU Time Token Admission

Authors: Wenyi Hu, Sumeer Bhola

This document describes the design and implementation of CTT-based
(CPU Time Token) admission control for SQL work, added in the
`admissionintegration` branch. It covers the motivation, architecture,
key design decisions, and how the implementation compares to an
earlier prototype.

## Background

CockroachDB's admission control system (see `admission_control.md`)
manages CPU and IO resources at the node level. For KV work, the
system uses either slot-based (`slotGranter`) or CPU time token
(`cpuTimeTokenGranter`) admission, selected by the
`admission.cpu_time_tokens.enabled` cluster setting. The CTT system
refills token buckets every ~1ms via a filler goroutine, with
per-tenant fair-sharing enforced through WorkQueue's tenant heap.

Prior to this work, SQL response processing was admitted through
legacy `SQLKVResponseWork` and `SQLSQLResponseWork` queues backed by
`tokenGranter`. These queues used burst tokens refreshed on each
`CPULoad` callback (~1s), providing coarse-grained backpressure. The
problem: these queues are decoupled from CTT's fine-grained CPU
measurement, and they don't participate in the same token pool as KV
work, making unified resource management impossible.

### Goals

1. Unify SQL and KV CPU admission under the CTT token pool, enabling
   shared per-tenant fair-sharing and a single resource budget.
2. Measure per-goroutine CPU consumption for SQL work, replacing the
   proxy-based response admission with direct CPU accounting.
3. Provide admission backpressure: when CPU tokens are exhausted, SQL
   work blocks until the filler refills tokens.
4. Minimize overhead on the hot path (cancel checker calls
   `MeasureAndAdmit` every 1024 rows across all concurrent queries).
5. Lay the groundwork for the Resource Manager, where resource groups
   (with configurable CPU limits) are modeled as tenants in the
   WorkQueue.

### Non-goals

- Removing the legacy `SQLKVResponseWork` / `SQLSQLResponseWork`
  queues (deferred until CTT-based SQL admission is validated).
- Per-query CPU budgets or query-level throttling.
- Elastic CPU integration for SQL work.


## Architecture

### Pipeline Overview

```
                    ┌─────────────────────────────────┐
                    │       conn_executor / DistSQL    │
                    │                                  │
                    │  MakeCPUHandle(provider, ...)    │
                    │     → SQLCPUHandle               │
                    │     → ctx = ContextWithSQLCPU... │
                    │     → gh = RegisterGoroutine()   │
                    └──────┬────────────────┬──────────┘
                           │                │
            ┌──────────────▼──┐     ┌───────▼──────────────┐
            │ Cancel Checker  │     │  KV Response Sites   │
            │ (every 1024     │     │  (kv_batch_fetcher,  │
            │  rows)          │     │   kvstreamer,        │
            │                 │     │   inbox, inbound,    │
            │ gh.Measure      │     │   tablewriter)       │
            │   AndAdmit()    │     │                      │
            └────────┬────────┘     │ h.MeasureAndAdmit    │
                     │              │   Response(ctx, q)   │
                     ▼              └──────────┬───────────┘
            ┌────────────────┐                 │
            │ reportCPU()    │◄────────────────┘
            │ (atomic        │
            │  decrement of  │
            │  reservation)  │
            └───────┬────────┘
                    │ reservation exhausted?
                    ▼
            ┌────────────────────────────────┐
            │     settleAndAdmit(ctx, q)     │
            │                                │
            │  1. AdmittedWorkDone(prev)     │
            │     → return used tokens       │
            │  2. Admit(next reservation)    │
            │     → blocks if exhausted      │
            │  3. Update EWMA               │
            └───────────────┬────────────────┘
                            │
                            ▼
            ┌────────────────────────────────┐
            │    CTT WorkQueue (per-node)    │
            │                                │
            │  Per-tenant fair-sharing       │
            │  Priority ordering             │
            │  Token-based granting          │
            └───────────────┬────────────────┘
                            │
                            ▼
            ┌────────────────────────────────┐
            │   cpuTimeTokenGranter          │
            │                                │
            │  Token buckets [tier][burst]   │
            │  Refilled every ~1ms by filler │
            └────────────────────────────────┘
```

### Key Types

**`SQLCPUProvider`** (`sql_cpu_handle.go`) — Factory for
`SQLCPUHandle`. One instance per node, shared across all SQL
connections. Stores cross-handle EWMA history for warm-starting new
handles.

**`SQLCPUHandle`** (`sql_cpu_handle.go`) — Per-query (or per-flow)
handle managing CPU accounting and admission across multiple
goroutines. Holds a token reservation and interacts with the CTT
WorkQueue when the reservation is exhausted.

**`GoroutineCPUHandle`** (`sql_cpu_handle.go`) — Per-goroutine handle
that measures CPU via `grunning.Time()` and reports to its parent
`SQLCPUHandle`. Pooled via `sync.Pool` to avoid allocations.

**`CPUGrantCoordinators`** (`cpu_time_token_grant_coordinator.go`) —
Shim that returns either slot-based or CTT WorkQueues depending on
the cluster setting. Provides `GetCTTWorkQueue` for SQL access to
the CTT token pool.


## Design Decisions

### 1. WorkQueue Path vs Direct Granter

The central architectural question: how should `SQLCPUHandle` acquire
CPU tokens from the CTT system?

Multiple approaches were explored. Sumeer's `cpu_token_all` prototype
established the per-goroutine CPU measurement and WorkQueue routing
pattern but called `Admit` on every `TryAdmit` (no reservation
buffer). Two further explorations followed: a "lease model"
(`wenyihu6/sqlcpu`) that bypassed WorkQueue for direct granter
calls with fixed 1ms chunks, and "Option A"
(`wenyihu6/sqlcpu-option-a`) that went through WorkQueue per-call
like sumeer's prototype but without the new granter infrastructure.
See the "Comparison with Earlier Approaches" section for full details.

**What we chose — WorkQueue with EWMA reservation**:
`SQLCPUHandle` calls `Admit`/`AdmittedWorkDone` on the existing CTT
`WorkQueue`, but only when the EWMA-sized reservation is exhausted:

```
SQLCPUHandle.reservedTokenNanos (atomic.Int64)
  → settleAndAdmit() [only when tokens exhausted]
    → q.AdmittedWorkDone(prevResp, actualUsed)  [settle previous]
    → q.Admit(ctx, workInfo)  [request next chunk]
  → Close: q.AdmittedWorkDone(resp, actualUsed)
```

This approach combines the strengths of the earlier explorations:
WorkQueue's tenant fair-sharing (from sumeer's prototype and
Option A) with amortized overhead (inspired by the lease model's
budget concept, but with adaptive EWMA instead of fixed chunks).
The Resource Manager requires per-tenant (and eventually per-
resource-group) CPU isolation. The WorkQueue already implements
tenant-weighted fair-sharing via a priority heap. Resource groups
will map to tenants with configurable weights — this would be
impossible with the direct granter model without reimplementing
fair-sharing from scratch. The higher per-call overhead is
acceptable because adaptive reservation sizing (see below)
dramatically reduces how often we access the WorkQueue.

Compared to sumeer's prototype, which required ~500 lines of
WorkQueue changes (new `AdmitResponse` return type, `getterKind`
priority classes, per-tenant CPU token tracking, `isSQLCPU` marker),
our approach reuses the existing CTT WorkQueues with minimal changes
(relaxed `AdmittedWorkDone` restriction, estimation guard for
caller-specified `RequestedCount`).

This required generalizing WorkQueue to accept SQL callers:
- `AdmittedWorkDone` was restricted to `KVWork` — we removed the
  restriction since the slot-return logic only runs for `usesSlots`
  mode, making it safe for token-based callers.
- The per-tenant estimator unconditionally sized requests — we added
  a `RequestedCount == 0` guard so callers that specify their own
  count skip the estimator.
- `CPUGrantCoordinators.GetCTTWorkQueue` was added to expose CTT
  queues for SQL use.

### 2. Token Reservation and Amortization

`MeasureAndAdmitResponse` is called per KV batch response — hundreds
of times per query across many concurrent queries. Without
amortization, each call acquires the per-node WorkQueue mutex.

Each `SQLCPUHandle` reserves a chunk of tokens from the WorkQueue.
`reportCPU` atomically decrements `reservedTokenNanos`. Only when the
reservation is exhausted does the handle interact with the WorkQueue.
Most checkpoints become a single atomic decrement + comparison
(~22ns) rather than a full `Admit` round-trip.

The reservation creates a tradeoff between **amortization** (larger
reservations = fewer settlements = less WorkQueue contention) and
**responsiveness** (smaller reservations = faster reaction to
overload). We handle this with adaptive sizing rather than a fixed
chunk.

### 3. Adaptive Reservation Sizing via EWMA

The reservation size adapts to the handle's CPU consumption rate via
an exponentially weighted moving average:

```
ewmaCPUNanos = α * actualUsed + (1 - α) * ewmaCPUNanos
nextReservation = clamp(ewmaCPUNanos * 2.0, 10ms, 1s)
```

Where `α = 0.5` matches KV's per-tenant `cpuTimeTokenEstimator`.
The `2.0` multiplier provides headroom to absorb variance. The range
is clamped to [10ms, 1s] — 10ms minimum prevents excessive settlement
for light queries; 1s maximum bounds how long a handle can hold tokens
before the system can react to overload.

**Compared to earlier approaches**: Sumeer's prototype and Option A
had no reservation at all — every CPU measurement triggered a full
WorkQueue round-trip. The lease model used fixed 1ms chunks
(acceptable with the direct granter's low overhead but excessive
for WorkQueue). Our EWMA ensures that after convergence, a typical
handle settles ~once per natural CPU interval.

**Tradeoff**: The EWMA can over-allocate — a handle reserving 1s of
tokens holds resources that other tenants could use. This is
acceptable because unused tokens are returned at Close via
`AdmittedWorkDone`, and over-reservation only delays other tenants by
one filler tick (~1ms).

### 4. Cross-Handle EWMA Learning

New `SQLCPUHandle` instances start with a cold initial reservation of
100ms. Without learning, each handle takes ~5 settlement cycles to
converge to an appropriate size, causing unnecessary blocking during
ramp-up.

The `sqlCPUProviderImpl` stores two atomic EWMA values — one for
gateway handles and one for remote-flow handles. On `Close()`, a
handle saves its EWMA to the provider. On `GetHandle()`, a new
handle seeds its EWMA from the provider's stored value, giving it a
warm start from the first settlement.

This mirrors KV's per-tenant `cpuTimeTokenEstimator` pattern,
adapted for SQL's handle-per-query model. Neither of the earlier
exploration approaches (lease model, Option A) included cross-handle
learning — each handle started cold.

### 5. Synchronous vs Background Token Acquisition

**Tried and rejected**: background pre-fetching. When the reservation
dropped below 25% (low water mark), a background goroutine would
proactively acquire the next chunk from the WorkQueue via a CAS-
guarded `refillTriggered` flag.

This was rejected because:

- The EWMA sizing already made settlement infrequent enough that
  synchronous blocking was negligible (benchmarks showed 0
  settlements/op on the cancel-checker hot path)
- Background goroutines added significant complexity: `bgCtx`,
  `cancelBg`, `refillTriggered`, `refillCh`, `refillResult` channel,
  `maybeStartBackgroundRefill`, `acquireTokens`, drain-on-Close
- Three data races were introduced by the background path
  (`ewmaCPUNanos`, `totalReserved`, `h.q` read without proper
  synchronization)

The synchronous path is simpler and correct: when tokens are
exhausted, one goroutine settles while others benefit from the
double-check pattern after acquiring `settleMu`.

### 6. Settlement Serialization

Multiple goroutines share a single `SQLCPUHandle` (main goroutine +
inbound stream handlers + async workers). The concurrency design
separates the fast path (atomics only) from the slow path (mutex):

```go
func (h *GoroutineCPUHandle) measureAndAdmit(ctx context.Context, noWait bool) error {
    diff := cpuUsed - h.cpuAccounted
    h.cpuAccounted += diff
    h.h.reportCPU(diff)                      // atomic decrement
    if !noWait && h.h.reservedTokenNanos.Load() <= 0 {
        return h.h.maybeSettleAndAdmit(ctx)  // slow path
    }
    return nil
}
```

`settleMu` is separate from `mu` (which protects goroutine handle
registration) to avoid blocking `RegisterGoroutine` during a
potentially blocking `Admit` call. A double-check pattern after
acquiring `settleMu` prevents redundant settlements when multiple
goroutines race to the slow path.

All three approaches use the same fundamental pattern: atomic budget
for the fast path, mutex for serializing refill/settlement. The
lease model used `refillMu` + `budget.Add(-diff)` and checked `< 0`;
we use `settleMu` + `reservedTokenNanos.Add(-nanos)` and check
`<= 0`. No meaningful difference in the concurrency control.

Note that `reportCPU` bundles both the cumulative counter update and
the reservation deduction in a single call. The lease model split
these — `reportCPU` only updated the counter, while budget deduction
happened separately in `measureAndAdmit`. Bundling is correct for our
settlement-based approach since reservation deduction is inherently
part of reporting CPU usage.

### 7. Legacy Queue Coexistence vs Removal

The lease model exploration (`wenyihu6/sqlcpu` branch, commit
`56957f4b8a8`) completely removed the legacy infrastructure:
`SQLKVResponseWork` and `SQLSQLResponseWork` WorkKind enum values,
`tokenGranter`, `SQLKVResponseAdmissionQ` and
`SQLSQLResponseAdmissionQ` plumbing from ~15 files, and the
`DisableCPUTimeTokenSQLBypass` testing knob.

We chose to keep the legacy queues alive behind the
`admission.sql_cpu_based_response_admission.enabled` cluster setting
(default: false). When disabled, `MeasureAndAdmitResponse` still
measures CPU (via `RegisterGoroutine` + `MeasureAndAdmit`) but admits
through the legacy queue.

**Rationale**: This preserves rollback ability during the validation
period. CPU accounting is always active regardless of the setting —
only the admission path changes — so we can observe CPU metrics in
production before enabling enforcement. The legacy queue removal is
planned as a follow-up once CTT-based SQL admission is validated.

### 8. Queue Parameter vs Embedded Reference

The legacy response queue is passed as a parameter to
`MeasureAndAdmitResponse` rather than embedded in the handle:

```go
func (h *SQLCPUHandle) MeasureAndAdmitResponse(
    ctx context.Context, q *WorkQueue,
) error
```

This is because the choice between `SQLKVResponseAdmissionQ` and
`SQLSQLResponseAdmissionQ` is determined by the call site (KV
response vs DistSQL response), not by the handle. The CTT queue, in
contrast, is resolved by tenant ID inside the handle via
`getCTTQueue`.

### 9. Goroutine Registration Model

Some goroutines performing SQL work (e.g. inbound stream readers)
don't inherit the flow's context, so `SQLCPUHandleFromContext` cannot
retrieve the handle. This required adding a `cpuHandle` field to
`FlowBase` with a `GetCPUHandle()` accessor.

The field is safe to read from inbound stream handler goroutines
without synchronization: `StartInternal` writes the field before
`RegisterFlow` makes the flow discoverable, and inbound stream
handlers cannot run until `ConnectInboundStream` finds the flow.

`RegisterGoroutine` is idempotent — if the goroutine is already
registered, the existing `GoroutineCPUHandle` is returned. This
simplifies call sites: they don't need to track whether registration
has already occurred.


## Goroutine Registration Sites

Every goroutine performing SQL work must be registered with its
`SQLCPUHandle` for CPU measurement to be accurate. The following
goroutines are registered:

| Site | Goroutine | How |
|------|-----------|-----|
| `MakeCPUHandle` (flow.go) | Main flow goroutine | `RegisterGoroutine()` at flow setup |
| `processInboundStreamHelper` (inbound.go) | Inbound stream reader | `GetCPUHandle()` from FlowBase |
| `performRequestAsync` (streamer.go) | KV streamer async worker | `SQLCPUHandleFromContext(ctx)` |
| `setupGenerator` (virtual_table.go) | Virtual table generator | `SQLCPUHandleFromContext(ctx)` |
| `accumulateAsyncComponent` (flow.go) | Vectorized routers/outboxes | `SQLCPUHandleFromContext(ctx)` |

`RegisterGoroutine` is idempotent — if the goroutine is already
registered, the existing `GoroutineCPUHandle` is returned.


## Response Admission Call Sites

Five sites call `MeasureAndAdmitResponse`, each on a different
goroutine and with a different legacy queue:

| Site | Legacy Queue | When |
|------|-------------|------|
| `kv_batch_fetcher.go` | `SQLKVResponseAdmissionQ` | After each KV batch response |
| `streamer.go` | `SQLKVResponseAdmissionQ` | After async KV request completes |
| `inbox.go` | `SQLSQLResponseAdmissionQ` | After receiving DistSQL message |
| `inbound.go` | `SQLSQLResponseAdmissionQ` | After processing inbound stream message |
| `tablewriter.go` | `SQLKVResponseAdmissionQ` | After table write mutation |


## Settlement and Lifecycle

### Per-Query Lifecycle

```
1. MakeCPUHandle → SQLCPUHandle created, main goroutine registered
2. Additional goroutines call RegisterGoroutine as they start
3. Cancel checker calls MeasureAndAdmit every 1024 rows
   - reportCPU: atomic decrement of reservedTokenNanos
   - If exhausted: settleAndAdmit → AdmittedWorkDone + Admit
4. Response sites call MeasureAndAdmitResponse per KV/DistSQL response
   - Same pipeline as (3), plus legacy queue fallback if CTT disabled
5. Goroutines call GoroutineCPUHandle.Close at their boundary
   - Final measureAndAdmit(noWait=true) for last CPU sample
6. SQLCPUHandle.Close after all goroutines finish
   - Final settlement: AdmittedWorkDone for remaining tokens
   - Save EWMA to provider for cross-handle learning
   - Pool closed GoroutineCPUHandles
```

### Settlement Details

Each settlement cycle:
1. Read and clear `lastAdmitResp` (under `mu`, single lock acquisition)
2. If previous response is enabled: `AdmittedWorkDone(prevResp, actualUsed)`
3. Update EWMA: `ewma = 0.5 * actualUsed + 0.5 * ewma`
4. Compute next reservation: `clamp(ewma * 2.0, 10ms, 1s)`
5. `Admit(ctx, workInfo{RequestedCount: nextSize})` — blocks if tokens
   exhausted
6. Store new `lastAdmitResp` (under `mu`)
7. `reservedTokenNanos.Store(nextSize)` — last, so other goroutines
   see tokens after `lastAdmitResp` is set


## Performance

### Benchmark Results (Apple M3 Pro)

**`BenchmarkSettleAndAdmit`** — Isolates the settlement slow path:

| Goroutines | ns/op | allocs/op |
|-----------|-------|-----------|
| 1 | ~327 | 0 |
| 2 | ~380 | 0 |
| 4 | ~485 | 0 |
| 8 | ~520 | 0 |

Zero allocations. Modest scaling with contention.

**`BenchmarkMeasureAndAdmit`** — Cancel-checker hot path with
simulated row processing:

| Metric | Value |
|--------|-------|
| allocs/op | 1 (doWork simulation) |
| settlements/op | 0 |

Zero settlements per `MeasureAndAdmit` call, confirming that the
EWMA-sized reservation outlasts typical measurement intervals.

**`BenchmarkSQLCPUHandleConcurrent`** — Multi-query, multi-goroutine
workload sharing one per-node WorkQueue:

No contention scaling with increasing handles/goroutines, confirming
that the reservation mechanism effectively amortizes WorkQueue access.

### Hot Path Cost

The cancel checker hot path (`GoroutineCPUHandle.measureAndAdmit`) is:
1. `grunning.Time()` — reads per-goroutine CPU clock (~15ns)
2. Subtraction + comparison (~1ns)
3. `atomic.Int64.Add` on `reservedTokenNanos` (~5ns)
4. `atomic.Int64.Load` to check if exhausted (~1ns)

Total: ~22ns when tokens remain (no locks, no WorkQueue interaction).


## Comparison with Earlier Approaches

### Sumeer's Prototype (`sumeer/cpu_token_all`)

Sumeer's `cpu_token_all` branch (3 commits above master) was the
original prototype for SQL CPU admission. It introduced
`SQLCPUAdmissionHandle` in `sql_cpu_admission.go` (318 lines) with
a fundamentally different architecture:

**Architecture**: The handle uses `SQLCPUAdmissionQueue`, an
interface wrapping `CPUGrantCoordinator.AdmitSQLCPU` /
`AdmittedSQLWorkDone`. Each `getCPU(ctx, diff)` call goes through
`AdmitSQLCPU` → `WorkQueue.Admit` (with `isSQLCPU: true` marker),
and settlement via `AdmittedSQLWorkDone` → `WorkQueue.AdmittedWorkDone`.
There is no reservation buffer — every `TryAdmit` that detects CPU
usage calls `getCPU` which calls `Admit` on the WorkQueue.

**Concurrency**: Uses a buffered `acquiringCh` channel (capacity 1)
as a turn-taking mechanism. Only one goroutine at a time can call
`AdmitSQLCPU` — others wait on the channel. This is conceptually
similar to our `settleMu`, but uses a channel instead of a mutex,
and the inner logic holds `mu` across the Admit/done sequence.

**Token estimation**: Relies on the WorkQueue's existing per-tenant
`cpuTimeTokenEstimator` (EWMA on past `AdmittedWorkDone` reports)
to size the `RequestedCount` at `Admit` time:
`RequestedCount = max(info.RequestedCount, tenant.cpuTimeTokenEstimator.meanCPUTokens())`.
No handle-level EWMA or cross-handle learning.

**Granter changes**: Introduced `CPUTimeTokenGrantCoordinator` with
a standalone `cpuTimeTokenGranter` (separate from the slot-based
`GrantCoordinator`), 2 WorkQueues by WorkClass (Regular/Elastic),
per-WorkClass×getterKind token buckets, and `cpuTimeTokenAdjusterNew`
for rate adjustment. Also added `slotAndCPUTimeTokenGranter` for the
slot+token hybrid path. The `isCPUTimeTokenQueue` flag on WorkQueue
enables per-tenant CPU token burst limits (`tenantCPUBurstLimit`),
`getterKind` (one/two) priority classes, and SQL-specific accounting
(`intervalStats.sqlCPUTokens`, `sqlAdmittedCount`).

**WorkQueue changes**: Significant — ~500 lines of diff. Restructured
`Admit` return type from `(enabled bool, err error)` to `AdmitResponse`
struct with `CPUTokensDeducted`, `Err`, `tenantID`, `isSQLCPU`.
Changed `hasWaitingRequests` from `bool` to `getterKind`. Added
`getterKindOne`/`getterKindTwo` priority classes. Renamed
`waitingWorkHeap` to `waitingForGranterHeap`. Added per-tenant CPU
token tracking (`tenantCPUTokens`, `tenantCPUBurstLimit`,
`adjustTenantCPUTokens`). Added `slowMutexWithLogging` throughout.

**Integration**: Wired through `execinfra.ServerConfig.SQLCPUAdmissionQ`,
`FlowBase.mu.ah`. Flow creates handle in `StartInternal` and
closes in `setStatus(flowFinished)`. All goroutines call
`TryAdmit` instead of the old per-response admission.

### Our Approach vs Sumeer's Prototype

The key architectural differences:

| Aspect | Sumeer's prototype | Our approach |
|--------|-------------------|-------------|
| **Reservation buffer** | None — every `getCPU` calls `Admit` | EWMA-sized; WorkQueue only on exhaustion |
| **Token sizing** | WorkQueue estimator (`meanCPUTokens`) | Handle-level EWMA with cross-handle learning |
| **Concurrency mechanism** | `acquiringCh` channel (turn-taking) | `settleMu` mutex + atomic double-check |
| **Granter infrastructure** | New `CPUTimeTokenGrantCoordinator`, 2 WorkQueues, `cpuTimeTokenChildGranter` | Uses existing CTT WorkQueues from master |
| **WorkQueue changes** | ~500 lines (new `AdmitResponse`, `getterKind`, per-tenant CPU tokens) | Minimal (relaxed `AdmittedWorkDone` restriction, estimation guard) |
| **Per-call overhead** | High (channel + mutex + `Admit` round-trip every `TryAdmit`) | Lowest (atomic deduct; WorkQueue on exhaustion only) |
| **Settlement frequency per 100ms CPU** | ~many (every CPU diff) | ~1x (after EWMA convergence) |
| **Close cleanup** | `AdmittedSQLWorkDone(resp, additionalCPU + CPUTokensDeducted)` | `AdmittedWorkDone(resp, actualUsed)` + EWMA save |
| **Cross-handle learning** | None | EWMA carried via `gatewayEWMANanos`/`flowEWMANanos` |
| **WorkQueue invasiveness** | Heavy — new return types, getterKind, per-tenant CPU tracking | Light — reuses existing infrastructure |
| **Cumulative CPU accounting** | Not tracked separately | `cumulativeGatewayCPUNanos`/`cumulativeDistSQLCPUNanos` |
| **Goroutine handle pooling** | No (allocates per goroutine) | Yes (`sync.Pool` for `GoroutineCPUHandle`) |

**Key takeaway**: Sumeer's prototype took an approach similar to
Option A (per-call `Admit`) but built substantial new infrastructure
in the granter and WorkQueue layers. Our approach achieves the same
goals with much less WorkQueue churn by adding the EWMA reservation
layer on top — the amortization makes per-call `Admit` overhead
irrelevant, so we don't need the granter changes.

**What we built on from sumeer's prototype**:
- The `SQLCPUAdmissionHandle` / `GoroutineCPUHandle` split (handle
  per query, sub-handle per goroutine)
- `grunning.Time()`-based per-goroutine CPU measurement
- `PauseMeasuring`/`UnpauseMeasuring` for KV work exclusion
- Context propagation via `ContextWithSQLCPUAdmissionHandle`
- The idea of routing SQL CPU through WorkQueue for tenant fair-sharing

**What we changed**:
- Added EWMA reservation buffer to amortize WorkQueue overhead
- Cross-handle EWMA learning for warm starts
- Used existing CTT WorkQueues instead of new
  `CPUTimeTokenGrantCoordinator`
- Simplified WorkQueue integration (no `getterKind`, no `isSQLCPU`
  marker, no per-tenant CPU token tracking)
- Added `sync.Pool` for goroutine handles
- Added cumulative CPU counters for observability

### What Sumeer's Prototype Had That We Don't Need

Several pieces of infrastructure in `cpu_token_all` are unnecessary
given our EWMA reservation approach or are already covered by
master's CTT implementation:

- **`slotAndCPUTimeTokenGranter`** — hybrid slot+token granter for
  the old serverless isolation path. Master already has the pure
  CTT `cpuTimeTokenGranter`; we reuse it directly.
- **`CPUTimeTokenGrantCoordinator`** — separate coordinator with
  its own `cpuTimeTokenGranter` and filler goroutine. We reuse the
  existing `CPUGrantCoordinators` and its `GetCTTWorkQueue`.
- **`getterKind` one/two priority classes** — infrastructure for
  burstable vs non-burstable admission at the WorkQueue level.
  Master handles this through `burstQualification` in the existing
  CTT token buckets.
- **`acquiringCh` turn-taking channel** — our `settleMu` mutex
  with atomic double-check is simpler and equivalent.
- **`cpuTimeTokenAdjusterNew`** — separate rate adjustment for the
  new coordinator. We use the existing `cpuTimeTokenFiller` which
  already handles rate adjustment and refill.

### What Sumeer's Prototype Had That We Could Consider

A few aspects of sumeer's prototype provide observability that we
don't currently have:

**`isSQLCPU` marker in `WorkInfo`**: Sumeer added `isSQLCPU bool`
to `WorkInfo` and tracked `intervalStats.sqlCPUTokens` and
`sqlAdmittedCount` separately per tenant. This gives visibility
into what fraction of CPU admission is SQL work vs KV work at the
WorkQueue level. Our cumulative CPU counters
(`cumulativeGatewayCPUNanos` / `cumulativeDistSQLCPUNanos`) track
total SQL CPU on the provider, but the WorkQueue doesn't
distinguish SQL vs KV `Admit` calls. Adding a marker would improve
observability but isn't needed for correctness or the Resource
Manager.

**`CPUTokensDeducted` in `AdmitResponse`**: Sumeer's
`AdmitResponse` included `CPUTokensDeducted` — the amount the
WorkQueue's estimator actually deducted. In `AdmittedWorkDone`,
the delta `cpuTokens - resp.CPUTokensDeducted` is the correction
to the estimator. In our approach, we specify `RequestedCount`
(our EWMA reservation) and pass `actualUsed` to
`AdmittedWorkDone`. The WorkQueue's internal
`cpuTimeTokenEstimator` adjusts from the `actualUsed` value. Both
paths feed the estimator correctly; our approach just doesn't
explicitly track the deducted amount since `totalReserved` serves
a similar purpose.

**Per-tenant interval stats**: Sumeer tracked `intervalStats` per
tenant — `admittedCount`, `getterOneCount`, `cpuTokens`,
`sqlCPUTokens`, `sqlAdmittedCount`, `waitTimeSum`. This is richer
per-tenant observability per adjustment interval. We rely on the
existing `tenantAggMetrics` for per-tenant admission counts and
wait times, which provides similar coverage through the standard
WorkQueue metrics path.

**`SlotsOrNoopQueueForOldSQL` wrapper**: Sumeer wrapped the legacy
queue in a struct that checks the cluster setting internally, so
call sites just call `admissionQ.Admit()` without knowing about
the toggle. Our approach passes the raw `*WorkQueue` and checks
the setting inside `MeasureAndAdmitResponse`. Sumeer's is slightly
cleaner but moot once we remove legacy queues.

**Net assessment**: Nothing in `cpu_token_all` is strictly better
in a way that warrants changes now. The `isSQLCPU` marker is the
one item worth considering as a future observability improvement.

### Earlier Exploration Branches

Two additional exploration branches were developed:

**Lease model (`wenyihu6/sqlcpu`)**: Direct granter interaction with
fixed 1ms token chunks. `SQLCPUHandle` held a `*cpuTimeTokenGranter`
directly, calling `tryGet`/`waitForTokens`. Blocking used
`tokenAvailCond.Broadcast()` every ~1ms (thundering herd pattern).
Also removed legacy queues completely. Rejected because it bypasses
WorkQueue's tenant fair-sharing, which is needed for the Resource
Manager.

**Option A (`wenyihu6/sqlcpu-option-a`)**: WorkQueue path with no
reservation buffer — every `measureAndAdmit` called
`WorkQueue.Admit` + `AdmittedWorkDone`. Full tenant fair-sharing but
maximum WorkQueue lock contention.

### Wakeup Mechanism: Targeted Grant

Our approach uses the WorkQueue's standard grant mechanism for
waking blocked waiters. When tokens are exhausted and `settleAndAdmit`
blocks in `WorkQueue.Admit()`, the filler refill cycle wakes exactly
one waiter via the tenant priority heap:

```
filler.refill() [every ~1ms]
  → cpuTimeTokenGranter.refill() [adds tokens to buckets]
    → grantUntilNoWaitingRequestsLocked()
      → tryGrantLocked()
        → WorkQueue.granted() [pops top tenant from heap]
          → item.ch <- grantChainID [sends on per-request channel]
            → blocked Admit() unblocks
```

This avoids the thundering herd pattern of the lease model's
`Broadcast()` and provides proper tenant ordering. Combined with
EWMA-sized reservations, most handles never block at all.


## Future Work

1. **Remove legacy SQL response queues**: Once CTT-based SQL
   admission is validated, remove `SQLKVResponseWork`,
   `SQLSQLResponseWork`, `tokenGranter`, and the
   `SQLKVResponseAdmissionQ` / `SQLSQLResponseAdmissionQ` plumbing
   from ~15 files. Remove the
   `admission.sql_cpu_based_response_admission.enabled` setting.

2. **Resource Manager integration**: Resource groups map to tenants
   in the WorkQueue with configurable CPU weights. The SQL CPU handle
   already routes through the CTT WorkQueue, so resource groups get
   CPU isolation with no changes to the handle code.

3. **Elastic CPU for SQL**: Background SQL work (statistics
   collection, schema changes) could use a similar handle pattern
   integrated with the `ElasticCPUGrantCoordinator`.


## File Map

| File | Role |
|------|------|
| `pkg/util/admission/sql_cpu_handle.go` | `SQLCPUHandle`, `GoroutineCPUHandle`, `SQLCPUProvider` |
| `pkg/util/admission/cpu_time_token_grant_coordinator.go` | `CPUGrantCoordinators.GetCTTWorkQueue`, knobs wiring |
| `pkg/util/admission/work_queue.go` | `AdmittedWorkDone` generalization, estimation guard |
| `pkg/sql/flowinfra/flow.go` | `MakeCPUHandle`, `FlowBase.GetCPUHandle` |
| `pkg/sql/flowinfra/inbound.go` | Inbound stream goroutine registration |
| `pkg/sql/row/kv_batch_fetcher.go` | `MeasureAndAdmitResponse` for KV batches |
| `pkg/kv/kvclient/kvstreamer/streamer.go` | `MeasureAndAdmitResponse` for async KV |
| `pkg/sql/colflow/colrpc/inbox.go` | `MeasureAndAdmitResponse` for DistSQL messages |
| `pkg/sql/tablewriter.go` | `MeasureAndAdmitResponse` for table writes |
| `pkg/kv/db.go` | `SQLCPUProvider` field on `kv.DB` |
| `pkg/server/server.go` | Wiring: CTT queues → `sqlCPUProviderImpl` → `kv.DB` |
| `pkg/server/sql_cpu_integration_test.go` | End-to-end integration test |
