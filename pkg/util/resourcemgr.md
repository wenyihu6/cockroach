# Resource Manager Prototype: Implementation Design

Author: Wenyi Hu
Date: 2026-03-14

## Overview

This document describes the implementation of the Resource Manager (RM)
prototype on the `rm` branch. The RM extends CockroachDB's CPU Time Token
(CTT) admission control to support N configurable resource groups, each
with a minimum CPU guarantee and optional full-utilization access.

The prototype builds on the existing CTT infrastructure in
`pkg/util/admission/`. It collapses the 2-queue, 4-bucket Serverless
design into a 1-queue, 2-bucket design where resource groups appear as
tenants with configurable weights and burst limits.

## Motivation

The existing CTT system was built for Serverless, where there are exactly
two classes of work: system tenant and app tenant. Priority is hardcoded
via separate WorkQueues and separate utilization targets (95% for system,
80% for app). This design cannot express N resource groups with
arbitrary CPU shares.

The Resource Manager design (from the internal design doc "CPU Time Token
based Admission Control") calls for:

- N resource groups defined via SQL DDL (`CREATE RESOURCE GROUP`)
- Each group has a minimum CPU share (`CPU_MIN`) and optional
`FULLY_UTILIZE` flag
- All groups share a single WorkQueue with weighted fair sharing
- Two global token buckets: 75% (noBurst) and 100% (canBurst)
- Groups that stay below their CPU_MIN qualify for burst (access to the
100% bucket); groups that exceed it are limited to the 75% bucket
- `FULLY_UTILIZE` groups always qualify for burst

## Design

### Architecture Change: 2 Queues → 1 Queue

**Before (Serverless)**:

```
cpuTimeTokenGrantCoordinator
├── WorkQueue[systemTenant]  (tier 0, targets: 95%/100%)
│    └── childGranter(tier=0) → cpuTimeTokenGranter
├── WorkQueue[appTenant]     (tier 1, targets: 80%/85%)
│    └── childGranter(tier=1) → cpuTimeTokenGranter
└── cpuTimeTokenGranter
     └── buckets[2 tiers][2 burst quals] = 4 buckets
```

**After (RM)**:

```
cpuTimeTokenGrantCoordinator
└── WorkQueue (single, all tenants)
     └── cpuTimeTokenGranter (implements granter directly)
          └── buckets[2 burst quals] = 2 buckets
               canBurst: 100% CPU
               noBurst:   75% CPU
```

Key simplifications:
- `resourceTier` type removed entirely
- `cpuTimeTokenChildGranter` removed — the granter implements `granter`
directly with a single `requester`
- All type aliases (`rates`, `capacities`, `minimums`, `tokenCounts`,
`targetUtilizations`) collapsed from `[numResourceTiers][numBurstQualifications]`
to `[numBurstQualifications]`

### Tradeoffs: What the Tier System Provided

The Serverless 2-queue, 4-bucket design gave the system tenant four
distinct advantages. Understanding how each is replaced (and what is
lost) is important context for the 1-queue design.

**1. `tryGrantLocked` tier ordering.** The granter iterates requesters
by tier — system first. System work gets every grant opportunity as
long as it has waiting work; app work only gets granted when system
has nothing waiting.

*RM replacement*: In a single WorkQueue, the tenant heap ordering
controls who gets granted first. A FULLY_UTILIZE group with
`burstLimitFrac >= 1.0` is always `canBurst`, so it sorts before
`noBurst` groups. Within the same burst qualification, `used/weight`
determines priority.

*What's different*: The old tier ordering was unconditional — system
always got the next grant regardless of how much CPU it had consumed.
The new heap ordering is conditional: it depends on burst qualification
and `used/weight`. A FULLY_UTILIZE group that has consumed a lot of
CPU (high `used`) can be sorted behind a light non-FULLY_UTILIZE group
that happens to be `canBurst`. Example on an 8-vCPU node:

```
Groups: online_rg (FULLY_UTILIZE, weight=80), support_rg (weight=10)

Both are canBurst (support_rg is light, under its CPU_MIN).
online_rg: used=4e9 (0.5s of CPU), weight=80 → used/weight = 50M
support_rg: used=0,  weight=10               → used/weight = 0

Heap order: support_rg first (lower used/weight).

Old design: system always won regardless of used.
New design: support_rg gets the next grant.
```

This is intentional — support_rg is light, so granting to it is
harmless and gives it better latency. But FULLY_UTILIZE groups no
longer have *unconditional* priority; they have priority that depends
on relative consumption.

The gap is most visible right after `used` resets (every ~1s). At that
moment all tenants have `used/weight = 0`, so if multiple tenants are
`canBurst`, tie-breaking goes to higher weight then lower tenant ID.
A non-FULLY_UTILIZE tenant with higher weight or lower ID gets granted
first. This gap lasts until `used` diverges (typically a few grants).

**2. Graduated admission via separate granter buckets per tier.**
Deduct-all means all work deducts from all 4 buckets. The invariant
(`systemCB >= systemNB >= appCB >= appNB`) creates graduated admission
levels — as total load increases, buckets go negative from bottom up:

```
Starting:      systemCB=6  systemNB=5  appCB=2  appNB=1

After 1 token:  5, 4, 1, 0    → app noBurst BLOCKED
After 2 tokens: 4, 3, 0, -1   → ALL app BLOCKED
After 5 tokens: 1, 0, -3, -4  → system noBurst BLOCKED
After 6 tokens: 0, -1, -4, -5 → ALL work BLOCKED
```

There is a window (between "all app blocked" and "system blocked")
where system work flows but app work is rejected.

*RM replacement*: With 2 buckets, FULLY_UTILIZE groups always check
the canBurst bucket, and heavy non-FULLY_UTILIZE groups check the
noBurst bucket. The key differentiation (FULLY_UTILIZE keeps going
when others are blocked) is preserved:

```
canBurst bucket (100%):  8 tokens
noBurst bucket (75%):    6 tokens

Heavy non-FULLY_UTILIZE work drains 7 tokens:
  canBurst: 1   ← still positive → FULLY_UTILIZE keeps going
  noBurst: -1   ← negative → heavy non-FULLY_UTILIZE BLOCKED
```

*What's different*: We go from 4 admission levels to 2. All heavy
non-FULLY_UTILIZE groups share the same noBurst bucket and block at
the same time — there is no bucket-level differentiation between them.
Example with 3 non-FULLY_UTILIZE groups:

```
Groups: batch_rg (weight=10), support_rg (weight=10), analytics_rg (weight=5)
All three are heavy (exceed CPU_MIN) → all noBurst.

noBurst bucket goes negative.
Result: ALL THREE are blocked simultaneously.

Old design with 3 tiers would have blocked them one at a time
(lowest tier first), creating windows where higher-tier work
continued while lower-tier work was rejected.
```

When noBurst is exhausted, the only thing differentiating these groups
is their position in the tenant heap (`used/weight`). When noBurst
refills, the group with the lowest `used/weight` gets the next grant.
But there is no window where one non-FULLY_UTILIZE group continues
while another is rejected — they all gate on the same bucket.

This is sufficient when the key distinction is FULLY_UTILIZE vs.
non-FULLY_UTILIZE. But if you need more than 2 priority tiers with
hard admission boundaries (not just heap ordering), the 2-bucket
design cannot express that without adding more buckets.

**3. Different utilization targets per tier.** System had a higher
target (e.g., 95%) than app (80%), so system buckets refilled faster
and had more tokens.

*RM replacement*: A single noBurst target (75%) and canBurst target
(100%) for all work. FULLY_UTILIZE groups don't need a higher refill
rate because they always check the canBurst bucket (which is refilled
at the 100% target). Non-FULLY_UTILIZE groups are limited to the 75%
bucket when heavy.

*What's different*: There is one pair of utilization targets shared
by all groups. You cannot give one group a higher refill rate than
another. Example:

```
Old design:
  system target: 95%  → system buckets refill at 95% of CPU capacity
  app target:    80%  → app buckets refill at 80% of CPU capacity
  Operator can raise system to 98% without touching app.

New design:
  noBurst target:  75%  → noBurst bucket refills at 75%
  canBurst target: 100% → canBurst bucket refills at 100%
  These are global. All groups share them.
```

If an operator wanted a specific non-FULLY_UTILIZE group to run at
90% utilization while others stay at 75%, there's no knob for that.
The per-group control is `burstLimitFrac`, which controls *whether*
a group accesses the 100% bucket, not *how fast* either bucket
refills. The refill rate is the same for everyone.

In practice this hasn't been needed — the Serverless per-tier targets
were fixed constants (not operator-tuned), and the RM design doc
doesn't call for per-group utilization targets. But it is a
capability that existed before and doesn't exist now.

**4. Separate WorkQueues.** System and app tenants never compete in
the same heap. A heavy app tenant cannot push a system tenant down
in priority because they are in different queues.

*RM replacement*: All tenants share one heap. A FULLY_UTILIZE group
stays `canBurst` and sorts before heavy non-FULLY_UTILIZE groups
(different burst qualification). Within the same burst qualification,
`used/weight` determines order.

*What's different*: With separate queues, a bug in heap ordering or
burst qualification only affected one tier. With a single shared heap,
any such bug affects all resource groups. Example:

```
Old design:
  Bug in tenant heap comparison → only app tenants are misordered.
  System tenant is in a separate queue, unaffected.

New design:
  Same bug → all resource groups are misordered, including
  FULLY_UTILIZE groups.
```

This is a standard isolation-vs-simplicity tradeoff. The mitigation
is that the single queue is simpler (fewer code paths, fewer edge
cases), which reduces the likelihood of bugs in the first place.
Additionally, a shared heap means more eyeballs on one code path
rather than two rarely-exercised paths.

**Summary**: The 1-queue design replaces tier-based **isolation** with
ordering-based **priority**. The key enabling mechanism is per-group
BQL: FULLY_UTILIZE groups are always `canBurst` and therefore always
access the higher bucket and sort first in the heap. The real costs
are: (1) no unconditional absolute priority for FULLY_UTILIZE groups,
(2) fewer graduated admission levels (2 vs 4), (3) no per-group
utilization tuning, and (4) shared-queue blast radius. These are
acceptable because the RM design doc explicitly calls for a 1-queue
design where burst qualification and `used/weight` provide the
necessary differentiation.

### Cluster Settings

Two per-tier settings replaced with one:

| Before | After |
|--------|-------|
| `admission.cpu_time_tokens.target_util.app_tenant` = 0.80 | `admission.cpu_time_tokens.target_util` = 0.75 |
| `admission.cpu_time_tokens.target_util.system_tenant` = 0.95 | (removed) |
| `admission.cpu_time_tokens.target_util.burst_delta` = 0.05 | `admission.cpu_time_tokens.target_util.burst_delta` = 0.25 |

The 75% noBurst target and 25% burst delta yield:
- noBurst bucket: 75% of CPU capacity
- canBurst bucket: 100% of CPU capacity

The 75% value is chosen to keep goroutine scheduling latency low (the
design doc notes this is configurable and conservative).

### Resource Group Configuration

Each resource group maps to a tenant in the WorkQueue with two properties:

- **Weight** (`uint32`): Equal to CPU_MIN. Controls proportional fair
sharing via `used/weight` in the tenant heap.
- **BurstLimitFrac** (`float64`): Controls burst qualification.
  - `>= 1.0`: FULLY_UTILIZE — always qualifies for canBurst
  - `< 1.0`: Scales the burst bucket's refill rate and capacity.
    The tenant qualifies for canBurst when `tokens > 90% of
    (scaled) capacity`. See "How Burst Qualification Works" below.

Configuration API:

```go
type ResourceGroupConfig struct {
    Weight         uint32
    BurstLimitFrac float64
}

func (coord *CPUGrantCoordinators) SetResourceGroupConfig(
    config map[uint64]ResourceGroupConfig,
)
```

This calls `SetTenantWeights` and `SetBurstLimits` internally.

For the prototype, two resource groups are hardcoded at init:
- System tenant (ID=1): weight=9, burstLimitFrac=1.0 (FULLY_UTILIZE)
- All others: weight=1 (default), burstLimitFrac=0.25 (default)

### How Fair Sharing Works

The `tenantHeap` orders tenants by:
1. **Burst qualification** (`canBurst` before `noBurst`)
2. **`used/weight`** (lowest ratio first)
3. Ties broken by higher weight, then lower tenant ID

When `granted()` is called, the tenant at the top of the heap gets the
next grant. Over time, all tenants converge toward CPU consumption
proportional to their weights.

Note that `used` and `weight` are in different units: `used` is CPU
nanoseconds consumed (reset every ~1s), while `weight` is derived from
`CPU_MIN` (a static configuration). The ratio `used/weight` doesn't
need them in the same units — it answers "how much CPU has this tenant
consumed per unit of its configured share?" A tenant with weight=80
needs 8x the usage of a weight=10 tenant before they're considered
equal.

**Weight=0 (best-effort groups)**: `used/weight` would divide by zero.
A floor of `weight = max(CPU_MIN, 1)` is needed. Alternatively,
best-effort groups could be treated as a third burst qualification
tier (`canBurst > noBurst > bestEffort`) that only gets CPU when no
other group has waiting work.

Example with 3 resource groups:

```
CREATE RESOURCE GROUP online_rg CPU_MIN=80 FULLY_UTILIZE
CREATE RESOURCE GROUP batch_rg CPU_MIN=10
CREATE RESOURCE GROUP support_rg CPU_MIN=10

Weights: online=80, batch=10, support=10 (total=100)
online share = 80/100 = 80%
batch share  = 10/100 = 10%
support share = 10/100 = 10%
```

**Interaction between burst qualification and weight**: Burst
qualification is the primary sort key, so a `canBurst` tenant with
weight=1 still sorts before a `noBurst` tenant with weight=100. This
is intentional — `canBurst` tenants have access to the higher (100%)
granter bucket, so they must go first to avoid leaving capacity on
the table. If a `noBurst` tenant went first and the noBurst bucket
happened to be empty, the grant would fail even though the canBurst
bucket has tokens.

**Why burst qualification can't be replaced by `used/weight` alone**:
`tenant.used` is reset to 0 every ~1s by `gcTenantsResetUsedAndUpdate
Estimators`. Right after a reset, all tenants have `used=0`, so
`used/weight` is tied — a previously heavy tenant looks equal to a
light one. The burst bucket carries state across resets (it refills
and drains gradually), so it correctly maintains the heavy tenant as
`noBurst` even immediately after `used` is reset.

### How Burst Qualification Works

Each tenant has a `cpuTimeBurstBucket` — a small per-tenant token bucket
that tracks whether the tenant is a light or heavy CPU user. The burst
bucket is a cheap rate meter: instead of tracking "how much CPU did this
tenant use over the last N seconds" with timestamps and windows, it uses
a single counter that refills at a fixed rate and drains with actual
usage. If the counter is high, usage has been below the refill rate
(light tenant). If low, usage has exceeded it (heavy tenant).

**Refill**: Every 1ms, each tenant's burst bucket is refilled at a rate
proportional to its `burstLimitFrac`. The capacity is also scaled:

```
scaledRefill   = canBurstAllocation × burstLimitFrac
scaledCapacity = canBurstRate × burstLimitFrac
```

For a tenant with burstLimitFrac=0.1 (CPU_MIN=10%) on an 8-vCPU machine:
```
canBurstRate    = 1.0 × 8e9 = 8e9 tokens/sec
scaledRefill    = 8e9 / 1000 × 0.1 = 800K tokens per tick
scaledCapacity  = 8e9 × 0.1 = 800M tokens
```

Capacity = 1 second of refill (same convention as the granter buckets).
This means the bucket represents ~1 second of "budget" — it fills in
~1s when idle and drains in ~1s when over-consuming.

**Drain**: When the tenant's work is admitted, tokens are deducted from
its burst bucket proportional to the CPU estimate.

**Steady-state behavior**: In steady state, the bucket is binary:
- If `usage < refill_rate`: bucket fills to capacity (capped), stays at
  100% full. Tenant is `canBurst`.
- If `usage > refill_rate`: bucket drains to floor (`-capacity/4`).
  Tenant is `noBurst`.
- There is no steady state where the bucket sits at an intermediate
  level. It either fills to the cap or drains to the floor.

**Break-even**: A tenant loses burst when usage exceeds the refill rate:

```
Break-even when drain = refill:
  CPU_usage% × cpuCapacity = canBurstRate × burstLimitFrac
  CPU_usage% = burstLimitFrac × (canBurstRate / cpuCapacity)
  CPU_usage% = burstLimitFrac × 1.0  (since canBurstRate = cpuCapacity)
  CPU_usage% = burstLimitFrac = CPU_MIN%
```

So a tenant with CPU_MIN=10% loses burst qualification at exactly 10%
CPU usage. This is the calibration that makes the design doc's scenarios
work correctly.

**Qualification check**:

```go
func (m *cpuTimeBurstBucket) burstQualification() burstQualification {
    if m.burstLimitFrac >= 1.0 {
        return canBurst  // FULLY_UTILIZE
    }
    if m.tokens > (m.capacity*9)/10 {
        return canBurst  // bucket >90% full = using less than CPU_MIN
    }
    return noBurst       // bucket depleted = using more than CPU_MIN
}
```

**Why 90%, not 100%?** The threshold is `>90% of capacity` rather than
`>= capacity` to provide a buffer against transient usage spikes.
Refill happens every 1ms, but deductions happen at admission time
(asynchronous). Between two refill ticks, several requests could be
admitted, each deducting from the bucket. If the threshold were 100%,
any single admission would instantly drop the tenant to `noBurst`,
even if the tenant is well under its CPU_MIN over longer timescales.
The 10% buffer (10% of capacity) absorbs these per-request jitters
without changing the steady-state consumption threshold.

**Why not multiply 90% × burstLimitFrac?** The per-tenant break-even
point is controlled by the **scaled capacity and refill rate**, not by
the threshold check. The capacity is already scaled by `burstLimitFrac`
in `refillBurstBuckets`. Using `burstLimitFrac × capacity` in the
threshold would double-apply the scaling, making it too easy for small
groups to qualify.

### Design Doc Scenarios

**Scenario 1**: OPG=45%, BPG=15%, SCG=15%. Sum=75%.

All groups using less than 75% total. noBurst bucket has capacity.
BPG and SCG exceed their 10% CPU_MIN → noBurst, but noBurst bucket
is not exhausted, so they're admitted. Node=75%.

**Scenario 2**: OPG=65%, BPG=15%, SCG=15%. Sum=95%.

BPG/SCG exceed 10% CPU_MIN → lose canBurst → noBurst → throttled.
Usage drops below 10% → regain canBurst → admitted from 100% bucket.
They oscillate around 10% CPU_MIN. OPG uses 65% from 100% bucket.
Node=65%+10%+10%=85%.

**Scenario 3**: OPG=5%, BPG=50%, SCG=70%. Sum=125%.

OPG uses 5%, always canBurst (FULLY_UTILIZE). BPG/SCG are noBurst
(exceed 10% CPU_MIN). noBurst bucket = 75%. OPG drains 5% from it
via deduct-all, leaving 70% for noBurst work. BPG/SCG fair-share
70% by weight (10:10) = 35% each. Node=75%.

### Granter Bucket Mechanics

Two global token buckets, shared by all tenants:

```
canBurst: refilled at 100% × cpuCapacity / multiplier
noBurst:  refilled at 75%  × cpuCapacity / multiplier
```

**Admission**: A request checks `buckets[qual].tokens > 0` for the
tenant's burst qualification. If positive, admitted. On admission,
tokens are deducted from **both** buckets.

**Deduct-all design**: This is intentional — both buckets represent
total node CPU. When any tenant uses CPU, it reduces the budget for
everyone. The per-tenant differentiation comes from which bucket
is checked (canBurst vs noBurst), not from separate budgets.

**Minimums**: Each bucket has a floor to prevent unbounded token debt:

```
canBurst minimum: 0
noBurst minimum:  noBurstRate - canBurstRate (always negative)
```

This ensures canBurst recovers to positive before noBurst during
overload recovery, preserving the priority hierarchy.

### Refill Pipeline

Unchanged from the existing CTT design:

```
Every 1ms:
  cpuTimeTokenFiller
    → cpuTimeTokenAllocator.allocateTokens()
       → cpuTimeTokenGranter.refill()        (2 global buckets)
       → WorkQueue.refillBurstBuckets()       (N per-tenant burst buckets)

Every 1s:
  cpuTimeTokenAllocator.resetInterval()
    → cpuTimeTokenLinearModel.fit()           (recompute multiplier)
    → apply delta to global buckets
    → apply delta to per-tenant burst buckets
```

The linear model, token-to-CPU multiplier, asymmetric smoothing, and
low-CPU recovery logic are all unchanged.

### Known Limitations / Open Design Questions

**Deduct-all + fast path fairness gap**: When a FULLY_UTILIZE group
(always canBurst) uses the fast path, its admissions drain the noBurst
bucket via deduct-all without any weight constraint. For example, if
OPG uses 65% CPU via the fast path, it drains the noBurst bucket by
65%, leaving only 10% (75% - 65%) for BPG + SCG combined — they each
get ~5%, not their CPU_MIN of 10%. The design doc's example 2 expects
BPG and SCG to each get 10%, but the deduct-all mechanism produces
~5% each. The `tenantHeap` weight-based fair sharing only applies on
the slow path (queued requests); the fast path bypasses it entirely.

Potential fixes to discuss with Sumeer:
1. Weight-constrain the fast path — don't let a tenant consume more
than its weight-proportional share of noBurst via fast path
2. Don't deduct canBurst admissions from noBurst — but this breaks
the "buckets represent the same physical CPU" invariant
3. Per-group admission budgets — fundamentally different mechanism

**No scheduling latency feedback in CTT**: The pure token-based system
has no direct signal for goroutine scheduling pressure. Slots had
`isOverloaded` (usedSlots >= totalSlots), elastic CPU has
`schedulerLatencyListener`. CTT relies on the utilization target being
conservative (75%) and the multiplier indirectly catching increased
runtime overhead. The design doc suggests using runnable goroutine
count to throttle token distribution but this is not implemented.

**Elastic work not integrated**: The design doc mentions elastic work
sharing the same queue with 5% weight and 5% burst qualification limit.
This is not implemented in the prototype.

**SQL DDL not wired**: `CREATE RESOURCE GROUP` DDL is not implemented.
The prototype hardcodes two resource groups. Production wiring will
translate SQL DDL into `SetResourceGroupConfig` calls.

## Files Changed

| File | Change |
|------|--------|
| `cpu_time_token_granter.go` | Removed `resourceTier`, `cpuTimeTokenChildGranter`; collapsed to 2 buckets; `cpuTimeTokenGranter` implements `granter` directly |
| `cpu_time_token_filler.go` | All type aliases collapsed to 1D; single queue; single utilization target; removed per-tier settings |
| `cpu_time_token_grant_coordinator.go` | Single WorkQueue; removed childGranters; added `ResourceGroupConfig`, `SetResourceGroupConfig`; hardcoded 2 groups at init |
| `cpu_time_token_metrics.go` | Collapsed from `numResourceTiers × numBurstQualifications` to `numBurstQualifications` counters |
| `cpu_time_token_burst.go` | Added `burstLimitFrac`; `burstQualification()` uses `>= 1.0` for FULLY_UTILIZE, `>90% of capacity` for others; per-tenant break-even achieved via scaled capacity/refill, not threshold |
| `work_queue.go` | Added `burstLimits` map, `SetBurstLimits`, `getBurstLimitFracLocked`; `refillBurstBuckets` scales toAdd and capacity per-tenant by `burstLimitFrac`; `newTenantInfo` scales initial burst bucket capacity |

## Future Work

1. **SQL DDL wiring**: `CREATE RESOURCE GROUP` → `SetResourceGroupConfig`
2. **Elastic work integration**: Add elastic work to the same queue with
5% weight and 5% burst qualification limit
3. **Per-group utilization targets**: The design doc's section 4.1
describes resource groups with levels that define a priority ordering.
The current prototype uses burst qualification for priority, not
per-group utilization targets.
4. **Testing**: Comprehensive tests verifying the design doc's scenarios
with multiple resource groups under various load patterns
