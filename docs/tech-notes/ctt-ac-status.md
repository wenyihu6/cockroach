# CPU Time Token Admission Control: Implementation Status

This document compares the CTT-AC prototype (`cpu_token_sim2` branch, Sumeer's
original exploration) against the production implementation on `origin/master`
(as of March 2026), and identifies remaining work.

## Background

CTT-AC replaces slot-based CPU admission control with a token-bucket approach:
1. Does not over-admit (keeps p99 goroutine scheduling latency <= 2ms)
2. Fair-shares CPU across tenants using actual CPU time, not concurrency slots

See the two design documents for full motivation:
- `[60%] Eng Design Doc: CPU Time Token (CTT) Admission Control For Serverless`
  (Josh Imhoff, eng design)
- `CPU Time Token based Admission Control (CTT-AC) in Serverless (and Beyond)`
  (Sumeer Bhola, design sketch with experimental results)

## Architecture: Prototype vs Master

### Prototype (`cpu_token_sim2`)

The prototype was built as modifications to existing admission control types:

- **Granter**: `slotAndCPUTimeTokenGranter` wrapping `slotGranter`, embedding
  two token counters (`cpuTimeTokensOne`, `cpuTimeTokensTwo`) directly as
  `int64` fields.
- **Adjuster**: `cpuTimeTokenAdjuster` embedded in `kv_slot_adjuster.go`,
  computing rates and fitting the `tokenToCPUTimeMultiplier`.
- **WorkQueue**: Modified in-place with `isCPUTimeTokenQueue` bool flag,
  adding per-tenant CPU token fields to `tenantInfo`.
- **Coordinator**: Background goroutine added directly to `GrantCoordinator`
  with 1ms ticker.
- **Naming**: Used `getterKind` (One=burst/85p, Two=main/80p), which was
  confusing.

### Master (Production)

Master has a clean, decomposed implementation in dedicated files:

| File | Lines | Purpose |
|------|-------|---------|
| `cpu_time_token_granter.go` | 303 | `cpuTimeTokenGranter` with `[tier][burstQual]` bucket matrix |
| `cpu_time_token_filler.go` | 652 | `cpuTimeTokenFiller` + `cpuTimeTokenAllocator` + `cpuTimeTokenLinearModel` |
| `cpu_time_token_grant_coordinator.go` | 198 | `CPUGrantCoordinators` shim + `cpuTimeTokenGrantCoordinator` |
| `cpu_time_token_burst.go` | 106 | `cpuTimeBurstBucket` per-tenant burst qualification |
| `cpu_time_token_estimation.go` | 80 | `cpuTimeTokenEstimator` for per-tenant mean CPU |
| `sql_cpu_handle.go` | 289 | `SQLCPUHandle` + `GoroutineCPUHandle` for SQL CPU tracking |

Key architectural differences from the prototype:

#### 1. Resource Tiers (master) vs Flat Buckets (prototype)

**Prototype**: Two shared buckets (`cpuTimeTokensOne` at 85%, `cpuTimeTokensTwo`
at 80%) with system tenant bypassing via cluster setting.

**Master**: `resourceTier` enum with `systemTenant` (tier 0) and `appTenant`
(tier 1). The granter has a `[numResourceTiers][numBurstQualifications]tokenBucket`
matrix. Each tier has its own WorkQueue and child granter. This cleanly supports
the design doc's hierarchy where system work gets priority.

#### 2. Burst Qualification (master) vs getterKind (prototype)

**Prototype**: `getterKind` (confusing naming: `getterKindOne` = burst-qualified,
`getterKindTwo` = not).

**Master**: `burstQualification` with `canBurst`/`noBurst`. Dedicated
`cpuTimeBurstBucket` type with proper `refill()`, `adjust()`, capacity tracking,
and `SafeFormat` for logging. The burst bucket is separated into its own file
with clear documentation.

#### 3. Grant Coordination (master) vs Embedded (prototype)

**Prototype**: CTT logic embedded directly in `GrantCoordinator` with
`isCPUTimeTokenGranter` flag. Background goroutine inline in
`makeRegularGrantCoordinator`.

**Master**: `CPUGrantCoordinators` shim that dynamically selects between
slot-based (`slotsCoord`) and CTT-based (`cpuTimeCoord`) depending on the
`admission.cpu_time_tokens.enabled` cluster setting. No process restart needed
to switch. The `cpuTimeTokenGrantCoordinator` is separate from the slot-based
`GrantCoordinator`.

#### 4. Token Filler (master) vs Adjuster (prototype)

**Prototype**: `cpuTimeTokenAdjuster` combined rate computation, linear model
fitting, and tick allocation in one struct.

**Master**: Decomposed into:
- `cpuTimeTokenFiller`: owns the 1ms ticker goroutine
- `cpuTimeTokenAllocator`: computes rates, manages allocations
- `cpuTimeTokenLinearModel`: fits `tokenToCPUTimeMultiplier`

#### 5. WorkQueue Mode (master) vs Bool Flag (prototype)

**Prototype**: `isCPUTimeTokenQueue` bool added to WorkQueue.

**Master**: `workQueueMode` enum (`usesSlots`, `usesTokens`, `usesCPUTimeTokens`).
Cleaner separation; the mode controls behavior throughout `Admit()`,
`AdmittedWorkDone()`, etc.

#### 6. SQL CPU Integration (master only)

Master has `SQLCPUHandle` and `GoroutineCPUHandle` for tracking SQL CPU across
goroutines. This was not in the prototype at all. Key components:
- `SQLCPUHandle`: manages CPU accounting for a SQL statement across goroutines
- `GoroutineCPUHandle`: per-goroutine CPU measurement using `grunning.Time()`
- `flowinfra.MakeCPUHandle`: helper for initializing and injecting into context
- Integrated at gateway (`connExecutor.dispatchToExecutionEngine`) and remote
  flows (`ServerImpl.setupFlow`)
- `SQLCPUProvider` interface with `GetCumulativeSQLCPUNanos()` for reporting

## What Exists on Master but NOT on Prototype

| Feature | Master Commit | Description |
|---------|--------------|-------------|
| Dedicated CTT files | `314400cf389`..`501ccea3f6a` | Clean decomposition into 5+ files |
| `resourceTier` hierarchy | `41ddf78bb90` | System vs app tenant tiers with separate WorkQueues |
| `burstQualification` naming | `314400cf389` | Clear `canBurst`/`noBurst` replacing confusing `getterKind` |
| `workQueueMode` enum | `60e0abcb5bf` | `usesCPUTimeTokens` mode vs bool flag |
| `cpuTimeBurstBucket` | `d40c7d36a55` | Dedicated burst bucket type with `refill()`/`adjust()` |
| Dynamic burst determination | `d40c7d36a55` | Per-tenant burst qualification tracked dynamically |
| `CPUGrantCoordinators` shim | `77dbb3084e2` | Live switching between slots and CTT without restart |
| SQL bypass when CTT enabled | `ab443fc82d6` | SQL-KV/SQL-SQL work bypasses AC under CTT |
| `SQLCPUHandle` | `fdff27dcf7f` | Per-statement CPU tracking across goroutines |
| `GoroutineCPUHandle` | `fdff27dcf7f` | Per-goroutine CPU measurement |
| SQL CPU cumulative tracking | `31b7e6ec05e` | `GetCumulativeSQLCPUNanos` for gateway/dist SQL |
| Env var kill switch | `7d78dbfa730` | `COCKROACH_DISABLE_CPU_TIME_TOKEN_AC` for emergencies |
| `cpuTimeTokenEstimator` file | `501ccea3f6a` | Separated into own file with full documentation |
| Filler/Allocator/Model split | `a973d972037`, `1acb16b77af` | Clean decomposition of rate computation |
| WorkloadID plumbing | `67495edb8f2` | `workloadID` plumbed through SQL execution |

## What Exists on Prototype but NOT on Master

| Feature | Prototype Detail | Status on Master |
|---------|-----------------|------------------|
| `slowMutexWithLogging` | Debug scaffold for mutex contention | Not needed (debugging aid) |
| Inline `log.Infof` per-tenant stats | 30s periodic tenant CPU stats logging | Different logging approach |
| `intUncontrolledTokens` tracking | Tokens used when main bucket exhausted | Not tracked separately |
| `tenantIntervalStats` struct | Per-tenant interval statistics (under/overestimate) | Different stats approach |
| Combined slot+token granter | `slotAndCPUTimeTokenGranter` wrapping `slotGranter` | Separate granters on master |
| Slot overload threshold = 8192 | Effectively disables slot-based AC | Master uses dynamic switching |

Note: The prototype's core *ideas* are all present on master, but refactored
into cleaner production code. The prototype's specific implementation details
(inline modifications, debug aids) are not on master.

## What's NOT Done on Either Branch

### From Eng Design Doc (Future Work)

#### Per-Tenant Rate Limiting
- Build rate limiter on top of CTT buckets to replace old eCPU-model limiter
- Two-pronged approach: rate limiting (25% per tenant) + fair sharing (80%)
- Needed to prevent single-tenant spike from causing noisy neighbor

#### Dedicated / Self-Hosted Integration
- Marked "Todo" in design doc
- Integrate CTT AC into single-tenant CRDB

#### Elastic CPU Unification
- Elastic work uses its own separate system
- `intElasticTokensUsed` hardcoded to 0 in prototype's adjuster
- Master similarly does not account for elastic CPU in token multiplier
- Design doc suggests merging CTT AC and elastic AC

### From Sumeer's Design Sketch (Section 4: Extending Beyond Serverless)

#### Generalized Resource Groups (Section 4.1)
- Resource group abstraction: (limit, burstable-limit, BQL, Level)
- Master has `resourceTier` (2 tiers), but not the full generalization
- No SQL syntax for creating resource groups

#### Resource Groups in Non-Serverless Clusters (Section 4.2)
- RG1 (level=1): all work >= normal-pri, limit=100%, burstable=100%
- RG2 (level=2): user-low-pri, limit=50%, burstable=80%, BQL=20%
- `CREATE RESOURCE GROUP <name> CPU_MIN=<pct> [FULLY_UTILIZE]`
- Weighted fair sharing across groups (weights = CPU_MIN values)
- Dynamic reduction of 100% limit when scheduling backlog builds

#### SQL Integration for Non-Serverless (Section 4.3)
- Use CPU time tokens for all of kv, sql-kv-response, sql-sql-response work
- Master currently bypasses SQL-KV/SQL-SQL under CTT
  (`ab443fc82d6`: "admission: bypass SQL if CPU time token AC")
- SQL CPU tracking infrastructure exists (`SQLCPUHandle`) but not wired
  into CTT token deduction

#### Additional Design Doc TODOs
- Different percentiles for initial deduction (not just mean)
- Write amplification: per-byte cost model for replication + compaction CPU
- Periodic CPU reporting for long-running admitted work
- Handle 100% utilization goal (burst budget concerns)

### Key Risks & Open Questions
- Tokens deducted may not be consumed immediately (lock/latch waits, IO
  misses) causing under-utilization then later burstiness
- `tokenToCPUTimeMultiplier` can be inaccurate in outlier cases (GC spikes)
- At 100% utilization target, no tokens exhausted (risky in open-loop)
- Only ~28% of CPU observed by AC in some experiments
- SQL CPU integration: infrastructure exists but not connected to CTT tokens

## Summary

The prototype (`cpu_token_sim2`) validated the core CTT-AC ideas and produced
experimental results showing the approach works (80% CPU utilization, <2ms p99
scheduling latency). Master has taken these ideas and built a clean, production-
quality implementation with:

1. Proper file decomposition and naming
2. Resource tier hierarchy (system vs app tenant)
3. Dynamic switching between slot-based and CTT AC
4. SQL CPU tracking infrastructure (`SQLCPUHandle`)
5. Environment variable kill switch for emergencies

The main remaining work is in the "Resource Manager" direction:
- Generalizing resource tiers into user-defined resource groups
- Wiring SQL CPU tracking into CTT token deduction
- Integrating elastic CPU with CTT
- Per-tenant rate limiting on top of CTT buckets
- `CREATE RESOURCE GROUP` SQL syntax for non-serverless clusters