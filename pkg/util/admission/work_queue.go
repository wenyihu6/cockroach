// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/ash"
	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// Use of the admission control package spans the SQL and KV layers. When
// running in a multi-tenant setting, we have per-tenant SQL-only servers and
// multi-tenant storage servers. These multi-tenant storage servers contain
// the multi-tenant KV layer, and the SQL layer for the system tenant. Most of
// the following settings are relevant to both kinds of servers (except for
// KVAdmissionControlEnabled). Only the system tenant can modify these
// settings in the storage servers, while a regular tenant can modify these
// settings for their SQL-only servers. Which is why these are typically
// ApplicationLevel.

// KVAdmissionControlEnabled controls whether KV server-side admission control
// is enabled.
var KVAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.enabled",
	"when true, work performed by the KV layer is subject to admission control",
	true,
	settings.WithPublic)

// KVBulkOnlyAdmissionControlEnabled controls whether user (normal and above
// priority) work is subject to admission control. If it is set to true, then
// user work will not be throttled by admission control but bulk work still will
// be. This setting is a preferable alternative to completely disabling
// admission control. It can be used reactively in cases where index backfill,
// schema modifications or other bulk operations are causing high latency due to
// io_overload on nodes.
// TODO(baptist): Find a better solution to this in v23.1.
var KVBulkOnlyAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.bulk_only.enabled",
	"when both admission.kv.enabled and this is true, only throttle bulk work",
	false)

// SQLKVResponseAdmissionControlEnabled controls whether response processing
// in SQL, for KV requests, is enabled.
var SQLKVResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.sql_kv_response.enabled",
	"when true, work performed by the SQL layer when receiving a KV response is subject to "+
		"admission control",
	true,
	settings.WithPublic)

// SQLSQLResponseAdmissionControlEnabled controls whether response processing
// in SQL, for DistSQL requests, is enabled.
var SQLSQLResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.sql_sql_response.enabled",
	"when true, work performed by the SQL layer when receiving a DistSQL response is subject "+
		"to admission control",
	true,
	settings.WithPublic)

var admissionControlEnabledSettings = [numWorkKinds]*settings.BoolSetting{
	KVWork:             KVAdmissionControlEnabled,
	SQLKVResponseWork:  SQLKVResponseAdmissionControlEnabled,
	SQLSQLResponseWork: SQLSQLResponseAdmissionControlEnabled,
}

// KVTenantWeightsEnabled controls whether tenant weights are enabled for KV
// admission control. This setting has no effect if admission.kv.enabled is
// false.
var KVTenantWeightsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.tenant_weights.enabled",
	"when true, tenant weights are enabled for KV admission control",
	false,
)

// KVStoresTenantWeightsEnabled controls whether tenant weights are enabled
// for KV-stores admission control. This setting has no effect if
// admission.kv.enabled is false.
var KVStoresTenantWeightsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.stores.tenant_weights.enabled",
	"when true, tenant weights are enabled for KV-stores admission control",
	false,
)

// EpochLIFOEnabled controls whether the adaptive epoch-LIFO scheme is enabled
// for admission control. Is only relevant when the above admission control
// settings are also set to true. Unlike those settings, which are granular
// for each kind of admission queue, this setting applies to all the queues.
// This is because we recommend that all those settings be enabled or none be
// enabled, and we don't want to carry forward unnecessarily granular
// settings.
var EpochLIFOEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.epoch_lifo.enabled",
	"when true, epoch-LIFO behavior is enabled when there is significant delay in admission",
	false,
	settings.WithPublic)

var epochLIFOEpochDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"admission.epoch_lifo.epoch_duration",
	"the duration of an epoch, for epoch-LIFO admission control ordering",
	epochLength,
	settings.DurationWithMinimum(time.Millisecond),
	settings.WithPublic)

var epochLIFOEpochClosingDeltaDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"admission.epoch_lifo.epoch_closing_delta_duration",
	"the delta duration before closing an epoch, for epoch-LIFO admission control ordering",
	epochClosingDelta,
	settings.DurationWithMinimum(time.Millisecond),
	settings.WithPublic)

var epochLIFOQueueDelayThresholdToSwitchToLIFO = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"admission.epoch_lifo.queue_delay_threshold_to_switch_to_lifo",
	"the queue delay encountered by a (tenant,priority) for switching to epoch-LIFO ordering",
	maxQueueDelayToSwitchToLifo,
	settings.DurationWithMinimum(time.Millisecond),
	settings.WithPublic)

var rangeSequencerGCThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"admission.replication_control.range_sequencer_gc_threshold",
	"the inactive duration for a range sequencer after it's garbage collected",
	5*time.Minute,
)

// WorkInfo provides information that is used to order work within an WorkQueue.
// The WorkKind is not included as a field since an WorkQueue deals with a
// single WorkKind.
type WorkInfo struct {
	// TenantID is the id of the tenant. For single-tenant clusters, this will
	// always be the SystemTenantID.
	TenantID roachpb.TenantID
	// Priority is utilized within a tenant.
	Priority admissionpb.WorkPriority
	// CreateTime is equivalent to Time.UnixNano() at the creation time of this
	// work or a parent work (e.g. could be the start time of the transaction,
	// if this work was created as part of a transaction). It is used to order
	// work within a (TenantID, Priority) pair -- earlier CreateTime is given
	// preference.
	CreateTime int64
	// BypassAdmission allows the work to bypass admission control (it will
	// never queue or block) while still being accounted for: the tokens are
	// deducted via tookWithoutPermission so the token budget reflects the
	// actual usage. This is used in three situations:
	//
	//  - High-priority intra-KV work whose source is OTHER (not FROM_SQL or
	//    ROOT_KV). Blocking such work could cause deadlocks because it is
	//    generated in the critical path of serving already-admitted requests
	//    (e.g., Raft proposals, intent resolution, lease requests).
	//
	//  - Normal-and-above priority KV work when
	//    KVBulkOnlyAdmissionControlEnabled is set. In this mode only bulk
	//    (low-priority) work is subject to admission control; everything at
	//    NormalPri or above bypasses.
	//
	//  - SQL CPU admission handles closing (noWait=true in reportCPU).
	//    SQL CPU uses an admit-after-consume model: goroutines run
	//    freely and retroactively deduct consumed CPU via Admit. During
	//    execution, an exhausted token bucket blocks the goroutine until
	//    tokens refill. At handle close (GoroutineCPUHandle.Close), a
	//    final measureAndAdmit accounts for the last CPU burst, but
	//    blocking serves no purpose — the work is done and there is
	//    nothing left to throttle. BypassAdmission deducts the tokens
	//    without blocking.
	BypassAdmission bool
	// RequestedCount is the requested number of tokens or slots. If unset:
	// - For slot-based queues we treat it as an implicit request of 1;
	// - For the store work queue, we use per-request estimates to deduct some
	//   number of tokens at-admit time. Note that this only applies to the
	//   legacy above-raft admission control. With admission control for
	//   replicated writes (done so asynchronously, below-raft; see
	//   ReplicatedWrite below), we do know the size of the write being
	//   admitted, so RequestedCount is set accordingly.
	RequestedCount int64
	// ReplicatedWorkInfo groups everything needed to admit replicated writes, done
	// so asynchronously below-raft as part of replication admission control.
	ReplicatedWorkInfo ReplicatedWorkInfo
	// WorkloadID is used for ASH sampling.
	WorkloadID uint64
	// AppNameID is the hash of the application name. Used for ASH sampling.
	AppNameID uint64
	// GatewayNodeID is the node that initiated the workload. Used for ASH
	// sampling.
	GatewayNodeID roachpb.NodeID
	// WorkloadType distinguishes the kind of workload that WorkloadID
	// represents. Used for ASH sampling.
	WorkloadType workloadid.WorkloadType
}

// ReplicatedWorkInfo groups everything needed to admit replicated writes, done
// so asynchronously below-raft as part of replication admission control.
type ReplicatedWorkInfo struct {
	// Enabled captures whether this work represents a replicated write,
	// subject to below-raft asynchronous admission control.
	Enabled bool
	// RangeID identifies the raft group on behalf of which work is being
	// admitted.
	RangeID roachpb.RangeID
	// Replica that asked for admission.
	ReplicaID roachpb.ReplicaID
	// LeaderTerm is the term of the leader that asked for this entry to be
	// appended.
	LeaderTerm uint64
	// LogPosition is the point on the raft log where the write was replicated.
	LogPosition LogPosition
	// RaftPri is the raft priority of the entry. Only populated for RACv2.
	RaftPri raftpb.Priority
	// Ingested captures whether the write work corresponds to an ingest
	// (for sstables, for example). This is used alongside RequestedCount to
	// maintain accurate linear models for L0 growth due to ingests and
	// regular write batches.
	Ingested bool
}

// LogPosition is a point on the raft log, identified by a term and an index.
type LogPosition struct {
	Term  uint64
	Index uint64
}

func (r LogPosition) String() string {
	return fmt.Sprintf("%d/%d", r.Term, r.Index)
}

func (r LogPosition) Less(o LogPosition) bool {
	if r.Term != o.Term {
		return r.Term < o.Term
	}
	return r.Index < o.Index
}

// WorkQueue maintains a queue of work waiting to be admitted. Ordering of
// work is achieved via 2 heaps: a group heap orders the groups with waiting
// work in increasing order of used slots or tokens, optionally adjusted by
// group weights. Within each group, the waiting work is ordered based on
// priority and create time. Groups with non-zero values of used slots or
// tokens are tracked even if they have no more waiting work. Token usage is
// reset to zero every second. The choice of 1 second of memory for token
// distribution fairness is somewhat arbitrary. The same 1 second interval is
// also used to garbage collect groups who have no waiting requests and no
// used slots or tokens.
//
// Usage example:
//
//	var grantCoord *GrantCoordinator
//	<initialize grantCoord>
//	kvQueue := grantCoord.GetWorkQueue(KVWork)
//	<hand kvQueue to the code that does kv server work>
//
//	// Before starting some kv server work
//	if enabled, err := kvQueue.Admit(ctx, WorkInfo{TenantID: tid, ...}); err != nil {
//	  return err
//	}
//	<do the work>
//	if enabled {
//	  kvQueue.AdmittedWorkDone(tid)
//	}
type WorkQueue struct {
	ambientCtx     context.Context
	workKind       WorkKind
	queueKind      QueueKind
	granter        granter
	mode           workQueueMode
	tiedToRange    bool
	usesAsyncAdmit bool
	settings       *cluster.Settings

	onAdmittedReplicatedWork onAdmittedReplicatedWork

	mu struct {
		syncutil.Mutex
		// Groups with waiting work. In serverless mode each group is a SQL tenant
		// keyed by tenant ID; in resource manager mode each group is a resource
		// group keyed by resource group ID. Ordered by burst qualification then
		// used/weight.
		groupHeap groupHeap
		// All groups, including those without waiting work. Keyed by resource group
		// ID. Periodically cleaned.
		groups       map[uint64]*groupInfo
		groupWeights struct {
			mu syncutil.Mutex
			// active refers to the currently active weights. mu is held for updates
			// to the inactive weights, to prevent concurrent updates. After
			// updating the inactive weights, it is made active by swapping with
			// active, while also holding WorkQueue.mu. Therefore, reading
			// groupWeights.active does not require groupWeights.mu. For lock
			// ordering, groupWeights.mu precedes WorkQueue.mu.
			//
			// The maps are lazily allocated.
			active, inactive map[uint64]uint32
		}
		// rmGroups holds the per-resource-group state for Resource
		// Manager mode: the input config (Weight, MaxCPU - copied from
		// SetResourceGroupConfig) plus the derived burstFrac. Always
		// reflects the most recent SetResourceGroupConfig call, or the
		// default seed installed at construction. burstFrac is set by
		// applyResourceGroupConfigLocked; read by refillRMGroupBurstBuckets
		// every 1ms to scale per-group refills.
		//
		// At our expected N (handful of resource groups), heavy
		// precompute (totalWeight, scaled weights, burstFracs) and the
		// per-group upsert loop run under q.mu directly without
		// measurable Admit impact. If N grows into the hundreds or
		// thousands, mirror SetTenantWeights's pattern: introduce a
		// dedicated sub-mutex, precompute under it, take q.mu briefly
		// to swap into the active map, then walk configured group IDs
		// in batches under q.mu (releasing between batches so Admit
		// can interleave).
		//
		// === Why WorkQueue owns this storage ===
		//
		// WorkQueue is already the home for per-RM-group state:
		// groupWeights.active holds per-group weights, groupInfo
		// holds per-group runtime (including
		// cpuTimeBurstBucket.maxCPU), useResourceGroup gates
		// priority-vs-tenant grouping, burstBucketCapacity holds the
		// unscaled-RM baseline used when new groupInfos are created.
		// ResourceGroupConfig is the source-of-truth for two of
		// those scattered fields (weight + maxCPU); putting the
		// source-of-truth next to the derived state it produces is
		// the simplest organization, and it matches what already
		// happens with serverless tenant weights (which also live
		// on WorkQueue, fed via SetTenantWeights).
		//
		// === Alternative owners considered and rejected ===
		//
		// (1) cpuTimeTokenAllocator: an earlier design (see git
		// history for baf469d24cf) put the config on the allocator
		// as an atomic.Pointer[map] plus an atomic.Bool dirty flag.
		// The coord then had to pierce in via raw pointers to those
		// atomics from SetResourceGroupConfig. The awkwardness
		// wasn't "atomics are bad" - it was that the data lived in
		// the wrong place (allocator's job is token allocation, not
		// config storage), so the API entry point had to reach
		// across components. Moving storage to its natural home
		// (WorkQueue, where derived state already lives) removes
		// the piercing entirely.
		//
		// (2) rmStrategy: tempting because rmStrategy is the RM-mode
		// component that consumes the config. Rejected because
		// rmStrategy is short-lived (destroyed on every mode swap).
		// SetResourceGroupConfig calls that arrive before RM mode
		// activates - or between strategy rebuilds - would have
		// nowhere to land. The storage needs to outlive any
		// specific strategy.
		//
		// (3) cpuTimeTokenGrantCoordinator: the API entry point and
		// long-lived. Tempting because adding storage there avoids
		// any q.mu interaction. Rejected because (a) coord is
		// otherwise a thin wrapper around objects (filler + queues),
		// not a data manager - adding state changes its character;
		// (b) the lock-isolation argument doesn't hold since
		// SetResourceGroupConfig does only one map + one bool write,
		// nanoseconds of q.mu hold time, with rare admin DDL
		// frequency; (c) coord-owns separates source-of-truth from
		// derived state, requiring rmStrategy and WorkQueue to read
		// from a third object. The eventual collapse to a single
		// WorkQueue (per the TODO on cpuTimeTokenFiller.activeMode)
		// makes WorkQueue-ownership even more natural - the "one
		// queue uses it, the other carries an unused field"
		// awkwardness disappears.
		//
		// (4) Dedicated resourceGroupRegistry type owned by coord:
		// hybrid of (3) with a separate type to encapsulate
		// "config storage" responsibility. Adds the type, requires
		// the registry pointer to be plumbed coord -> allocator ->
		// (passed-as-arg-to) strategy, and gives both rmStrategy
		// and WorkQueue a third object to consult. With (3)'s
		// lock-isolation premise broken, all this machinery is
		// solving a non-problem; the simpler design wins.
		//
		// === Immediate apply when in RM mode; setUseResourceGroup
		// applies on serverless-to-RM transition ===
		//
		// SetResourceGroupConfig in RM mode (useResourceGroup=true)
		// applies derived state synchronously under q.mu via
		// applyResourceGroupConfigLocked: groupWeights.active swap,
		// per-group weight + maxCPU on existing groupInfos (with
		// heap fix on qualification flips), pre-creation of newly-
		// configured IDs, and refresh of rmGroupConfigs.
		//
		// In serverless mode, SetResourceGroupConfig only stores
		// config. Derived state stays untouched (applying RM-style
		// scaled weights and per-group maxCPU during serverless mode
		// would either be wrong or pollute serverless tenant state).
		// The accumulated config is materialized on the next swap
		// into RM mode: setUseResourceGroup(true) detects the
		// false→true transition and unconditionally calls
		// applyResourceGroupConfigLocked. The same mechanism handles
		// the construction bootstrap on first activation (constructor
		// seeds defaultRMResourceGroupConfig).
		//
		// There is no dirty bit. The two apply triggers are
		// (1) Set arriving while useResourceGroup=true and (2) the
		// false→true transition itself. Together they cover every
		// case where derived state could lag the source: Sets in
		// serverless mode accumulate harmlessly in resourceGroupConfig
		// until the next mode swap, and Sets in RM mode apply
		// immediately. There is no third "apply if config changed"
		// path that needs a dirty signal.
		//
		// === Tradeoffs of immediate apply ===
		//
		// Earlier designs deferred all derived-state updates to the
		// next resetInterval (~1s after Set). That gave a clean
		// atomic transition - a single critical section read config
		// and produced all derived state, with no torn snapshots
		// across config generations. We rejected it because it
		// imposed three real costs:
		//
		//   1. Configured groups could be lazy-created during the
		//      deferral window with default weight + maxCPU=false,
		//      then corrected by the next apply. Up to 1s of
		//      "configured group running with defaults".
		//   2. Per-group refill continued at the old burstFrac
		//      scaling for up to 1s, so a freshly-Set weight=80%
		//      group still got refilled at its old (e.g. 10%) rate
		//      until the next resetInterval.
		//   3. Operators couldn't observe the new config in metrics
		//      or behavior until the next cycle - debugging gotcha.
		//
		// Immediate apply trades atomicity for shorter, narrower
		// inconsistency. Specifically:
		//
		//   - All non-bucket-capacity derived state transitions in
		//     one critical section under q.mu (atomic per-reader).
		//   - The one piece that lags is per-group
		//     cpuTimeBurstBucket.capacity. The next refill (within
		//     1ms) installs the per-group scaled capacity from the
		//     freshly-stored rmGroupConfigs. During that ≤1ms
		//     window, burstQualification computes against the old
		//     capacity denominator - a group's qualification can be
		//     briefly wrong by one tier (canBurst vs noBurst).
		//   - This brief mis-qualification only affects heap order
		//     and which granter token pool the group draws from. It
		//     cannot cause aggregate CPU over-consumption: the
		//     granter's noBurst/canBurst pools, sized by cluster-
		//     setting-derived rates, are the binding constraint on
		//     actual CPU. The bucket only governs qualification.
		//
		// Net: ≤1s of broad config staleness across 5+ pieces of
		// derived state -> ≤1ms of narrow staleness in one piece
		// (bucket.capacity) that doesn't gate CPU consumption.
		//
		// === Operational hazards ===
		//
		// Three behaviors to know when running this in production:
		//
		//   1. q.mu hold-time spike on admin DDL.
		//      SetResourceGroupConfig does microseconds-to-ms of work
		//      under q.mu (totalWeight pass, scaled-weight derivation,
		//      per-group upsert with possible heap fix, pre-creation).
		//      The DDL caller is fine; the cost is paid by whatever
		//      Admit / refill / AdmittedWorkDone calls are queued on
		//      q.mu at that moment. On busy KV nodes a large config
		//      rollout (e.g. N=100 groups) can show as a brief tail-
		//      latency event. Scales with len(rmGroups). At expected
		//      N (handful), invisible; if N grows into the hundreds
		//      or thousands, mirror SetTenantWeights's sub-mutex
		//      pattern (precompute scaled state under a sub-mutex,
		//      then take q.mu briefly to swap and walk in batches
		//      that release q.mu between batches).
		//
		//   2. Stitched-generation window in admission decisions.
		//      Derived state has two writers. apply touches
		//      rmGroups[].burstFrac, groupWeights.active,
		//      groupInfo.weight, and groupInfo.cpuTimeBurstBucket.maxCPU
		//      directly under q.mu. It does NOT touch
		//      groupInfo.cpuTimeBurstBucket.capacity or .tokens -
		//      those are written only by refill (every 1ms). So
		//      between an apply and the next refill, heap fairness
		//      reads NEW weight while burstQualification reads OLD
		//      bucket capacity. Concretely: a group whose weight just
		//      jumped 10%->80% sits at the front of its tier (NEW
		//      weight) but evaluates burst qualification against its
		//      still-small old bucket; a group whose burstFrac just
		//      dropped retains a temporarily-large bucket relative to
		//      its new allocation. Window bounded by refill cadence
		//      (≤1ms). Cannot cause aggregate CPU over-consumption:
		//      the granter's noBurst/canBurst pools, sized by cluster
		//      settings, are the binding constraint on real CPU. The
		//      bucket only governs heap position and which pool the
		//      group draws from.
		//
		//   3. Removed groups drain naturally rather than evicting.
		//      A Set that omits a previously-configured ID does not
		//      actively delete the corresponding groupInfo. apply
		//      walks the new rmGroups (the dropped ID is absent), so
		//      that group's weight / maxCPU / burstFrac aren't
		//      refreshed. refill stops iterating it (no entry in
		//      rmGroups). Existing tokens drain over subsequent
		//      admits routed to that ID, then
		//      gcGroupsResetUsedAndUpdateEstimators evicts the
		//      groupInfo when used==0, not in heap, and not in
		//      rmGroups. Net: a "DROP RESOURCE GROUP" doesn't
		//      immediately stop work for that group - it stops new
		//      burst budget and drains the existing budget over the
		//      next few seconds. If immediate eviction is ever needed,
		//      apply would have to gain a removed-group sweep that
		//      zeros the bucket and removes from the heap.
		//
		// Two API-shape constraints, separate from the operational
		// hazards above:
		//
		//   - Intermediate states between rapid Sets are observable.
		//     Set(C1); Set(C2) within microseconds: derived state
		//     briefly reflects C1 before being overwritten by C2.
		//     Fine for human admin DDL; matters for any programmatic
		//     batch-style config rollout that needs atomicity.
		//   - Mode-swap interaction: immediate-apply gates on
		//     useResourceGroup so serverless-mode Sets don't pollute
		//     serverless tenant state. The deferred-apply path
		//     doesn't fully disappear - it migrates to "false→true
		//     setUseResourceGroup transition fires apply once."
		//
		// === Single map, no pending/active separation ===
		//
		// We considered a pending+active design where pending is the
		// just-stored config and active is the last-applied config,
		// with lazy creation in Admit reading active to stay
		// consistent with existing groupInfos. Rejected because lazy
		// creation in Admit deliberately passes maxCPU=false and
		// never reads any config map (configured groups are
		// pre-created at apply time, so the lazy path is only a
		// fallback for truly-unknown IDs). With no external reader
		// of this map other than the apply path itself, the
		// pending+active separation only buys naming clarity, not
		// correctness; not worth the extra fields and code.
		//
		// === No hard "must be configured before Admit" invariant ===
		//
		// We considered enforcing that any group ID seen by Admit
		// must already be in resourceGroupConfig (i.e., reject or
		// panic on unknown IDs). Rejected because (a) it doesn't
		// fit serverless mode where TenantIDs are arbitrary and
		// unbounded; (b) it's brittle in the face of ordering
		// races (e.g., Admit arriving in the startup window before
		// setUseResourceGroup(true) materializes the config); (c)
		// the soft invariant we get from pre-creation - "after each
		// apply, every configured ID has a backing groupInfo with
		// agreeing weight/maxCPU" - is enough to make reasoning
		// easy without the brittleness. Lazy creation is kept as
		// a true fallback for unknown IDs (defaults: maxCPU=false,
		// weight=defaultGroupWeight).
		rmGroups map[uint64]rmGroup

		// useResourceGroup, when true, derives the resource group ID from
		// WorkInfo.Priority instead of WorkInfo.TenantID. Used in Resource
		// Manager mode to split work into foreground (priority >= NormalPri)
		// and background (priority < NormalPri) groups.
		//
		// This is a bool set by the filler goroutine rather than a direct
		// read of the cpuTimeTokenMode cluster setting because mode
		// transitions also affect the allocator strategy, queue
		// configuration (burst fractions, maxCPU), and work routing
		// (activeMode). The filler coordinates all of these together in
		// resetInterval. Reading the cluster setting directly here could
		// observe RM mode before the other components are configured for it.
		//
		// === Why one WorkQueue serves both modes (not one queue per mode) ===
		//
		// queues[0] is reused across serverless and Resource Manager
		// modes: useResourceGroup is toggled to switch group derivation.
		// We considered (and rejected) splitting modes into separate
		// dedicated WorkQueues. The root reason multi-queue designs
		// are hard is an asymmetry in how WorkQueue and the granter
		// are wired:
		//
		//   - Outbound (WorkQueue -> granter) is naturally N:1.
		//     Each WorkQueue holds its own cpuTimeTokenChildGranter
		//     wrapper that forwards tryGet/tookWithoutPermission/
		//     returnGrant to a shared granter. Adding queues here is
		//     cheap: mint another childGranter pointing at the same
		//     granter and the bucket math just works.
		//
		//   - Inbound (granter -> WorkQueue) is structurally 1:1 per
		//     tier. The granter holds exactly one requester slot per
		//     tier (cpuTimeTokenGranter.requester[tier]) and pushes
		//     work down by calling granted() on that one slot. There
		//     is no built-in way for a single tier slot to fan out
		//     to multiple queues - whichever queue is not in the
		//     slot has no callback path.
		//
		// "Just add another childGranter" addresses only the outbound
		// side, so the inbound mismatch remains. The two real
		// alternatives below each try to fix the inbound side:
		//
		// (1) Three queues - serverless tier-0, serverless tier-1, and
		// a dedicated rmQueue - with the granter's tier-0 requester
		// slot routed via a tier0Router that picks based on activeMode.
		// Each queue's mode is fixed at construction; mode swap just
		// flips the router's pick. This looks structurally cleaner
		// (queue identity = mode, no flag dispatch internally) but
		// has a real regression: pending work in the now-inactive
		// queue's waitingWorkHeap becomes structurally unreachable
		// from the granter. Because the granter only ever asks the
		// one requester slot, and the router resolves contention by
		// picking one queue per call, the unpicked queue is never
		// asked. Queued work that was admitted before the mode swap
		// waits for a grant that never comes and ultimately fails
		// with a deadline-exceeded error. Mode swaps are rare
		// operator actions, but pending-work stalls were a real
		// client-visible regression with no matching benefit
		// (WorkQueue's mode-conditional code - groupIDForWorkLocked,
		// GC's rmGroups skip, two refill methods - did not actually
		// simplify, since each queue still carried the dispatch
		// logic; only the flag's mutability changed).
		//
		// (2) Two requester slots per tier on the granter (so it
		// asks both serverless tier-0 queue and rmQueue, granting to
		// whichever has work). This solves the pending-work stall
		// but moves complexity into the granter: the granter must
		// pick between two requesters, decide what burst
		// qualification to use when both have work, and grants from
		// a token pool sized for the active mode regardless of which
		// queue receives the grant - blurring per-mode token
		// isolation during the drain window. New policy decisions
		// in the granter that don't exist in the single-queue
		// design.
		//
		// The single-queue design avoids the contention by not
		// creating multiple queues that compete for the granter's
		// per-tier requester slot. Mode is a routing concern within
		// the queue (group derivation, refill scaling), not a queue
		// identity. The granter's binding to the queue is permanent;
		// mode swap touches strategy, useResourceGroup, and refill
		// rates, but the granter still asks the same queue for
		// waiting work, so pending requests are reachable across
		// every mode swap. Less novel machinery, no pending-work
		// stall, and aligned with the eventual consolidation TODO
		// (serverless tenants modeled as resource groups) that
		// collapses everything to one queue anyway.
		//
		// The brief window inside resetInterval where a.strategy
		// reflects the new mode but useResourceGroup still reflects
		// the old one is safe under goroutine sequencing: only the
		// filler reads a.strategy, and external Admit callers read
		// useResourceGroup but never a.strategy, so no observer can
		// see an inconsistent (strategy, flag) pair. See the long
		// comment in resetInterval for the full safety argument.
		useResourceGroup bool

		// The highest epoch that is closed.
		closedEpochThreshold int64
		// Following values are copied from the cluster settings.
		epochLengthNanos            int64
		epochClosingDeltaNanos      int64
		maxQueueDelayToSwitchToLifo time.Duration
		// Only used if mode == usesCPUTimeTokens.
		defaultCPUTimeTokenEstimator cpuTimeTokenEstimator
		// burstBucketCapacity is the seed capacity used when creating a
		// new groupInfo via the Admit lazy path or
		// applyResourceGroupConfigIfChanged pre-creation. The bucket is
		// initialized with this value as both tokens and capacity (full
		// bucket, so new groups can burst immediately).
		//
		// Serverless mode: updated every 1ms by
		// serverlessStrategy.refillBurst with the uniform per-tenant
		// capacity. Every tenant gets the same capacity, so this value
		// is the right per-tenant seed.
		//
		// RM mode: left at zero. RM groups have per-group scaled
		// capacities (cap100 * burstFrac), so no globally-keyed
		// capacity is correct for every group at lazy-create time.
		// Seeding with the unscaled (100%) value would over-allocate
		// non-MAX_CPU groups for up to one refill cycle (1ms): the
		// bucket would grant burst budget that the group's eventual
		// scaled capacity won't allow, and that budget can be consumed
		// before the next refill caps it. Seeding with 0 instead
		// leaves new RM groups noBurst until the next refill installs
		// the correct per-group capacity. Under-allocation for <=1ms
		// is the safer failure mode for an admission-control system -
		// briefly throttling a brand-new group is harmless, briefly
		// granting unearned budget can cascade into real CPU pressure
		// if many groups start at once. burstQualification at
		// capacity=0 is already exercised at startup and explicitly
		// handled in cpuTimeBurstBucket.
		//
		// Only used if mode == usesCPUTimeTokens.
		burstBucketCapacity int64
		// overrideAllToBypassAdmission, when true, causes all work to bypass
		// admission control. Used by CPU time token AC.
		overrideAllToBypassAdmission bool
	}
	logThreshold log.EveryN
	metrics      *WorkQueueMetrics
	stopCh       chan struct{}

	// perGroupAggMetrics holds the parent AggCounters for per-group
	// metrics. Only set when mode == usesCPUTimeTokens.
	perGroupAggMetrics *groupAggMetrics

	timeSource timeutil.TimeSource
	knobs      *TestingKnobs
}

// groupAggMetrics groups the parent AggCounters from which per-group
// child counters are created via AddChild.
type groupAggMetrics struct {
	admittedCount  *aggmetric.AggCounter
	waitTimeNanos  *aggmetric.AggCounter
	tokensUsed     *aggmetric.AggCounter
	tokensReturned *aggmetric.AggCounter
}

var _ requester = &WorkQueue{}

type workQueueOptions struct {
	mode           workQueueMode
	tiedToRange    bool
	usesAsyncAdmit bool
	// perGroupAggMetrics holds the parent AggCounters for per-group
	// metrics. Only set when mode == usesCPUTimeTokens. See
	// cpuTimeTokenMetrics for details.
	perGroupAggMetrics *groupAggMetrics

	// timeSource can be set to non-nil for tests. If nil,
	// the timeutil.DefaultTimeSource will be used.
	timeSource timeutil.TimeSource
	// The epoch closing goroutine can be disabled for tests.
	disableEpochClosingGoroutine bool
	// The background resetting of used and GC'ing of groups can be disabled
	// for tests.
	disableGCGroupsAndResetUsed bool
	// knobs, if set, provides testing knobs to the work queue.
	knobs *TestingKnobs
}

func makeWorkQueueOptions(workKind WorkKind) workQueueOptions {
	switch workKind {
	case KVWork:
		// CPU bound KV work uses slots. We also use KVWork for the per-store
		// queues, which use tokens -- the caller overrides the mode value
		// in that case.
		return workQueueOptions{mode: usesSlots, tiedToRange: true}
	case SQLKVResponseWork, SQLSQLResponseWork:
		return workQueueOptions{mode: usesTokens, tiedToRange: false}
	default:
		panic(errors.AssertionFailedf("unexpected workKind %d", workKind))
	}
}

func makeWorkQueue(
	ambientCtx log.AmbientContext,
	workKind WorkKind,
	granter granter,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
) requester {
	q := &WorkQueue{}
	var queueKind QueueKind
	if workKind == KVWork {
		queueKind = "kv-regular-cpu-queue"
	}
	initWorkQueue(q, ambientCtx, workKind, queueKind, granter, settings, metrics, opts, opts.knobs)
	return q
}

func initWorkQueue(
	q *WorkQueue,
	ambientCtx log.AmbientContext,
	workKind WorkKind,
	queueKind QueueKind,
	granter granter,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
	knobs *TestingKnobs,
) {
	if knobs == nil {
		knobs = &TestingKnobs{}
	}
	stopCh := make(chan struct{})

	timeSource := opts.timeSource
	if timeSource == nil {
		timeSource = timeutil.DefaultTimeSource{}
	}

	if queueKind == "" {
		queueKind = QueueKind(workKind.String())
	}

	q.ambientCtx = ambientCtx.AnnotateCtx(context.Background())
	q.workKind = workKind
	q.queueKind = queueKind
	q.granter = granter
	q.mode = opts.mode
	q.tiedToRange = opts.tiedToRange
	q.usesAsyncAdmit = opts.usesAsyncAdmit
	q.settings = settings
	q.logThreshold = log.Every(5 * time.Minute)
	q.metrics = metrics
	q.stopCh = stopCh
	q.perGroupAggMetrics = opts.perGroupAggMetrics
	q.timeSource = timeSource
	q.knobs = knobs
	q.mu.defaultCPUTimeTokenEstimator = cpuTimeTokenEstimator{}

	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.mu.groups = make(map[uint64]*groupInfo)
		// Seed rmGroups with the RM-mode default. burstFrac is left
		// at zero; the first setUseResourceGroup(true) call (when the
		// queue transitions into RM mode) computes it via
		// applyResourceGroupConfigLocked. Harmless in serverless mode,
		// where the seed sits unused.
		q.mu.rmGroups = make(map[uint64]rmGroup, len(defaultRMResourceGroupConfig))
		for id, c := range defaultRMResourceGroupConfig {
			q.mu.rmGroups[id] = rmGroup{ResourceGroupConfig: c}
		}
		q.sampleEpochLIFOSettingsLocked()
	}()
	if !opts.disableGCGroupsAndResetUsed {
		go func() {
			ticker := time.NewTicker(time.Second)
			for {
				select {
				case <-ticker.C:
					q.gcGroupsResetUsedAndUpdateEstimators()
				case <-stopCh:
					// Channel closed.
					return
				}
			}
		}()
	}
	q.tryCloseEpoch(q.timeNow())
	if !opts.disableEpochClosingGoroutine {
		q.startClosingEpochs()
	}
}

// WorkQueue is mostly agnostic to what kind of resource is being managed.
// With that said, there are a few different types of resources. If the
// resource type is a slot, what the WorkQueue is really doing is enforcing
// a concurrency limit. On the other hand, if the resource type is a token,
// the WorkQueue is enforcing a rate limit -- there is some maximum number
// of tokens that can be used per second. These are different things. In
// particular, a slot must be returned, after it is acquired, so that
// additional work can execute. Tokens are not returned in this way at all.
// This difference is an important thing controlled by the workQueueMode
// option. See AdmittedWorkDone for more. Note that we plan to deprecate
// slots soon, in favor of tokens, even in case of foreground CPU AC.
//
// It is somewhat unfortunate that we need both usesTokens and
// usesCPUTimeTokens. If these were one option instead of two, then when
// slots goes away, we could delete this entire option, which would be
// good for simplicity. The basic difference is that with CPU time token
// AC, AdmittedWorkDone is supported -- and needed to incorporate
// grunning-based measurements of CPU time post execution of a request.
// With other token-based WorkQueues, AdmittedWorkDone is not supported.
// Note that elastic CPU currently uses usesTokens, but in the long term,
// it may be able to use usesCPUTimeTokens. Then, all uses of usesTokens
// will be for IO. So at that point, there will be just two modes:
// usesIOTokens (currently, usesTokens) and usesCPUTimeTokens (for both
// foreground and elastic CPU AC).
type workQueueMode uint8

const (
	usesSlots workQueueMode = iota
	usesTokens
	// TODO(josh): In next commit, implement.
	usesCPUTimeTokens
)

func isInGroupHeap(group *groupInfo) bool {
	// If there is some waiting work, this group is in groupHeap.
	return len(group.waitingWorkHeap) > 0 || len(group.openEpochsHeap) > 0
}

func (q *WorkQueue) timeNow() time.Time {
	return q.timeSource.Now()
}

func (q *WorkQueue) epochLIFOEnabled() bool {
	// We don't use epoch LIFO for below-raft admission control. See I12 from
	// kvflowcontrol/doc.go.
	return EpochLIFOEnabled.Get(&q.settings.SV) && !q.usesAsyncAdmit
}

// Samples the latest cluster settings for epoch-LIFO.
func (q *WorkQueue) sampleEpochLIFOSettingsLocked() {
	epochLengthNanos := int64(epochLIFOEpochDuration.Get(&q.settings.SV))
	if epochLengthNanos != q.mu.epochLengthNanos {
		// Reset what is closed. A proper closed value will be calculated when the
		// next epoch closes. This ensures that if we are increasing the epoch
		// length, we will regress what epoch number is closed. Meanwhile, all
		// work subject to LIFO queueing will get queued in the openEpochsHeap,
		// which is fine (we admit from there too).
		q.mu.closedEpochThreshold = 0
	}
	q.mu.epochLengthNanos = epochLengthNanos
	q.mu.epochClosingDeltaNanos = int64(epochLIFOEpochClosingDeltaDuration.Get(&q.settings.SV))
	q.mu.maxQueueDelayToSwitchToLifo = epochLIFOQueueDelayThresholdToSwitchToLIFO.Get(&q.settings.SV)
}

func (q *WorkQueue) startClosingEpochs() {
	go func() {
		// If someone sets the epoch length to a huge value by mistake, we will
		// still sample every second, so that we can adjust when they fix their
		// mistake.
		const maxTimerDur = time.Second
		// This is the min duration we set the timer for, to avoid setting smaller
		// and smaller timers, in case the timer fires slightly early.
		const minTimerDur = time.Millisecond
		var timer *time.Timer
		for {
			nextCloseTime := func() time.Time {
				q.mu.Lock()
				defer q.mu.Unlock()
				q.sampleEpochLIFOSettingsLocked()
				return q.nextEpochCloseTimeLocked()
			}()
			timeNow := q.timeNow()
			timerDur := nextCloseTime.Sub(timeNow)
			if timerDur > 0 {
				if timerDur > maxTimerDur {
					timerDur = maxTimerDur
				} else if timerDur < minTimerDur {
					timerDur = minTimerDur
				}
				if timer == nil {
					timer = time.NewTimer(timerDur)
				} else {
					timer.Reset(timerDur)
				}
				select {
				case <-timer.C:
				case <-q.stopCh:
					// Channel closed.
					return
				}
			} else {
				q.tryCloseEpoch(timeNow)
			}
		}
	}()
}

func (q *WorkQueue) nextEpochCloseTimeLocked() time.Time {
	// +2 since we need to advance the threshold by 1, and another 1 since the
	// epoch closes at its end time.
	timeUnixNanos :=
		(q.mu.closedEpochThreshold+2)*q.mu.epochLengthNanos + q.mu.epochClosingDeltaNanos
	return timeutil.Unix(0, timeUnixNanos)
}

func (q *WorkQueue) tryCloseEpoch(timeNow time.Time) {
	epochLIFOEnabled := q.epochLIFOEnabled()
	q.mu.Lock()
	defer q.mu.Unlock()
	epochClosingTimeNanos := timeNow.UnixNano() - q.mu.epochLengthNanos - q.mu.epochClosingDeltaNanos
	epoch := epochForTimeNanos(epochClosingTimeNanos, q.mu.epochLengthNanos)
	if epoch <= q.mu.closedEpochThreshold {
		return
	}
	q.mu.closedEpochThreshold = epoch
	initializedDoLog := false
	doLog := false
	// doLogFunc is called inside the for loop, whenever a caller has something
	// interesting to log. It delays sampling logThreshold until it is actually
	// needed. Once logThreshold is sampled, it is not sampled again.
	doLogFunc := func() bool {
		if initializedDoLog {
			return doLog
		}
		initializedDoLog = true
		// Log only if epochLIFOEnabled.
		doLog = epochLIFOEnabled && q.logThreshold.ShouldLog()
		return doLog
	}
	for _, group := range q.mu.groups {
		prevThreshold := group.fifoPriorityThreshold
		group.fifoPriorityThreshold =
			group.priorityStates.getFIFOPriorityThresholdAndReset(
				group.fifoPriorityThreshold, q.mu.epochLengthNanos, q.mu.maxQueueDelayToSwitchToLifo)
		if !epochLIFOEnabled {
			group.fifoPriorityThreshold = int(admissionpb.LowPri)
		}
		if group.fifoPriorityThreshold != prevThreshold && doLogFunc() {
			logVerb := redact.SafeString("is")
			if group.fifoPriorityThreshold != prevThreshold {
				logVerb = "changed to"
			}
			// TODO(sumeer): export this as a per-group metric somehow. We could
			// start with this being a per-WorkQueue metric for only the system
			// group. However, currently we share metrics across WorkQueues --
			// specifically all the store WorkQueues share the same metric. We
			// should eliminate that sharing and make those per store metrics.
			log.Dev.Infof(q.ambientCtx, "%s: FIFO threshold for group %d %s %d",
				q.workKind, group.id, logVerb, group.fifoPriorityThreshold)
		}
		// Note that we are ignoring the new priority threshold and only
		// dequeueing the ones that are in the closed epoch. It is possible to
		// have work items that are not in the closed epoch and whose priority
		// makes them no longer subject to LIFO, but they will need to wait here
		// until their epochs close. This is considered acceptable since the
		// priority threshold should not fluctuate rapidly.
		for len(group.openEpochsHeap) > 0 {
			work := group.openEpochsHeap[0]
			if work.epoch > epoch {
				break
			}
			heap.Pop(&group.openEpochsHeap)
			heap.Push(&group.waitingWorkHeap, work)
		}
	}
}

// AdmitResponse is the return value of Admit. We use a struct to enable
// passing certain internal information such as requestedCount from Admit
// to AdmittedWorkDone.
type AdmitResponse struct {
	// If true, admission control is enabled.
	Enabled bool
	// groupID is the groupID under which this work was admitted. Used by
	// AdmittedWorkDone to look up the correct groupInfo entry. groupID represents
	// tenant in Serverless and resource group in resource group manager.
	// TODO(wenyihu6): need to figure out the proto changes here
	// TODO(wenyihu6): captures the key at admission time, so AdmittedWorkDone
	// finds the right entry even if the mode switches between Admit and
	// AdmittedWorkDone. Need to make sure lookup misses fail gracefully if the
	// entry has been GCed after a mode switch.
	groupID roachpb.TenantID
	// requestedCount is the number of slots or tokens taken at Admit time.
	// It is useful to return, so that in AdmittedWorkDone, we can adjust
	// the deduction, in cases where we have more information, such as in
	// CPU time token AC, where a grunning-based measurement of CPU time
	// is available by the time AdmittedWorkDone is called.
	requestedCount int64
}

// Resource group IDs used in Resource Manager mode when
// useResourceGroup is true. Work is split into two groups based on
// WorkInfo.Priority.
const (
	// highResourceGroupID is used for work with priority >= NormalPri.
	highResourceGroupID uint64 = 1
	// lowResourceGroupID is used for work with priority < NormalPri.
	lowResourceGroupID uint64 = 2
)

// ResourceGroupConfig holds per-resource-group configuration in
// Resource Manager mode.
type ResourceGroupConfig struct {
	// Weight controls the group's share of fair-shared resources.
	// Pushed onto WorkQueue.groupWeights.active by
	// applyResourceGroupConfigIfChanged and onto each existing
	// groupInfo.weight in the same call.
	Weight uint32
	// MaxCPU=true means the group always qualifies for burst
	// regardless of its burst bucket level. Used for groups that
	// should be allowed to consume up to full node CPU on demand.
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the configuration used until an
// explicit SetResourceGroupConfig call replaces it. The two
// hardcoded groups (high/low) match the two outputs of
// priorityToResourceGroup.
var defaultRMResourceGroupConfig = map[uint64]ResourceGroupConfig{
	highResourceGroupID: {Weight: 1, MaxCPU: true},
	lowResourceGroupID:  {Weight: 1, MaxCPU: false},
}

// rmGroup is the per-resource-group state for RM mode. It embeds
// the input ResourceGroupConfig (Weight, MaxCPU - copied from
// SetResourceGroupConfig) and adds the derived burstFrac.
//
// burstFrac is the group's share of burst bucket refill: 1.0 if
// MaxCPU is true (the group can burst to full node CPU), otherwise
// Weight / sum-of-weights across all configured groups.
// applyResourceGroupConfigLocked sets it; rmStrategy.refillBurst
// reads it via snapshotRMGroups.
type rmGroup struct {
	ResourceGroupConfig
	burstFrac float64
}

// priorityToResourceGroup maps a WorkPriority to one of the two
// hardcoded resource groups. Used in Resource Manager mode.
func priorityToResourceGroup(pri admissionpb.WorkPriority) uint64 {
	if pri >= admissionpb.NormalPri {
		return highResourceGroupID
	}
	return lowResourceGroupID
}

// setUseResourceGroup enables or disables priority-based resource
// group derivation. When enabled, the resource group ID is derived
// from WorkInfo.Priority instead of WorkInfo.TenantID.
//
// On a false→true transition (entering RM mode), this also applies
// the current rmGroups state via applyResourceGroupConfigLocked: it
// drains the constructor seed on first activation and any
// SetResourceGroupConfig calls that arrived while in serverless mode
// (where Set just stores config without applying derived state).
// This is the single mechanism for materializing accumulated config
// on mode swap; there is no separate dirty-bit-driven apply path.
//
// Callers (cpuTimeTokenAllocator.resetInterval) must invoke this
// before strategy.refillBurst in the same cycle, otherwise the
// first RM-mode refill iterates an empty rmGroups map.
func (q *WorkQueue) setUseResourceGroup(enabled bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	wasEnabled := q.mu.useResourceGroup
	q.mu.useResourceGroup = enabled
	if enabled && !wasEnabled {
		q.applyResourceGroupConfigLocked()
	}
}

// groupIDForWorkLocked returns the resource group ID for the given
// WorkInfo. In RM mode (useResourceGroup), the group is derived from
// WorkInfo.Priority; otherwise it is the TenantID.
//
// REQUIRES: q.mu is held.
func (q *WorkQueue) groupIDForWorkLocked(info WorkInfo) uint64 {
	q.mu.AssertHeld()
	if q.mu.useResourceGroup {
		return priorityToResourceGroup(info.Priority)
	}
	return info.TenantID.ToUint64()
}

// Admit is called when requesting admission for some work. If
// error!=nil, the request was not admitted, potentially
// due to the deadline being exceeded. The AdmitResponse return value is
// relevant when error=nil, and includes info on whether admission control
// is enabled. AdmittedWorkDone must be called iff
// AdmitResponse.Enabled=true && error==nil, and the WorkKind for this
// queue is KVWork, UNLESS the caller explicitly set RequestedCount (i.e.,
// the exact resource usage is already known). In that case, the estimator
// is skipped at Admit time and there is no estimate to correct, so
// AdmittedWorkDone should not be called. See the callerSetRequestedCount
// logic below for details.
func (q *WorkQueue) Admit(ctx context.Context, info WorkInfo) (AdmitResponse, error) {
	if fn := q.knobs.WorkQueueAdmitInterceptor; fn != nil {
		fn(info)
	}
	if !info.ReplicatedWorkInfo.Enabled {
		enabledSetting := admissionControlEnabledSettings[q.workKind]
		if enabledSetting != nil && !enabledSetting.Get(&q.settings.SV) {
			q.metrics.recordBypassedAdmission(info.Priority)
			return AdmitResponse{Enabled: false}, nil
		}
	}

	// TODO(irfansharif): When enabling replication admission control for
	// regular writes with arbitrary concurrency (part of #95563), measure
	// the memory overhead of enqueueing each raft command to see whether we
	// need to do some coalescing at this level.

	// Track whether the caller explicitly set RequestedCount. When true, the
	// caller knows the exact resource usage (e.g., SQL CPU admission uses
	// grunning to measure actual CPU consumed) and the CPU time token
	// estimator should not override it. Currently, only SQL CPU admission
	// sets RequestedCount in usesCPUTimeTokens mode; above-raft KV leaves
	// it at 0 and relies on the estimator. Below-raft KV does set
	// RequestedCount (to the raft command byte size), but uses usesTokens
	// mode, so it never reaches the estimator guard.
	callerSetRequestedCount := info.RequestedCount > 0
	if info.RequestedCount == 0 {
		// We treat unset RequestCounts as an implicit request of 1.
		info.RequestedCount = 1
	}
	if q.mode == usesSlots && info.RequestedCount != 1 {
		panic(errors.AssertionFailedf("unexpected RequestedCount for slot-based queue: %d", info.RequestedCount))
	}
	q.metrics.incRequested(info.Priority)

	// The code in this method does not use defer to unlock the mutex because it
	// needs the flexibility of selectively unlocking on a certain code path.
	// When changing the code, be careful in making sure the mutex is properly
	// unlocked on all code paths.
	q.mu.Lock()

	groupID := q.groupIDForWorkLocked(info)

	group, ok := q.mu.groups[groupID]
	if !ok {
		// See comment below about CPU time token estimation. If no groupInfo
		// struct exists for a group, then there is no cpuTimeTokenEstimator
		// dedicated to that group yet. When we create the groupInfo struct
		// here, we also create the estimator. We init the estimator using a
		// global estimator that sees workload across all groups.
		//
		// maxCPU=false is the right default for the lazy-creation path
		// in both modes:
		//
		//   - Serverless mode: groupID is a TenantID. rmGroups is
		//     RM-specific state and tenants aren't expected to be in
		//     it; maxCPU has no meaning here and false is correct.
		//
		//   - RM mode: applyResourceGroupConfigLocked pre-creates a
		//     groupInfo for every ID in rmGroups before releasing
		//     q.mu (apply runs synchronously inside both
		//     SetResourceGroupConfig and setUseResourceGroup
		//     false→true). So once an ID has been Set, lazy-create
		//     can never fire for it again - this branch is reached
		//     only by:
		//       a) IDs the operator's config never mentioned (e.g., a
		//          priority-derived ID omitted from a custom config).
		//          For those, defaults are correct.
		//       b) IDs that get Set AFTER Admit first sees them
		//          (operator races behind workload). The
		//          lazy-created groupInfo gets corrected when the
		//          eventual Set runs apply, via the upsert branch in
		//          applyResourceGroupConfigLocked.
		//
		// Compared to the old deferred-apply design, immediate apply
		// eliminates the "Set-then-Admit-within-deferral-window"
		// case (b' formerly: pre-creation now happens synchronously
		// at Set time, not up to 1s later at the next resetInterval).
		// The "Admit-then-Set" race in (b) above still exists but
		// the correction window is bounded by the operator's next
		// Set, not by the system's resetInterval delay.
		//
		// We considered reading from q.mu.rmGroups here to "look up
		// the real maxCPU." Redundant: if id is in rmGroups,
		// pre-creation already created the groupInfo and this branch
		// wouldn't run; if id isn't in rmGroups, the lookup returns
		// zero/false anyway. Lazy-create deliberately doesn't reach
		// into rmGroups - that keeps apply as the single moment of
		// truth for materializing config into groupInfos.
		group = newGroupInfo(groupID, q.getGroupWeightLocked(groupID),
			q.mode, q.mu.defaultCPUTimeTokenEstimator.estimateTokensToBeUsed(), q.mu.burstBucketCapacity,
			false /* maxCPU */, q.perGroupAggMetrics)
		q.mu.groups[groupID] = group
	}
	// If mode == usesCPUTimeTokens, WorkQueue does CPU time token estimation.
	// When Admit is called, the request hasn't yet executed, so we do not
	// know how much CPU time will be used by goroutines used to service it.
	// When AdmittedWorkDone is called, the request is done executing, so we have
	// a measurement of CPU time used servicing the request, courtesy of grunning.
	// group.estimator uses past measurements from grunning to make estimates
	// in this code path, that is, at admission time.
	//
	// Skip the estimator when callerSetRequestedCount is true (see above).
	if q.mode == usesCPUTimeTokens && !q.knobs.DisableCPUTimeTokenEstimation &&
		!callerSetRequestedCount {
		info.RequestedCount = group.cpuTimeTokenEstimator.estimateTokensToBeUsed()
	}
	admitResponse := AdmitResponse{
		groupID:        roachpb.TenantID{InternalValue: groupID},
		requestedCount: info.RequestedCount,
	}

	if info.ReplicatedWorkInfo.Enabled {
		if info.BypassAdmission {
			// TODO(irfansharif): "Admin" work (like splits, scatters, lease
			// transfers, etc.), and work originating from AdmissionHeader_OTHER,
			// don't use flow control tokens above-raft. So there's nothing to
			// virtually enqueue below-raft, since we have nothing to return. That
			// said, it might still be useful to physically admit these proposals
			// for correct token modeling. To do that, we'd have to pass down
			// information about it being bypassed above-raft.
			panic("unexpected BypassAdmission bit set for below raft admission")
		}
		if q.mode != usesTokens {
			panic(errors.AssertionFailedf("unexpected ReplicatedWrite.Enabled in mode %v", q.mode))
		}
	}
	if q.mu.overrideAllToBypassAdmission {
		info.BypassAdmission = true
	}
	if info.BypassAdmission {
		q.adjustGroupUsedLocked(group, info.RequestedCount)
		if group.perGroupMetrics.admittedCount != nil {
			group.perGroupMetrics.admittedCount.Inc(1)
		}
		q.mu.Unlock()
		q.granter.tookWithoutPermission(info.RequestedCount)
		q.metrics.incAdmitted(info.Priority)
		q.metrics.recordBypassedAdmission(info.Priority)
		admitResponse.Enabled = true
		return admitResponse, nil
	}
	// Work is subject to admission control.

	// Tell priorityStates about this received work. We don't tell it about work
	// that has bypassed admission control, since priorityStates is deciding the
	// threshold for LIFO queueing based on observed admission latency.
	group.priorityStates.requestAtPriority(info.Priority)

	burstQual := group.cpuTimeBurstBucket.burstQualification()
	if (len(q.mu.groupHeap) == 0 ||
		// group not in heap, so doesn't have waiting requests, and is canBurst, while
		// the top of the heap was noBurst.
		(group.heapIndex < 0 &&
			burstQual == canBurst &&
			q.mu.groupHeap[0].cpuTimeBurstBucket.burstQualification() == noBurst)) &&
		!q.knobs.DisableWorkQueueFastPath {
		// Fast-path. Try to grab token/slot.
		// Optimistically update used to avoid locking again.
		q.adjustGroupUsedLocked(group, info.RequestedCount)
		// Save the admittedCount counter before releasing the mutex,
		// since group may be GC'd and its counters Unlink'd after
		// unlock. Inc after Unlink is safe: the aggregate parent counter
		// still gets the increment (see aggmetric.Counter.Unlink docs).
		admittedCount := group.perGroupMetrics.admittedCount
		q.mu.Unlock()
		// We have unlocked q.mu, so another concurrent request can also do tryGet
		// and get ahead of this request. We don't need to be fair for such
		// concurrent requests.
		if q.granter.tryGet(burstQual, info.RequestedCount) {
			if admittedCount != nil {
				admittedCount.Inc(1)
			}
			q.metrics.incAdmitted(info.Priority)
			if info.ReplicatedWorkInfo.Enabled {
				// TODO(irfansharif): There's a race here, and could lead to
				// over-admission. It's possible that there are enqueued work
				// items with lower log positions than the request that just got
				// through using the fast-path, and since we're returning flow
				// tokens by specifying a log prefix, we'd be returning more
				// flow tokens than actually admitted. Fix it as part of #95563,
				// by either adding more synchronization, getting rid of this
				// fast path, or swapping this entry from the top-most one in
				// the waiting heap (and fixing the heap).
				if log.V(1) {
					log.Dev.Infof(ctx, "fast-path: admitting t%d pri=%s r%s log-position=%s ingested=%t",
						groupID, info.Priority,
						info.ReplicatedWorkInfo.RangeID,
						info.ReplicatedWorkInfo.LogPosition.String(),
						info.ReplicatedWorkInfo.Ingested,
					)
				}
				q.onAdmittedReplicatedWork.admittedReplicatedWork(
					roachpb.MustMakeTenantID(groupID),
					info.Priority,
					info.ReplicatedWorkInfo,
					info.RequestedCount,
					info.CreateTime,
					false, /* coordMuLocked */
				)
			}
			q.metrics.recordFastPathAdmission(info.Priority)
			admitResponse.Enabled = true
			return admitResponse, nil
		}
		// Did not get token/slot.
		//
		// There is a race here: before q.mu is acquired, the granter could
		// experience a reduction in load and call
		// WorkQueue.hasWaitingRequests to see if it should grant, but since
		// there is nothing in the queue that method will return false. Then the
		// work here queues up even though granter has spare capacity. We could
		// add additional synchronization (and complexity to the granter
		// interface) to deal with this, by keeping the granter's lock
		// (GrantCoordinator.mu) locked when returning from tryGrant and call
		// granter again to release that lock after this work has been queued.
		// But it has the downside of extending the scope of
		// GrantCoordinator.mu. Instead we tolerate this race in the knowledge
		// that GrantCoordinator will periodically, at a high frequency, look at
		// the state of the requesters to see if there is any queued work that
		// can be granted admission.
		q.mu.Lock()

		// Re-derive groupID: the mode may have changed while the lock
		// was released (though mode transitions are not yet supported).
		groupID = q.groupIDForWorkLocked(info)
		admitResponse.groupID = roachpb.TenantID{InternalValue: groupID}

		// The group could have been removed. See the comment where the
		// groupInfo struct is declared. maxCPU=false here for the same
		// reason as the lazy-creation branch above; see that comment
		// for the full discussion of why we don't consult the registry
		// on the lazy path.
		group, ok = q.mu.groups[groupID]
		if !ok {
			group = newGroupInfo(groupID, q.getGroupWeightLocked(groupID),
				q.mode, q.mu.defaultCPUTimeTokenEstimator.estimateTokensToBeUsed(), q.mu.burstBucketCapacity,
				false /* maxCPU */, q.perGroupAggMetrics)
			q.mu.groups[groupID] = group
		}
		q.adjustGroupUsedLocked(group, -info.RequestedCount)
	}

	// Check for cancellation.
	startTime := q.timeNow()
	if ctx.Err() != nil {
		if info.ReplicatedWorkInfo.Enabled {
			panic("not equipped to deal with cancelable contexts below raft")
		}
		// Already canceled. More likely to happen if cpu starvation is
		// causing entering into the work queue to be delayed.
		q.mu.Unlock()
		q.metrics.incErrored(info.Priority)
		var deadlineSubstring string
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			deadlineSubstring = fmt.Sprintf("deadline: %v, ", deadline)
		}
		return AdmitResponse{}, errors.Wrapf(ctx.Err(), "work %s context canceled before queueing: %snow: %v",
			q.workKind, deadlineSubstring, startTime)
	}
	// Push onto heap(s).
	ordering := fifoWorkOrdering
	if int(info.Priority) < group.fifoPriorityThreshold {
		ordering = lifoWorkOrdering
	}
	work := newWaitingWork(info.Priority, ordering, info.CreateTime, info.RequestedCount, startTime, q.mu.epochLengthNanos)
	work.replicated = info.ReplicatedWorkInfo

	inGroupHeap := isInGroupHeap(group)
	if work.epoch <= q.mu.closedEpochThreshold || ordering == fifoWorkOrdering {
		heap.Push(&group.waitingWorkHeap, work)
	} else {
		heap.Push(&group.openEpochsHeap, work)
	}
	if !inGroupHeap {
		heap.Push(&q.mu.groupHeap, group)
	}
	// Else already in groupHeap.

	// Release the lock.
	q.mu.Unlock()

	q.metrics.recordStartWait(info.Priority)
	if info.ReplicatedWorkInfo.Enabled {
		if log.V(1) {
			q.mu.Lock()
			queueLen := group.waitingWorkHeap.Len()
			q.mu.Unlock()

			log.Dev.Infof(ctx, "async-path: len(waiting-work)=%d: enqueued t%d pri=%s r%s log-position=%s ingested=%t",
				queueLen, groupID, info.Priority,
				info.ReplicatedWorkInfo.RangeID,
				info.ReplicatedWorkInfo.LogPosition,
				info.ReplicatedWorkInfo.Ingested,
			)
		}
		admitResponse.Enabled = false
		return admitResponse, nil
	}

	// Start waiting for admission.
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, "admissionWorkQueueWait")
	defer span.Finish()
	defer releaseWaitingWork(work)
	cleanup := ash.SetWorkState(
		info.TenantID, ash.WorkloadInfo{
			WorkloadID:    info.WorkloadID,
			AppNameID:     info.AppNameID,
			GatewayNodeID: info.GatewayNodeID,
			WorkloadType:  info.WorkloadType,
		},
		ash.WorkAdmission, string(q.queueKind))
	defer cleanup()
	select {
	case <-ctx.Done():
		waitDur := q.timeNow().Sub(startTime)
		q.mu.Lock()
		// The work was cancelled, so waitDur is less than the wait time this work
		// would have encountered if it actually waited until admission. However,
		// this lower bound is still useful for calculating the FIFO=>LIFO switch
		// since it is possible that all work at this priority is exceeding the
		// deadline and being cancelled. The risk here is that if the deadlines
		// are too short, we could underestimate the actual wait time.
		group.priorityStates.updateDelayLocked(work.priority, waitDur, true /* canceled */)
		if work.heapIndex == -1 {
			// No longer in heap. Raced with token/slot grant. Don't bother
			// decrementing group.used since we don't want to race with the gc
			// goroutine that sets used=0 and could have GC'd group and returned it
			// to the sync.Pool. We can fix this if needed by calling
			// adjustGroupUsedLocked.
			q.mu.Unlock()
			q.granter.returnGrant(info.RequestedCount)
			// The channel is sent to after releasing mu, so we don't need to hold
			// mu when receiving from it. Additionally, we've already called
			// returnGrant so we're not holding back future grant chains if this one
			// chain gets terminated.
			chainID := <-work.ch
			q.granter.continueGrantChain(chainID)
		} else {
			if work.inWaitingWorkHeap {
				group.waitingWorkHeap.remove(work)
			} else {
				group.openEpochsHeap.remove(work)
			}
			if !isInGroupHeap(group) {
				q.mu.groupHeap.remove(group)
			}
			q.mu.Unlock()
		}
		q.metrics.incErrored(info.Priority)
		q.metrics.recordFinishWait(info.Priority, waitDur)
		recordAdmissionWorkQueueStats(span, waitDur, q.queueKind, info.Priority, true)
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			log.Eventf(ctx, "deadline expired, waited in %s queue with pri %s for %v", q.queueKind, admissionpb.WorkPriorityDict[info.Priority], waitDur)
			return AdmitResponse{}, errors.Wrapf(ctx.Err(),
				"deadline expired while waiting in queue: %s, pri: %s, deadline: %v, start: %v, dur: %v",
				q.queueKind, admissionpb.WorkPriorityDict[info.Priority], deadline, startTime, waitDur)
		}
		// This is a pure context cancellation.
		log.Eventf(ctx, "context canceled, waited in %s queue with pri %s for %v", q.queueKind, admissionpb.WorkPriorityDict[info.Priority], waitDur)
		return AdmitResponse{}, errors.Wrapf(ctx.Err(),
			"context canceled while waiting in queue: %s, pri: %s, start: %v, dur: %v",
			q.queueKind, admissionpb.WorkPriorityDict[info.Priority], startTime, waitDur)
	case chainID, ok := <-work.ch:
		if !ok {
			panic(errors.AssertionFailedf("channel should not be closed"))
		}
		q.metrics.incAdmitted(info.Priority)
		waitDur := q.timeNow().Sub(startTime)
		q.metrics.recordFinishWait(info.Priority, waitDur)
		if work.heapIndex != -1 {
			panic(errors.AssertionFailedf("grantee should be removed from heap"))
		}
		recordAdmissionWorkQueueStats(span, waitDur, q.queueKind, info.Priority, false)
		q.granter.continueGrantChain(chainID)
		admitResponse.Enabled = true
		return admitResponse, nil
	}
}

func recordAdmissionWorkQueueStats(
	span *tracing.Span,
	waitDur time.Duration,
	queueKind QueueKind,
	workPriority admissionpb.WorkPriority,
	deadlineExceeded bool,
) {
	if span == nil {
		return
	}
	var deadlineExceededCount int32
	if deadlineExceeded {
		deadlineExceededCount = 1
	}
	span.RecordStructured(&admissionpb.AdmissionWorkQueueStats{
		WaitDurationNanos:     waitDur,
		QueueKind:             string(queueKind),
		DeadlineExceededCount: deadlineExceededCount,
		WorkPriority:          int32(workPriority),
	})
}

// AdmittedWorkDone is used to inform the WorkQueue that some admitted work is
// finished. It must be called iff the WorkKind of this WorkQueue is for KVWork
// and the caller did not explicitly set RequestedCount at Admit time. When
// RequestedCount is explicitly set (e.g., SQL CPU admission using grunning
// measurements), the exact amount was already deducted at Admit time, so there
// is no estimate to correct and AdmittedWorkDone must not be called.
//
// Note that cpuTime is an argument to AdmittedWorkDone. So, even though
// WorkQueue supports various resources other than CPU, AdmittedWorkDone is
// special-cased for CPU time admission control.
func (q *WorkQueue) AdmittedWorkDone(resp AdmitResponse, cpuTime time.Duration) {
	if q.workKind != KVWork {
		panic(errors.AssertionFailedf("AdmittedWorkDone only supports KVWork but got %v", q.workKind))
	}
	if q.mode != usesSlots && q.mode != usesCPUTimeTokens {
		panic(
			errors.AssertionFailedf("AdmittedWorkDone only supports usesSlots & usesCPUTimeTokens but got %v", q.mode))
	}

	additionalUsed := cpuTime.Nanoseconds() - resp.requestedCount
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		// adjustGroupUsed adjusts `group.used`. This tracks usage of resources
		// over a time interval. `group.used` is how the WorkQueue implements
		// fair-sharing of resources.
		//
		// At admission time (as part of the call to Admit),
		// q.adjustGroupUsed(groupID, resp.requestedCount) is called. Now that the
		// request is done executing, we have a measurement of CPU usage incurred by
		// the request from grunning. So we adjust group.used again, correcting our
		// earlier estimate. (In the case of slot-based AC, resp.requestedCount
		// equals 1 nanosecond, so the change to group.used made here is the only
		// significant one. With CPU time token AC, a more plausible estimate of CPU
		// time incurred by a request is available at admission time (see
		// cpu_time_token_estimation.go for more).
		//
		// NB: additionalUsed can be negative here (in case the initial estimate was
		// too pessimistic).
		if additionalUsed != 0 {
			group, ok := q.mu.groups[resp.groupID.ToUint64()]
			if ok {
				q.adjustGroupUsedLocked(group, additionalUsed)
			}
		}

		// If mode == usesCPUTimeTokens, WorkQueue does CPU time token estimation.
		// When Admit is called, the request hasn't yet executed, so we do not
		// know how much CPU time will be used by goroutines used to service it.
		// When AdmittedWorkDone is called, the request is done executing, so we have
		// a measurement of CPU time used servicing the request, courtesy of grunning.
		// group.estimator uses past measurements from grunning to make estimates
		// in this code path, that is, at admission time.
		if q.mode == usesCPUTimeTokens {
			q.mu.defaultCPUTimeTokenEstimator.workDone(cpuTime.Nanoseconds())
			group, ok := q.mu.groups[resp.groupID.ToUint64()]
			// If the group struct doesn't exist, it has been GCed due to a lack of
			// activity. In this case, we do not leverage the grunning measurement
			// for future estimates.
			if ok {
				group.cpuTimeTokenEstimator.workDone(cpuTime.Nanoseconds())
			}
		}
	}()

	// Slot-based AC sets a concurrency limit on requests. A single slot is
	// taken at admission time. That slot must be returned here, for the
	// concurrency limit to be a concurrency limit.
	//
	// CPU time token AC represents CPU usage via nanosecond tokens. Tokens
	// are refilled at some rate (see cpu_time_token_filler.go for more).
	// Since this is a rate limit instead of a concurrency limit, used tokens
	// should not be returned. But the initial deduction of tokens is based
	// on an estimate of CPU usage (see cpu_time_token_estimation.go for more).
	// In AdmittedWorkDone, we have a measurement of usage. So the below calls
	// to tookWithoutPermission / returnGrant correct the estimate.
	//
	// Even though a call to returnGrant is made in both of these cases, they
	// are quite different. Note that in the long term we will deprecate
	// slot-based AC.
	if q.mode == usesCPUTimeTokens {
		if additionalUsed > 0 {
			q.granter.tookWithoutPermission(additionalUsed)
		} else if additionalUsed < 0 {
			q.granter.returnGrant(-additionalUsed)
		}
	} else { // q.mode == usesSlots
		q.granter.returnGrant(1)
	}
}

func (q *WorkQueue) hasWaitingRequests() (bool, burstQualification) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.mu.groupHeap) == 0 {
		return false, noBurst /*arbitrary*/
	}
	return true, q.mu.groupHeap[0].cpuTimeBurstBucket.burstQualification()
}

func (q *WorkQueue) granted(grantChainID grantChainID) int64 {
	// Reduce critical section by getting time before mutex acquisition.
	now := q.timeNow()
	q.mu.Lock()
	if len(q.mu.groupHeap) == 0 {
		q.mu.Unlock()
		return 0
	}
	if fn := q.knobs.DisableWorkQueueGranting; fn != nil && fn() {
		q.mu.Unlock()
		return 0
	}
	group := q.mu.groupHeap[0]
	var item *waitingWork
	if len(group.waitingWorkHeap) > 0 {
		item = heap.Pop(&group.waitingWorkHeap).(*waitingWork)
	} else {
		item = heap.Pop(&group.openEpochsHeap).(*waitingWork)
	}
	waitDur := now.Sub(item.enqueueingTime)
	group.priorityStates.updateDelayLocked(item.priority, waitDur, false /* canceled */)
	q.adjustGroupUsedLocked(group, item.requestedCount)
	if group.perGroupMetrics.admittedCount != nil {
		group.perGroupMetrics.admittedCount.Inc(1)
		group.perGroupMetrics.waitTimeNanos.Inc(waitDur.Nanoseconds())
	}
	if !isInGroupHeap(group) {
		q.mu.groupHeap.remove(group)
	}
	// Get the value of requestedCount before releasing the mutex, since after
	// releasing Admit can notice that item is no longer in the heap and call
	// releaseWaitingWork to return item to the waitingWorkPool.
	requestedCount := item.requestedCount
	// Cannot read group after release q.mu, since group may get GC'd and
	// reused.
	groupID := group.id
	q.mu.Unlock()

	if !item.replicated.Enabled {
		// Reduce critical section by sending on channel after releasing mutex.
		item.ch <- grantChainID
	} else {
		// NB: We don't use grant chains for store tokens, so they don't apply
		// to replicated writes.
		if log.V(1) {
			q.mu.Lock()
			queueLen := group.waitingWorkHeap.Len()
			q.mu.Unlock()

			log.Dev.Infof(q.ambientCtx, "async-path: len(waiting-work)=%d dequeued t%d pri=%s r%s log-position=%s ingested=%t",
				queueLen, groupID, item.priority,
				item.replicated.RangeID,
				item.replicated.LogPosition,
				item.replicated.Ingested,
			)
		}
		defer releaseWaitingWork(item)
		q.onAdmittedReplicatedWork.admittedReplicatedWork(
			roachpb.MustMakeTenantID(groupID),
			item.priority,
			item.replicated,
			item.requestedCount,
			item.createTime,
			true, /* coordMuLocked */
		)

		q.metrics.incAdmitted(item.priority)
		waitDur := q.timeNow().Sub(item.enqueueingTime)
		q.metrics.recordFinishWait(item.priority, waitDur)
		if item.heapIndex != -1 {
			panic(errors.AssertionFailedf("grantee should be removed from heap"))
		}
	}
	return requestedCount
}

// gcGroupsResetUsedAndUpdateEstimators does three things:
//  1. It resets group.used, which is the resource count over which the
//     WorkQueue does fair-sharing. That is, fair-sharing is done over
//     intervals that are sized at the frequency with which this
//     function is called (as of 1/9/26, every 1s).
//  2. It GCs groupInfo entries, if a group has seen no workload over
//     the interval. Groups whose IDs appear in resourceGroupConfig are
//     never GCed: their (weight, maxCPU) is the source of truth for
//     RM mode, and dropping the groupInfo would silently lose that
//     state until the next SetResourceGroupConfig call (since
//     applyResourceGroupConfigIfChanged is gated on the dirty bit).
//     Lazy re-creation in Admit defaults maxCPU=false, so a GC + Admit
//     interleaving on a configured group would leave it stuck at
//     defaults. Keeping configured entries also preserves their
//     warmed-up cpuTimeTokenEstimator and burst bucket state across
//     idle periods.
//  3. It updates CPU time token estimators. The estimators are only used
//     if mode == usesCPUTimeTokens.
func (q *WorkQueue) gcGroupsResetUsedAndUpdateEstimators() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.defaultCPUTimeTokenEstimator.update()
	// With large numbers of active groups, this iteration could hold the lock
	// longer than desired. We could break this iteration into smaller parts if
	// needed.
	for id, info := range q.mu.groups {
		// In RM mode, skip GC for IDs that rmGroups owns. Otherwise
		// a GC + lazy re-create would leave the group stuck at
		// maxCPU=false defaults until the next SetResourceGroupConfig
		// or mode swap. The check is gated on useResourceGroup so
		// the defaultRMResourceGroupConfig seed (which is installed
		// even in serverless mode and otherwise sits unused) does
		// not silently change serverless GC behavior for groups
		// whose IDs collide with the seed (e.g., tenant 1).
		configured := false
		if q.mu.useResourceGroup {
			_, configured = q.mu.rmGroups[id]
		}
		if info.used == 0 && !isInGroupHeap(info) && !configured {
			delete(q.mu.groups, id)
			releaseGroupInfo(info)
		} else {
			info.cpuTimeTokenEstimator.update()
			info.used = 0
			// All the heap members will reset used=0, so no need to change heap
			// ordering.
		}
	}
}

// adjustGroupUsed is used internally by StoreWorkQueue, and by the KV queue
// in AdmittedWorkDone. The additionalUsed count can be negative, in which
// case it is returning unused resources. This is only for WorkQueue's own
// accounting -- it should not call into granter.
func (q *WorkQueue) adjustGroupUsed(groupID roachpb.TenantID, delta int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	tid := groupID.ToUint64()
	group, ok := q.mu.groups[tid]
	if !ok {
		return
	}
	q.adjustGroupUsedLocked(group, delta)
}

func (q *WorkQueue) adjustGroupUsedLocked(group *groupInfo, delta int64) {
	if delta < 0 {
		toReturn := uint64(-delta)
		if group.used < toReturn {
			group.used = 0
		} else {
			group.used -= toReturn
		}
	} else {
		group.used += uint64(delta)
	}
	if group.perGroupMetrics.tokensUsed != nil {
		if delta > 0 {
			group.perGroupMetrics.tokensUsed.Inc(delta)
		} else if delta < 0 {
			group.perGroupMetrics.tokensReturned.Inc(-delta)
		}
	}
	if q.mode == usesCPUTimeTokens {
		// Burst bucket tracks available budget, so we negate delta: consuming
		// resources (positive delta to used) depletes the burst bucket.
		group.cpuTimeBurstBucket.adjust(-delta)
	}
	if isInGroupHeap(group) {
		q.mu.groupHeap.fix(group)
	}
}

// AdmittedSQLWorkDone returns unused reservation tokens to the granter
// when a SQL statement closes. remaining is always non-negative since
// the CAS-based deduction in SQLCPUHandle never drives reservation
// below zero.
func (q *WorkQueue) AdmittedSQLWorkDone(tenantID roachpb.TenantID, remaining int64) {
	if remaining == 0 {
		return
	}
	if remaining < 0 && buildutil.CrdbTestBuild {
		log.Dev.Fatalf(q.ambientCtx, "AdmittedSQLWorkDone: remaining %d is negative", remaining)
	}
	q.adjustGroupUsed(tenantID, -remaining)
	if remaining < 0 {
		// Should never happen, but account for it defensively.
		q.granter.tookWithoutPermission(-remaining)
	} else {
		q.granter.returnGrant(remaining)
	}
}

// refillBurstBuckets adds tokens to all group burst buckets and updates
// their capacity. This is called by serverlessStrategy.refillBurst
// periodically (every 1ms). toAdd and capacity are passed uniformly to
// all tenants with no per-tenant scaling.
//
// If a group's burst qualification changes as a result of the refill,
// the group's position in the groupHeap is updated to maintain correct
// priority ordering.
func (q *WorkQueue) refillBurstBuckets(toAdd int64, capacity int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.burstBucketCapacity = capacity
	for _, group := range q.mu.groups {
		q.refillBurstBucketLocked(group, toAdd, capacity)
	}
}

// refillBurstBucketForGroup adds tokens to a specific resource group's
// burst bucket and updates its capacity. Called by rmStrategy.refillBurst
// with pre-scaled per-group amounts. For example, a group with
// WEIGHT_CPU=10% gets toAdd and capacity equal to 10% of the 100% CPU
// rate, so its burst bucket stays at steady state when the group uses
// ~10% of node CPU.
//
// Unlike refillBurstBuckets (the serverless path), this does not
// update q.mu.burstBucketCapacity. RM mode leaves that field at zero
// so lazy-created RM groups init their bucket at capacity=0 and pick
// up the correct per-group scaled capacity on the next refill (within
// 1ms). See burstBucketCapacity's field comment for why the 0 seed is
// preferred over the unscaled 100% value.
//
// maxCPU is not a parameter: the flag lives in resourceGroupConfig
// and is pushed into existing groupInfos by SetResourceGroupConfig
// (and seeded on new groupInfos via getMaxCPULocked at creation).
//
// If the group's burst qualification changes (because the refill
// crossed a token threshold), its position in the groupHeap is
// updated.
func (q *WorkQueue) refillBurstBucketForGroup(groupID uint64, toAdd int64, capacity int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	group, ok := q.mu.groups[groupID]
	if !ok {
		return
	}
	q.refillBurstBucketLocked(group, toAdd, capacity)
}

// refillBurstBucketLocked refills a group's burst bucket and fixes its
// heap position if the burst qualification changed. q.mu must be held.
func (q *WorkQueue) refillBurstBucketLocked(group *groupInfo, toAdd int64, capacity int64) {
	q.mu.AssertHeld()
	prevBurstQual := group.cpuTimeBurstBucket.burstQualification()
	group.cpuTimeBurstBucket.refill(toAdd, capacity)
	curBurstQual := group.cpuTimeBurstBucket.burstQualification()
	if prevBurstQual != curBurstQual && isInGroupHeap(group) {
		q.mu.groupHeap.fix(group)
	}
}

func (q *WorkQueue) String() string {
	return redact.StringWithoutMarkers(q)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (q *WorkQueue) SafeFormat(s redact.SafePrinter, _ rune) {
	q.mu.Lock()
	defer q.mu.Unlock()
	s.Printf("closed epoch: %d ", q.mu.closedEpochThreshold)
	s.Printf("groupHeap len: %d", len(q.mu.groupHeap))
	if len(q.mu.groupHeap) > 0 {
		s.Printf(" top group: %d", q.mu.groupHeap[0].id)
	}
	var ids []uint64
	for id := range q.mu.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		group := q.mu.groups[id]
		s.Printf("\n group-id: %d used: %d, w: %d, fifo: %d", group.id, group.used,
			group.weight, group.fifoPriorityThreshold)
		if len(group.waitingWorkHeap) > 0 {
			// Sort items within waitingWorkHeap
			sortedWaitingWorkHeap := slices.Clone(group.waitingWorkHeap)
			sort.Sort(&sortedWaitingWorkHeap)
			s.Printf(" waiting work heap:")
			for i := range sortedWaitingWorkHeap {
				var workOrdering string
				if sortedWaitingWorkHeap[i].arrivalTimeWorkOrdering == lifoWorkOrdering {
					workOrdering = ", lifo-ordering"
				}
				s.Printf(" [%d: pri: %d, ct: %d, epoch: %d, qt: %d%s]", i,
					sortedWaitingWorkHeap[i].priority,
					sortedWaitingWorkHeap[i].createTime/int64(time.Millisecond),
					sortedWaitingWorkHeap[i].epoch,
					sortedWaitingWorkHeap[i].enqueueingTime.UnixNano()/int64(time.Millisecond), workOrdering)
			}
		}
		if len(group.openEpochsHeap) > 0 {
			// Sort items within openEpochsHeap
			sortedOpenEpochsHeap := slices.Clone(group.openEpochsHeap)
			sort.Sort(&sortedOpenEpochsHeap)
			s.Printf(" open epochs heap:")
			for i := range sortedOpenEpochsHeap {
				s.Printf(" [%d: pri: %d, ct: %d, epoch: %d, qt: %d]", i,
					sortedOpenEpochsHeap[i].priority,
					sortedOpenEpochsHeap[i].createTime/int64(time.Millisecond),
					sortedOpenEpochsHeap[i].epoch,
					sortedOpenEpochsHeap[i].enqueueingTime.UnixNano()/int64(time.Millisecond))
			}
		}
	}
	if q.mode == usesCPUTimeTokens && len(ids) > 0 {
		s.Printf("\nburst-buckets: ")
		for i, id := range ids {
			if i > 0 {
				s.Printf(" ")
			}
			group := q.mu.groups[id]
			s.Printf("t%d=%s", id, &group.cpuTimeBurstBucket)
		}
	}
}

// Weight for groups that are not assigned a weight. This typically applies
// to groups which weren't on this node in the prior call to
// SetTenantWeights. Additionally, it is also the minimum group weight.
const defaultGroupWeight = 1

// The current cap on the weight of a group. We don't allow a single group
// to use more than cap times the number of resources of the smallest group.
// For KV slots, we have seen a range of slot counts from 50-200 for 16 cpu
// nodes, for a KV50 workload, depending on how we set
// admission.kv_slot_adjuster.overload_threshold. We don't want to starve
// small groups, so the cap is currently set to 20. A more sophisticated fair
// sharing scheme would not need such a cap.
const groupWeightCap = 20

func (q *WorkQueue) getGroupWeightLocked(groupID uint64) uint32 {
	weight, ok := q.mu.groupWeights.active[groupID]
	if !ok {
		weight = defaultGroupWeight
	}
	return weight
}

// SetResourceGroupConfig installs a new per-resource-group
// configuration (weight + maxCPU) for Resource Manager mode. This
// is the external API entry point - typically called from the SQL
// CREATE/ALTER RESOURCE GROUP path via
// CPUGrantCoordinators.SetResourceGroupConfig, which forwards here.
//
// Behavior depends on whether RM mode is active:
//
//   - Always: assigns config to q.mu.resourceGroupConfig.
//   - If useResourceGroup is true (RM mode active): calls
//     applyResourceGroupConfigLocked synchronously to materialize
//     all derived state (groupWeights.active, per-group weight +
//     maxCPU on existing groupInfos with heap fixes, pre-creation
//     of newly-configured IDs, refresh of rmGroupConfigs).
//   - If useResourceGroup is false (serverless mode): no derived
//     state is touched. The accumulated config is materialized
//     when the queue eventually transitions into RM mode -
//     setUseResourceGroup(true) detects the false→true transition
//     and unconditionally calls applyResourceGroupConfigLocked.
//
// === What lags the immediate apply ===
//
// One piece of derived state is not updated synchronously:
// groupInfo.cpuTimeBurstBucket.capacity. The per-group scaled
// capacity is installed by the next refillBurstBucketForGroup
// tick (within 1ms), reading the freshly-stored rmGroupConfigs.
// During that lag, burstQualification computes against the old
// capacity denominator, so a group's qualification could be
// briefly wrong by one tier (canBurst vs noBurst). This is bounded
// and self-correcting: the granter's noBurst/canBurst pools - not
// this bucket - are the binding constraint on actual CPU
// consumption, so the brief mis-qualification only affects heap
// ordering and which pool the group draws from. No aggregate CPU
// over-consumption is possible.
//
// === Tradeoff vs deferred apply ===
//
// The previous design deferred all derived-state updates to the
// next resetInterval (~1s after Set). That gave a clean atomic
// transition (no torn snapshots) but at three real costs: (1)
// configured groups could be lazy-created with default weight +
// maxCPU during the deferral window, (2) per-group refill
// continued at old burstFrac scaling for up to 1s, and (3)
// operators couldn't observe new config in metrics until the
// next cycle. Immediate apply trades atomicity (≤1ms of stitched
// bucket.capacity) for shorter, narrower inconsistency windows.
// See the resourceGroupConfig field comment for the full
// rationale.
//
// The map is captured by reference. The caller must not modify it
// after calling.
func (q *WorkQueue) SetResourceGroupConfig(config map[uint64]ResourceGroupConfig) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Replace rmGroups wholesale. burstFrac is left at zero in each
	// new entry; if RM mode is active, the apply call below computes
	// it. Otherwise the next setUseResourceGroup(true) on mode swap
	// will compute it.
	q.mu.rmGroups = make(map[uint64]rmGroup, len(config))
	for id, c := range config {
		q.mu.rmGroups[id] = rmGroup{ResourceGroupConfig: c}
	}
	if q.mu.useResourceGroup {
		q.applyResourceGroupConfigLocked()
	}
}

// applyResourceGroupConfigLocked materializes the derived state from
// q.mu.rmGroups: computes per-group burstFrac in place, refreshes
// scaled groupWeights.active, and upserts each configured group's
// groupInfo (weight + maxCPU + heap fix on qualification flip;
// pre-create if missing).
//
// q.mu must be held. Two callers, mutually exclusive:
//
//   - SetResourceGroupConfig when useResourceGroup is true (the
//     immediate-apply path during RM steady-state).
//   - setUseResourceGroup on a false→true transition (the
//     mode-swap path; also handles the construction bootstrap on
//     first activation of RM mode).
//
// All updates land in one critical section, so any concurrent reader
// that takes q.mu sees fully-old or fully-new derived state - never
// a stitched generation. The one piece that catches up later is
// per-group cpuTimeBurstBucket.capacity, installed by the next
// refillRMGroupBurstBuckets tick (within 1ms).
//
// At our expected N (handful of groups), the q.mu hold here is
// microseconds and not a hot-path concern. If N grows into the
// hundreds or thousands, mirror SetTenantWeights's sub-mutex
// pattern: pre-compute scaled weights and burstFracs under a
// dedicated sub-mutex, take q.mu briefly to swap into active state,
// then process per-group upserts in batches with q.mu released
// between batches.
//
// === Mode-flip mechanics (serverless ↔ RM) ===
//
// This is the canonical place to read about what happens to existing
// queued work, container identity, and concurrent admits when the
// cpuTimeTokenMode cluster setting flips. The mode swap touches two
// pieces of state on the filler goroutine inside resetInterval:
//
//	W1: a.strategy = newStrategy            (filler-only field, no lock)
//	W2: setUseResourceGroup(...)            (q.mu, calls applyResourceGroupConfigLocked
//	                                         on a false→true transition)
//
// W1 and W2 are not under one lock, so there is a microseconds-scale
// window where one reflects the new mode and the other still reflects
// the old. The current code orders W1 before W2 (strategy-first),
// matching the natural cause-and-effect of "decide mode → execute
// setup for that mode." See the long comment in resetInterval for the
// full safety argument; the short version is that no observer reads
// both pieces of state, so the inconsistency is unobservable in
// practice. The W1/W2 ordering choice produces no functional
// difference - either order is safe, just slightly different transient
// bookkeeping during the window.
//
// What happens to the WorkQueue's existing q.mu.groups entries when
// W2 fires (i.e., serverless → RM):
//
//   - Colliding IDs (container repurposed). The hardcoded RM
//     resource group IDs are highResourceGroupID=1 and
//     lowResourceGroupID=2, which alias the system tenant
//     (TenantID=1) and the default app tenant (TenantID=2). For
//     these IDs, the loop above takes the "exists, update" branch:
//     the same *groupInfo is reused, with weight/maxCPU updated to
//     the rmGroup values and the heap fixed if those changes
//     affected groupHeap.Less. The container's queued work,
//     cpuTimeBurstBucket tokens, cpuTimeTokenEstimator,
//     priorityStates, and perGroupMetrics counters all carry over.
//     burstBucket capacity is the one piece that lags - it stays at
//     the serverless-mode value until the next
//     refillRMGroupBurstBuckets tick (~1ms) installs the proper
//     RM-scaled capacity.
//
//   - Non-colliding IDs (container orphaned). Tenant containers
//     whose IDs aren't in rmGroups (e.g., groups[5] for some app
//     tenant 5) aren't touched by the loop above. They remain in
//     q.mu.groups and, if they have queued work, in q.mu.groupHeap.
//     Future Admits don't route to them anymore - in RM mode,
//     groupIDForWorkLocked returns priorityToResourceGroup(...) for
//     every tenant, so all new work goes to groups[1] or groups[2].
//
// What happens to queued requests across the flip:
//
//   - Repurposed containers continue to serve their existing queued
//     work alongside new RM admits. Nothing dequeues or moves the
//     pre-existing items; they just compete in the (now-RM-weighted)
//     heap.
//
//   - Orphan containers continue to serve their existing queued work
//     too. q.mu.groupHeap is the universal pool that hasWaitingRequests
//     and granted() look at; it does not filter by mode. As long as an
//     orphan has waiting work, it's eligible for selection by granted()
//     and its requests get admitted normally. No special "drain orphan"
//     code path exists or is needed.
//
//   - Once an orphan empties, the heap removal in granted() drops it
//     from groupHeap. The next gcGroupsResetUsedAndUpdateEstimators
//     sweep finds info.used == 0 && !isInGroupHeap && !configured and
//     deletes the empty entry. See that function's comment for why the
//     "configured" check is gated on useResourceGroup (it's the same
//     ID-collision concern, handled in the GC).
//
//   - GC never discards queued requests. The deletion conditions
//     require the container to be empty (!isInGroupHeap), so any
//     container with pending work is immune.
//
// Concurrent Admits during the W1/W2 window:
//
//   - They take q.mu, observe the old useResourceGroup (false), derive
//     groupID by TenantID, and queue into a tenant-keyed group. Their
//     fate after W2 is identical to pre-existing work: if the tenant
//     ID collides with an rmGroup ID, the container gets repurposed
//     and they ride along; if not, they sit in an orphan that drains.
//     Either way the request is admitted via the normal granted() path.
//
// Transient quirks during the post-flip drain (none affect correctness,
// throughput, or work admission):
//
//   - Metric label semantic shift. perGroupMetrics children are keyed
//     by stringified ID. The counter labeled "1" represents
//     system-tenant admits before the flip and high-pri RM admits
//     after - same metric label, different semantic meaning. A
//     time-series shows a continuous counter with a discontinuity in
//     meaning at the flip moment.
//
//   - Mixed-weight heap. groupHeap may contain repurposed RM
//     containers (with new RM weights) and orphan tenant containers
//     (with serverless weights) competing simultaneously. The two
//     weight scales aren't necessarily commensurate, so heap ordering
//     during the drain can be slightly off relative to either pure
//     mode. Resolves once orphans drain (typically milliseconds).
//
//   - burstQualification briefly off for repurposed containers. Their
//     burst bucket carries over the serverless capacity until the next
//     refill tick installs RM-scaled capacity. burstQualification can
//     therefore report canBurst/noBurst slightly off from steady-state
//     RM behavior for ≤1ms. Burst qualification only governs which
//     granter qual-slot the request consumes from; it does not gate
//     admission, since the granter's central token pool meters work.
//
// Reverse flip (RM → serverless): symmetric. setUseResourceGroup(false)
// just clears the flag; no apply runs (apply only fires on false→true).
// The repurposed RM containers (groups[1], groups[2]) carry over with
// their RM-era state and get treated as system/app tenant containers
// for new admits. Same drain story applies to anything still queued.
func (q *WorkQueue) applyResourceGroupConfigLocked() {
	// Compute per-group burstFrac in place. MaxCPU groups get 1.0
	// (burst to full node CPU); other groups get their normalized
	// weight share. Skip the burstFrac update if all weights sum to
	// zero - leaves rmGroups entries with burstFrac=0, which the
	// refill caller treats as no-burst-budget.
	var totalWeight uint32
	for _, g := range q.mu.rmGroups {
		totalWeight += g.Weight
	}
	if totalWeight > 0 {
		for id, g := range q.mu.rmGroups {
			if g.MaxCPU {
				g.burstFrac = 1.0
			} else {
				g.burstFrac = float64(g.Weight) / float64(totalWeight)
			}
			q.mu.rmGroups[id] = g
		}
	}

	weights := computeScaledGroupWeights(q.mu.rmGroups)
	q.mu.groupWeights.active = weights

	for id, g := range q.mu.rmGroups {
		group, ok := q.mu.groups[id]
		if !ok {
			group = newGroupInfo(id, weights[id], q.mode,
				q.mu.defaultCPUTimeTokenEstimator.estimateTokensToBeUsed(),
				q.mu.burstBucketCapacity, g.MaxCPU, q.perGroupAggMetrics)
			q.mu.groups[id] = group
			continue
		}
		// Track whether anything that affects groupHeap.Less changed,
		// so we can fix the heap once per group at the end. Less
		// orders by burstQualification then by used/weight, so both a
		// weight change and a maxCPU-driven qualification flip can
		// invalidate the heap invariant. Mirrors SetTenantWeights's
		// fix-on-weight-change behavior; without this the heap can
		// transiently violate its invariant after a weight-change Set
		// until subsequent ops drift it back into order.
		needsHeapFix := false
		if group.weight != weights[id] {
			group.weight = weights[id]
			needsHeapFix = true
		}
		if group.cpuTimeBurstBucket.maxCPU != g.MaxCPU {
			prevQual := group.cpuTimeBurstBucket.burstQualification()
			group.cpuTimeBurstBucket.maxCPU = g.MaxCPU
			curQual := group.cpuTimeBurstBucket.burstQualification()
			if prevQual != curQual {
				needsHeapFix = true
			}
		}
		if needsHeapFix && isInGroupHeap(group) {
			q.mu.groupHeap.fix(group)
		}
	}
}

// refillRMGroupBurstBuckets refills every configured RM group's burst
// bucket in one q.mu critical section. rate100 and cap100 are the
// 100% CPU per-tick refill rate and the 100% CPU bucket capacity
// respectively; per-group amounts are scaled by the group's
// burstFrac. Called by rmStrategy.refillBurst on every refill tick.
//
// Iterating under one q.mu hold (rather than snapshotting and
// calling per-group methods) costs one lock acquire per refill
// instead of N+1, eliminates the per-tick snapshot allocation, and
// gives an atomic refill across all groups - no other goroutine
// can observe a partial-refill state.
func (q *WorkQueue) refillRMGroupBurstBuckets(rate100, cap100 float64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for rgID, g := range q.mu.rmGroups {
		group, ok := q.mu.groups[rgID]
		if !ok {
			continue
		}
		toAdd := int64(rate100 * g.burstFrac)
		capacity := int64(cap100 * g.burstFrac)
		q.refillBurstBucketLocked(group, toAdd, capacity)
	}
}

// computeScaledGroupWeights derives the groupWeights.active map from
// the per-resource-group state, applying the same cap+scaling rules
// as SetTenantWeights so RM and serverless treat weights identically.
//
// Runs under q.mu in applyResourceGroupConfigLocked. See that
// method's comment for why we accept the q.mu hold time at our
// expected N (small).
func computeScaledGroupWeights(groups map[uint64]rmGroup) map[uint64]uint32 {
	maxWeight := uint32(1)
	for _, g := range groups {
		if g.Weight > maxWeight {
			maxWeight = g.Weight
		}
	}
	scaling := float64(1)
	if maxWeight > groupWeightCap {
		scaling = groupWeightCap / float64(maxWeight)
	}
	out := make(map[uint64]uint32, len(groups))
	for id, g := range groups {
		w := uint32(math.Ceil(float64(g.Weight) * scaling))
		if w < defaultGroupWeight {
			w = defaultGroupWeight
		}
		out[id] = w
	}
	return out
}

// SetOverrideAllToBypassAdmission sets whether all work should bypass
// admission control. Used by CPU time token AC.
func (q *WorkQueue) SetOverrideAllToBypassAdmission(override bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.overrideAllToBypassAdmission = override
}

// SetTenantWeights sets the weight of tenants, using the provided tenant ID
// => weight map. A nil map will result in all tenants having the same weight.
//
// TODO(wenyihu): rename to SetGroupWeights.
func (q *WorkQueue) SetTenantWeights(groupWeights map[uint64]uint32) {
	q.mu.groupWeights.mu.Lock()
	defer q.mu.groupWeights.mu.Unlock()
	if q.mu.groupWeights.inactive == nil {
		q.mu.groupWeights.inactive = make(map[uint64]uint32)
	}
	// Remove all elements from the inactive map.
	for k := range q.mu.groupWeights.inactive {
		delete(q.mu.groupWeights.inactive, k)
	}
	// Compute the max weight in the new map, for enforcing the groupWeightCap.
	maxWeight := uint32(1)
	for _, v := range groupWeights {
		if v > maxWeight {
			maxWeight = v
		}
	}
	scaling := float64(1)
	if maxWeight > groupWeightCap {
		scaling = groupWeightCap / float64(maxWeight)
	}
	// Populate the weights in the inactive map.
	for k, v := range groupWeights {
		w := uint32(math.Ceil(float64(v) * scaling))
		if w < defaultGroupWeight {
			w = defaultGroupWeight
		}
		q.mu.groupWeights.inactive[k] = w
	}
	// Establish the new active map.
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.mu.groupWeights.active, q.mu.groupWeights.inactive =
			q.mu.groupWeights.inactive, q.mu.groupWeights.active
	}()
	// Create a slice for storing all the groupIDs. We use this to split the
	// update to the data-structures that require holding q.mu, in case there
	// are 1000s of groups (we don't want to hold q.mu for long durations).
	groupIDs := func() []uint64 {
		q.mu.Lock()
		defer q.mu.Unlock()
		gIDs := make([]uint64, len(q.mu.groups))
		i := 0
		for k := range q.mu.groups {
			gIDs[i] = k
			i++
		}
		return gIDs
	}()
	// Any groups not in groupIDs will see the latest weight when their
	// groupInfo is created. The existing ones need their weights to be
	// updated.

	// groupIDs[index] represents the next groupID that needs to be updated.
	var index int
	n := len(groupIDs)
	// updateNextBatch acquires q.mu and updates a batch of groups.
	updateNextBatch := func() (repeat bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		// Arbitrary batch size of 5.
		const batchSize = 5
		for i := 0; i < batchSize; i++ {
			if index >= n {
				return false
			}
			groupID := groupIDs[index]
			gi := q.mu.groups[groupID]
			weight := q.getGroupWeightLocked(groupID)
			if gi != nil && gi.weight != weight {
				gi.weight = weight
				if isInGroupHeap(gi) {
					q.mu.groupHeap.fix(gi)
				}
			}
			index++
		}
		return true
	}
	for updateNextBatch() {
	}
}

// close tells the gc goroutine to stop.
func (q *WorkQueue) close() {
	close(q.stopCh)
}

type workOrderingKind int8

const (
	fifoWorkOrdering workOrderingKind = iota
	lifoWorkOrdering
)

type priorityState struct {
	priority admissionpb.WorkPriority
	// maxQueueDelay includes the delay of both successfully admitted and
	// canceled requests.
	//
	// NB: The maxQueueDelay value is an incomplete picture of delay since it
	// does not have visibility into work that is still waiting in the queue.
	// However, since we use the maxQueueDelay across a collection of priorities
	// to set a priority threshold, we expect that usually there will be some
	// work just below the priority threshold that does dequeue (with high
	// latency) -- if not, it is likely that the next high priority is actually
	// the one experiencing some instances of high latency. That is, it is very
	// unlikely to be the case that a certain priority sees admission with no
	// high latency while the next lower priority never gets work dequeued
	// because of resource saturation.
	maxQueueDelay time.Duration
	// Count of requests that were successfully admitted (not canceled). This is
	// used in concert with lowestPriorityWithRequests to detect priorities
	// where work was queued but nothing was successfully admitted.
	admittedCount int
}

// priorityStates tracks information about admission requests and admission
// grants at various priorities. It is used to set a priority threshold for
// LIFO queuing. There is one priorityStates per group, since it is embedded
// in a groupInfo.
type priorityStates struct {
	// In increasing order of priority. Expected to not have more than 10
	// elements, so a linear search is fast. The slice is emptied after each
	// epoch is closed.
	ps                         []priorityState
	lowestPriorityWithRequests int
}

// makePriorityStates returns an empty priorityStates, that reuses the
// ps slice.
func makePriorityStates(ps []priorityState) priorityStates {
	return priorityStates{ps: ps[:0], lowestPriorityWithRequests: admissionpb.OneAboveHighPri}
}

// requestAtPriority is called when a request is received at the given
// priority.
func (ps *priorityStates) requestAtPriority(priority admissionpb.WorkPriority) {
	if int(priority) < ps.lowestPriorityWithRequests {
		ps.lowestPriorityWithRequests = int(priority)
	}
}

// updateDelayLocked is called with the delay experienced by work at the given
// priority. This is used to compute priorityState.maxQueueDelay. Canceled
// indicates whether the request was canceled while waiting in the queue, or
// successfully admitted.
func (ps *priorityStates) updateDelayLocked(
	priority admissionpb.WorkPriority, delay time.Duration, canceled bool,
) {
	i := 0
	n := len(ps.ps)
	for ; i < n; i++ {
		pri := ps.ps[i].priority
		if pri == priority {
			if !canceled {
				ps.ps[i].admittedCount++
			}
			if ps.ps[i].maxQueueDelay < delay {
				ps.ps[i].maxQueueDelay = delay
			}
			return
		}
		if pri > priority {
			break
		}
	}
	admittedCount := 1
	if canceled {
		admittedCount = 0
	}
	state := priorityState{priority: priority, maxQueueDelay: delay, admittedCount: admittedCount}
	if i == n {
		ps.ps = append(ps.ps, state)
	} else {
		ps.ps = append(ps.ps[:i+1], ps.ps[i:]...)
		ps.ps[i] = state
	}
}

func (ps *priorityStates) getFIFOPriorityThresholdAndReset(
	curPriorityThreshold int, epochLengthNanos int64, maxQueueDelayToSwitchToLifo time.Duration,
) int {
	// priority is monotonically increasing in the calculation below.
	priority := int(admissionpb.LowPri)
	foundLowestPriority := false
	handlePriorityState := func(p priorityState) {
		if p.maxQueueDelay > maxQueueDelayToSwitchToLifo {
			// LIFO.
			priority = int(p.priority) + 1
		} else if int(p.priority) < curPriorityThreshold {
			// Currently LIFO. If the delay is above some fraction of the threshold,
			// we continue as LIFO. If the delay is below that fraction, we could
			// have a situation where requests were made at this priority but
			// nothing was admitted -- we continue with LIFO in that case too.
			if p.maxQueueDelay > time.Duration(epochLengthNanos)/10 ||
				(p.admittedCount == 0 && int(p.priority) >= ps.lowestPriorityWithRequests) {
				priority = int(p.priority) + 1
			}
			// Else, can switch to FIFO, at least based on queue delay at this
			// priority. But we examine the other higher priorities too, since it is
			// possible that few things were received for this priority and it got
			// lucky in getting them admitted.
		}
	}
	for i := range ps.ps {
		p := ps.ps[i]
		if int(p.priority) == ps.lowestPriorityWithRequests {
			foundLowestPriority = true
		}
		handlePriorityState(p)
	}
	if !foundLowestPriority && ps.lowestPriorityWithRequests != admissionpb.OneAboveHighPri &&
		priority <= ps.lowestPriorityWithRequests {
		// The new threshold will cause lowestPriorityWithRequests to be FIFO, and
		// we know nothing exited admission control for this lowest priority.
		// Since !foundLowestPriority, we know we haven't explicitly considered
		// this priority in the above loop. So we consider it now.
		handlePriorityState(priorityState{
			priority:      admissionpb.WorkPriority(ps.lowestPriorityWithRequests),
			maxQueueDelay: 0,
			admittedCount: 0,
		})
	}
	ps.ps = ps.ps[:0]
	ps.lowestPriorityWithRequests = admissionpb.OneAboveHighPri
	return priority
}

// groupInfo is the per-group information in the groupHeap. In Serverless mode,
// the resource group ID is the tenant ID. In RM mode with useResourceGroup,
// the resource group ID is derived from the work's priority (see
// priorityToResourceGroup).
type groupInfo struct {
	id uint64
	// The weight assigned to the resource group. Must be > 0. For
	// resource groups, this is WEIGHT_CPU.
	weight uint32
	// used is computed over an interval and periodically reset. Ordering
	// between groups, for fair sharing, utilizes this value.
	//
	// - For slots, used represents cpu time duration consumed by the group. It
	//   is incremented by 1 (for non-elastic work) or some prediction of cpu
	//   time (for elastic work) when the work is admitted. A correction is
	//   applied when the work is done based on the actual cpu time consumed.
	// - For tokens, used represents the tokens consumed. A prediction of tokens
	//   that will be consumed is deducted at admission time, and a correction
	//   is applied later.
	//
	// groupInfo will not be GC'd until both used==0 and
	// len(waitingWorkHeap)==0.
	//
	// The used value is reset to 0 periodically. This creates a risk since
	// callers of Admit hold references to groupInfo. We do not want a race
	// condition where the groupInfo held in Admit is returned to the
	// sync.Pool. Note that this race is almost impossible to reproduce in
	// practice since GC loop runs at 1s intervals and needs two iterations to
	// GC a groupInfo -- first to reset used=0 and then the next time to GC it.
	// We fix this by being careful in the code of Admit by not reusing a
	// reference to groupInfo, and instead grab a new reference from the map.
	//
	// The above fix for the GC race condition is insufficient to prevent
	// overflow of the used field if the reset to used=0 happens between used++
	// and used-- within Admit. Properly fixing that would need to track the
	// count of used==0 resets and gate the used-- on the count not having
	// changed. This was considered unnecessarily complicated and instead we
	// simply (a) do not do used--, if used is already zero, or (b) do not do
	// used-- if the request was canceled. This does imply some inaccuracy in
	// accounting -- it can be fixed if needed.
	used            uint64
	waitingWorkHeap waitingWorkHeap
	openEpochsHeap  openEpochsHeap

	priorityStates priorityStates
	// priority >= fifoPriorityThreshold is FIFO. This uses a larger sized type
	// than WorkPriority since the threshold can be > MaxPri.
	fifoPriorityThreshold int

	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap.
	heapIndex int

	// If mode == usesCPUTimeTokens, WorkQueue does CPU time token estimation.
	// See the code in Admit that calls estimateTokensToBeUsed for more on this.
	cpuTimeTokenEstimator cpuTimeTokenEstimator

	// cpuTimeBurstBucket tracks whether this group qualifies for burst
	// priority. Only used if mode == usesCPUTimeTokens. See
	// cpu_time_token_burst.go for more.
	cpuTimeBurstBucket cpuTimeBurstBucket

	// perGroupMetrics holds per-group admission metric children. Only
	// set when mode == usesCPUTimeTokens. See cpuTimeTokenMetrics for
	// details.
	perGroupMetrics groupMetrics
}

// groupMetrics groups the per-group metric children that are created
// via AggCounter.AddChild for each group.
type groupMetrics struct {
	admittedCount  *aggmetric.Counter
	waitTimeNanos  *aggmetric.Counter
	tokensUsed     *aggmetric.Counter
	tokensReturned *aggmetric.Counter
}

// groupHeap is a heap of groups with waiting work, ordered by burst
// qualification (canBurst before noBurst) then by used/weight ratio
// in increasing order (weights are an optional feature, and default
// to 1). That is, we prefer groups that are using less.
type groupHeap []*groupInfo

var _ heap.Interface = (*groupHeap)(nil)

var groupInfoPool = sync.Pool{
	New: func() interface{} {
		return &groupInfo{}
	},
}

func newGroupInfo(
	id uint64,
	weight uint32,
	mode workQueueMode,
	cpuTimeTokenEstimate int64,
	burstBucketCapacity int64,
	maxCPU bool,
	aggMetrics *groupAggMetrics,
) *groupInfo {
	ti := groupInfoPool.Get().(*groupInfo)
	*ti = groupInfo{
		id:                    id,
		weight:                weight,
		waitingWorkHeap:       ti.waitingWorkHeap,
		openEpochsHeap:        ti.openEpochsHeap,
		priorityStates:        makePriorityStates(ti.priorityStates.ps),
		fifoPriorityThreshold: int(admissionpb.LowPri),
		heapIndex:             -1,
	}
	// This is only used if mode == usesCPUTimeTokens.
	ti.cpuTimeTokenEstimator.init(cpuTimeTokenEstimate)
	// If mode != usesCPUTimeTokens, cpuTimeBurstBucket.burstQualification
	// always returns noBurst. This effectively disables the
	// burstQualification functionality.
	ti.cpuTimeBurstBucket.init(
		burstBucketCapacity, mode != usesCPUTimeTokens /* disable */, maxCPU)
	if aggMetrics != nil {
		tid := strconv.FormatUint(id, 10)
		ti.perGroupMetrics.admittedCount = aggMetrics.admittedCount.AddChild(tid)
		ti.perGroupMetrics.waitTimeNanos = aggMetrics.waitTimeNanos.AddChild(tid)
		ti.perGroupMetrics.tokensUsed = aggMetrics.tokensUsed.AddChild(tid)
		ti.perGroupMetrics.tokensReturned = aggMetrics.tokensReturned.AddChild(tid)
	}
	return ti
}

func releaseGroupInfo(ti *groupInfo) {
	if isInGroupHeap(ti) {
		panic("groupInfo has non-empty heap")
	}
	if ti.perGroupMetrics.admittedCount != nil {
		ti.perGroupMetrics.admittedCount.Unlink()
		ti.perGroupMetrics.waitTimeNanos.Unlink()
		ti.perGroupMetrics.tokensUsed.Unlink()
		ti.perGroupMetrics.tokensReturned.Unlink()
	}
	// NB: {waitingWorkHeap,openEpochsHeap}.Pop nil the slice elements when
	// removing, so we are not inadvertently holding any references.
	if cap(ti.waitingWorkHeap) > 100 {
		ti.waitingWorkHeap = nil
	}
	if cap(ti.openEpochsHeap) > 100 {
		ti.openEpochsHeap = nil
	}

	*ti = groupInfo{
		waitingWorkHeap: ti.waitingWorkHeap,
		openEpochsHeap:  ti.openEpochsHeap,
		priorityStates:  makePriorityStates(ti.priorityStates.ps),
	}
	groupInfoPool.Put(ti)
}

func (th *groupHeap) fix(item *groupInfo) {
	heap.Fix(th, item.heapIndex)
}

func (th *groupHeap) remove(item *groupInfo) {
	heap.Remove(th, item.heapIndex)
}

func (th *groupHeap) Len() int {
	return len(*th)
}

func (th *groupHeap) Less(i, j int) bool {
	// First, order by burstQualification: canBurst groups come before
	// noBurst groups. canBurst groups have access to more CPU time
	// than noBurst -- see cpu_time_token_granter.go for details -- so
	// it is important that work from a canBurst group always sorts
	// before work from a noBurst group -- else available capacity is
	// left on the table.
	iBurstQual := (*th)[i].cpuTimeBurstBucket.burstQualification()
	jBurstQual := (*th)[j].cpuTimeBurstBucket.burstQualification()
	if iBurstQual != jBurstQual {
		return iBurstQual < jBurstQual
	}
	// Beyond burstQualification (which is only enabled on CPU time token
	// AC today), for group fairness, we use used_i/weight_i <
	// used_j/weight_j to determine order. In case of a tie, prioritize
	// items with higher weight, and then items with lower group id.
	//
	// A reader may wonder if sorting on just used has the same effect
	// as sorting on burstQualification first and used second. It is indeed
	// similar, but it is not the same -- for example, used is reset every
	// 1s, thus right after a reset it can fall out of sync with
	// cpuTimeBurstBucket's burstQualification method. The source of truth
	// for whether a group can burst is cpuTimeBurstBucket's
	// burstQualification method, so we must call it here.
	if (*th)[i].used*uint64((*th)[j].weight) == (*th)[j].used*uint64((*th)[i].weight) {
		if (*th)[i].weight == (*th)[j].weight {
			return (*th)[i].id < (*th)[j].id
		}
		return (*th)[i].weight > (*th)[j].weight
	}
	return (*th)[i].used*uint64((*th)[j].weight) < (*th)[j].used*uint64((*th)[i].weight)
}

func (th *groupHeap) Swap(i, j int) {
	(*th)[i], (*th)[j] = (*th)[j], (*th)[i]
	(*th)[i].heapIndex = i
	(*th)[j].heapIndex = j
}

func (th *groupHeap) Push(x interface{}) {
	n := len(*th)
	item := x.(*groupInfo)
	item.heapIndex = n
	*th = append(*th, item)
}

func (th *groupHeap) Pop() interface{} {
	old := *th
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*th = old[0 : n-1]
	return item
}

// waitingWork is the per-work information in the waitingWorkHeap.
type waitingWork struct {
	priority admissionpb.WorkPriority
	// The workOrderingKind for this priority when this work was queued.
	arrivalTimeWorkOrdering workOrderingKind
	createTime              int64
	requestedCount          int64
	// epoch is a function of the createTime.
	epoch int64

	// ch is used to communicate a grant to the waiting goroutine. The
	// grantChainID is used by the waiting goroutine to call continueGrantChain.
	ch chan grantChainID
	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap. -1 when not in the heap. The same
	// heapIndex is used by the waitingWorkHeap and the openEpochsHeap since a
	// waitingWork is only in one of them.
	heapIndex int
	// Set to true when added to waitingWorkHeap. Only used to disambiguate
	// which heap the waitingWork is in, when we know it is in one of the heaps.
	// The only state transition is from false => true, and never restored back
	// to false.
	inWaitingWorkHeap bool
	enqueueingTime    time.Time
	replicated        ReplicatedWorkInfo
}

var waitingWorkPool = sync.Pool{
	New: func() interface{} {
		return &waitingWork{}
	},
}

// The default epoch length for doing epoch-LIFO. The epoch-LIFO scheme relies
// on clock synchronization and the expectation that transaction/query
// deadlines will be significantly higher than execution time under low load.
// A standard LIFO scheme suffers from a severe problem when a single user
// transaction can result in many lower-level work that get distributed to
// many nodes, and previous work execution can result in new work being
// submitted for admission: the later work for a transaction may no longer be
// the latest seen by the system, so will not be preferred. This means LIFO
// would do some work items from each transaction and starve the remaining
// work, so nothing would complete. This is even worse than FIFO which at
// least prefers the same transactions until they are complete (FIFO and LIFO
// are using the transaction CreateTime, and not the work arrival time).
//
// Consider a case where transaction deadlines are 1s (note this may not
// necessarily be an actual deadline, and could be a time duration after which
// the user impact is extremely negative), and typical transaction execution
// times (under low load) of 10ms. A 100ms epoch will increase transaction
// latency to at most 100ms + 5ms + 10ms, since execution will not start until
// the epoch of the transaction's CreateTime is closed. At that time, due to
// clock synchronization, all nodes will start executing that epoch and will
// implicitly have the same set of competing transactions. By the time the
// next epoch closes and the current epoch's transactions are deprioritized,
// 100ms will have elapsed, which is enough time for most of these
// transactions that get admitted to have finished all their work.
//
// Note that LIFO queueing will only happen at bottleneck nodes, and decided
// on a (group, priority) basis. So if there is even a single bottleneck node
// for a (group, priority), the above delay will occur. When the epoch closes
// at the bottleneck node, the creation time for this transaction will be
// sufficiently in the past, so the non-bottleneck nodes (using FIFO) will
// prioritize it over recent transactions. Note that there is an inversion in
// that the non-bottleneck nodes are ordering in the opposite way for such
// closed epochs, but since they are not bottlenecked, the queueing delay
// should be minimal.
//
// These are defaults and can be overridden using cluster settings. Increasing
// the epoch length will cause the epoch number to decrease. This will cause
// some confusion in the ordering between work that was previously queued with
// a higher epoch number. We accept that temporary confusion (it will clear
// once old queued work is admitted or canceled). We do not try to maintain a
// monotonic epoch, based on the epoch number already in place before the
// change, since different nodes will see the cluster setting change at
// different times.

const epochLength = time.Millisecond * 100
const epochClosingDelta = time.Millisecond * 5

// Latency threshold for switching to LIFO queuing. Once we switch to LIFO,
// the minimum latency will be epochLenghNanos+epochClosingDeltaNanos, so it
// makes sense not to switch until the observed latency is around the same.
const maxQueueDelayToSwitchToLifo = epochLength + epochClosingDelta

func epochForTimeNanos(t int64, epochLengthNanos int64) int64 {
	return t / epochLengthNanos
}

func newWaitingWork(
	priority admissionpb.WorkPriority,
	arrivalTimeWorkOrdering workOrderingKind,
	createTime int64,
	requestedCount int64,
	enqueueingTime time.Time,
	epochLengthNanos int64,
) *waitingWork {
	ww := waitingWorkPool.Get().(*waitingWork)
	ch := ww.ch
	if ch == nil {
		ch = make(chan grantChainID, 1)
	}
	*ww = waitingWork{
		priority:                priority,
		arrivalTimeWorkOrdering: arrivalTimeWorkOrdering,
		createTime:              createTime,
		requestedCount:          requestedCount,
		epoch:                   epochForTimeNanos(createTime, epochLengthNanos),
		ch:                      ch,
		heapIndex:               -1,
		enqueueingTime:          enqueueingTime,
	}
	return ww
}

// releaseWaitingWork must be called with an empty waitingWork.ch.
func releaseWaitingWork(ww *waitingWork) {
	ch := ww.ch
	select {
	case <-ch:
		panic("channel must be empty and not closed")
	default:
	}
	*ww = waitingWork{
		ch: ch,
	}
	waitingWorkPool.Put(ww)
}

// waitingWorkHeap is a heap of waiting work within a group. It is ordered in
// decreasing order of priority, and within the same priority in increasing
// order of createTime (to prefer older work) for FIFO, and in decreasing
// order of createTime for LIFO. In the LIFO case the heap only contains
// epochs that are closed.
type waitingWorkHeap []*waitingWork

var _ heap.Interface = (*waitingWorkHeap)(nil)

func (wwh *waitingWorkHeap) remove(item *waitingWork) {
	heap.Remove(wwh, item.heapIndex)
}

func (wwh *waitingWorkHeap) Len() int { return len(*wwh) }

// Less does LIFO or FIFO ordering among work with the same priority. The
// ordering to use is specified by the arrivalTimeWorkOrdering. When
// transitioning from LIFO => FIFO or FIFO => LIFO, we can have work with
// different arrivalTimeWorkOrderings in the heap (for the same priority). In
// this case we err towards LIFO since this indicates a new or recent overload
// situation. If it was a recent overload that no longer exists, we will be
// able to soon drain these LIFO work items from the queue since they will get
// admitted. Erring towards FIFO has the danger that if we are transitioning
// to LIFO we will need to wait for those old queued items to be serviced
// first, which will delay the transition.
//
// Less is not strict weak ordering since the transitivity property is not
// satisfied in the presence of elements that have different values of
// arrivalTimeWorkOrdering. This is acceptable for heap maintenance.
// Example: Three work items with the same epoch where t1 < t2 < t3
//
//	w3: (fifo, create: t3, epoch: e)
//	w2: (lifo, create: t2, epoch: e)
//	w1: (fifo, create: t1, epoch: e)
//	w1 < w3, w3 < w2, w2 < w1, which is a cycle.
func (wwh *waitingWorkHeap) Less(i, j int) bool {
	if (*wwh)[i].priority == (*wwh)[j].priority {
		if (*wwh)[i].arrivalTimeWorkOrdering == lifoWorkOrdering ||
			(*wwh)[i].arrivalTimeWorkOrdering != (*wwh)[j].arrivalTimeWorkOrdering {
			// LIFO, and the epoch is closed, so can simply use createTime.
			return (*wwh)[i].createTime > (*wwh)[j].createTime
		}
		// FIFO.
		return (*wwh)[i].createTime < (*wwh)[j].createTime
	}
	return (*wwh)[i].priority > (*wwh)[j].priority
}

func (wwh *waitingWorkHeap) Swap(i, j int) {
	(*wwh)[i], (*wwh)[j] = (*wwh)[j], (*wwh)[i]
	(*wwh)[i].heapIndex = i
	(*wwh)[j].heapIndex = j
}

func (wwh *waitingWorkHeap) Push(x interface{}) {
	n := len(*wwh)
	item := x.(*waitingWork)
	item.heapIndex = n
	item.inWaitingWorkHeap = true
	*wwh = append(*wwh, item)
}

func (wwh *waitingWorkHeap) Pop() interface{} {
	old := *wwh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*wwh = old[0 : n-1]
	return item
}

// openEpochsHeap is a heap of waiting work within a group that will be
// subject to LIFO ordering (when transferred to the waitingWorkHeap) and
// whose epoch is not yet closed. See the Less method for the ordering applied
// here.
type openEpochsHeap []*waitingWork

var _ heap.Interface = (*openEpochsHeap)(nil)

func (oeh *openEpochsHeap) remove(item *waitingWork) {
	heap.Remove(oeh, item.heapIndex)
}

func (oeh *openEpochsHeap) Len() int { return len(*oeh) }

// Less orders in increasing order of epoch, and within the same epoch, with
// decreasing priority and with the same priority with increasing CreateTime.
// It is not typically dequeued from to admit work, but if it is, it will
// behave close to the FIFO ordering in the waitingWorkHeap (not exactly FIFO
// because work items with higher priority can be later than those with lower
// priority if they have a higher epoch -- but epochs are coarse enough that
// this should not be a factor). This close-to-FIFO is preferable since
// dequeuing from this queue may be an indicator that the overload is going
// away. There is also a risk with this close-to-FIFO behavior if we rapidly
// fluctuate between overload and normal: doing FIFO here could cause
// transaction work to start but not finish because the rest of the work may
// be done using LIFO ordering. When an epoch closes, a prefix of this heap
// will be dequeued and added to the waitingWorkHeap.
func (oeh *openEpochsHeap) Less(i, j int) bool {
	if (*oeh)[i].epoch == (*oeh)[j].epoch {
		if (*oeh)[i].priority == (*oeh)[j].priority {
			return (*oeh)[i].createTime < (*oeh)[j].createTime
		}
		return (*oeh)[i].priority > (*oeh)[j].priority
	}
	return (*oeh)[i].epoch < (*oeh)[j].epoch
}

func (oeh *openEpochsHeap) Swap(i, j int) {
	(*oeh)[i], (*oeh)[j] = (*oeh)[j], (*oeh)[i]
	(*oeh)[i].heapIndex = i
	(*oeh)[j].heapIndex = j
}

func (oeh *openEpochsHeap) Push(x interface{}) {
	n := len(*oeh)
	item := x.(*waitingWork)
	item.heapIndex = n
	*oeh = append(*oeh, item)
}

func (oeh *openEpochsHeap) Pop() interface{} {
	old := *oeh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*oeh = old[0 : n-1]
	return item
}

var (
	requestedMeta = metric.Metadata{
		Name:        "admission.requested.",
		Help:        "Number of requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	admittedMeta = metric.Metadata{
		Name:        "admission.admitted.",
		Help:        "Number of requests admitted",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	erroredMeta = metric.Metadata{
		Name:        "admission.errored.",
		Help:        "Number of requests not admitted due to error",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	waitDurationsMeta = metric.Metadata{
		Name:        "admission.wait_durations.",
		Help:        "Wait time durations for requests that waited",
		Measurement: "Wait time Duration",
		Unit:        metric.Unit_NANOSECONDS,
		Category:    metric.Metadata_OVERLOAD,
		HowToUse:    "This is a latency histogram of wait time in the admission control queue. Non-zero wait times are expected when the corresponding resource is saturated.",
	}
	kvWaitDurationsMeta = metric.Metadata{
		Name:        "admission.wait_durations.",
		Help:        "Wait time durations for requests that waited",
		Measurement: "Wait time Duration",
		Unit:        metric.Unit_NANOSECONDS,
		Category:    metric.Metadata_OVERLOAD,
		HowToUse:    "This is a latency histogram of wait time in the CPU utilization-based admission control queue. Non-zero wait times are expected when CPU is saturated.",
	}
	kvStoresWaitDurationsMeta = metric.Metadata{
		Name:        "admission.wait_durations.",
		Help:        "Wait time durations for requests that waited",
		Measurement: "Wait time Duration",
		Unit:        metric.Unit_NANOSECONDS,
		Category:    metric.Metadata_OVERLOAD,
		HowToUse:    "This is a latency histogram of wait time in the I/O utilization-based admission control queue. Non-zero wait times are expected when I/O is saturated.",
	}
	waitQueueLengthMeta = metric.Metadata{
		Name:        "admission.wait_queue_length.",
		Help:        "Length of wait queue",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

func addName(name string, meta metric.Metadata) metric.Metadata {
	rv := meta
	rv.Name = rv.Name + name
	return rv
}

// WorkQueueMetrics are metrics associated with a WorkQueue. These can be
// shared across WorkQueues, so Gauges should only be updated using deltas
// instead of by setting values.
type WorkQueueMetrics struct {
	name       string
	total      *workQueueMetricsSingle
	byPriority syncutil.Map[admissionpb.WorkPriority, workQueueMetricsSingle]
	registry   *metric.Registry
}

// getOrCreate will return the metric if it exists or create it and then return
// it if it didn't previously exist.
// TODO(abaptist): Until https://github.com/cockroachdb/cockroach/issues/88846
// is fixed, this code is not useful since late registered metrics are not
// visible.
func (m *WorkQueueMetrics) getOrCreate(priority admissionpb.WorkPriority) *workQueueMetricsSingle {
	// Try loading from the map first.
	val, ok := m.byPriority.Load(priority)
	if !ok {
		// This will only happen the first time it is requested. Doing this lazily
		// prevents unnecessary creation of unused priorities. Note that it is
		// necessary to call LoadOrStore here as this could be called concurrently.
		// It is not called the first Load so that we don't have to unnecessarily
		// create the metrics.
		statPrefix := fmt.Sprintf("%v.%v", m.name, priority.String())
		val, ok = m.byPriority.LoadOrStore(priority, makeWorkQueueMetricsSingle(statPrefix, false /* essential */))
		if !ok {
			m.registry.AddMetricStruct(val)
		}
	}
	return val
}

type workQueueMetricsSingle struct {
	Requested       *metric.Counter
	Admitted        *metric.Counter
	Errored         *metric.Counter
	WaitDurations   metric.IHistogram
	WaitQueueLength *metric.Gauge
}

func (m *WorkQueueMetrics) incRequested(priority admissionpb.WorkPriority) {
	m.total.Requested.Inc(1)
	m.getOrCreate(priority).Requested.Inc(1)
}

func (m *WorkQueueMetrics) incAdmitted(priority admissionpb.WorkPriority) {
	m.total.Admitted.Inc(1)
	m.getOrCreate(priority).Admitted.Inc(1)
}

func (m *WorkQueueMetrics) incErrored(priority admissionpb.WorkPriority) {
	m.total.Errored.Inc(1)
	m.getOrCreate(priority).Errored.Inc(1)
}

func (m *WorkQueueMetrics) recordStartWait(priority admissionpb.WorkPriority) {
	m.total.WaitQueueLength.Inc(1)
	m.getOrCreate(priority).WaitQueueLength.Inc(1)
}

func (m *WorkQueueMetrics) recordFinishWait(priority admissionpb.WorkPriority, dur time.Duration) {
	m.total.WaitQueueLength.Dec(1)
	m.total.WaitDurations.RecordValue(dur.Nanoseconds())

	priorityStats := m.getOrCreate(priority)
	priorityStats.WaitQueueLength.Dec(1)
	priorityStats.WaitDurations.RecordValue(dur.Nanoseconds())
}

func (m *WorkQueueMetrics) recordBypassedAdmission(priority admissionpb.WorkPriority) {
	// For work that either bypasses admission queues (because of the nature of
	// the work itself or because certain queues are disabled), we'll explicit
	// record a zero wait duration so that the histogram percentiles remain
	// accurate.
	m.total.WaitDurations.RecordValue(0)
	priorityStats := m.getOrCreate(priority)
	priorityStats.WaitDurations.RecordValue(0)
}

func (m *WorkQueueMetrics) recordFastPathAdmission(priority admissionpb.WorkPriority) {
	// Explicitly record a zero wait queue duration when we're able to acquire
	// tokens/slots without needing to add ourselves to group heaps. Explicitly
	// recording zeros ensure that our histograms are accurate with respect to
	// all work going through admission control.
	m.total.WaitDurations.RecordValue(0)
	priorityStats := m.getOrCreate(priority)
	priorityStats.WaitDurations.RecordValue(0)
}

// MetricStruct implements the metric.Struct interface.
func (*WorkQueueMetrics) MetricStruct() {}

func makeWorkQueueMetrics(name string, registry *metric.Registry) *WorkQueueMetrics {
	totalMetric := makeWorkQueueMetricsSingle(name, true /* essential */)
	registry.AddMetricStruct(totalMetric)
	wqm := &WorkQueueMetrics{
		name:     name,
		total:    totalMetric,
		registry: registry,
	}
	for pri := range admissionpb.WorkPriorityDict {
		wqm.getOrCreate(pri)
	}

	return wqm
}

func makeWorkQueueMetricsSingle(name string, essential bool) *workQueueMetricsSingle {
	wdm := waitDurationsMeta
	if name == KVWork.String() {
		wdm = kvWaitDurationsMeta
	} else if name == fmt.Sprintf("%s-stores", KVWork.String()) {
		wdm = kvStoresWaitDurationsMeta
	}
	if essential {
		wdm.Visibility = metric.Metadata_ESSENTIAL
	}

	return &workQueueMetricsSingle{
		Requested: metric.NewCounter(addName(name, requestedMeta)),
		Admitted:  metric.NewCounter(addName(name, admittedMeta)),
		Errored:   metric.NewCounter(addName(name, erroredMeta)),
		WaitDurations: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     addName(name, wdm),
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
		WaitQueueLength: metric.NewGauge(addName(name, waitQueueLengthMeta)),
	}
}

// StoreWriteWorkInfo is the information that needs to be provided for work
// seeking admission from a StoreWorkQueue.
type StoreWriteWorkInfo struct {
	WorkInfo
}

// StoreWorkQueue is responsible for admission to a store.
type StoreWorkQueue struct {
	storeID roachpb.StoreID
	q       [admissionpb.NumWorkClasses]WorkQueue
	// Only calls storeReplicatedWorkAdmittedLocked. The rest of the interface is used by
	// WorkQueue.
	granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted
	coordMu  *syncutil.Mutex
	mu       struct {
		syncutil.RWMutex
		// estimates is used to determine how many tokens are deducted at-admit
		// time for each request. It's not used for replication admission
		// control (below-raft) where we do know the size of the write being
		// admitted.
		estimates storeRequestEstimates
		// stats are used to maintain L0 {write,ingest} linear models, modeling
		// the relation between accounted for "physical" {write,ingest} bytes
		// and observed L0 growth (which factors in state machine application).
		stats storeAdmissionStats
	}
	sequencersMu struct {
		syncutil.Mutex
		s map[roachpb.RangeID]*sequencer // cleaned up periodically
	}
	stopCh             chan struct{}
	timeSource         timeutil.TimeSource
	settings           *cluster.Settings
	onLogEntryAdmitted OnLogEntryAdmitted

	ioTokensBypassed *metric.Counter

	knobs *TestingKnobs
}

// StoreWorkHandle is returned by StoreWorkQueue.Admit, and contains state
// needed by the caller (see StoreWorkHandle.UseAdmittedWorkDone) and by
// StoreWorkQueue.AdmittedWorkDone.
type StoreWorkHandle struct {
	tenantID roachpb.TenantID
	// The writeTokens acquired by this request. Must be > 0.
	writeTokens         int64
	workClass           admissionpb.WorkClass
	useAdmittedWorkDone bool
}

// UseAdmittedWorkDone indicates whether we need to invoke
// StoreWorkQueue.AdmittedWorkDone. It's false if AC is disabled or if we're
// using below-raft admission control.
func (h StoreWorkHandle) UseAdmittedWorkDone() bool {
	return h.useAdmittedWorkDone
}

// Admit is called when requesting admission for store work. If err!=nil, the
// request was not admitted, potentially due to a deadline being exceeded. If
// err=nil and handle.UseAdmittedWorkDone() is true, AdmittedWorkDone must be
// called when the admitted work is done.
func (q *StoreWorkQueue) Admit(
	ctx context.Context, info StoreWriteWorkInfo,
) (handle StoreWorkHandle, err error) {
	wc := admissionpb.WorkClassFromPri(info.Priority)
	if info.RequestedCount == 0 {
		// We use a per-request estimate only when no requested count is
		// provided. It's always provided for below-raft admission where we know
		// the size of the work being admitted. For below-raft admission when
		// work is admitted[1], we first deduct the requested number of tokens.
		// This just corresponds to the known size of the write/ingest, but
		// could be insufficient since we haven't applied the granter's linear
		// models. This is accounted for in
		// StoreWorkQueue.admittedReplicatedWork(), which is invoked right after
		// admission. There is no risk of over-admission since this adjustment
		// is being done in the same goroutine that did the granting.
		//
		// [1]: This happens asynchronously -- i.e. we may have already returned
		//      from StoreWorkQueue.Admit().
		info.RequestedCount = func() int64 {
			q.mu.RLock()
			defer q.mu.RUnlock()
			return q.mu.estimates.writeTokens
		}()
	}
	if info.ReplicatedWorkInfo.Enabled {
		info.CreateTime = q.sequenceReplicatedWork(info.CreateTime, info.ReplicatedWorkInfo)
	}

	resp, err := q.q[wc].Admit(ctx, info.WorkInfo)
	if err != nil {
		return StoreWorkHandle{}, err
	}

	h := StoreWorkHandle{
		tenantID:            info.TenantID,
		workClass:           wc,
		writeTokens:         info.RequestedCount,
		useAdmittedWorkDone: resp.Enabled,
	}
	if !info.ReplicatedWorkInfo.Enabled {
		return h, nil
	}

	h.useAdmittedWorkDone = false
	var storeWorkDoneInfo StoreWorkDoneInfo
	if info.ReplicatedWorkInfo.Ingested {
		storeWorkDoneInfo.IngestedBytes = info.RequestedCount
	} else {
		storeWorkDoneInfo.WriteBytes = info.RequestedCount
	}

	// Update store admission stats, because the write is happening ~this
	// point. These statistics are used to maintain the underlying linear
	// models (modeling relation between physical log writes and total L0
	// growth, which includes the state machine application).
	q.updateStoreStatsAfterWorkDone(1, storeWorkDoneInfo, false, false)
	return h, nil
}

// StoreWorkDoneInfo provides information about the work size after the work
// is done. This allows for correction of estimates made at admission time.
type StoreWorkDoneInfo struct {
	// The size of the Pebble write-batch, for normal writes. It is zero when
	// the write-batch is empty, which happens when all the bytes are being
	// added via sstable ingestion. NB: it is possible for both WriteBytes and
	// IngestedBytes to be 0 if nothing was actually written.
	WriteBytes int64
	// The size of the sstables, for ingests. Zero if there were no ingests.
	IngestedBytes int64
}

// storeReplicatedWorkAdmittedInfo provides information about the size of
// replicated work once it's admitted (which happens asynchronously from the
// work itself). This lets us use the underlying linear models for L0
// {writes,ingests} to deduct an appropriate number of tokens from the granter,
// for the admitted work size.
//
// TODO(irfansharif): This post-admission adjustment of tokens is odd -- when
// the replicated work is being enqueued, we already know its size, so we could
// have applied the linear models upfront and determine what the right # of
// tokens to deduct all at once. We're doing it this way because we've written
// the WorkQueue and granter interactions to be very general, but it can be hard
// to follow. See review discussions over at #97599. It's worth noting that
// there isn't really a lag in the adjustment, so it is harmless from an
// operational perspective of admission control.
type storeReplicatedWorkAdmittedInfo StoreWorkDoneInfo

type onAdmittedReplicatedWork interface {
	admittedReplicatedWork(
		tenantID roachpb.TenantID,
		pri admissionpb.WorkPriority,
		rwi ReplicatedWorkInfo,
		requestedTokens int64,
		createTime int64,
		coordMuLocked bool,
	)

	// TODO(irfansharif): This coordMuLocked parameter is gross.
}

var _ onAdmittedReplicatedWork = &StoreWorkQueue{}

// admittedReplicatedWork indicates to the queue that replicated write work was
// admitted.
func (q *StoreWorkQueue) admittedReplicatedWork(
	tenantID roachpb.TenantID,
	pri admissionpb.WorkPriority,
	rwi ReplicatedWorkInfo,
	originalTokens int64,
	createTime int64, // only used in tests
	coordMuLocked bool,
) {
	if !rwi.Enabled {
		panic("unexpected call to admittedReplicatedWork for work that's not a replicated write")
	}
	if fn := q.knobs.AdmittedReplicatedWorkInterceptor; fn != nil {
		fn(tenantID, pri, rwi, originalTokens, createTime)
	}

	var replicatedWorkAdmittedInfo storeReplicatedWorkAdmittedInfo
	if rwi.Ingested {
		replicatedWorkAdmittedInfo.IngestedBytes = originalTokens
	} else {
		replicatedWorkAdmittedInfo.WriteBytes = originalTokens
	}

	// We've already used RequestedCount for replicated writes to deduct tokens
	// in the granter. RequestedCount corresponded to the size of the
	// write/ingest, which we knew when enqueuing the write in the WorkQueue for
	// (asynchronous) admission. That token deduction however did not use the
	// underlying linear models, and we may have under-deducted -- we account
	// for this below.
	wc := admissionpb.WorkClassFromPri(pri)
	if !coordMuLocked {
		q.coordMu.Lock()
	}
	additionalTokensNeeded := q.granters[wc].storeReplicatedWorkAdmittedLocked(originalTokens, replicatedWorkAdmittedInfo)
	if !coordMuLocked {
		q.coordMu.Unlock()
	}
	q.q[wc].adjustGroupUsed(tenantID, additionalTokensNeeded)

	// Inform callers of the entry we just admitted.
	//
	// TODO(irfansharif): It's bad that we're extending coord.mu's critical
	// section to this callback. We can't prevent it when this is happening via
	// WorkQueue.granted since it was called while holding coord.mu. We should
	// revisit -- one possibility is to add this to a notification queue and
	// have a separate goroutine invoke these callbacks (without holding
	// coord.mu). We could directly invoke here too if not holding the lock.
	cbState := LogEntryAdmittedCallbackState{
		StoreID:    q.storeID,
		RangeID:    rwi.RangeID,
		ReplicaID:  rwi.ReplicaID,
		LeaderTerm: rwi.LeaderTerm,
		Pos:        rwi.LogPosition,
		Pri:        pri,
		RaftPri:    rwi.RaftPri,
	}
	q.onLogEntryAdmitted.AdmittedLogEntry(q.q[wc].ambientCtx, cbState)
}

// OnLogEntryAdmitted is used to observe the specific entries that were
// admitted. Since admission control for log entries is
// asynchronous/non-blocking, this allows callers to do requisite
// post-admission bookkeeping.
type OnLogEntryAdmitted interface {
	AdmittedLogEntry(ctx context.Context, cbState LogEntryAdmittedCallbackState)
}

// LogEntryAdmittedCallbackState is passed to AdmittedLogEntry.
type LogEntryAdmittedCallbackState struct {
	// Store on which the entry was admitted.
	StoreID roachpb.StoreID
	// Range that contained that entry.
	RangeID roachpb.RangeID
	// Replica that asked for admission.
	ReplicaID roachpb.ReplicaID
	// LeaderTerm is the term of the leader that asked for this entry to be
	// appended.
	LeaderTerm uint64
	// Pos is the position of the entry in the log.
	//
	// TODO(sumeer): when the RACv1 protocol is deleted, drop the Term from this
	// struct, and replace LeaderTerm/Pos.Index with a LogMark.
	Pos LogPosition
	// Pri is the admission priority used for admission.
	Pri admissionpb.WorkPriority
	// RaftPri is only populated for replication admission control v2 (RACv2).
	// It is the raft priority for the entry. Technically, it could be derived
	// from Pri, but we do not want the admission package to be aware of this
	// translation.
	RaftPri raftpb.Priority
}

// AdmittedWorkDone indicates to the queue that the admitted work has completed.
// It's used for the legacy above-raft admission control where we Admit()
// upfront, with just an estimate of the write size, and after the write is
// done, invoke AdmittedWorkDone with the now-known size.
func (q *StoreWorkQueue) AdmittedWorkDone(h StoreWorkHandle, doneInfo StoreWorkDoneInfo) error {
	if !h.UseAdmittedWorkDone() {
		return nil // nothing to do
	}
	q.updateStoreStatsAfterWorkDone(1, doneInfo, false, true)
	additionalTokens := q.granters[h.workClass].storeWriteDone(h.writeTokens, doneInfo)
	q.q[h.workClass].adjustGroupUsed(h.tenantID, additionalTokens)
	return nil
}

// BypassedWorkDone is called for follower writes, so that admission control
// can (a) adjust remaining tokens, (b) account for this in the per-work token
// estimation model.
func (q *StoreWorkQueue) BypassedWorkDone(workCount int64, doneInfo StoreWorkDoneInfo) {
	q.updateStoreStatsAfterWorkDone(uint64(workCount), doneInfo, true, false)
	// Since we have no control over such work, we choose to count it as
	// regularWorkClass.
	additionalTokensTaken := q.granters[admissionpb.RegularWorkClass].storeWriteDone(0 /* originalTokens */, doneInfo)
	q.ioTokensBypassed.Inc(additionalTokensTaken)
}

// StatsToIgnore is called for range snapshot ingestion -- see the comment in
// storeAdmissionStats.
func (q *StoreWorkQueue) StatsToIgnore(ingestStats pebble.IngestOperationStats, writeBytes uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.stats.statsToIgnore.ingestStats.Bytes += ingestStats.Bytes
	q.mu.stats.statsToIgnore.ingestStats.ApproxIngestedIntoL0Bytes += ingestStats.ApproxIngestedIntoL0Bytes
	q.mu.stats.statsToIgnore.writeBytes += writeBytes
}

func (q *StoreWorkQueue) updateStoreStatsAfterWorkDone(
	workCount uint64, doneInfo StoreWorkDoneInfo, bypassed bool, aboveRaft bool,
) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.stats.workCount += workCount
	q.mu.stats.writeAccountedBytes += uint64(doneInfo.WriteBytes)
	q.mu.stats.ingestedAccountedBytes += uint64(doneInfo.IngestedBytes)
	if bypassed {
		q.mu.stats.aux.bypassedCount += workCount
		q.mu.stats.aux.writeBypassedAccountedBytes += uint64(doneInfo.WriteBytes)
		q.mu.stats.aux.ingestedBypassedAccountedBytes += uint64(doneInfo.IngestedBytes)
	}
	if aboveRaft {
		q.mu.stats.aboveRaftStats.workCount += workCount
		q.mu.stats.aboveRaftStats.writeAccountedBytes += uint64(doneInfo.WriteBytes)
		q.mu.stats.aboveRaftStats.ingestedAccountedBytes += uint64(doneInfo.IngestedBytes)
	}
}

// SetTenantWeights passes through to WorkQueue.SetTenantWeights.
func (q *StoreWorkQueue) SetTenantWeights(groupWeights map[uint64]uint32) {
	for i := range q.q {
		q.q[i].SetTenantWeights(groupWeights)
	}
}

// getRequesters implements storeRequester.
func (q *StoreWorkQueue) getRequesters() [admissionpb.NumWorkClasses]requester {
	var result [admissionpb.NumWorkClasses]requester
	for i := range q.q {
		result[i] = &q.q[i]
	}
	return result
}

func (q *StoreWorkQueue) close() {
	for i := range q.q {
		q.q[i].close()
	}
	close(q.stopCh)
}

func (q *StoreWorkQueue) getStoreAdmissionStats() storeAdmissionStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mu.stats
}

func (q *StoreWorkQueue) setStoreRequestEstimates(estimates storeRequestEstimates) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.estimates = estimates
}

func makeStoreWorkQueue(
	ambientCtx log.AmbientContext,
	storeID roachpb.StoreID,
	granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
	settings *cluster.Settings,
	metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics,
	opts workQueueOptions,
	knobs *TestingKnobs,
	onLogEntryAdmitted OnLogEntryAdmitted,
	ioTokensBypassedMetric *metric.Counter,
	coordMu *syncutil.Mutex,
) storeRequester {
	if knobs == nil {
		knobs = &TestingKnobs{}
	}
	if opts.timeSource == nil {
		opts.timeSource = timeutil.DefaultTimeSource{}
	}
	q := &StoreWorkQueue{
		coordMu:            coordMu,
		storeID:            storeID,
		granters:           granters,
		knobs:              knobs,
		stopCh:             make(chan struct{}),
		timeSource:         opts.timeSource,
		settings:           settings,
		onLogEntryAdmitted: onLogEntryAdmitted,
		ioTokensBypassed:   ioTokensBypassedMetric,
	}

	opts.usesAsyncAdmit = true
	for i := range q.q {
		var queueKind QueueKind
		if i == int(admissionpb.RegularWorkClass) {
			queueKind = "kv-regular-store-queue"
		} else if i == int(admissionpb.ElasticWorkClass) {
			queueKind = "kv-elastic-store-queue"
		}
		initWorkQueue(&q.q[i], ambientCtx, KVWork, queueKind, granters[i], settings, metrics[i], opts, knobs)
		q.q[i].onAdmittedReplicatedWork = q
	}
	// Arbitrary initial value. This will be replaced before any meaningful
	// token constraints are enforced.
	q.mu.estimates = storeRequestEstimates{
		writeTokens: 1,
	}

	q.sequencersMu.s = make(map[roachpb.RangeID]*sequencer)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-ticker.C:
				q.gcSequencers()
			case <-q.stopCh:
				return
			}
		}
	}()
	return q
}

func (q *StoreWorkQueue) gcSequencers() {
	q.sequencersMu.Lock()
	defer q.sequencersMu.Unlock()

	for rangeID, seq := range q.sequencersMu.s {
		maxCreateTime := timeutil.FromUnixNanos(atomic.LoadInt64(&seq.maxCreateTime))
		if q.timeSource.Now().Sub(maxCreateTime) > rangeSequencerGCThreshold.Get(&q.settings.SV) {
			delete(q.sequencersMu.s, rangeID)
		}
	}
}

func (q *StoreWorkQueue) sequenceReplicatedWork(createTime int64, info ReplicatedWorkInfo) int64 {
	seq := func() *sequencer {
		q.sequencersMu.Lock()
		defer q.sequencersMu.Unlock()
		sqr, ok := q.sequencersMu.s[info.RangeID]
		if !ok {
			sqr = &sequencer{}
			q.sequencersMu.s[info.RangeID] = sqr
		}
		return sqr
	}()
	// We're assuming sequenceReplicatedWork is never invoked concurrently for a
	// given RangeID.
	return seq.sequence(createTime)
}
