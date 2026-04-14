// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Serverless per-tier utilization goals.
var KVCPUTimeAppUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.app_tenant",
	"the target CPU utilization for app tenant work if using the KV CPU time "+
		"token system, value is in the interval [0,1] where 1 means all cores",
	0.8,
	settings.FloatWithMinimum(minTargetUtilFrac))

var KVCPUTimeSystemUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.system_tenant",
	"the target CPU utilization for system tenant work if using the KV CPU "+
		"time token system, value is in the interval [0,1] where 1 means all cores",
	0.95,
	settings.FloatWithMinimum(minTargetUtilFrac))

// Resource Manager single utilization goal.
var KVCPUTimeUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util",
	"the target CPU utilization for work if using the KV CPU time "+
		"token system, value is in the interval [0,1] where 1 means all cores",
	0.75,
	settings.FloatWithMinimum(minTargetUtilFrac))

// Burstable work is given this much CPU headroom above non-burstable. See
// resetInterval for more.
var KVCPUTimeUtilBurstDelta = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.burst_delta",
	"the delta between non-burstable & burstable CPU utilization target if "+
		"using the KV CPU time token system (value is in the interval [0,1] "+
		"where 1 means all cores)",
	0.05,
	settings.PositiveFloat)

const (
	// See the extensive comments near isLowCPUUtil declaration for info
	// regarding this constant.
	lowCPUUtilFrac = 0.25
	// minTargetUtilFrac is the lowest that the admission.cpu_time_tokens.target_util
	// settings can be set to. < 50% CPU utilization is not a cost-effective choice,
	// as it leads lots of hardware resources unused, even in case of short spikes.
	minTargetUtilFrac = lowCPUUtilFrac + 0.25
)

// timePerTick is how frequently cpuTimeTokenFiller ticks its time.Ticker & adds
// tokens to the buckets. Must be < 1s. Must divide 1s evenly.
const timePerTick = 1 * time.Millisecond

// cpuTimeTokenFiller starts a goroutine which periodically calls
// cpuTimeTokenAllocator to add tokens to a cpuTimeTokenGranter. For example,
// on an 8 vCPU machine, we may want to allow burstable tier-0 work to use 6
// seconds of CPU time per second. Then the refill rates for tier0 burstable
// work would equal 6 seconds per second, and cpuTimeTokenFiller would add 6
// seconds of token every second, but smoothly -- 1ms at a time. See
// cpuTimeTokenGranter for details on the token buckets; the TLDR is there is
// one bucket per <resource tier, burst qualification> pair.
//
// cpuTimeTokenFiller owns the time.Ticker logic. The details of the token
// allocation are left to the cpuTimeTokenAllocator, in order to improve
// clarity & testability.
//
// Note that the combination of cpuTimeTokenFiller & cpuTimeTokenAllocator are
// written to be robust against delayed and dropped time.Timer ticks. That is,
// in the presence of delayed and dropped ticks, the correct number of tokens
// will be added to the buckets; they just may be added in a less smooth fashion
// than normal.
//
// The mechanism by which the goroutine adds the correct number of tokens, in
// the presence of delayed or dropped ticks, is:
//   - time is split into intervals of 1s
//   - intervals are split into 1s / timePerTick(=1ms) time.Ticker ticks
//   - cpuTimeTokenAllocator attempts to allocate remaining tokens for interval
//     evenly across remaining ticks in the interval
//   - once interval is complete, all remaining tokens needed for that interval
//     are added (e.g. see t.allocateTokens(1) below), then a new interval
//     starts
type cpuTimeTokenFiller struct {
	allocator  cpuTimeTokenAllocatorI
	timeSource timeutil.TimeSource
	closeCh    chan struct{}
	// activeMode is the cpuTimeTokenMode after the most recent
	// resetInterval. Written by the filler goroutine, read by
	// GetKVWorkQueue via cpuTimeTokenGrantCoordinator. This ensures
	// routing and bucket configuration change together at interval
	// boundaries.
	activeMode atomic.Int64
	// Used only in unit tests.
	tickCh *chan struct{}
}

func (f *cpuTimeTokenFiller) start(ctx context.Context) {
	// The token buckets should start full. The first call to resetInterval will
	// fill the buckets.
	f.activeMode.Store(int64(f.allocator.resetInterval(ctx)))

	ticker := f.timeSource.NewTicker(timePerTick)
	intervalStart := f.timeSource.Now()
	go func() {
		lastRemainingTicks := int64(time.Second / timePerTick)
		for {
			select {
			case t := <-ticker.Ch():
				var remainingTicks int64
				elapsedSinceIntervalStart := t.Sub(intervalStart)
				if elapsedSinceIntervalStart >= time.Second {
					if lastRemainingTicks > 1 {
						f.allocator.allocateTokens(1)
					}
					intervalStart = t
					f.activeMode.Store(
						int64(f.allocator.resetInterval(ctx)))
					remainingTicks = int64(time.Second / timePerTick)
				} else {
					remainingSinceIntervalStart := time.Second - elapsedSinceIntervalStart
					if remainingSinceIntervalStart <= 0 {
						panic(errors.AssertionFailedf(
							"remainingSinceIntervalStart %d is <= 0",
							remainingSinceIntervalStart))
					}
					remainingTicks =
						int64((remainingSinceIntervalStart + timePerTick - 1) / timePerTick)
				}
				f.allocator.allocateTokens(max(1, remainingTicks))
				lastRemainingTicks = remainingTicks
				// Only non-nil in unit tests.
				if f.tickCh != nil {
					*f.tickCh <- struct{}{}
				}
			case <-f.closeCh:
				return
			}
		}
	}()
}

func (f *cpuTimeTokenFiller) close() {
	close(f.closeCh)
}

// cpuTimeTokenAllocatorI abstracts cpuTimeTokenAllocator for testing.
type cpuTimeTokenAllocatorI interface {
	allocateTokens(expectedRemainingTicksInInterval int64)
	resetInterval(context.Context) cpuTimeTokenMode
}

var _ cpuTimeTokenAllocatorI = &cpuTimeTokenAllocator{}

// cpuTimeTokenAllocator allocates tokens to a cpuTimeTokenGranter.
// See the comment above cpuTimeTokenFiller for a high level picture.
// The allocator gradually distributes tokens every interval, while
// respecting bucket capacities. The computation of the rate of tokens
// to add every interval is left to cpuTimeModel.
//
// Mode-specific behavior (target computation and burst bucket refill)
// is delegated to a modeStrategy, which is swapped on mode transitions.
type cpuTimeTokenAllocator struct {
	granter  *cpuTimeTokenGranter
	queues   [numResourceTiers]workQueueIForAllocator
	settings *cluster.Settings
	model    cpuTimeModel
	metrics  *cpuTimeTokenMetrics
	strategy modeStrategy
	// refillRates stores the number of CPU time tokens to add to each
	// bucket per interval (1s).
	refillRates rates
	// allocated stores the number of tokens added to each bucket in the
	// current interval. No mutex, since only a single goroutine will
	// call the allocator.
	allocated tokenCounts
}

// modeStrategy encapsulates the mode-specific behavior of the
// cpuTimeTokenAllocator. The two implementations are
// serverlessStrategy and rmStrategy.
type modeStrategy interface {
	mode() cpuTimeTokenMode
	// computeTargets reads mode-specific cluster settings and returns
	// the target utilizations for model fitting.
	computeTargets(sv *settings.Values, burstDelta float64) targetUtilizations
	// refillBurst refills per-tenant burst buckets. tokens is the
	// per-tick allocation (from allocateTokens) or per-interval delta
	// (from resetInterval). refillRates is the current refill rates.
	refillBurst(tokens tokenCounts, refillRates rates)
}

// serverlessStrategy implements modeStrategy for Serverless mode,
// which uses 2 WorkQueues (systemTenant, appTenant) with per-tier
// utilization targets. Each tier's burst bucket gets noBurst/4 of
// that tier's allocation and rate.
type serverlessStrategy struct {
	queues [numResourceTiers]workQueueIForAllocator
}

func (s *serverlessStrategy) mode() cpuTimeTokenMode {
	return serverlessMode
}

func (s *serverlessStrategy) computeTargets(
	sv *settings.Values, burstDelta float64,
) targetUtilizations {
	if numResourceTiers != 2 || numBurstQualifications != 2 {
		panic(fmt.Sprintf(
			"computeTargets requires numResourceTiers=2 and "+
				"numBurstQualifications=2 but got %d, %d",
			numResourceTiers, numBurstQualifications))
	}
	var targets targetUtilizations
	appTarget := KVCPUTimeAppUtilGoal.Get(sv)
	targets[appTenant][noBurst] = appTarget
	targets[appTenant][canBurst] = appTarget + burstDelta
	systemTarget := KVCPUTimeSystemUtilGoal.Get(sv)
	targets[systemTenant][noBurst] = systemTarget
	targets[systemTenant][canBurst] = systemTarget + burstDelta
	return targets
}

func (s *serverlessStrategy) refillBurst(tokens tokenCounts, refillRates rates) {
	for tier := range s.queues {
		toAdd := tokens[tier][noBurst] / 4
		burstCapacity := refillRates[tier][noBurst] / 4
		s.queues[tier].refillBurstBuckets(toAdd, burstCapacity)
	}
}

// rmStrategy implements modeStrategy for Resource Manager mode, which
// uses a single WorkQueue with N resource groups and a single
// utilization target. Burst bucket refill normalizes to 100% CPU by
// dividing out canBurstTarget, then pre-scales per-group amounts
// by that group's CPU_MIN fraction before calling
// refillBurstBucketForGroup.
type rmStrategy struct {
	queue workQueueIForAllocator
	// canBurstTarget stores the canBurst utilization target (e.g., 1.0
	// when KVCPUTimeUtilGoal=0.75 + KVCPUTimeUtilBurstDelta=0.25).
	// Updated by computeTargets every interval.
	canBurstTarget float64
	// groupBurstFracs maps resource group ID to its CPU_MIN fraction
	// for burst bucket refill scaling. For example, foreground=1.0
	// (gets 100% of the 100% CPU rate) and background=0.25 (gets 25%).
	groupBurstFracs map[uint64]float64
}

func (s *rmStrategy) mode() cpuTimeTokenMode {
	return resourceManagerMode
}

func (s *rmStrategy) computeTargets(sv *settings.Values, burstDelta float64) targetUtilizations {
	var targets targetUtilizations
	noBurstTarget := KVCPUTimeUtilGoal.Get(sv)
	targets[0][noBurst] = noBurstTarget
	targets[0][canBurst] = noBurstTarget + burstDelta
	s.canBurstTarget = targets[0][canBurst]
	// Mirror tier-0 targets to tier-1 so all array slots have valid
	// values. Tier-1 sits idle in RM mode (no work is routed to it)
	// but receives the same refill rates and token deductions as
	// tier-0. This is harmless: tryGrantLocked skips tier-1 because
	// its requester has no waiting work, and the symmetric deductions
	// keep both tiers' token counts in lockstep, preserving the
	// granter's bucket ordering invariants without special-casing.
	targets[1] = targets[0]
	return targets
}

func (s *rmStrategy) refillBurst(tokens tokenCounts, refillRates rates) {
	if s.canBurstTarget <= 0 {
		return
	}
	// Normalize to the 100% CPU rate by dividing out canBurstTarget.
	rate100 := float64(tokens[0][canBurst]) / s.canBurstTarget
	cap100 := float64(refillRates[0][canBurst]) / s.canBurstTarget
	// Pre-scale per-group and call refillBurstBucketForGroup directly.
	for rgID, frac := range s.groupBurstFracs {
		s.queue.refillBurstBucketForGroup(
			rgID, int64(rate100*frac), int64(cap100*frac))
	}
}

// rates stores a token count per second, for example, the refill
// rates at which we add tokens per second, one per bucket in
// cpuTimeTokenGranter.
type rates [numResourceTiers][numBurstQualifications]int64

func (r rates) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r rates) SafeFormat(s redact.SafePrinter, _ rune) {
	s.SafeRune('[')
	first := true
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
			if !first {
				s.SafeRune(' ')
			}
			first = false
			s.Printf("%s-%s=%s",
				tier, qual, redact.Safe(time.Duration(r[tier][qual])))
		}
	}
	s.SafeRune(']')
}

// capacities stores the maximum number of tokens that can be in the
// buckets, one per bucket in cpuTimeTokenGranter.
type capacities [numResourceTiers][numBurstQualifications]int64

// minimums stores the minimum number of tokens that can be in the
// buckets, one per bucket in cpuTimeTokenGranter.
type minimums [numResourceTiers][numBurstQualifications]int64

// tokenCounts stores unit-less token counts, one per bucket in
// cpuTimeTokenGranter.
type tokenCounts [numResourceTiers][numBurstQualifications]int64

// targetUtilizations stores a target CPU utilization, as a float64 (so
// 0.8 for 80% CPU utilization), one per bucket in cpuTimeTokenGranter.
// This is aggregate CPU usage, so 0.8 means 80% of CPU time across all
// cores.
type targetUtilizations [numResourceTiers][numBurstQualifications]float64

// computeMinimums computes per-bucket minimums from refill rates. These
// minimums prevent higher priority work from putting lower priority buckets
// into unbounded token debt.
//
// The top priority bucket (tier0/canBurst) has a floor of 0. Each subsequent
// bucket's floor is its rate minus the top priority rate, which is always
// negative.
func computeMinimums(r rates) minimums {
	var m minimums
	topRate := r[0][0]
	for tier := range r {
		for qual := range r[tier] {
			m[tier][qual] = r[tier][qual] - topRate
		}
	}
	return m
}

// allocateTokensFn distributes refillRates across remaining ticks in
// the interval, returning the per-tick allocations. This is the shared
// allocation logic used by both serverlessAllocator and rmAllocator.
func allocateTokensFn(refillRates rates, allocated *tokenCounts, remainingTicks int64) tokenCounts {
	var allocations tokenCounts
	for tier := range refillRates {
		for qual := range refillRates[tier] {
			remainingTokens := refillRates[tier][qual] - allocated[tier][qual]
			toAllocate :=
				(remainingTokens + remainingTicks - 1) / remainingTicks
			if toAllocate < 0 {
				panic(errors.AssertionFailedf(
					"toAllocate is negative %d", toAllocate))
			}
			if toAllocate+allocated[tier][qual] > refillRates[tier][qual] {
				toAllocate = refillRates[tier][qual] - allocated[tier][qual]
			}
			allocated[tier][qual] += toAllocate
			allocations[tier][qual] = toAllocate
		}
	}
	return allocations
}

// refillGranter increments per-bucket refill metrics, then delegates
// to granter.refill. Positive toAdd values are tracked as tokens
// added; negative values (which occur when refill rates decrease
// between intervals) are tracked as tokens removed.
func refillGranter(
	granter *cpuTimeTokenGranter,
	metrics *cpuTimeTokenMetrics,
	toAdd tokenCounts,
	bucketCapacities capacities,
	bucketMinimums minimums,
	updateMetrics bool,
) {
	for tier := range toAdd {
		for qual := range toAdd[tier] {
			idx := perBucketIdx(
				resourceTier(tier), burstQualification(qual))
			if v := toAdd[tier][qual]; v > 0 {
				metrics.RefillAdded[idx].Inc(v)
			} else if v < 0 {
				metrics.RefillRemoved[idx].Inc(-v)
			}
		}
	}
	granter.refill(toAdd, bucketCapacities, bucketMinimums, updateMetrics)
}

// allocateTokens distributes tokens evenly across remaining ticks in
// the interval, then delegates burst bucket refill to the strategy.
func (a *cpuTimeTokenAllocator) allocateTokens(expectedRemainingTicksInInterval int64) {
	allocations := allocateTokensFn(
		a.refillRates, &a.allocated, expectedRemainingTicksInInterval)
	bucketCapacities := capacities(a.refillRates)
	bucketMinimums := computeMinimums(a.refillRates)
	refillGranter(a.granter, a.metrics, allocations,
		bucketCapacities, bucketMinimums, false /* updateMetrics */)
	a.strategy.refillBurst(allocations, a.refillRates)
}

// resetInterval recomputes refill rates and applies the delta to the
// granter and burst buckets. If the mode cluster setting has changed,
// the strategy is swapped before computing targets.
func (a *cpuTimeTokenAllocator) resetInterval(ctx context.Context) cpuTimeTokenMode {
	// Check for mode transition.
	newMode := cpuTimeTokenMode(
		KVCPUTimeTokenACMode.Get(&a.settings.SV))
	if newMode != a.strategy.mode() {
		a.strategy = a.newStrategy(newMode)
	}

	burstDelta := KVCPUTimeUtilBurstDelta.Get(&a.settings.SV)
	targets := a.strategy.computeTargets(&a.settings.SV, burstDelta)
	newRefillRates := a.model.fit(ctx, targets)

	var deltaRefillRates tokenCounts
	for tier := range newRefillRates {
		for qual := range newRefillRates[tier] {
			deltaRefillRates[tier][qual] =
				newRefillRates[tier][qual] - a.refillRates[tier][qual]
		}
	}
	bucketCapacities := capacities(newRefillRates)
	bucketMinimums := computeMinimums(newRefillRates)
	refillGranter(a.granter, a.metrics, deltaRefillRates,
		bucketCapacities, bucketMinimums, true /* updateMetrics */)
	a.refillRates = newRefillRates

	a.strategy.refillBurst(deltaRefillRates, a.refillRates)
	a.allocated = tokenCounts{}
	return a.strategy.mode()
}

// newStrategy constructs the modeStrategy for the given mode and
// configures the queue's fullyUtilize and priority-based group settings.
//
// No explicit bucket reset is needed on mode switch. The granter's
// tier-1 buckets stay alive across transitions, and the delta
// mechanism in resetInterval (deltaRefillRates) adjusts all bucket
// token counts to converge to the new mode's rates within one
// interval (1s). In-flight work in queue[1] during a serverless-to-RM
// switch may stall (no new refill routed there), but will time out
// via the WorkQueue's normal deadline handling.
func (a *cpuTimeTokenAllocator) newStrategy(mode cpuTimeTokenMode) modeStrategy {
	switch mode {
	case serverlessMode:
		a.queues[0].setDefaultFullyUtilize(false)
		a.queues[0].setPriorityBasedGroups(false)
		a.queues[0].SetFullyUtilizeGroups(nil)
		return &serverlessStrategy{queues: a.queues}
	case resourceManagerMode:
		a.queues[0].setDefaultFullyUtilize(true)
		a.queues[0].setPriorityBasedGroups(true)
		a.queues[0].SetFullyUtilizeGroups(map[uint64]bool{
			foregroundResourceGroupID: true,
			backgroundResourceGroupID: false,
		})
		return &rmStrategy{
			queue: a.queues[0],
			groupBurstFracs: map[uint64]float64{
				foregroundResourceGroupID: 1.0,
				backgroundResourceGroupID: 0.25,
			},
		}
	default:
		panic(fmt.Sprintf("unknown cpuTimeTokenMode: %d", mode))
	}
}

// workQueueIForAllocator abstracts the burst bucket refill methods in
// WorkQueue, to enable unit testing.
type workQueueIForAllocator interface {
	refillBurstBuckets(toAdd int64, capacity int64)
	refillBurstBucketForGroup(rgID uint64, toAdd int64, capacity int64)
	setDefaultFullyUtilize(fullyUtilize bool)
	setPriorityBasedGroups(enabled bool)
	SetFullyUtilizeGroups(groups map[uint64]bool)
}

// cpuTimeModel abstracts cpuTimeLinearModel for testing.
type cpuTimeModel interface {
	fit(ctx context.Context, targets targetUtilizations) rates
}

var _ cpuTimeModel = &cpuTimeTokenLinearModel{}

// cpuTimeTokenLinearModel computes the number of CPU time tokens to add
// to each bucket in the cpuTimeTokenGranter, per interval (per 1s).
//
// The refill rate is chosen such that the rate at which tokens are added
// results in an (actual measured) CPU utilization matching the target
// utilization. See the detailed comments in fit() for the multiplier
// computation and smoothing.
type cpuTimeTokenLinearModel struct {
	granter            tokenUsageTracker
	cpuMetricsProvider CPUMetricsProvider
	timeSource         timeutil.TimeSource
	metrics            *cpuTimeTokenMetrics

	// True after first call to fit.
	init bool
	// The time that fit was called last.
	lastFitTime time.Time
	// The cumulative user/sys CPU time used since process start.
	totalCPUTime time.Duration
	// The linear correction term, see the docs above cpuTimeTokenLinearModel.
	tokenToCPUTimeMultiplier float64

	logger fitLogger
}

// tokenUsageTracker is implemented by cpuTimeTokenGranter. It provides
// information regarding the net token deduction since the last call to
// resetTokensUsedInInterval. This information is needed to model the
// relationship between token usage and actual CPU usage.
type tokenUsageTracker interface {
	// resetTokensUsedInInterval resets the tracked used tokens to zero.
	// The previous value is returned.
	resetTokensUsedInInterval() int64
}

var _ tokenUsageTracker = &cpuTimeTokenGranter{}

type CPUMetricsProvider interface {
	// GetCPUUsage returns the cumulative user/sys CPU time used since
	// process start.
	GetCPUUsage() (totalCPUTime time.Duration, err error)
	// GetCPUCapacity returns the cpuCapacity measured in vCPUs.
	GetCPUCapacity() (cpuCapacity float64)
}

// fit adjusts tokenToCPUTimeMultiplier based on CPU usage & token usage.
// fit computes refill rates from tokenToCPUTimeMultiplier and the targets
// parameter. targets tracks a target CPU utilization for all active
// buckets in the cpuTimeTokenGranter. fit returns the refill rates.
func (m *cpuTimeTokenLinearModel) fit(ctx context.Context, targets targetUtilizations) rates {
	if !m.init {
		m.init = true
		m.lastFitTime = m.timeSource.Now()
		totalCPUTime, err := m.cpuMetricsProvider.GetCPUUsage()
		if err != nil {
			log.Dev.Fatalf(ctx,
				"GetCPUUsage returned %q in cpuTimeTokenLinearModel.fit init", err)
		}
		m.totalCPUTime = totalCPUTime
		m.tokenToCPUTimeMultiplier = 1
		return m.computeRefillRates(
			targets, m.tokenToCPUTimeMultiplier,
			m.cpuMetricsProvider.GetCPUCapacity())
	}

	cpuCapacity := m.cpuMetricsProvider.GetCPUCapacity()
	totalCPUTime, err := m.cpuMetricsProvider.GetCPUUsage()
	if err != nil {
		log.Dev.Fatalf(ctx,
			"GetCPUUsage returned %q in cpuTimeTokenLinearModel.fit", err)
	}

	intCPUTime := totalCPUTime - m.totalCPUTime
	if intCPUTime < 0 {
		intCPUTime = 0
	}
	m.totalCPUTime = totalCPUTime

	now := m.timeSource.Now()
	elapsedSinceLastFit := now.Sub(m.lastFitTime)
	m.lastFitTime = now

	tokensUsed := m.granter.resetTokensUsedInInterval()
	if tokensUsed <= 0 {
		tokensUsed = 1
	}

	// Update multiplier.
	isLowCPUUtil := int64(intCPUTime) < int64(
		float64(elapsedSinceLastFit.Nanoseconds())*cpuCapacity*lowCPUUtilFrac)
	if isLowCPUUtil {
		// Use the smallest target util across all tiers to compute
		// the upper bound.
		smallestTargetUtil := math.MaxFloat64
		for tier := range targets {
			for qual := range targets[tier] {
				if targets[tier][qual] < smallestTargetUtil {
					smallestTargetUtil = targets[tier][qual]
				}
			}
		}
		upperBound := smallestTargetUtil / lowCPUUtilFrac
		if mult := m.tokenToCPUTimeMultiplier; mult > upperBound {
			m.tokenToCPUTimeMultiplier = max(mult/1.5, upperBound)
		}
	} else {
		tokenToCPUTimeMultiplier :=
			float64(intCPUTime) / float64(tokensUsed)
		if tokenToCPUTimeMultiplier > 20 {
			tokenToCPUTimeMultiplier = 20
		} else if tokenToCPUTimeMultiplier < 1 {
			tokenToCPUTimeMultiplier = 1
		}
		alpha := 0.5
		if tokenToCPUTimeMultiplier < m.tokenToCPUTimeMultiplier {
			alpha = 0.8
		}
		m.tokenToCPUTimeMultiplier =
			alpha*tokenToCPUTimeMultiplier +
				(1-alpha)*m.tokenToCPUTimeMultiplier
	}

	refillRates := m.computeRefillRates(
		targets, m.tokenToCPUTimeMultiplier, cpuCapacity)

	m.metrics.Multiplier.Update(m.tokenToCPUTimeMultiplier)
	if msg, shouldLog := m.logger.accumulate(
		m.tokenToCPUTimeMultiplier, isLowCPUUtil,
		intCPUTime, tokensUsed,
		elapsedSinceLastFit, cpuCapacity,
	); shouldLog {
		log.Dev.Infof(ctx, "%s", msg)
	}

	return refillRates
}

// computeRefillRates is a pure helper function that computes refill rates.
func (*cpuTimeTokenLinearModel) computeRefillRates(
	targets targetUtilizations, tokenToCPUTimeMultiplier float64, cpuCapacity float64,
) rates {
	var refillRates rates
	for tier := range targets {
		for qual := range targets[tier] {
			refillRates[tier][qual] = int64(
				cpuCapacity * float64(time.Second) *
					targets[tier][qual] / tokenToCPUTimeMultiplier)
		}
	}
	return refillRates
}
