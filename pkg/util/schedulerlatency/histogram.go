// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import (
	"math"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// newSchedulerLatencyHistogram creates a standard CRDB histogram for tracking
// Go scheduler latency. The histogram properly handles both cumulative data
// (for Prometheus export) and windowed data (for TSDB percentile calculations).
func newSchedulerLatencyHistogram(
	metadata metric.Metadata, buckets []float64, windowDuration time.Duration,
) metric.IHistogram {
	// Remove -Inf from buckets if present - prometheus histograms don't allow it.
	if len(buckets) > 0 && math.IsInf(buckets[0], -1) {
		buckets = buckets[1:]
	}

	// Convert bucket boundaries from seconds to nanoseconds for CRDB histogram.
	nanosBuckets := make([]float64, len(buckets))
	for i, b := range buckets {
		if math.IsInf(b, 0) {
			nanosBuckets[i] = b
		} else {
			nanosBuckets[i] = b * float64(time.Second.Nanoseconds())
		}
	}

	return metric.NewHistogram(metric.HistogramOptions{
		Metadata: metadata,
		Duration: windowDuration,
		Buckets:  nanosBuckets,
		Mode:     metric.HistogramModePrometheus,
	})
}

// recordRuntimeHistogramDelta records a delta histogram from the Go runtime
// into a standard CRDB histogram. The delta histogram contains observations
// since the last sample, with counts per bucket. This function iterates through
// each bucket and calls RecordValue for each observation using the bucket's
// midpoint as the representative value.
//
// The targetBuckets parameter specifies the reduced bucket boundaries (a subset
// of the Go runtime's ~720 buckets) that we want to use. The delta histogram
// is re-bucketed to match these boundaries before recording.
func recordRuntimeHistogramDelta(
	h metric.IHistogram, delta *metrics.Float64Histogram, targetBuckets []float64,
) {
	// Remove -Inf from target buckets if present for consistency.
	if len(targetBuckets) > 0 && math.IsInf(targetBuckets[0], -1) {
		targetBuckets = targetBuckets[1:]
	}

	// Re-bucket the delta from Go's ~720 buckets to our reduced bucket set.
	// This merges counts from multiple Go buckets into each target bucket.
	rebucketedCounts := rebucketCounts(delta, targetBuckets)

	// Record each observation into the histogram using bucket midpoints.
	for i, count := range rebucketedCounts {
		if count == 0 {
			continue
		}

		// Calculate the midpoint of the bucket in nanoseconds.
		// For the last bucket (which ends at +Inf), use the lower bound.
		var midpointNanos int64
		lowerBound := targetBuckets[i]
		upperBound := targetBuckets[i+1]

		if math.IsInf(upperBound, 1) {
			// +Inf bucket: use lower bound as representative value
			midpointNanos = int64(lowerBound * float64(time.Second.Nanoseconds()))
		} else if math.IsInf(lowerBound, -1) {
			// -Inf bucket: use upper bound as representative value
			midpointNanos = int64(upperBound * float64(time.Second.Nanoseconds()))
		} else {
			// Normal bucket: use midpoint
			midpointSeconds := (lowerBound + upperBound) / 2
			midpointNanos = int64(midpointSeconds * float64(time.Second.Nanoseconds()))
		}

		// Record each observation. While this is O(count), it correctly
		// populates both the cumulative and windowed histograms maintained
		// by metric.Histogram.
		for j := uint64(0); j < count; j++ {
			h.RecordValue(midpointNanos)
		}
	}
}

// rebucketCounts takes a Go runtime histogram and re-buckets its counts into
// the target bucket boundaries. The target buckets must be a subset of the
// Go runtime buckets (i.e., each target bucket boundary must align with a
// Go runtime bucket boundary).
func rebucketCounts(delta *metrics.Float64Histogram, targetBuckets []float64) []uint64 {
	numTargetBuckets := len(targetBuckets) - 1
	rebucketed := make([]uint64, numTargetBuckets)

	deltaCounts := delta.Counts
	deltaBuckets := delta.Buckets

	// j tracks the current target bucket index
	var j int
	for i, count := range deltaCounts {
		if j >= numTargetBuckets {
			break
		}
		// Accumulate count into current target bucket
		rebucketed[j] += count
		// Move to next target bucket when we've reached its boundary
		if deltaBuckets[i+1] == targetBuckets[j+1] {
			j++
		}
	}

	return rebucketed
}

// reBucketExpAndTrim takes a list of bucket boundaries (lower bound inclusive)
// and down samples the buckets to those a multiple of base apart. The end
// result is a roughly exponential (in many cases, perfectly exponential)
// bucketing scheme. It also trims the bucket range to the specified min and max
// values -- everything outside the range is merged into (-Inf, ..] and [..,
// +Inf) buckets. The following example shows how it works, lifted from
// testdata/histogram_buckets.
//
//		rebucket base=10 min=0ns max=100000h
//		----
//		bucket[  0] width=0s                 boundary=[-Inf, 0s)
//	    bucket[  1] width=1ns                boundary=[0s, 1ns)
//	    bucket[  2] width=9ns                boundary=[1ns, 10ns)
//	    bucket[  3] width=90ns               boundary=[10ns, 100ns)
//	    bucket[  4] width=924ns              boundary=[100ns, 1.024µs)
//	    bucket[  5] width=9.216µs            boundary=[1.024µs, 10.24µs)
//	    bucket[  6] width=92.16µs            boundary=[10.24µs, 102.4µs)
//	    bucket[  7] width=946.176µs          boundary=[102.4µs, 1.048576ms)
//	    bucket[  8] width=9.437184ms         boundary=[1.048576ms, 10.48576ms)
func reBucketExpAndTrim(buckets []float64, base, min, max float64) []float64 {
	// Re-bucket as powers of the given base.
	b := reBucketExp(buckets, base)

	// Merge all buckets greater than the max value into the +Inf bucket.
	for i := range b {
		if i == 0 {
			continue
		}
		if b[i-1] <= max {
			continue
		}

		// We're looking at the boundary after the first time we've crossed the
		// max limit. Since we expect recordings near the max value, we don't
		// want that bucket to end at +Inf, so we merge the bucket after.
		b[i] = math.Inf(1)
		b = b[:i+1]
		break
	}

	// Merge all buckets less than the min value into the -Inf bucket.
	j := 0
	for i := range b {
		if b[i] > min {
			j = i
			break
		}
	}
	// b[j] > min and is the lower-bound of the j-th bucket. The min must be
	// contained in the (j-1)-th bucket. We want to merge 0th bucket
	// until the (j-2)-th one.
	if j <= 2 {
		// Nothing to do (we either have one or no buckets to merge together).
	} else {
		// We want trim the bucket list to start at (j-2)-th bucket, so just
		// have one bucket before the one containing the min.
		b = b[j-2:]
		// b[0] now refers the lower bound of what was previously the (j-2)-th
		// bucket. We make it start at -Inf.
		b[0] = math.Inf(-1)
	}

	return b
}

// reBucketExp is like reBucketExpAndTrim but without the trimming logic.
func reBucketExp(buckets []float64, base float64) []float64 {
	bucket := buckets[0]
	var newBuckets []float64
	// We may see -Inf here, in which case, add it and continue the rebucketing
	// scheme from the next one it since we risk producing NaNs otherwise. We
	// need to preserve -Inf values to maintain runtime/metrics conventions
	if bucket == math.Inf(-1) {
		newBuckets = append(newBuckets, bucket)
		buckets = buckets[1:]
		bucket = buckets[0]
	}

	// From now on, bucket should always have a non-Inf value because Infs are
	// only ever at the ends of the bucket lists, so arithmetic operations on it
	// are non-NaN.
	for i := 1; i < len(buckets); i++ {
		// bucket is the lower bound of the lowest bucket that has not been
		// added to newBuckets. We will add it to newBuckets, but we wait to add
		// it until we find the next bucket that is >= bucket*base.

		if bucket >= 0 && buckets[i] < bucket*base {
			// The next bucket we want to include is at least bucket*base.
			continue
		} else if bucket < 0 && buckets[i] < bucket/base {
			// In this case the bucket we're targeting is negative, and since
			// we're ascending through buckets here, we need to divide to get
			// closer to zero exponentially.
			continue
		}
		newBuckets = append(newBuckets, bucket)
		bucket = buckets[i]
	}

	// The +Inf bucket will always be the last one, and we'll always
	// end up including it here.
	return append(newBuckets, bucket)
}
