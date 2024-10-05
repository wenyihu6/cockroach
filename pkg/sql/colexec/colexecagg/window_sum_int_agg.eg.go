// Code generated by execgen; DO NOT EDIT.
// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ tree.AggType
	_ apd.Context
	_ duration.Duration
	_ = typeconv.TypeFamilyToCanonicalTypeFamily
)

func newSumIntWindowAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch t.Family() {
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return &sumIntInt16WindowAggAlloc{aggAllocBase: allocBase}, nil
		case 32:
			return &sumIntInt32WindowAggAlloc{aggAllocBase: allocBase}, nil
		case -1:
		default:
			return &sumIntInt64WindowAggAlloc{aggAllocBase: allocBase}, nil
		}
	}
	return nil, errors.Errorf("unsupported sum agg type %s", t.Name())
}

type sumIntInt16WindowAgg struct {
	unorderedAggregateFuncBase
	// curAgg holds the running total, so we can index into the slice once per
	// group, instead of on each iteration.
	curAgg int64
	// numNonNull tracks the number of non-null values we have seen for the group
	// that is currently being aggregated.
	numNonNull uint64
}

var _ AggregateFunc = &sumIntInt16WindowAgg{}

func (a *sumIntInt16WindowAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int16(), vec.Nulls()
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *sumIntInt16WindowAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// null.
	col := a.vec.Int64()
	if a.numNonNull == 0 {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
}

func (a *sumIntInt16WindowAgg) Reset() {
	a.curAgg = zeroInt64Value
	a.numNonNull = 0
}

type sumIntInt16WindowAggAlloc struct {
	aggAllocBase
	aggFuncs []sumIntInt16WindowAgg
}

var _ aggregateFuncAlloc = &sumIntInt16WindowAggAlloc{}

const sizeOfSumIntInt16WindowAgg = int64(unsafe.Sizeof(sumIntInt16WindowAgg{}))
const sumIntInt16WindowAggSliceOverhead = int64(unsafe.Sizeof([]sumIntInt16WindowAgg{}))

func (a *sumIntInt16WindowAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sumIntInt16WindowAggSliceOverhead + sizeOfSumIntInt16WindowAgg*a.allocSize)
		a.aggFuncs = make([]sumIntInt16WindowAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go).
func (a *sumIntInt16WindowAgg) Remove(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int16(), vec.Nulls()
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

type sumIntInt32WindowAgg struct {
	unorderedAggregateFuncBase
	// curAgg holds the running total, so we can index into the slice once per
	// group, instead of on each iteration.
	curAgg int64
	// numNonNull tracks the number of non-null values we have seen for the group
	// that is currently being aggregated.
	numNonNull uint64
}

var _ AggregateFunc = &sumIntInt32WindowAgg{}

func (a *sumIntInt32WindowAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int32(), vec.Nulls()
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *sumIntInt32WindowAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// null.
	col := a.vec.Int64()
	if a.numNonNull == 0 {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
}

func (a *sumIntInt32WindowAgg) Reset() {
	a.curAgg = zeroInt64Value
	a.numNonNull = 0
}

type sumIntInt32WindowAggAlloc struct {
	aggAllocBase
	aggFuncs []sumIntInt32WindowAgg
}

var _ aggregateFuncAlloc = &sumIntInt32WindowAggAlloc{}

const sizeOfSumIntInt32WindowAgg = int64(unsafe.Sizeof(sumIntInt32WindowAgg{}))
const sumIntInt32WindowAggSliceOverhead = int64(unsafe.Sizeof([]sumIntInt32WindowAgg{}))

func (a *sumIntInt32WindowAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sumIntInt32WindowAggSliceOverhead + sizeOfSumIntInt32WindowAgg*a.allocSize)
		a.aggFuncs = make([]sumIntInt32WindowAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go).
func (a *sumIntInt32WindowAgg) Remove(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int32(), vec.Nulls()
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

type sumIntInt64WindowAgg struct {
	unorderedAggregateFuncBase
	// curAgg holds the running total, so we can index into the slice once per
	// group, instead of on each iteration.
	curAgg int64
	// numNonNull tracks the number of non-null values we have seen for the group
	// that is currently being aggregated.
	numNonNull uint64
}

var _ AggregateFunc = &sumIntInt64WindowAgg{}

func (a *sumIntInt64WindowAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int64(), vec.Nulls()
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) + int64(v)
					if (result < int64(a.curAgg)) != (int64(v) < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull++
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *sumIntInt64WindowAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// null.
	col := a.vec.Int64()
	if a.numNonNull == 0 {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
}

func (a *sumIntInt64WindowAgg) Reset() {
	a.curAgg = zeroInt64Value
	a.numNonNull = 0
}

type sumIntInt64WindowAggAlloc struct {
	aggAllocBase
	aggFuncs []sumIntInt64WindowAgg
}

var _ aggregateFuncAlloc = &sumIntInt64WindowAggAlloc{}

const sizeOfSumIntInt64WindowAgg = int64(unsafe.Sizeof(sumIntInt64WindowAgg{}))
const sumIntInt64WindowAggSliceOverhead = int64(unsafe.Sizeof([]sumIntInt64WindowAgg{}))

func (a *sumIntInt64WindowAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sumIntInt64WindowAggSliceOverhead + sizeOfSumIntInt64WindowAgg*a.allocSize)
		a.aggFuncs = make([]sumIntInt64WindowAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go).
func (a *sumIntInt64WindowAgg) Remove(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	var oldCurAggSize uintptr
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Int64(), vec.Nulls()
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				//gcassert:bce
				v := col.Get(i)

				{
					result := int64(a.curAgg) - int64(v)
					if (result < int64(a.curAgg)) != (int64(v) > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					a.curAgg = result
				}

				a.numNonNull--
			}
		}
	}
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}
