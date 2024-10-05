// Code generated by execgen; DO NOT EDIT.
// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

func newConcatWindowAggAlloc(allocator *colmem.Allocator, allocSize int64) aggregateFuncAlloc {
	return &concatWindowAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type concatWindowAgg struct {
	unorderedAggregateFuncBase
	// curAgg holds the running total.
	curAgg []byte
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

func (a *concatWindowAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	oldCurAggSize := len(a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bytes(), vec.Nulls()
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = nulls.NullAt(i)
			if !isNull {
				a.curAgg = append(a.curAgg, col.Get(i)...)
				a.foundNonNullForCurrentGroup = true
			}
		}
	} else {
		for i := startIdx; i < endIdx; i++ {

			var isNull bool
			isNull = false
			if !isNull {
				a.curAgg = append(a.curAgg, col.Get(i)...)
				a.foundNonNullForCurrentGroup = true
			}
		}
	}
	newCurAggSize := len(a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *concatWindowAgg) Flush(outputIdx int) {
	col := a.vec.Bytes()
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
	// Release the reference to curAgg eagerly.
	a.allocator.AdjustMemoryUsage(-int64(len(a.curAgg)))
	a.curAgg = nil
}

func (a *concatWindowAgg) Reset() {
	a.curAgg = nil
	a.foundNonNullForCurrentGroup = false
}

type concatWindowAggAlloc struct {
	aggAllocBase
	aggFuncs []concatWindowAgg
}

var _ aggregateFuncAlloc = &concatWindowAggAlloc{}

const sizeOfConcatWindowAgg = int64(unsafe.Sizeof(concatWindowAgg{}))
const concatWindowAggSliceOverhead = int64(unsafe.Sizeof([]concatWindowAgg{}))

func (a *concatWindowAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(concatWindowAggSliceOverhead + sizeOfConcatWindowAgg*a.allocSize)
		a.aggFuncs = make([]concatWindowAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}
