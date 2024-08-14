// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type unbufferedRegistration struct {
	baseRegistration
	// Input.
	metrics *Metrics

	// Output.
	stream BufferedStream

	mu struct {
		sync.Locker
		// Once set, cannot unset.
		catchUpOverflowed bool
		// Nil if catch up scan has done (either success or unsuccess). In the case
		// of unsuccess, disconnected flag is set. Safe to send to underlying stream
		// if catchUpBuf is nil and disconnected is false. After catch up buffer is done,
		catchUpBuf chan *sharedEvent
		// Fine to repeated cancel context.
		catchUpScanCancelFn func()
		// Once set, cannot unset.
		disconnected bool
		catchUpIter  *CatchUpIterator
		caughtUp     bool
	}
}

var _ registration = (*unbufferedRegistration)(nil)

func (ubr *unbufferedRegistration) setDisconnectedIfNot() {
	//TODO implement me
	panic("implement me")
}

func (ubr *unbufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	//TODO implement me
	panic("implement me")
}

func (ubr *unbufferedRegistration) disconnect(pErr *kvpb.Error) {
	//TODO implement me
	panic("implement me")
}

func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	//TODO implement me
	panic("implement me")
}

func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (ubr *unbufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
