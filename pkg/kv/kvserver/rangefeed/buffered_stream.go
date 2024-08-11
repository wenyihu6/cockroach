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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type BufferedStreamSender struct {
	ServerStreamSender

	taskCancel context.CancelFunc
	wg         sync.WaitGroup

	errCh   chan error
	queueMu struct {
		syncutil.Mutex
		capacity int64
		buffer   muxEventQueue
		overflow bool
	}
}

func NewBufferedStreamSender(wrapped ServerStreamSender) *BufferedStreamSender {
	l := &BufferedStreamSender{
		ServerStreamSender: wrapped,
	}
	l.queueMu.capacity = defaultEventChanCap
	return l
}

var _ ServerStreamSender = (*BufferedStreamSender)(nil)

func (bs *BufferedServerStreamSenderAdapter) SendBuffered(
	*kvpb.MuxRangeFeedEvent, *SharedBudgetAllocation,
) error {
	log.Fatalf(context.Background(), "unimplemented: buffered stream sender")
	return nil
}
