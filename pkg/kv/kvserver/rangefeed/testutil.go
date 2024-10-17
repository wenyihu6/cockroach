// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import "github.com/cockroachdb/cockroach/pkg/util/log"

func NewTestProcessor(id int64) Processor {
	if id > 0 {
		return &ScheduledProcessor{
			scheduler: ClientScheduler{id: id},
		}
	}
	log.Fatalf("invalid processor id: %d", id)
	return nil
}
