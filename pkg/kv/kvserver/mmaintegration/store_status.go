// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// StoreStatusIntegration manages the periodic updates of store status from
// StorePool to MMA. It translates the outside world's (liveness, membership,
// draining, throttled, suspect) signals into MMA's (health, disposition) model.
//
// Design doc: https://gist.github.com/tbg/48e52790c9e9f50046583a94dbed856f
type StoreStatusIntegration struct {
	storePool *storepool.StorePool
	mma       mmaprototype.Allocator
}

// NewStoreStatusIntegration creates a new StoreStatusIntegration.
func NewStoreStatusIntegration(
	sp *storepool.StorePool, mma mmaprototype.Allocator,
) *StoreStatusIntegration {
	return &StoreStatusIntegration{
		storePool: sp,
		mma:       mma,
	}
}

// Start launches a background goroutine that periodically queries StorePool
// and updates MMA with the current store status.
func (s *StoreStatusIntegration) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "mma-store-status-updater", func(ctx context.Context) {
		// Update status every 10 seconds, matching the gossip interval.
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.updateAllStoreStatus(ctx)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// updateAllStoreStatus queries StorePool for all known stores and updates MMA
// with the translated status.
func (s *StoreStatusIntegration) updateAllStoreStatus(ctx context.Context) {
	for storeID := range s.mma.KnownStores() {
		status, err := s.ComputeMMAStatus(storeID)
		if err != nil {
			// Log at a low level - this is expected for stores that are not yet
			// known to StorePool but are known to MMA.
			log.VEventf(ctx, 2, "failed to compute MMA status for store %d: %v", storeID, err)
			continue
		}
		s.mma.UpdateStoreStatus(storeID, status)
	}
}

// ComputeMMAStatus computes the MMA status for a store by querying StorePool
// and translating its status to MMA's (health, disposition) model.
//
// Translation table (from design doc):
//
//	| StorePool Status    | MMA Health      | MMA Lease Disposition | MMA Replica Disposition |
//	|---------------------|-----------------|-----------------------|-------------------------|
//	| Dead                | HealthDead      | Shedding              | Shedding                |
//	| Unknown             | HealthUnknown   | Refusing              | Refusing                |
//	| Decommissioning     | HealthOK        | Shedding              | Shedding                |
//	| Draining            | HealthOK        | Shedding              | Refusing (keep, no add) |
//	| Throttled           | HealthOK        | OK                    | Refusing (temp)         |
//	| Suspect             | HealthUnhealthy | Shedding              | Shedding                |
//	| Available           | HealthOK        | OK                    | OK                      |
func (s *StoreStatusIntegration) ComputeMMAStatus(
	storeID roachpb.StoreID,
) (mmaprototype.Status, error) {
	spStatus, err := s.storePool.GetStoreStatus(storeID)
	if err != nil {
		return mmaprototype.Status{}, err
	}
	return TranslateStorePoolStatusToMMA(spStatus), nil
}

// TranslateStorePoolStatusToMMA translates a StorePool status to MMA's (health,
// disposition) model. This is a pure function that implements the translation
// table from the design doc.
func TranslateStorePoolStatusToMMA(spStatus storepool.StoreStatus) mmaprototype.Status {
	switch spStatus {
	case storepool.StoreStatusDead:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthDead,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusUnknown:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnknown,
			mmaprototype.LeaseDispositionRefusing,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusDecommissioning:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusDraining:
		// Draining stores should shed leases but keep existing replicas.
		// We use Refusing (not OK) to prevent adding NEW replicas - that would be
		// wasted work since the node is going away. But we don't use Shedding
		// because moving replicas is slow and unnecessary for a temporary drain.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusThrottled:
		// Throttled stores can hold leases but shouldn't receive new replicas.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionOK,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusSuspect:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnhealthy,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusAvailable:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionOK,
			mmaprototype.ReplicaDispositionOK,
		)
	default:
		// Unknown status - treat as unavailable.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnknown,
			mmaprototype.LeaseDispositionRefusing,
			mmaprototype.ReplicaDispositionRefusing,
		)
	}
}

