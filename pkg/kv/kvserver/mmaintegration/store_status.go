// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RefreshStoreStatus queries StorePool for all stores known to MMA and updates
// MMA with the translated status. This should be called before making allocation
// decisions to ensure MMA has fresh store health and disposition information.
//
// Design doc: https://gist.github.com/tbg/48e52790c9e9f50046583a94dbed856f
//
// We chose an on-demand approach (refresh before decision) over periodic background
// updates because:
//  1. Always-fresh data at decision time
//  2. No background goroutine to manage
//  3. No wasted updates when no decisions are being made
//  4. Matches StorePool's pattern of computing status on-demand
func RefreshStoreStatus(
	ctx context.Context,
	sp *storepool.StorePool,
	mma mmaprototype.Allocator,
) {
	for storeID := range mma.KnownStores() {
		spStatus, err := sp.GetStoreStatus(storeID)
		if err != nil {
			// Log at a low level - this is expected for stores that are not yet
			// known to StorePool but are known to MMA.
			log.VEventf(ctx, 2, "failed to get store status for s%d: %v", storeID, err)
			continue
		}
		mma.UpdateStoreStatus(storeID, TranslateStorePoolStatusToMMA(spStatus))
	}
}

// TranslateStorePoolStatusToMMA translates a StorePool status to MMA's (health,
// disposition) model. This is a pure function that implements the translation
// table from the design doc:
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

