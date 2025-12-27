// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
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
	_ context.Context,
	sp *storepool.StorePool,
	mma mmaprototype.Allocator,
) {
	// Use batch method to compute shared values (clock, settings) once.
	// Stores not found in StorePool are silently skipped.
	statuses := sp.GetStoreStatuses(mma.KnownStores())
	for storeID, spStatus := range statuses {
		mma.UpdateStoreStatus(storeID, TranslateStorePoolStatusToMMA(spStatus))
	}
}

// TranslateStorePoolStatusToMMA translates a StorePool status to MMA's (health,
// disposition) model. This is a pure function that implements the translation
// table from the design doc:
//
//	| StorePool Status    | MMA Health      | MMA Lease Disposition | MMA Replica Disposition | SMA Behavior Reference                          |
//	|---------------------|-----------------|-----------------------|-------------------------|-------------------------------------------------|
//	| Dead                | HealthDead      | Shedding              | Shedding                | Replicas marked dead, triggers replacement      |
//	| Unknown             | HealthUnknown   | Refusing              | Refusing                | No-op: neither live nor dead (store_pool:934)   |
//	| Decommissioning     | HealthOK        | Shedding              | Shedding                | Triggers ReplaceDecommissioning (allocator:1120)|
//	| Draining            | HealthOK        | Shedding              | Refusing (keep, no add) | Lease shed active (store:1874), no replica move |
//	| Throttled           | HealthOK        | OK                    | Refusing (temp)         | Filtered for replicas only (store_pool:1231)    |
//	| Suspect             | HealthUnhealthy | Shedding              | Refusing                | Filtered as target (allocator:2481), no removal |
//	| Available           | HealthOK        | OK                    | OK                      | Normal allocation target                        |
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
		// Suspect stores were recently unavailable. SMA excludes them as lease
		// targets (allocator.go:2481, 2881) so we shed leases. For replicas,
		// SMA just filters them out (store_pool.go:1245) without actively removing,
		// so we use Refusing (don't add new, but don't actively remove existing).
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnhealthy,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
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
