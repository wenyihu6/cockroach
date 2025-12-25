// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RefreshMMAStoreStatus queries the simulator state for all stores known to MMA
// and updates MMA with the translated status. This should be called before
// making allocation decisions to ensure MMA has fresh store health and
// disposition information.
//
// This mirrors the kvserver mmaintegration.RefreshStoreStatus function, but
// translates from asim's state instead of StorePool.
func RefreshMMAStoreStatus(s state.State, mma mmaprototype.Allocator) {
	for storeID := range mma.KnownStores() {
		store, ok := s.Store(state.StoreID(storeID))
		if !ok {
			// Store not found in simulator state - this can happen if MMA knows
			// about a store that hasn't been added to the simulation yet.
			continue
		}
		storeStatus := s.StoreStatus(state.StoreID(storeID))
		nodeStatus := s.NodeStatus(store.NodeID())
		mma.UpdateStoreStatus(storeID, TranslateAsimStatusToMMA(storeStatus.Liveness, nodeStatus))
	}
}

// TranslateAsimStatusToMMA translates asim's store/node status to MMA's (health,
// disposition) model. This translation mirrors the behavior of the production
// kvserver mmaintegration.TranslateStorePoolStatusToMMA function.
//
// The translation table (matches production behavior):
//
//	| Asim State                              | MMA Health      | MMA Lease Disp | MMA Replica Disp |
//	|-----------------------------------------|-----------------|----------------|------------------|
//	| Decommissioning/Decommissioned          | HealthOK        | Shedding       | Shedding         |
//	| Draining                                | HealthOK        | Shedding       | Refusing         |
//	| LivenessDead                            | HealthDead      | Shedding       | Shedding         |
//	| LivenessUnavailable (suspect)           | HealthUnhealthy | Shedding       | Shedding         |
//	| LivenessLive (active, not draining)     | HealthOK        | OK             | OK               |
//
// Note: asim doesn't simulate throttling or unknown states, so those production
// cases are not represented here.
func TranslateAsimStatusToMMA(
	storeLiveness state.LivenessState, nodeStatus state.NodeStatus,
) mmaprototype.Status {
	// Handle membership first - decommissioning/decommissioned takes priority.
	// Matches production: StoreStatusDecommissioning → HealthOK + Shedding both.
	switch nodeStatus.Membership {
	case livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	}

	// Handle draining - sheds leases but refuses new replicas (keeps existing).
	// Matches production: StoreStatusDraining → HealthOK + Shedding/Refusing.
	if nodeStatus.Draining {
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
		)
	}

	// Handle store liveness.
	switch storeLiveness {
	case state.LivenessDead:
		// Matches production: StoreStatusDead → HealthDead + Shedding both.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthDead,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case state.LivenessUnavailable:
		// Unavailable maps to suspect behavior - recently failed heartbeat.
		// Matches production: StoreStatusSuspect → HealthUnhealthy + Shedding both.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnhealthy,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	default: // LivenessLive
		// Matches production: StoreStatusAvailable → HealthOK + OK both.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionOK,
			mmaprototype.ReplicaDispositionOK,
		)
	}
}

// TranslateAsimStatusToMMAForStore is a convenience function that looks up the
// store and node status from the state and translates to MMA status.
func TranslateAsimStatusToMMAForStore(
	s state.State, storeID roachpb.StoreID,
) (mmaprototype.Status, bool) {
	store, ok := s.Store(state.StoreID(storeID))
	if !ok {
		return mmaprototype.Status{}, false
	}
	storeStatus := s.StoreStatus(state.StoreID(storeID))
	nodeStatus := s.NodeStatus(store.NodeID())
	return TranslateAsimStatusToMMA(storeStatus.Liveness, nodeStatus), true
}
