// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/mmaintegration"
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
		spStatus := TranslateAsimStatusToStorePoolStatus(storeStatus.Liveness, nodeStatus)
		// Reuse the production translation logic.
		mma.UpdateStoreStatus(storeID, mmaintegration.TranslateStorePoolStatusToMMA(spStatus))
	}
}

// TranslateAsimStatusToStorePoolStatus translates asim's store/node status to
// storepool.StoreStatus. This allows us to reuse the production
// TranslateStorePoolStatusToMMA logic, ensuring asim behavior matches
// production as closely as possible.
//
// The translation table from asim state to StorePool status:
//
//	| Asim State                              | StorePool Status        |
//	|-----------------------------------------|-------------------------|
//	| Decommissioning                         | StoreStatusDecommissioning |
//	| Decommissioned                          | StoreStatusDecommissioning |
//	| Draining                                | StoreStatusDraining     |
//	| LivenessDead                            | StoreStatusDead         |
//	| LivenessUnavailable                     | StoreStatusSuspect      |
//	| LivenessLive (active, not draining)     | StoreStatusAvailable    |
//
// Note: asim doesn't simulate throttling, so StoreStatusThrottled is never
// returned. StoreStatusUnknown is also not used since asim always has
// explicit liveness state.
func TranslateAsimStatusToStorePoolStatus(
	storeLiveness state.LivenessState, nodeStatus state.NodeStatus,
) storepool.StoreStatus {
	// Handle membership first - decommissioning/decommissioned takes priority.
	// Note: In production, decommissioned nodes eventually become dead, but in
	// asim we treat both as decommissioning to match the StorePool behavior
	// during the decommissioning process.
	switch nodeStatus.Membership {
	case livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED:
		return storepool.StoreStatusDecommissioning
	}

	// Handle draining.
	if nodeStatus.Draining {
		return storepool.StoreStatusDraining
	}

	// Handle store liveness.
	switch storeLiveness {
	case state.LivenessDead:
		return storepool.StoreStatusDead
	case state.LivenessUnavailable:
		// Unavailable maps to Suspect - the store recently failed its liveness
		// heartbeat but hasn't been down long enough to be considered dead.
		return storepool.StoreStatusSuspect
	default: // LivenessLive
		return storepool.StoreStatusAvailable
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
	spStatus := TranslateAsimStatusToStorePoolStatus(storeStatus.Liveness, nodeStatus)
	return mmaintegration.TranslateStorePoolStatusToMMA(spStatus), true
}
