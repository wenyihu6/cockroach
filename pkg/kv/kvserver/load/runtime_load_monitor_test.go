// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCumulativeDelta(t *testing.T) {
	// Normal increase: delta = 150 - 100 = 50, last advances.
	delta, last := cumulativeDelta(150, 100)
	require.Equal(t, int64(50), delta)
	require.Equal(t, int64(150), last)

	// No change: delta = 0, last unchanged.
	delta, last = cumulativeDelta(150, 150)
	require.Equal(t, int64(0), delta)
	require.Equal(t, int64(150), last)

	// Decrease (anomaly): delta clamped to 0, last preserved.
	delta, last = cumulativeDelta(80, 150)
	require.Equal(t, int64(0), delta)
	require.Equal(t, int64(150), last)

	// Recovery after clamping: delta computed from the preserved last.
	delta, last = cumulativeDelta(200, last) // last is still 150
	require.Equal(t, int64(50), delta)
	require.Equal(t, int64(200), last)

	// First sample (last = 0): full value is the delta.
	delta, last = cumulativeDelta(500, 0)
	require.Equal(t, int64(500), delta)
	require.Equal(t, int64(500), last)
}
