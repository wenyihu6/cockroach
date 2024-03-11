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
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// BenchmarkRangefeedBudget benchmarks the effect of enabling/disabling the
// processor budget. It sets up a single processor and registration, and
// processes a set of events.
func BenchmarkRandomizedRangefeedBudget(b *testing.B) {
	for _, numRegistration := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("with_budget"), func(b *testing.B) {
			b.ReportAllocs()
			runBenchmarkRandomizedRangefeed(b, benchmarkRangefeedOpts{
				numRegistrations: numRegistration,
				budget:           math.MaxInt64,
			})
		})
	}
}

func generateRandomizedTs(rand *rand.Rand) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(rand.Intn(100))}
}
func generateRandomizedRawBytes(rand *rand.Rand) []byte {
	const valSize = 16 << 10
	return randutil.RandBytes(rand, valSize)
}

func generateRandomizedValue(rand *rand.Rand) roachpb.Value {
	return roachpb.Value{
		RawBytes:  generateRandomizedRawBytes(rand),
		Timestamp: generateRandomizedTs(rand),
	}
}

func generateRandomizedBytes(rand *rand.Rand) []byte {
	const tableID = 42
	dataTypes := []*types.T{types.String, types.Int, types.Decimal, types.Bytes, types.Bool, types.Date, types.Timestamp, types.Float}
	randType := dataTypes[rand.Intn(len(dataTypes))]

	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		randgen.RandDatumSimple(rand, randType),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return key
}

func generateStartAndEndKey(rand *rand.Rand) (roachpb.Key, roachpb.Key) {
	start := rand.Intn(2 << 20)
	end := start + rand.Intn(2<<20)
	startDatum := tree.NewDInt(tree.DInt(start))
	endDatum := tree.NewDInt(tree.DInt(end))
	const tableID = 42

	startKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		startDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return startKey, endKey
}

func generateRandomizedSpan(rand *rand.Rand) roachpb.RSpan {
	startKey, endKey := generateStartAndEndKey(rand)
	return roachpb.RSpan{
		Key:    roachpb.RKey(startKey),
		EndKey: roachpb.RKey(endKey),
	}
}

func generateWriteValueAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	return p.ConsumeLogicalOps(ctx, makeLogicalOp(&enginepb.MVCCWriteValueOp{
		Key:       generateRandomizedBytes(rand),
		Timestamp: generateRandomizedTs(rand),
		Value:     generateRandomizedBytes(rand),
	}))
}

func generateWriteIntentOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	txnID := generateRandomizedTxnId(rand)
	key := generateRandomizedBytes(rand)
	ts := generateRandomizedTs(rand)
	return p.ConsumeLogicalOps(ctx, writeIntentOpWithKey(txnID, key, isolation.Serializable, ts))
}

func generateRandomizedTxnId(rand *rand.Rand) uuid.UUID {
	var txnID uuid.UUID
	n := rand.Intn(100)
	txnID.DeterministicV4(uint64(rand.Intn(n)), uint64(n))
	return txnID
}

func generateCommitIntentOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	txnID := generateRandomizedTxnId(rand)
	key := generateRandomizedBytes(rand)
	ts := generateRandomizedTs(rand)
	value := generateRandomizedBytes(rand)
	return p.ConsumeLogicalOps(ctx, commitIntentOpWithKV(txnID, key, ts, value, false /* omitInRangefeeds */))
}

func generateAbortIntentOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	txnID := generateRandomizedTxnId(rand)
	return p.ConsumeLogicalOps(ctx, abortIntentOp(txnID))
}

func generateUpdateIntentOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	txnID := generateRandomizedTxnId(rand)
	ts := generateRandomizedTs(rand)
	return p.ConsumeLogicalOps(ctx, updateIntentOp(txnID, ts))
}

func generateAbortTxnOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	txnID := generateRandomizedTxnId(rand)
	return p.ConsumeLogicalOps(ctx, abortTxnOp(txnID))
}

func generateDeleteRangeOpAndConsume(ctx context.Context, rand *rand.Rand, p Processor) bool {
	startKey, endKey := generateStartAndEndKey(rand)
	ts := generateRandomizedTs(rand)
	return p.ConsumeLogicalOps(ctx, deleteRangeOp(startKey, endKey, ts))
}

func generateLogicalOpEvent(ctx context.Context, rand *rand.Rand, p Processor) bool {
	typesOfOps := []string{
		"write_value", "delete_range", "write_intent", "update_intent", "commit_intent", "abort_intent", "abort_txn"}
	randomlyChosenTypeOfOp := typesOfOps[rand.Intn(len(typesOfOps))]
	switch randomlyChosenTypeOfOp {
	case "write_value":
		return generateWriteValueAndConsume(ctx, rand, p)
	case "delete_range":
		return generateDeleteRangeOpAndConsume(ctx, rand, p)
	case "write_intent":
		return generateWriteIntentOpAndConsume(ctx, rand, p)
	case "update_intent":
		return generateUpdateIntentOpAndConsume(ctx, rand, p)
	case "commit_intent":
		return generateCommitIntentOpAndConsume(ctx, rand, p)
	case "abort_intent":
		return generateAbortIntentOpAndConsume(ctx, rand, p)
	case "abort_txn":
		return generateAbortTxnOpAndConsume(ctx, rand, p)
	}
	return true
}

func generateCtEvent(ctx context.Context, rand *rand.Rand, p Processor) bool {
	ts := generateRandomizedTs(rand)
	return p.ForwardClosedTS(ctx, ts)
}

func generateSSTTable(ctx context.Context, rand *rand.Rand, p Processor) bool {
	sst := generateRandomizedRawBytes(rand)
	sstSpan := generateRandomizedSpan(rand).AsRawSpanWithNoLocals()
	writeTS := generateRandomizedTs(rand)
	return p.ConsumeSSTable(ctx, sst, sstSpan, writeTS)
}

func generateRandomizedEventAndSend(ctx context.Context, rand *rand.Rand, p Processor) bool {
	typesOfEvents := []string{"logicalsOps", "ct", "initRTS", "sst", "sync"}
	randomlyChosenEvent := typesOfEvents[rand.Intn(len(typesOfEvents))]
	switch randomlyChosenEvent {
	case "logicalsOps":
		return generateLogicalOpEvent(ctx, rand, p)
	case "ct":
		return generateCtEvent(ctx, rand, p)
	case "sst":
		return generateSSTTable(ctx, rand, p)
	}
	return true
}

// runBenchmarkRangefeed runs a rangefeed benchmark, emitting b.N events across
// a rangefeed.
func runBenchmarkRandomizedRangefeed(b *testing.B, opts benchmarkRangefeedOpts) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var budget *FeedBudget
	if opts.budget > 0 {
		budget = newTestBudget(opts.budget)
	}
	rand, _ := randutil.NewTestRand()
	span := generateRandomizedSpan(rand)
	p, h, stopper := newTestProcessor(b, withSpan(span), withBudget(budget), withChanCap(b.N),
		withEventTimeout(time.Hour))
	defer stopper.Stop(ctx)

	// Add registrations.
	streams := make([]*noopStream, opts.numRegistrations)
	futures := make([]*future.ErrorFuture, opts.numRegistrations)
	for i := 0; i < opts.numRegistrations; i++ {
		// withDiff does not matter for these benchmarks, since the previous value
		// is fetched and populated during Raft application.
		const withDiff = false
		// withFiltering does not matter for these benchmarks because doesn't fetch
		// extra data.
		const withFiltering = false
		streams[i] = &noopStream{ctx: ctx}
		futures[i] = &future.ErrorFuture{}
		ok, _ := p.Register(span, hlc.MinTimestamp, nil,
			withDiff, withFiltering, streams[i], nil, futures[i])
		require.True(b, ok)
	}

	// Wait for catchup scans and flush checkpoint events.
	h.syncEventAndRegistrations()

	// Run the benchmark. We accounted for b.N when constructing events.
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !generateRandomizedEventAndSend(ctx, rand, p) {
			b.Fatalf("failed to generate event")
		}
	}

	h.syncEventAndRegistrations()

	// Check that all registrations ended successfully, and emitted the expected
	// number of events.
	b.StopTimer()
	p.Stop()

	for i, f := range futures {
		regErr, err := future.Wait(ctx, f)
		require.NoError(b, err)
		require.NoError(b, regErr)
		require.Equal(b, b.N, streams[i].events-1) // ignore checkpoint after catchup
	}
}
