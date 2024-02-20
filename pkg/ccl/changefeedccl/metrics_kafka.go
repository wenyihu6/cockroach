// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/rcrowley/go-metrics"
)

type kafkaHistogramAdapter struct {
	settings *cluster.Settings
	wrapped  *aggmetric.Histogram
}

var _ metrics.Histogram = (*kafkaHistogramAdapter)(nil)

func (k *kafkaHistogramAdapter) Update(valueInMs int64) {
	if k != nil {
		// valueInMs is passed in from sarama with a unit of milliseconds. To
		// convert this value to nanoseconds, valueInMs * 10^6 is recorded here.
		k.wrapped.RecordValue(valueInMs * 1000000)
	}
}

func (k *kafkaHistogramAdapter) Clear() {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Sum on kafkaHistogramAdapter")
}

func (k *kafkaHistogramAdapter) Count() (_ int64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Count on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Max() (_ int64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Max on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Mean() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Mean on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Min() (_ int64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Min on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Percentile(float64) (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Percentile on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Percentiles([]float64) (_ []float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Percentiles on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Sample() (_ metrics.Sample) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Sample on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Snapshot() (_ metrics.Histogram) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Snapshot on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) StdDev() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to StdDev on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Sum() (_ int64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Sum on kafkaHistogramAdapter")
	return
}

func (k *kafkaHistogramAdapter) Variance() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Variance on kafkaHistogramAdapter")
	return
}

type kafkaMeterAdapter struct {
	settings *cluster.Settings
	wrapped  *aggmetric.Counter
}

var _ metrics.Meter = (*kafkaMeterAdapter)(nil)

func (k *kafkaMeterAdapter) Count() (_ int64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Count on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) Mark(i int64) {
	k.wrapped.Inc(i)
}

func (k *kafkaMeterAdapter) Rate1() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Rate1 on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) Rate5() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Rate5 on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) Rate15() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Rate15 on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) RateMean() (_ float64) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to RateMean on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) Snapshot() (_ metrics.Meter) {
	logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Snapshot on kafkaMeterAdapter")
	return
}

func (k *kafkaMeterAdapter) Stop() {
	// TODO: this function will likely being called
	// logcrash.ReportOrPanic(context.Background(), &k.settings.SV /*settings.Values*/, "unexpected call to Variance on kafkaMeterAdapter")
}
