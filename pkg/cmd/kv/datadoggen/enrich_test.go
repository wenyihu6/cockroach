// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

func TestExtractMetricNamesFromQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "sum counter query",
			query:    "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
			expected: []string{"cockroachdb.sql.select.count"},
		},
		{
			name:     "avg gauge query",
			query:    "avg:cockroachdb.sys.cpu.combined.percent.normalized{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
			expected: []string{"cockroachdb.sys.cpu.combined.percent.normalized"},
		},
		{
			name:     "p99 histogram query",
			query:    "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			expected: []string{"cockroachdb.sql.service.latency"},
		},
		{
			name:     "p99.9 histogram query",
			query:    "p99.9:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			expected: []string{"cockroachdb.sql.service.latency"},
		},
		{
			name:     "tsdump metric with percentile suffix",
			query:    "avg:crdb.tsdump.sql.service.latency_p99{$upload_id,$node_id} by {node_id}.rollup(max, 10)",
			expected: []string{"crdb.tsdump.sql.service.latency"},
		},
		{
			name:     "max aggregation",
			query:    "max:cockroachdb.ranges{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			expected: []string{"cockroachdb.ranges"},
		},
		{
			name:     "min aggregation",
			query:    "min:cockroachdb.liveness.livenodes{$cluster} by {cluster}.rollup(min, 10)",
			expected: []string{"cockroachdb.liveness.livenodes"},
		},
		{
			name:     "empty query",
			query:    "",
			expected: nil,
		},
		{
			name:     "no metric in query",
			query:    "some random text",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractMetricNamesFromQuery(tt.query)
			if len(result) != len(tt.expected) {
				t.Errorf("extractMetricNamesFromQuery(%q) returned %d metrics, want %d", tt.query, len(result), len(tt.expected))
				return
			}
			for i, metric := range tt.expected {
				if result[i] != metric {
					t.Errorf("extractMetricNamesFromQuery(%q)[%d] = %q, want %q", tt.query, i, result[i], metric)
				}
			}
		})
	}
}

func TestLookupMetricDescription(t *testing.T) {
	// Load metrics.yaml using datapathutils for correct Bazel paths
	metricsPath := datapathutils.RewritableDataPath(t, "docs", "generated", "metrics", "metrics.yaml")
	if err := LoadMetricDescriptions(metricsPath); err != nil {
		skip.IgnoreLint(t, "Could not load metrics.yaml for test")
	}

	tests := []struct {
		name           string
		metricName     string
		expectNonEmpty bool
		containsSubstr string
	}{
		{
			name:           "sql.service.latency with prefix",
			metricName:     "cockroachdb.sql.service.latency",
			expectNonEmpty: true,
			containsSubstr: "latency",
		},
		{
			name:           "sql.service.latency without prefix",
			metricName:     "sql.service.latency",
			expectNonEmpty: true,
			containsSubstr: "latency",
		},
		{
			name:           "changefeed metric",
			metricName:     "changefeed.admit_latency",
			expectNonEmpty: true,
			containsSubstr: "admission",
		},
		{
			name:           "tsdump prefix metric",
			metricName:     "crdb.tsdump.sql.service.latency",
			expectNonEmpty: true,
			containsSubstr: "latency",
		},
		{
			name:           "nonexistent metric",
			metricName:     "this.metric.does.not.exist",
			expectNonEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := LookupMetricDescription(tt.metricName)
			if tt.expectNonEmpty && desc == "" {
				t.Errorf("LookupMetricDescription(%q) returned empty, expected non-empty", tt.metricName)
			}
			if !tt.expectNonEmpty && desc != "" {
				t.Errorf("LookupMetricDescription(%q) = %q, expected empty", tt.metricName, desc)
			}
		})
	}
}

func TestEnrichTitles(t *testing.T) {
	// Load metrics.yaml using datapathutils for correct Bazel paths
	metricsPath := datapathutils.RewritableDataPath(t, "docs", "generated", "metrics", "metrics.yaml")
	if err := LoadMetricDescriptions(metricsPath); err != nil {
		skip.IgnoreLint(t, "Could not load metrics.yaml for test")
	}

	// Save and restore separator
	oldSeparator := enrichTitleSeparator
	enrichTitleSeparator = " - "
	defer func() { enrichTitleSeparator = oldSeparator }()

	dashboard := &DatadogDashboard{
		Title: "Test Dashboard",
		Widgets: []Widget{
			{
				Definition: WidgetDefinition{
					Type:  "timeseries",
					Title: "SQL Service Latency",
					Requests: []WidgetRequest{
						{
							Queries: []WidgetQuery{
								{
									Query: "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
								},
							},
						},
					},
				},
			},
			{
				Definition: WidgetDefinition{
					Type:  "note",
					Title: "Some Note",
				},
			},
		},
	}

	enrichedCount, _, _ := enrichTitles(dashboard)

	if enrichedCount != 1 {
		t.Errorf("Expected 1 enriched widget, got %d", enrichedCount)
	}

	// Check that the first widget's title was enriched
	if dashboard.Widgets[0].Definition.Title == "SQL Service Latency" {
		t.Error("Widget title was not enriched")
	}

	// The title should contain a separator
	if !containsSubstring(dashboard.Widgets[0].Definition.Title, " - ") {
		t.Errorf("Widget title should contain separator, got: %q", dashboard.Widgets[0].Definition.Title)
	}

	// The note widget should be unchanged
	if dashboard.Widgets[1].Definition.Title != "Some Note" {
		t.Errorf("Note widget title was modified, got: %q", dashboard.Widgets[1].Definition.Title)
	}
}

func TestEnrichGroupWidgets(t *testing.T) {
	// Load metrics.yaml using datapathutils for correct Bazel paths
	metricsPath := datapathutils.RewritableDataPath(t, "docs", "generated", "metrics", "metrics.yaml")
	if err := LoadMetricDescriptions(metricsPath); err != nil {
		skip.IgnoreLint(t, "Could not load metrics.yaml for test")
	}

	// Save and restore separator
	oldSeparator := enrichTitleSeparator
	enrichTitleSeparator = " - "
	defer func() { enrichTitleSeparator = oldSeparator }()

	dashboard := &DatadogDashboard{
		Title: "Test Dashboard",
		Widgets: []Widget{
			{
				Definition: WidgetDefinition{
					Type:  "group",
					Title: "SQL Metrics",
					Widgets: []Widget{
						{
							Definition: WidgetDefinition{
								Type:  "timeseries",
								Title: "SQL Service Latency",
								Requests: []WidgetRequest{
									{
										Queries: []WidgetQuery{
											{
												Query: "p99:cockroachdb.sql.service.latency{$cluster} by {node_id}.rollup(max, 10)",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	enrichedCount, _, _ := enrichTitles(dashboard)

	if enrichedCount != 1 {
		t.Errorf("Expected 1 enriched widget in group, got %d", enrichedCount)
	}

	// Check the nested widget was enriched
	nestedWidget := dashboard.Widgets[0].Definition.Widgets[0]
	if nestedWidget.Definition.Title == "SQL Service Latency" {
		t.Error("Nested widget title was not enriched")
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
