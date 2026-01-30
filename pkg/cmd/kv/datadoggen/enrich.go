// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

var (
	enrichOutputFile     string
	enrichMetricsYAML    string
	enrichAddNotes       bool
	enrichTitleSeparator string
	enrichQuiet          bool
	enrichDashboardTitle string
	enrichKeepTitle      bool
)

func newEnrichDashboardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enrich-dashboard <dashboard.json>",
		Short: "Add metric descriptions from metrics.yaml to a Datadog dashboard",
		Long: `Enrich a Datadog dashboard JSON by adding metric descriptions from metrics.yaml.

This command reads an existing Datadog dashboard JSON and adds help text/descriptions
for metrics by looking them up in CockroachDB's metrics.yaml file.

Since Datadog timeseries widgets don't have a native "description" field, this tool
offers two ways to add help text:

1. Title enrichment (default): Appends the description to the widget title
   Example: "SQL Service Latency" -> "SQL Service Latency - Time to service SQL requests"

2. Note widgets (--add-notes): Adds a note widget after each timeseries with the description

The dashboard title is automatically set to match the output filename by default.
Use --keep-title to preserve the original title, or --title to set a custom title.

Examples:
  # Enrich dashboard (title becomes "my_dashboard_enriched")
  datadoggen enrich-dashboard my_dashboard.json

  # Custom output file (title becomes "production_metrics")
  datadoggen enrich-dashboard my_dashboard.json -o production_metrics.json

  # Keep the original dashboard title
  datadoggen enrich-dashboard my_dashboard.json --keep-title

  # Set a custom dashboard title
  datadoggen enrich-dashboard my_dashboard.json --title "My Custom Dashboard"

  # Add note widgets instead of enriching titles
  datadoggen enrich-dashboard my_dashboard.json --add-notes

  # Use custom metrics.yaml location
  datadoggen enrich-dashboard my_dashboard.json --yaml /path/to/metrics.yaml

  # Change title separator (default is " - ")
  datadoggen enrich-dashboard my_dashboard.json --separator " | "`,
		Args: cobra.ExactArgs(1),
		RunE: runEnrichDashboard,
	}

	cmd.Flags().StringVarP(&enrichOutputFile, "output", "o", "", "Output file (default: <input>_enriched.json)")
	cmd.Flags().StringVar(&enrichMetricsYAML, "yaml", "", fmt.Sprintf("Path to metrics.yaml (default: %s)", defaultYAMLPath))
	cmd.Flags().BoolVar(&enrichAddNotes, "add-notes", false, "Add note widgets with descriptions instead of enriching titles")
	cmd.Flags().StringVar(&enrichTitleSeparator, "separator", " - ", "Separator between title and description (for title enrichment)")
	cmd.Flags().BoolVar(&enrichQuiet, "quiet", false, "Suppress info messages, output only results")
	cmd.Flags().StringVar(&enrichDashboardTitle, "title", "", "Dashboard title (default: derived from output filename)")
	cmd.Flags().BoolVar(&enrichKeepTitle, "keep-title", false, "Keep the original dashboard title instead of renaming")

	return cmd
}

// metricDescriptionLookup maps metric names (both dot and underscore formats) to descriptions.
var metricDescriptionLookup map[string]string

// LoadMetricDescriptions loads metric descriptions from metrics.yaml.
// It builds a lookup table mapping both the internal name (with dots) and
// exported name (with underscores) to their descriptions.
func LoadMetricDescriptions(yamlPath string) error {
	file, err := os.Open(yamlPath)
	if err != nil {
		return err
	}
	defer file.Close()

	metricDescriptionLookup = make(map[string]string)
	scanner := bufio.NewScanner(file)

	var currentName, currentExportedName, currentDescription string

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "- name:") {
			// Save previous metric if we have a description
			if currentDescription != "" {
				if currentName != "" {
					metricDescriptionLookup[currentName] = currentDescription
					// Also store with "cockroachdb." prefix
					metricDescriptionLookup["cockroachdb."+currentName] = currentDescription
					metricDescriptionLookup["crdb.tsdump."+currentName] = currentDescription
				}
				if currentExportedName != "" {
					metricDescriptionLookup[currentExportedName] = currentDescription
				}
			}
			currentName = strings.TrimSpace(strings.TrimPrefix(trimmed, "- name:"))
			currentExportedName = ""
			currentDescription = ""
		} else if strings.HasPrefix(trimmed, "exported_name:") {
			currentExportedName = strings.TrimSpace(strings.TrimPrefix(trimmed, "exported_name:"))
		} else if strings.HasPrefix(trimmed, "description:") {
			desc := strings.TrimSpace(strings.TrimPrefix(trimmed, "description:"))
			// Remove surrounding quotes if present
			desc = strings.Trim(desc, "'\"")
			currentDescription = desc
		}
	}

	// Don't forget the last metric
	if currentDescription != "" {
		if currentName != "" {
			metricDescriptionLookup[currentName] = currentDescription
			metricDescriptionLookup["cockroachdb."+currentName] = currentDescription
			metricDescriptionLookup["crdb.tsdump."+currentName] = currentDescription
		}
		if currentExportedName != "" {
			metricDescriptionLookup[currentExportedName] = currentDescription
		}
	}

	return scanner.Err()
}

// LookupMetricDescription looks up the description for a metric name.
// It tries various formats: with/without prefix, dots/underscores.
func LookupMetricDescription(metricName string) string {
	if metricDescriptionLookup == nil {
		return ""
	}

	// Try direct lookup first
	if desc, ok := metricDescriptionLookup[metricName]; ok {
		return desc
	}

	// Strip common prefixes and try again
	stripped := metricName
	for _, prefix := range []string{"cockroachdb.", "crdb.tsdump.", "crdb."} {
		stripped = strings.TrimPrefix(stripped, prefix)
	}

	if desc, ok := metricDescriptionLookup[stripped]; ok {
		return desc
	}

	// Try converting underscores to dots
	withDots := strings.ReplaceAll(stripped, "_", ".")
	if desc, ok := metricDescriptionLookup[withDots]; ok {
		return desc
	}

	// Try converting dots to underscores
	withUnderscores := strings.ReplaceAll(stripped, ".", "_")
	if desc, ok := metricDescriptionLookup[withUnderscores]; ok {
		return desc
	}

	return ""
}

// extractFirstSentence extracts the first sentence from a description for use as
// a short, human-readable title. It looks for the first period followed by a space
// or end of string. If no sentence boundary is found, it truncates at 100 chars.
func extractFirstSentence(description string) string {
	// Look for first period followed by space (end of sentence)
	for i := 0; i < len(description)-1; i++ {
		if description[i] == '.' && (description[i+1] == ' ' || description[i+1] == '\n') {
			return description[:i+1]
		}
	}
	// If description ends with period, return as-is
	if len(description) > 0 && description[len(description)-1] == '.' {
		return description
	}
	// No sentence found, truncate if too long
	if len(description) > 100 {
		return description[:97] + "..."
	}
	return description
}

// metricNameToTitle converts a metric name like "mma.overloaded_store.lease_grace.success"
// into a human-readable title like "MMA Overloaded Store Lease Grace Success".
func metricNameToTitle(metricName string) string {
	// Strip common prefixes
	name := metricName
	for _, prefix := range []string{"cockroachdb.", "crdb.tsdump.", "crdb."} {
		name = strings.TrimPrefix(name, prefix)
	}

	// Replace dots and underscores with spaces
	name = strings.ReplaceAll(name, ".", " ")
	name = strings.ReplaceAll(name, "_", " ")

	// Title case each word, with special handling for acronyms
	words := strings.Fields(name)
	for i, word := range words {
		upper := strings.ToUpper(word)
		// Keep common acronyms uppercase
		if upper == "MMA" || upper == "SMA" || upper == "CPU" || upper == "IO" ||
			upper == "SQL" || upper == "KV" || upper == "ID" || upper == "P99" ||
			upper == "P50" || upper == "P90" || upper == "P999" {
			words[i] = upper
		} else {
			words[i] = strings.Title(word)
		}
	}

	return strings.Join(words, " ")
}

// extractMetricNamesFromQuery extracts metric names from a Datadog query string.
// Query format examples:
//   - sum:cockroachdb.sql.select.count{...}
//   - p99:cockroachdb.sql.service.latency{...}
//   - avg:crdb.tsdump.metric_name_p99{...}
func extractMetricNamesFromQuery(query string) []string {
	// Match metric names after aggregation prefix and before {
	// Pattern: agg:metric.name{
	re := regexp.MustCompile(`(?:avg|sum|max|min|p\d+(?:\.\d+)?):([a-zA-Z0-9_\.]+)\{`)
	matches := re.FindAllStringSubmatch(query, -1)

	var metrics []string
	seen := make(map[string]bool)

	for _, match := range matches {
		if len(match) >= 2 {
			metric := match[1]
			// Strip percentile suffixes for tsdump format (e.g., _p99)
			for _, suffix := range []string{"_p50", "_p75", "_p90", "_p99", "_p999", "_p9999"} {
				metric = strings.TrimSuffix(metric, suffix)
			}
			if !seen[metric] {
				seen[metric] = true
				metrics = append(metrics, metric)
			}
		}
	}

	return metrics
}

// deriveDashboardTitle creates a readable dashboard title from a filename.
// Example: "mma_enriched.json" -> "Mma Enriched"
// Example: "sql-metrics_enriched.json" -> "Sql Metrics Enriched"
func deriveDashboardTitle(filename string) string {
	// Get just the filename without path
	base := filepath.Base(filename)
	// Remove .json extension
	name := strings.TrimSuffix(base, ".json")
	// Replace underscores and hyphens with spaces
	name = strings.ReplaceAll(name, "_", " ")
	name = strings.ReplaceAll(name, "-", " ")
	// Title case
	return strings.Title(name)
}

func runEnrichDashboard(cmd *cobra.Command, args []string) error {
	inputFile := args[0]

	// Generate output filename if not provided
	outputFile := enrichOutputFile
	if outputFile == "" {
		outputFile = strings.TrimSuffix(inputFile, ".json") + "_enriched.json"
	}

	// Load metrics.yaml
	yamlFile := enrichMetricsYAML
	if yamlFile == "" {
		yamlFile = defaultYAMLPath
	}

	// Helper functions for logging
	logf := func(format string, args ...interface{}) {
		if !enrichQuiet {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}
	logln := func(args ...interface{}) {
		if !enrichQuiet {
			fmt.Fprintln(os.Stderr, args...)
		}
	}

	logln(strings.Repeat("=", 70))
	logln("DATADOG DASHBOARD ENRICHER")
	logln(strings.Repeat("=", 70))

	logf("\nInput:       %s\n", inputFile)
	logf("Output:      %s\n", outputFile)
	logf("Metrics YAML: %s\n", yamlFile)
	logf("Mode:        %s\n", func() string {
		if enrichAddNotes {
			return "add note widgets"
		}
		return "enrich titles"
	}())

	// Load metric descriptions
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("LOADING METRIC DESCRIPTIONS")
	logln(strings.Repeat("-", 70))

	if err := LoadMetricDescriptions(yamlFile); err != nil {
		return fmt.Errorf("failed to load metrics.yaml: %w", err)
	}
	logf("✓ Loaded %d metric descriptions\n", len(metricDescriptionLookup))

	// Load dashboard JSON
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("LOADING DASHBOARD")
	logln(strings.Repeat("-", 70))

	data, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	var dashboard DatadogDashboard
	if err := json.Unmarshal(data, &dashboard); err != nil {
		return fmt.Errorf("failed to parse dashboard JSON: %w", err)
	}

	logf("✓ Loaded dashboard: %s\n", dashboard.Title)
	logf("  Widgets: %d\n", len(dashboard.Widgets))

	// Update dashboard title
	originalTitle := dashboard.Title
	if !enrichKeepTitle {
		if enrichDashboardTitle != "" {
			dashboard.Title = enrichDashboardTitle
		} else {
			// Derive title from output filename
			dashboard.Title = deriveDashboardTitle(outputFile)
		}
		if dashboard.Title != originalTitle {
			logf("  New title: %s\n", dashboard.Title)
		}
	}

	// Enrich dashboard
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("ENRICHING DASHBOARD")
	logln(strings.Repeat("-", 70))

	var enrichedCount, notFoundCount int
	var notFoundMetrics []string

	if enrichAddNotes {
		// Add note widgets with descriptions
		enrichedCount, notFoundCount, notFoundMetrics = enrichWithNotes(&dashboard)
	} else {
		// Enrich widget titles
		enrichedCount, notFoundCount, notFoundMetrics = enrichTitles(&dashboard)
	}

	logf("  Widgets enriched: %d\n", enrichedCount)
	logf("  Metrics without descriptions: %d\n", notFoundCount)

	if len(notFoundMetrics) > 0 && !enrichQuiet {
		logf("\n  Metrics not found in YAML (first 10):\n")
		for i, m := range notFoundMetrics {
			if i >= 10 {
				logf("    ... and %d more\n", len(notFoundMetrics)-10)
				break
			}
			logf("    - %s\n", m)
		}
	}

	// Write output
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("WRITING OUTPUT")
	logln(strings.Repeat("-", 70))

	outputData, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := os.WriteFile(outputFile, outputData, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	logf("✓ Enriched dashboard written to: %s\n", outputFile)
	logln(strings.Repeat("=", 70))

	return nil
}

// enrichTitles adds metric descriptions to widget titles.
func enrichTitles(dashboard *DatadogDashboard) (enrichedCount, notFoundCount int, notFoundMetrics []string) {
	seenNotFound := make(map[string]bool)

	for i := range dashboard.Widgets {
		e, nf, nfm := enrichWidgetTitles(&dashboard.Widgets[i], seenNotFound)
		enrichedCount += e
		notFoundCount += nf
		notFoundMetrics = append(notFoundMetrics, nfm...)
	}

	// Standardize all widget layouts to width=6, height=4
	standardizeWidgetLayouts(dashboard)

	return
}

func enrichWidgetTitles(widget *Widget, seenNotFound map[string]bool) (enrichedCount, notFoundCount int, notFoundMetrics []string) {
	// Handle group widgets recursively
	if widget.Definition.Type == "group" {
		for i := range widget.Definition.Widgets {
			e, nf, nfm := enrichWidgetTitles(&widget.Definition.Widgets[i], seenNotFound)
			enrichedCount += e
			notFoundCount += nf
			notFoundMetrics = append(notFoundMetrics, nfm...)
		}
		return
	}

	// Only process timeseries widgets
	if widget.Definition.Type != "timeseries" {
		return
	}

	// Add legend configuration
	addLegendConfig(widget)

	// Extract metrics from queries
	var allMetrics []string
	for _, req := range widget.Definition.Requests {
		for _, q := range req.Queries {
			metrics := extractMetricNamesFromQuery(q.Query)
			allMetrics = append(allMetrics, metrics...)
		}
	}

	if len(allMetrics) == 0 {
		return
	}

	// Find description for the first metric (main metric)
	var description string
	var mainMetric string
	for _, metric := range allMetrics {
		desc := LookupMetricDescription(metric)
		if desc != "" {
			description = desc
			mainMetric = metric
			break
		}
	}

	if description != "" {
		// Create human-readable title from metric name
		metricTitle := metricNameToTitle(mainMetric)

		// Extract short summary (first sentence) for additional context
		shortSummary := extractFirstSentence(description)

		// Build new title: "Metric Title - Short description summary."
		newTitle := metricTitle + enrichTitleSeparator + shortSummary

		// Don't add if it's already in the title
		if !strings.Contains(widget.Definition.Title, metricTitle) {
			widget.Definition.Title = newTitle
			enrichedCount = 1
		}
	} else {
		// Track metrics without descriptions
		for _, metric := range allMetrics {
			if !seenNotFound[metric] {
				seenNotFound[metric] = true
				notFoundMetrics = append(notFoundMetrics, metric)
				notFoundCount++
			}
		}
	}

	return
}

// enrichWithNotes adds note widgets with descriptions after timeseries widgets.
func enrichWithNotes(dashboard *DatadogDashboard) (enrichedCount, notFoundCount int, notFoundMetrics []string) {
	seenNotFound := make(map[string]bool)

	var newWidgets []Widget
	for i := range dashboard.Widgets {
		widget := &dashboard.Widgets[i]

		if widget.Definition.Type == "group" {
			// Process group widgets
			e, nf, nfm, groupWidgets := enrichGroupWithNotes(widget, seenNotFound)
			enrichedCount += e
			notFoundCount += nf
			notFoundMetrics = append(notFoundMetrics, nfm...)
			widget.Definition.Widgets = groupWidgets
			newWidgets = append(newWidgets, *widget)
		} else if widget.Definition.Type == "timeseries" {
			addLegendConfig(widget)
			newWidgets = append(newWidgets, *widget)

			// Add note widget if we find a description
			desc, metrics := getDescriptionForWidget(widget)
			if desc != "" {
				noteWidget := createDescriptionNoteWidget(widget.Definition.Title, desc)
				newWidgets = append(newWidgets, noteWidget)
				enrichedCount++
			} else {
				for _, m := range metrics {
					if !seenNotFound[m] {
						seenNotFound[m] = true
						notFoundMetrics = append(notFoundMetrics, m)
						notFoundCount++
					}
				}
			}
		} else {
			newWidgets = append(newWidgets, *widget)
		}
	}

	dashboard.Widgets = newWidgets

	// Standardize all widget layouts to width=6, height=4
	standardizeWidgetLayouts(dashboard)

	return
}

func enrichGroupWithNotes(widget *Widget, seenNotFound map[string]bool) (enrichedCount, notFoundCount int, notFoundMetrics []string, newWidgets []Widget) {
	for i := range widget.Definition.Widgets {
		w := &widget.Definition.Widgets[i]

		if w.Definition.Type == "timeseries" {
			addLegendConfig(w)
			newWidgets = append(newWidgets, *w)

			desc, metrics := getDescriptionForWidget(w)
			if desc != "" {
				noteWidget := createDescriptionNoteWidget(w.Definition.Title, desc)
				newWidgets = append(newWidgets, noteWidget)
				enrichedCount++
			} else {
				for _, m := range metrics {
					if !seenNotFound[m] {
						seenNotFound[m] = true
						notFoundMetrics = append(notFoundMetrics, m)
						notFoundCount++
					}
				}
			}
		} else {
			newWidgets = append(newWidgets, *w)
		}
	}
	return
}

func getDescriptionForWidget(widget *Widget) (string, []string) {
	var allMetrics []string
	for _, req := range widget.Definition.Requests {
		for _, q := range req.Queries {
			metrics := extractMetricNamesFromQuery(q.Query)
			allMetrics = append(allMetrics, metrics...)
		}
	}

	for _, metric := range allMetrics {
		desc := LookupMetricDescription(metric)
		if desc != "" {
			return desc, allMetrics
		}
	}

	return "", allMetrics
}

func createDescriptionNoteWidget(title, description string) Widget {
	content := fmt.Sprintf("**%s**\n\n%s", title, description)
	return Widget{
		Definition: WidgetDefinition{
			Type:            "note",
			Content:         content,
			BackgroundColor: "gray",
			FontSize:        "12",
			TextAlign:       "left",
			ShowTick:        false,
			TickPos:         "50%",
			TickEdge:        "left",
		},
		Layout: &WidgetLayout{X: 0, Y: 0, Width: 4, Height: 1},
	}
}

// addLegendConfig always sets legend configuration on a timeseries widget.
func addLegendConfig(widget *Widget) {
	showLegend := true
	widget.Definition.ShowLegend = &showLegend
	widget.Definition.LegendLayout = "vertical"
	widget.Definition.LegendColumns = []string{"avg", "min", "max", "value", "sum"}
}

// standardizeWidgetLayouts sets all timeseries widgets to width=6, height=4
// and recalculates positions to avoid overlaps (2 widgets per row).
func standardizeWidgetLayouts(dashboard *DatadogDashboard) {
	for i := range dashboard.Widgets {
		widget := &dashboard.Widgets[i]
		if widget.Definition.Type == "group" {
			standardizeGroupLayouts(widget)
		}
	}
}

func standardizeGroupLayouts(group *Widget) {
	const widgetWidth = 6
	const widgetHeight = 4
	const gridWidth = 12

	col := 0
	row := 0

	for i := range group.Definition.Widgets {
		widget := &group.Definition.Widgets[i]
		if widget.Definition.Type == "timeseries" {
			widget.Layout = &WidgetLayout{
				X:      col * widgetWidth,
				Y:      row * widgetHeight,
				Width:  widgetWidth,
				Height: widgetHeight,
			}
			col++
			if col*widgetWidth >= gridWidth {
				col = 0
				row++
			}
		}
	}
}
