#!/usr/bin/env python3
"""
Convert Grafana dashboard JSON to Datadog dashboard JSON.
Handles all query patterns found in CockroachDB Grafana dashboards.

Query format examples:
- Gauge: avg:cockroachdb.sys.cpu.combined.percent.normalized{$cluster, $node_id, $store} by {node_id}
- Counter: sum:cockroachdb.storage.iterator.block.load.bytes{$cluster, $node_id, $store} by {store}.as_rate().rollup(30)
- Histogram: p99.99:cockroachdb.sql.service.latency{$cluster, $node_id, $store} by {node_id}
"""

import json
import re
import sys

# Datadog metric prefix
METRIC_PREFIX = "cockroachdb"

# Default rollup interval
ROLLUP_INTERVAL = 30

# Default tags
DEFAULT_TAGS = "$cluster, $node_id, $store"


def clean_labels(labels_str):
    """Convert Prometheus label selectors to Datadog tag format using template variables."""
    if not labels_str:
        return DEFAULT_TAGS
    
    result = []
    has_cluster = False
    has_node = False
    has_store = False
    
    # Split on comma, but be careful with nested structures
    parts = re.split(r',\s*(?=[a-zA-Z_])', labels_str)
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        # Handle =~ (regex match)
        match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=~\s*"([^"]*)"', part)
        if match:
            key, val = match.groups()
            if key == 'cluster' or val == '$cluster':
                has_cluster = True
            elif key == 'instance' or val == '$node':
                has_node = True
            elif key == 'store':
                has_store = True
            elif key == 'job':
                continue  # Skip job label
            else:
                result.append(f"{key}:{val}")
            continue
        
        # Handle = (exact match)
        match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"', part)
        if match:
            key, val = match.groups()
            if key == 'cluster' or val == '$cluster':
                has_cluster = True
            elif key == 'instance' or val == '$node':
                has_node = True
            elif key == 'store':
                has_store = True
            elif key == 'job':
                continue  # Skip job label
            else:
                result.append(f"{key}:{val}")
            continue
    
    # Always include the standard template variables
    return DEFAULT_TAGS


def convert_metric_name(metric):
    """Convert Prometheus metric name to Datadog format.
    
    Examples:
        sql_service_latency_bucket -> cockroachdb.sql.service.latency.bucket
        sys_cpu_combined_percent_normalized -> cockroachdb.sys.cpu.combined.percent.normalized
    """
    # Replace underscores with dots
    dd_metric = metric.replace('_', '.')
    return f"{METRIC_PREFIX}.{dd_metric}"


def format_percentile(quantile):
    """Format quantile value as Datadog percentile prefix.
    
    Examples:
        0.90 -> p90
        0.99 -> p99
        0.999 -> p99.9
        0.9999 -> p99.99
        1 -> max
    """
    if quantile == 1:
        return "max"
    
    # Convert to percentage
    pct = quantile * 100
    
    # Check if it's a clean integer
    if pct == int(pct):
        return f"p{int(pct)}"
    else:
        # Format with appropriate decimal places
        return f"p{pct:g}"


def is_counter_metric(metric_name):
    """Determine if a metric is a counter based on naming conventions."""
    counter_suffixes = ['_count', '_total', '_sum', '_bucket', '.count', '.total', '.sum', '.bucket',
                       '_bytes', '.bytes', '_ops', '.ops']
    
    metric_lower = metric_name.lower()
    
    for suffix in counter_suffixes:
        if metric_lower.endswith(suffix):
            return True
    
    return False


def build_counter_query(metric, labels, aggregator="sum", group_by=None):
    """Build a Datadog query for counter metrics.
    
    Format: sum:metric{tags} by {group}.as_rate().rollup(30)
    """
    if group_by:
        return f"{aggregator}:{metric}{{{labels}}} by {{{group_by}}}.as_rate().rollup({ROLLUP_INTERVAL})"
    else:
        return f"{aggregator}:{metric}{{{labels}}} by {{node_id}}.as_rate().rollup({ROLLUP_INTERVAL})"


def build_gauge_query(metric, labels, aggregator="avg", group_by=None):
    """Build a Datadog query for gauge metrics.
    
    Format: avg:metric{tags} by {group}
    """
    if group_by:
        return f"{aggregator}:{metric}{{{labels}}} by {{{group_by}}}"
    else:
        return f"{aggregator}:{metric}{{{labels}}} by {{node_id}}"


def build_histogram_query(metric_base, labels, percentile_prefix, group_by=None):
    """Build a Datadog query for histogram/percentile metrics.
    
    Format: p99.99:metric{tags} by {group}
    """
    # Remove .bucket suffix if present
    if metric_base.endswith('.bucket'):
        metric_base = metric_base[:-7]  # Remove '.bucket'
    
    if group_by:
        return f"{percentile_prefix}:{metric_base}{{{labels}}} by {{{group_by}}}"
    else:
        return f"{percentile_prefix}:{metric_base}{{{labels}}} by {{node_id}}"


def extract_all_metrics_as_queries(expr):
    """
    Extract all metrics from a PromQL expression and return them as separate Datadog queries.
    This handles complex expressions with multiple metrics (e.g., division expressions).
    """
    if not expr or expr.strip() == "":
        return []
    
    # Remove time range parameters
    expr_clean = re.sub(r'\[\$__rate_interval\]', '', expr)
    expr_clean = re.sub(r'\[\$__range\]', '', expr_clean)
    expr_clean = re.sub(r'\[\d+[smhd]\]', '', expr_clean)
    
    # Check if rate() is used (indicates counter metric)
    has_rate = 'rate(' in expr.lower()
    
    # Find all metric{labels} patterns
    all_metrics = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}', expr_clean)
    
    # Filter out PromQL functions
    promql_funcs = {'rate', 'sum', 'avg', 'max', 'min', 'histogram_quantile', 
                   'avg_over_time', 'min_over_time', 'max_over_time', 'stddev',
                   'ignoring', 'group_left', 'group_right', 'by', 'without'}
    
    queries = []
    seen_metrics = set()
    
    for metric, labels in all_metrics:
        if metric.lower() in promql_funcs:
            continue
        if metric in seen_metrics:
            continue
        seen_metrics.add(metric)
        
        clean_label = clean_labels(labels)
        dd_metric = convert_metric_name(metric)
        
        # Determine if this is a counter (rate metric) or gauge
        if has_rate or is_counter_metric(metric):
            queries.append(build_counter_query(dd_metric, clean_label))
        else:
            queries.append(build_gauge_query(dd_metric, clean_label))
    
    return queries


def extract_metric_and_labels(expr):
    """Extract metric name and labels from a PromQL expression fragment."""
    # Match metric_name{labels}
    match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}', expr.strip())
    if match:
        return match.group(1), clean_labels(match.group(2))
    
    # Match just metric_name
    match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)$', expr.strip())
    if match:
        return match.group(1), DEFAULT_TAGS
    
    return None, None


def convert_promql_to_datadog(promql_expr):
    """
    Convert a PromQL expression to Datadog query format.
    Handles all common patterns found in CockroachDB Grafana dashboards.
    """
    if not promql_expr or promql_expr.strip() == "":
        return None
    
    expr = promql_expr.strip()
    original_expr = expr
    
    # Remove time range parameters
    expr = re.sub(r'\[\$__rate_interval\]', '', expr)
    expr = re.sub(r'\[\$__range\]', '', expr)
    expr = re.sub(r'\[\d+[smhd]\]', '', expr)
    
    # Pattern 1: histogram_quantile with sum by (labels,le) (rate(...))
    # histogram_quantile(0.99, sum by (instance,le) (rate(metric_bucket{labels})))
    # -> p99:cockroachdb.metric{labels} by {instance}
    match = re.match(
        r'histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)\)',
        expr
    )
    if match:
        quantile = float(match.group(1))
        group_by = match.group(2).replace('le', '').strip(' ,')
        inner = match.group(3)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            p_prefix = format_percentile(quantile)
            dd_metric = convert_metric_name(metric)
            return build_histogram_query(dd_metric, labels, p_prefix, group_by if group_by else None)
    
    # Pattern 2: histogram_quantile with sum(rate(...)) by (labels) - group by after rate
    # histogram_quantile(0.99, sum(rate(metric_bucket{labels})) by (le, instance))
    match = re.match(
        r'histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\(rate\(([^)]+)\)\)\s*by\s*\(([^)]+)\)\)',
        expr
    )
    if match:
        quantile = float(match.group(1))
        inner = match.group(2)
        group_by = match.group(3).replace('le', '').strip(' ,')
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            p_prefix = format_percentile(quantile)
            dd_metric = convert_metric_name(metric)
            return build_histogram_query(dd_metric, labels, p_prefix, group_by if group_by else None)
    
    # Pattern 2b: histogram_quantile with sum(rate(...)) - no group by
    # histogram_quantile(0.99, sum(rate(metric_bucket{labels})))
    match = re.match(
        r'histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\(rate\(([^)]+)\)\)\)',
        expr
    )
    if match:
        quantile = float(match.group(1))
        inner = match.group(2)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            p_prefix = format_percentile(quantile)
            dd_metric = convert_metric_name(metric)
            return build_histogram_query(dd_metric, labels, p_prefix)
    
    # Pattern 3: sum by (label) (rate(metric{labels})) - Counter
    match = re.match(
        r'sum\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)',
        expr
    )
    if match:
        group_by = match.group(1)
        inner = match.group(2)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_counter_query(dd_metric, labels, "sum", group_by)
    
    # Pattern 4: sum by (label) (metric{labels}) - no rate
    match = re.match(
        r'sum\s+by\s*\(([^)]+)\)\s*\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)',
        expr
    )
    if match:
        group_by = match.group(1)
        metric = match.group(2)
        labels = clean_labels(match.group(3))
        dd_metric = convert_metric_name(metric)
        if is_counter_metric(metric):
            return build_counter_query(dd_metric, labels, "sum", group_by)
        else:
            return build_gauge_query(dd_metric, labels, "sum", group_by)
    
    # Pattern 5: sum(rate(metric{labels})) - no group by - Counter
    match = re.match(
        r'sum\(rate\(([^)]+)\)\)',
        expr
    )
    if match:
        inner = match.group(1)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_counter_query(dd_metric, labels, "sum")
    
    # Pattern 6: sum(metric{labels})
    match = re.match(
        r'sum\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)',
        expr
    )
    if match:
        metric = match.group(1)
        labels = clean_labels(match.group(2))
        dd_metric = convert_metric_name(metric)
        if is_counter_metric(metric):
            return build_counter_query(dd_metric, labels, "sum")
        else:
            return build_gauge_query(dd_metric, labels, "sum")
    
    # Pattern 7: avg by (label) (metric{labels}) - Gauge
    match = re.match(
        r'avg\s+by\s*\(([^)]+)\)\s*\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)',
        expr
    )
    if match:
        group_by = match.group(1)
        metric = match.group(2)
        labels = clean_labels(match.group(3))
        dd_metric = convert_metric_name(metric)
        return build_gauge_query(dd_metric, labels, "avg", group_by)
    
    # Pattern 8: avg by (label) (rate(metric{labels})) - Counter
    match = re.match(
        r'avg\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)',
        expr
    )
    if match:
        group_by = match.group(1)
        inner = match.group(2)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_counter_query(dd_metric, labels, "avg", group_by)
    
    # Pattern 9: avg by (label) (histogram_quantile(...))
    match = re.match(
        r'avg\s+by\s*\(([^)]+)\)\s*\(histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)\)\)',
        expr
    )
    if match:
        outer_group = match.group(1)
        quantile = float(match.group(2))
        inner_group = match.group(3)
        inner = match.group(4)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            p_prefix = format_percentile(quantile)
            dd_metric = convert_metric_name(metric)
            return build_histogram_query(dd_metric, labels, p_prefix, outer_group)
    
    # Pattern 10: rate(metric{labels}) - Counter
    match = re.match(
        r'rate\(([^)]+)\)',
        expr
    )
    if match:
        inner = match.group(1)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_counter_query(dd_metric, labels, "avg")
    
    # Pattern 11: avg_over_time(metric{labels}) - Gauge
    match = re.match(
        r'avg_over_time\(([^)]+)\)',
        expr
    )
    if match:
        inner = match.group(1)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_gauge_query(dd_metric, labels, "avg")
    
    # Pattern 12: max by (label) (avg_over_time(metric{labels})) - Gauge
    match = re.match(
        r'max\s+by\s*\(([^)]+)\)\s*\(avg_over_time\(([^)]+)\)\)',
        expr
    )
    if match:
        group_by = match.group(1)
        inner = match.group(2)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_gauge_query(dd_metric, labels, "max", group_by)
    
    # Pattern 13: min(min_over_time(metric{labels})) - Gauge
    match = re.match(
        r'min\(min_over_time\(([^)]+)\)\)',
        expr
    )
    if match:
        inner = match.group(1)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_gauge_query(dd_metric, labels, "min")
    
    # Pattern 14: max(max_over_time(metric{labels})) - Gauge
    match = re.match(
        r'max\(max_over_time\(([^)]+)\)\)',
        expr
    )
    if match:
        inner = match.group(1)
        metric, labels = extract_metric_and_labels(inner)
        if metric:
            dd_metric = convert_metric_name(metric)
            return build_gauge_query(dd_metric, labels, "max")
    
    # Pattern 15: Simple metric{labels} - Gauge
    match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}$', expr)
    if match:
        metric = match.group(1)
        labels = clean_labels(match.group(2))
        dd_metric = convert_metric_name(metric)
        if is_counter_metric(metric):
            return build_counter_query(dd_metric, labels)
        else:
            return build_gauge_query(dd_metric, labels)
    
    # Pattern 16: Just metric name without labels - Gauge
    match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)$', expr)
    if match:
        metric = match.group(1)
        dd_metric = convert_metric_name(metric)
        if is_counter_metric(metric):
            return build_counter_query(dd_metric, DEFAULT_TAGS)
        else:
            return build_gauge_query(dd_metric, DEFAULT_TAGS)
    
    # Pattern 17: Division expressions - extract metrics from both sides
    if '/' in expr:
        # Find all metrics in the expression
        all_metrics = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}', expr)
        promql_funcs = {'rate', 'sum', 'avg', 'max', 'min', 'histogram_quantile', 
                       'avg_over_time', 'min_over_time', 'max_over_time', 'stddev'}
        valid_metrics = [(m, l) for m, l in all_metrics if m.lower() not in promql_funcs]
        
        # Check if rate() is used
        has_rate = 'rate(' in expr.lower()
        
        if len(valid_metrics) >= 1:
            metric, labels = valid_metrics[0]
            labels = clean_labels(labels)
            dd_metric = convert_metric_name(metric)
            if has_rate or is_counter_metric(metric):
                return build_counter_query(dd_metric, labels)
            else:
                return build_gauge_query(dd_metric, labels)
    
    # For complex expressions, try to extract the main metric
    metric_match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}', expr)
    if metric_match:
        metric = metric_match.group(1)
        labels = clean_labels(metric_match.group(2))
        dd_metric = convert_metric_name(metric)
        has_rate = 'rate(' in expr.lower()
        if has_rate or is_counter_metric(metric):
            return build_counter_query(dd_metric, labels)
        else:
            return build_gauge_query(dd_metric, labels)
    
    # If nothing matched, return None
    print(f"  WARNING: Could not convert expression: {original_expr[:100]}...")
    return None


def convert_panel_to_widget(panel):
    """Convert a Grafana panel to a Datadog widget."""
    
    panel_type = panel.get("type", "")
    title = panel.get("title", "Untitled")
    description = panel.get("description", "")
    
    # Handle row panels (groups in Datadog)
    if panel_type == "row":
        return {
            "definition": {
                "type": "group",
                "layout_type": "ordered",
                "title": title,
                "show_title": True,
                "widgets": []
            }
        }
    
    # Get display style
    draw_style = panel.get("fieldConfig", {}).get("defaults", {}).get("custom", {}).get("drawStyle", "line")
    stacking_mode = panel.get("fieldConfig", {}).get("defaults", {}).get("custom", {}).get("stacking", {}).get("mode", "none")
    
    display_type = "line"
    if draw_style == "bars":
        display_type = "bars"
    elif stacking_mode == "normal":
        display_type = "area"
    
    # Get unit for y-axis
    unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "")
    
    # Handle timeseries, state-timeline, and heatmap panels
    if panel_type in ["timeseries", "state-timeline", "heatmap"]:
        targets = panel.get("targets", [])
        queries = []
        formulas = []
        query_names = []
        
        for i, target in enumerate(targets):
            expr = target.get("expr", "")
            ref_id = target.get("refId", chr(65 + i)).lower()
            legend = target.get("legendFormat", "")
            
            if not expr or expr.strip() == "":
                continue
            
            # First try the smart conversion which handles histogram_quantile, sum by, etc.
            dd_query = convert_promql_to_datadog(expr)
            
            # For division expressions, we may have multiple metrics to extract
            if '/' in expr:
                dd_queries = extract_all_metrics_as_queries(expr)
            elif dd_query:
                dd_queries = [dd_query]
            else:
                dd_queries = extract_all_metrics_as_queries(expr)
            
            for idx, dd_query in enumerate(dd_queries):
                query_name = ref_id if idx == 0 else f"{ref_id}_{idx}"
                counter = 1
                while query_name in query_names:
                    query_name = f"{ref_id}_{idx}_{counter}"
                    counter += 1
                query_names.append(query_name)
                
                queries.append({
                    "data_source": "metrics",
                    "name": query_name,
                    "query": dd_query
                })
                
                formula_obj = {"formula": query_name}
                if legend and legend not in ["__auto", "{{label_name}}"] and idx == 0:
                    alias = legend.replace("{{", "").replace("}}", "").replace("instance", "$instance")
                    formula_obj["alias"] = alias
                
                formulas.append(formula_obj)
        
        if panel_type == "state-timeline":
            display_type = "bars"
        elif panel_type == "heatmap":
            display_type = "area"
        
        widget = {
            "definition": {
                "type": "timeseries",
                "title": title,
                "title_size": "16",
                "requests": []
            }
        }
        
        if description:
            widget["definition"]["custom_links"] = []
        
        if queries:
            request = {
                "response_format": "timeseries",
                "queries": queries,
                "formulas": formulas,
                "display_type": display_type
            }
            
            if unit == "percentunit":
                request["style"] = {"line_type": "solid", "line_width": "normal"}
            
            widget["definition"]["requests"].append(request)
            
            if unit == "percentunit":
                widget["definition"]["yaxis"] = {
                    "scale": "linear",
                    "min": "0",
                    "max": "1"
                }
        
        grid_pos = panel.get("gridPos", {})
        widget["layout"] = {
            "x": grid_pos.get("x", 0) // 2,
            "y": grid_pos.get("y", 0) // 2,
            "width": max(grid_pos.get("w", 6) // 2, 2),
            "height": max(grid_pos.get("h", 4) // 2, 2)
        }
        
        return widget
    
    # For unsupported panel types, create a note
    return {
        "definition": {
            "type": "note",
            "content": f"Panel: {title}\nType: {panel_type} (not converted)",
            "background_color": "yellow",
            "font_size": "14",
            "text_align": "left",
            "show_tick": False,
            "tick_pos": "50%",
            "tick_edge": "left"
        },
        "layout": {
            "x": 0,
            "y": 0,
            "width": 3,
            "height": 2
        }
    }


def convert_grafana_to_datadog(grafana_json):
    """Convert a Grafana dashboard JSON to Datadog dashboard JSON."""
    
    # Set up template variables: cluster, node_id, store
    template_vars = [
        {
            "name": "cluster",
            "prefix": "cluster",
            "available_values": [],
            "default": "*"
        },
        {
            "name": "node_id",
            "prefix": "node_id",
            "available_values": [],
            "default": "*"
        },
        {
            "name": "store",
            "prefix": "store",
            "available_values": [],
            "default": "*"
        }
    ]
    
    panels = grafana_json.get("panels", [])
    widgets = []
    current_group = None
    group_widgets = []
    
    converted_count = 0
    skipped_count = 0
    
    def process_panel(panel):
        nonlocal converted_count, skipped_count
        
        widget = convert_panel_to_widget(panel)
        if widget:
            requests = widget.get("definition", {}).get("requests", [])
            has_queries = any(req.get("queries", []) for req in requests)
            
            if has_queries or widget["definition"]["type"] == "note":
                converted_count += 1
                return widget
            else:
                skipped_count += 1
        return None
    
    for panel in panels:
        panel_type = panel.get("type", "")
        
        if panel_type == "row":
            if current_group and group_widgets:
                current_group["definition"]["widgets"] = group_widgets
                widgets.append(current_group)
            
            current_group = convert_panel_to_widget(panel)
            group_widgets = []
            row_title = panel.get('title', 'Untitled')
            print(f"  Group: {row_title}")
            
            if panel.get("collapsed", False):
                nested_panels = panel.get("panels", [])
                if nested_panels:
                    print(f"    (collapsed row with {len(nested_panels)} nested panels)")
                    for nested_panel in nested_panels:
                        widget = process_panel(nested_panel)
                        if widget:
                            group_widgets.append(widget)
        else:
            widget = process_panel(panel)
            if widget:
                if current_group:
                    group_widgets.append(widget)
                else:
                    widgets.append(widget)
    
    if current_group and group_widgets:
        current_group["definition"]["widgets"] = group_widgets
        widgets.append(current_group)
    
    print(f"\n  Converted: {converted_count} panels")
    print(f"  Skipped (no valid queries): {skipped_count} panels")
    
    datadog_dashboard = {
        "title": grafana_json.get("title", "Converted Dashboard"),
        "description": f"Converted from Grafana dashboard: {grafana_json.get('title', 'Unknown')}",
        "layout_type": "ordered",
        "is_read_only": False,
        "template_variables": template_vars,
        "widgets": widgets
    }
    
    return datadog_dashboard


def validate_conversion(datadog_json):
    """Validate the converted dashboard."""
    errors = []
    warnings = []
    
    if not datadog_json.get("template_variables"):
        warnings.append("No template variables defined")
    
    widgets = datadog_json.get("widgets", [])
    if not widgets:
        errors.append("No widgets in dashboard")
    
    total_queries = 0
    empty_widgets = 0
    counter_queries = 0
    gauge_queries = 0
    histogram_queries = 0
    
    def check_widgets(widget_list, path=""):
        nonlocal total_queries, empty_widgets, counter_queries, gauge_queries, histogram_queries
        
        for i, widget in enumerate(widget_list):
            widget_path = f"{path}[{i}]"
            definition = widget.get("definition", {})
            widget_type = definition.get("type", "")
            
            if widget_type == "group":
                nested = definition.get("widgets", [])
                check_widgets(nested, f"{widget_path}.widgets")
            elif widget_type == "timeseries":
                requests = definition.get("requests", [])
                for req in requests:
                    queries = req.get("queries", [])
                    total_queries += len(queries)
                    for q in queries:
                        query_str = q.get("query", "")
                        if query_str.startswith("p") and ":" in query_str[:10]:
                            histogram_queries += 1
                        elif ".as_rate()" in query_str:
                            counter_queries += 1
                        else:
                            gauge_queries += 1
                    if not queries:
                        empty_widgets += 1
                        warnings.append(f"Empty timeseries widget at {widget_path}: {definition.get('title', 'Untitled')}")
    
    check_widgets(widgets)
    
    print(f"\n  Validation Results:")
    print(f"    Total queries: {total_queries}")
    print(f"      - Counter queries (as_rate): {counter_queries}")
    print(f"      - Gauge queries: {gauge_queries}")
    print(f"      - Histogram queries (pXX:): {histogram_queries}")
    print(f"    Empty widgets: {empty_widgets}")
    
    if errors:
        print(f"    Errors: {len(errors)}")
        for e in errors:
            print(f"      - {e}")
    
    if warnings:
        print(f"    Warnings: {len(warnings)}")
        for w in warnings[:10]:
            print(f"      - {w}")
        if len(warnings) > 10:
            print(f"      ... and {len(warnings) - 10} more")
    
    return len(errors) == 0


def count_grafana_expressions(grafana_json):
    """Count total expressions in Grafana dashboard."""
    count = 0
    division_count = 0
    
    def count_exprs(panels):
        nonlocal count, division_count
        for panel in panels:
            for target in panel.get('targets', []):
                expr = target.get('expr', '')
                if expr and expr.strip():
                    count += 1
                    if '/' in expr:
                        division_count += 1
            if 'panels' in panel:
                count_exprs(panel['panels'])
    
    count_exprs(grafana_json.get('panels', []))
    return count, division_count


def count_datadog_queries(datadog_json):
    """Count total queries in Datadog dashboard."""
    count = 0
    
    def count_queries(widgets):
        nonlocal count
        for widget in widgets:
            definition = widget.get('definition', {})
            if definition.get('type') == 'group':
                count_queries(definition.get('widgets', []))
            else:
                for req in definition.get('requests', []):
                    count += len(req.get('queries', []))
    
    count_queries(datadog_json.get('widgets', []))
    return count


def main():
    # Allow command line arguments for input/output files
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]
        # Generate output filename based on input
        if input_file.endswith('.json'):
            output_file = input_file.replace('.json', '_datadog_converted.json')
        else:
            output_file = input_file + '_datadog_converted.json'
    else:
        input_file = "/Users/wenyihu/go/src/github.com/cockroachdb/cockroach/grafana.json"
        output_file = "/Users/wenyihu/go/src/github.com/cockroachdb/cockroach/datadog_dashboard_converted.json"
    
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    
    print("=" * 70)
    print("GRAFANA TO DATADOG DASHBOARD CONVERTER")
    print("=" * 70)
    print(f"\nInput:  {input_file}")
    print(f"Output: {output_file}")
    print(f"\nDatadog query configuration:")
    print(f"  Metric prefix: {METRIC_PREFIX}")
    print(f"  Default tags: {DEFAULT_TAGS}")
    print(f"  Rollup interval: {ROLLUP_INTERVAL}s")
    print(f"  Counter format: sum:metric{{tags}} by {{group}}.as_rate().rollup({ROLLUP_INTERVAL})")
    print(f"  Gauge format: avg:metric{{tags}} by {{group}}")
    print(f"  Histogram format: pXX:metric{{tags}} by {{group}}")
    
    # Load Grafana dashboard
    print(f"\n" + "-" * 70)
    print("LOADING GRAFANA DASHBOARD")
    print("-" * 70)
    
    try:
        with open(input_file, 'r') as f:
            grafana_json = json.load(f)
        print(f"✓ Successfully loaded: {input_file}")
    except FileNotFoundError:
        print(f"✗ ERROR: File not found: {input_file}")
        return 1
    except json.JSONDecodeError as e:
        print(f"✗ ERROR: Invalid JSON in input file: {e}")
        return 1
    
    print(f"  Dashboard title: {grafana_json.get('title', 'Unknown')}")
    print(f"  Total panels: {len(grafana_json.get('panels', []))}")
    
    # Count Grafana expressions
    grafana_expr_count, division_count = count_grafana_expressions(grafana_json)
    print(f"  Total expressions: {grafana_expr_count}")
    print(f"  Division expressions: {division_count}")
    
    # Convert
    print(f"\n" + "-" * 70)
    print("CONVERTING PANELS")
    print("-" * 70)
    
    datadog_json = convert_grafana_to_datadog(grafana_json)
    
    # Count widgets
    def count_widgets(widgets):
        count = 0
        for w in widgets:
            if w.get("definition", {}).get("type") == "group":
                count += 1 + count_widgets(w["definition"].get("widgets", []))
            else:
                count += 1
        return count
    
    total_widgets = count_widgets(datadog_json.get("widgets", []))
    print(f"\nTotal Datadog widgets: {total_widgets}")
    
    # Validate conversion
    is_valid = validate_conversion(datadog_json)
    
    # Write output
    print(f"\n" + "-" * 70)
    print("WRITING OUTPUT")
    print("-" * 70)
    
    with open(output_file, 'w') as f:
        json.dump(datadog_json, f, indent=2)
    print(f"✓ Dashboard written to: {output_file}")
    
    # Validate JSON
    print(f"\n" + "-" * 70)
    print("FINAL VALIDATION")
    print("-" * 70)
    
    json_valid = False
    try:
        with open(output_file, 'r') as f:
            json.load(f)
        json_valid = True
        print(f"✓ JSON validation: PASSED")
    except json.JSONDecodeError as e:
        print(f"✗ JSON validation: FAILED - {e}")
    
    # Query count comparison
    datadog_query_count = count_datadog_queries(datadog_json)
    
    print(f"\n  Query Count Comparison:")
    print(f"    Grafana expressions:  {grafana_expr_count}")
    print(f"    Datadog queries:      {datadog_query_count}")
    
    if datadog_query_count >= grafana_expr_count:
        extra = datadog_query_count - grafana_expr_count
        print(f"    ✓ All expressions converted! (+{extra} from division expressions)")
    else:
        missing = grafana_expr_count - datadog_query_count
        print(f"    ✗ WARNING: {missing} expressions may not have been converted")
    
    # Final summary
    print(f"\n" + "=" * 70)
    print("CONVERSION SUMMARY")
    print("=" * 70)
    
    all_passed = json_valid and is_valid and (datadog_query_count >= grafana_expr_count)
    
    if all_passed:
        print("✓ CONVERSION SUCCESSFUL")
        print(f"  - JSON is valid")
        print(f"  - All {grafana_expr_count} Grafana expressions converted to {datadog_query_count} Datadog queries")
        print(f"  - Output: {output_file}")
    else:
        print("✗ CONVERSION COMPLETED WITH WARNINGS")
        if not json_valid:
            print("  - JSON validation failed")
        if not is_valid:
            print("  - Dashboard validation had errors")
        if datadog_query_count < grafana_expr_count:
            print(f"  - Missing {grafana_expr_count - datadog_query_count} queries")
    
    print("=" * 70)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
