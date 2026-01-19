#!/usr/bin/env python3
"""
Generate Datadog dashboard JSON from CockroachDB metrics.

This script can generate dashboards from:
1. Go source files with metric.Metadata{} definitions
2. Metric prefix filters against metrics.yaml

Default YAML: docs/generated/metrics/metrics.yaml (used if --yaml not specified)

Usage:
    # From Go file:
    python3 go_metrics_to_datadog.py <go_file.go> [-o output.json]
    
    # From metric prefix (uses default metrics.yaml):
    python3 go_metrics_to_datadog.py --prefix <prefix> [-o output.json]
    python3 go_metrics_to_datadog.py --prefix mma
    python3 go_metrics_to_datadog.py --prefix sql.service
    
    # With custom yaml file:
    python3 go_metrics_to_datadog.py --prefix mma --yaml /path/to/metrics.yaml

Examples:
    # Generate dashboard from Go file
    python3 go_metrics_to_datadog.py pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go
    
    # Generate dashboard for all metrics starting with 'mma.'
    python3 go_metrics_to_datadog.py --prefix mma
    
    # Generate dashboard for all metrics starting with 'sql.service'
    python3 go_metrics_to_datadog.py --prefix sql.service
    
    # Multiple prefixes
    python3 go_metrics_to_datadog.py --prefix mma,rebalancing
    
    # Custom output name
    python3 go_metrics_to_datadog.py --prefix mma -o my_dashboard.json
"""

import json
import re
import sys
import os
from dataclasses import dataclass
from typing import List, Dict, Optional

# yaml is optional - we have a fallback parser
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# Datadog configuration
METRIC_PREFIX = "cockroachdb"
DEFAULT_TAGS = "$cluster,$node_id"  # No spaces - Datadog is sensitive to this
ROLLUP_INTERVAL = 30

# Default metrics.yaml path (relative to script or repo root)
DEFAULT_YAML_PATH = "docs/generated/metrics/metrics.yaml"


@dataclass
class MetricDef:
    """Represents a metric definition."""
    name: str
    help: str
    measurement: str
    unit: str
    metric_type: str  # 'counter', 'gauge', 'histogram'
    labeled_name: Optional[str] = None
    static_labels: Optional[Dict[str, str]] = None
    field_name: Optional[str] = None  # The Go struct field name (if from Go file)


def parse_go_metric_metadata(content: str) -> List[MetricDef]:
    """
    Parse Go source code to extract metric.Metadata definitions.
    
    Looks for patterns like:
        metaFooBar = metric.Metadata{
            Name:        "foo.bar",
            Help:        "Description here",
            Measurement: "Count",
            Unit:        metric.Unit_COUNT,
        }
    """
    metrics = []
    
    # Pattern to match metric.Metadata blocks
    metadata_pattern = re.compile(
        r'(\w+)\s*=\s*metric\.Metadata\{([^}]+(?:\{[^}]*\}[^}]*)*)\}',
        re.MULTILINE | re.DOTALL
    )
    
    for match in metadata_pattern.finditer(content):
        var_name = match.group(1)
        block_content = match.group(2)
        
        # Extract fields from the block
        name_match = re.search(r'Name:\s*"([^"]+)"', block_content)
        help_match = re.search(r'Help:\s*"([^"]+)"', block_content)
        if not help_match:
            help_match = re.search(r'Help:\s*"([^"]+)"(?:\s*\+\s*"([^"]+)")*', block_content)
        measurement_match = re.search(r'Measurement:\s*"([^"]+)"', block_content)
        unit_match = re.search(r'Unit:\s*metric\.(\w+)', block_content)
        labeled_name_match = re.search(r'LabeledName:\s*"([^"]+)"', block_content)
        
        if name_match:
            name = name_match.group(1)
            help_text = help_match.group(1) if help_match else ""
            measurement = measurement_match.group(1) if measurement_match else ""
            unit = unit_match.group(1) if unit_match else "Unit_COUNT"
            labeled_name = labeled_name_match.group(1) if labeled_name_match else None
            
            # Extract static labels if present
            static_labels = {}
            labels_match = re.search(r'StaticLabels:\s*metric\.MakeLabelPairs\(([^)]+)\)', block_content)
            if labels_match:
                labels_str = labels_match.group(1)
                label_parts = re.findall(r'metric\.Label(\w+),\s*"([^"]+)"', labels_str)
                for label_key, label_val in label_parts:
                    static_labels[label_key.lower()] = label_val
            
            metrics.append(MetricDef(
                name=name,
                help=help_text,
                measurement=measurement,
                unit=unit,
                metric_type='unknown',
                labeled_name=labeled_name,
                static_labels=static_labels if static_labels else None,
                field_name=var_name
            ))
    
    return metrics


def determine_metric_types(content: str, metrics: List[MetricDef]) -> None:
    """
    Determine metric types (counter, gauge, histogram) by looking at
    how they're instantiated in the Go code.
    """
    for metric in metrics:
        if re.search(rf'metric\.NewCounter\({metric.field_name}\)', content):
            metric.metric_type = 'counter'
        elif re.search(rf'metric\.NewGauge\({metric.field_name}\)', content):
            metric.metric_type = 'gauge'
        elif re.search(rf'metric\.NewHistogram\({metric.field_name}', content):
            metric.metric_type = 'histogram'
        elif re.search(rf'metric\.NewLatency\({metric.field_name}', content):
            metric.metric_type = 'histogram'
        else:
            if 'NANOSECONDS' in metric.unit or 'SECONDS' in metric.unit:
                metric.metric_type = 'histogram'
            else:
                metric.metric_type = 'gauge'


def parse_metrics_yaml_simple(content: str) -> Dict[str, dict]:
    """
    Simple regex-based parser for metrics.yaml that doesn't require PyYAML.
    Extracts metric definitions by looking for the pattern of fields.
    """
    metrics_map = {}
    
    # Split by '- name:' to get individual metric blocks
    # Each metric block starts with '    - name:' (indented)
    blocks = re.split(r'\n\s*- name:', content)
    
    for block in blocks[1:]:  # Skip first empty block
        lines = block.split('\n')
        
        # First line is the metric name
        name = lines[0].strip()
        
        metric_def = {'name': name}
        
        # Parse other fields
        for line in lines[1:]:
            line = line.strip()
            if not line or line.startswith('- name:'):
                break
            
            # Match key: value patterns
            match = re.match(r'(\w+):\s*(.+)', line)
            if match:
                key = match.group(1)
                value = match.group(2).strip()
                # Remove quotes if present
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                elif value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                metric_def[key] = value
        
        if 'name' in metric_def and 'type' in metric_def:
            metrics_map[metric_def['name']] = metric_def
    
    return metrics_map


def load_metrics_yaml(yaml_path: str) -> Dict[str, dict]:
    """Load metrics.yaml and return a dict mapping metric names to their definitions."""
    metrics_map = {}
    
    try:
        with open(yaml_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"ERROR: Could not read {yaml_path}: {e}")
        return metrics_map
    
    # Try PyYAML first if available
    if YAML_AVAILABLE:
        try:
            data = yaml.safe_load(content)
            
            def extract_metrics(obj):
                if isinstance(obj, dict):
                    if 'name' in obj and 'type' in obj:
                        metrics_map[obj['name']] = obj
                    for v in obj.values():
                        extract_metrics(v)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_metrics(item)
            
            extract_metrics(data)
            return metrics_map
        except Exception as e:
            print(f"Warning: PyYAML parsing failed, using fallback parser: {e}")
    
    # Fallback to simple regex-based parser
    metrics_map = parse_metrics_yaml_simple(content)
    
    return metrics_map


def filter_metrics_by_prefix(yaml_metrics: Dict[str, dict], prefixes: List[str]) -> List[MetricDef]:
    """
    Filter metrics from YAML by prefix and convert to MetricDef objects.
    
    Args:
        yaml_metrics: Dict of metric name -> metric definition from YAML
        prefixes: List of prefixes to filter by (e.g., ['mma', 'sql.service'])
    
    Returns:
        List of MetricDef objects matching the prefixes
    """
    metrics = []
    
    for name, yaml_def in sorted(yaml_metrics.items()):
        # Check if metric matches any prefix
        matches = False
        for prefix in prefixes:
            if name.startswith(prefix + '.') or name == prefix:
                matches = True
                break
        
        if not matches:
            continue
        
        # Convert YAML type to our metric type
        yaml_type = yaml_def.get('type', 'GAUGE')
        if yaml_type == 'COUNTER':
            metric_type = 'counter'
        elif yaml_type == 'HISTOGRAM':
            metric_type = 'histogram'
        else:
            metric_type = 'gauge'
        
        # Get description
        description = yaml_def.get('description', '')
        
        # Get unit
        unit = yaml_def.get('unit', 'COUNT')
        
        metrics.append(MetricDef(
            name=name,
            help=description,
            measurement=yaml_def.get('y_axis_label', ''),
            unit=unit,
            metric_type=metric_type,
            labeled_name=yaml_def.get('labeled_name'),
            static_labels=None,
            field_name=None
        ))
    
    return metrics


def convert_metric_name(name: str) -> str:
    """Convert metric name to Datadog format."""
    return f"{METRIC_PREFIX}.{name}"


def build_query(metric: MetricDef) -> str:
    """Build a Datadog query for a metric based on its type."""
    dd_name = convert_metric_name(metric.name)
    tags = DEFAULT_TAGS
    
    if metric.metric_type == 'counter':
        # Use as_rate() for counters to show per-second rate
        return f"sum:{dd_name}{{{tags}}} by {{node_id}}.as_rate()"
    elif metric.metric_type == 'histogram':
        # Use p99 for histograms
        return f"p99:{dd_name}{{{tags}}} by {{node_id}}"
    else:
        # Use avg for gauges
        return f"avg:{dd_name}{{{tags}}} by {{node_id}}"


def create_widget(metric: MetricDef, index: int) -> dict:
    """Create a Datadog timeseries widget for a metric."""
    query = build_query(metric)
    
    # Create a readable title from the metric name
    title = metric.name.replace('.', ' ').replace('_', ' ').title()
    if metric.static_labels:
        label_str = ', '.join(f"{k}={v}" for k, v in metric.static_labels.items())
        title = f"{title} ({label_str})"
    
    widget = {
        "definition": {
            "type": "timeseries",
            "title": title,
            "title_size": "16",
            "requests": [
                {
                    "response_format": "timeseries",
                    "queries": [
                        {
                            "data_source": "metrics",
                            "name": f"q{index}",
                            "query": query
                        }
                    ],
                    "formulas": [
                        {"formula": f"q{index}"}
                    ],
                    "display_type": "line"
                }
            ]
        }
    }
    
    return widget


def group_metrics_by_prefix(metrics: List[MetricDef], depth: int = 2) -> Dict[str, List[MetricDef]]:
    """Group metrics by their name prefix for organizing into dashboard groups."""
    groups = {}
    for metric in metrics:
        parts = metric.name.split('.')
        if len(parts) >= depth:
            group_key = '.'.join(parts[:depth])
        else:
            group_key = parts[0]
        
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(metric)
    
    return groups


def create_dashboard(metrics: List[MetricDef], title: str) -> dict:
    """Create a complete Datadog dashboard from a list of metrics."""
    
    # Group metrics by top-level prefix only (depth=1)
    # This groups all "mma.*" metrics together and all "rebalancing.*" metrics together
    groups = group_metrics_by_prefix(metrics, depth=1)
    
    widgets = []
    
    for group_name, group_metrics in sorted(groups.items()):
        group_widgets = []
        for i, metric in enumerate(group_metrics):
            widget = create_widget(metric, i)
            group_widgets.append(widget)
        
        group_title = group_name.replace('.', ' ').replace('_', ' ').title()
        
        widgets.append({
            "definition": {
                "type": "group",
                "layout_type": "ordered",
                "title": group_title,
                "show_title": True,
                "widgets": group_widgets
            }
        })
    
    dashboard = {
        "title": title,
        "description": f"Auto-generated dashboard for {title}",
        "layout_type": "ordered",
        "is_read_only": False,
        "template_variables": [
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
            }
        ],
        "widgets": widgets
    }
    
    return dashboard


def print_usage():
    """Print usage information."""
    print(__doc__)


def main():
    if len(sys.argv) < 2:
        print_usage()
        return 1
    
    go_file = None
    yaml_file = None
    output_file = None
    prefixes = []
    
    # Parse arguments
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--yaml' and i + 1 < len(args):
            yaml_file = args[i + 1]
            i += 2
        elif args[i] == '--prefix' and i + 1 < len(args):
            # Support comma-separated prefixes
            prefixes.extend(args[i + 1].split(','))
            i += 2
        elif args[i] == '-o' and i + 1 < len(args):
            output_file = args[i + 1]
            i += 2
        elif args[i] in ['--help', '-h']:
            print_usage()
            return 0
        elif not args[i].startswith('-'):
            if go_file is None and args[i].endswith('.go'):
                go_file = args[i]
            elif output_file is None:
                output_file = args[i]
            i += 1
        else:
            print(f"Unknown argument: {args[i]}")
            print_usage()
            return 1
    
    # Validate arguments
    if not go_file and not prefixes:
        print("ERROR: Must specify either a Go file or --prefix")
        print_usage()
        return 1
    
    # Use default YAML path if not specified and using --prefix
    if prefixes and not yaml_file:
        yaml_file = DEFAULT_YAML_PATH
        if not os.path.exists(yaml_file):
            print(f"ERROR: Default YAML file not found: {yaml_file}")
            print("  Specify --yaml with a valid path to metrics.yaml")
            return 1
    
    # Generate output filename if not provided
    if not output_file:
        if prefixes:
            base_name = '_'.join(prefixes).replace('.', '_')
            output_file = f"{base_name}_datadog_dashboard.json"
        else:
            base_name = os.path.splitext(os.path.basename(go_file))[0]
            output_file = f"{base_name}_datadog_dashboard.json"
    
    print("=" * 70)
    print("METRICS TO DATADOG DASHBOARD GENERATOR")
    print("=" * 70)
    
    if go_file:
        print(f"\nInput Go file: {go_file}")
    if prefixes:
        print(f"Metric prefixes: {', '.join(prefixes)}")
    if yaml_file:
        print(f"Metrics YAML:  {yaml_file}")
    print(f"Output:        {output_file}")
    
    metrics = []
    
    # Mode 1: From prefix filter
    if prefixes:
        print(f"\n" + "-" * 70)
        print("LOADING METRICS FROM YAML")
        print("-" * 70)
        
        yaml_metrics = load_metrics_yaml(yaml_file)
        if not yaml_metrics:
            return 1
        
        print(f"  Loaded {len(yaml_metrics)} total metrics from YAML")
        
        metrics = filter_metrics_by_prefix(yaml_metrics, prefixes)
        print(f"  Found {len(metrics)} metrics matching prefix(es): {', '.join(prefixes)}")
        
        if not metrics:
            print(f"\nERROR: No metrics found matching prefix(es): {', '.join(prefixes)}")
            print("\nAvailable metric prefixes (first 20):")
            seen_prefixes = set()
            for name in sorted(yaml_metrics.keys())[:100]:
                parts = name.split('.')
                if len(parts) >= 2:
                    p = f"{parts[0]}.{parts[1]}"
                else:
                    p = parts[0]
                seen_prefixes.add(p)
            for p in sorted(seen_prefixes)[:20]:
                print(f"  - {p}")
            return 1
    
    # Mode 2: From Go file
    elif go_file:
        print(f"\n" + "-" * 70)
        print("PARSING GO FILE")
        print("-" * 70)
        
        try:
            with open(go_file, 'r') as f:
                content = f.read()
            print(f"✓ Loaded {go_file}")
        except FileNotFoundError:
            print(f"✗ ERROR: File not found: {go_file}")
            return 1
        
        metrics = parse_go_metric_metadata(content)
        print(f"  Found {len(metrics)} metric definitions")
        
        # Determine metric types from Go code
        determine_metric_types(content, metrics)
        
        # Optionally enrich with YAML data
        if yaml_file:
            print(f"\n" + "-" * 70)
            print("ENRICHING WITH YAML DATA")
            print("-" * 70)
            yaml_metrics = load_metrics_yaml(yaml_file)
            print(f"  Loaded {len(yaml_metrics)} metrics from YAML")
            
            for metric in metrics:
                if metric.name in yaml_metrics:
                    yaml_def = yaml_metrics[metric.name]
                    if yaml_def.get('type') == 'COUNTER':
                        metric.metric_type = 'counter'
                    elif yaml_def.get('type') == 'GAUGE':
                        metric.metric_type = 'gauge'
                    elif yaml_def.get('type') == 'HISTOGRAM':
                        metric.metric_type = 'histogram'
    
    if not metrics:
        print("ERROR: No metrics found")
        return 1
    
    # Count by type
    counter_count = sum(1 for m in metrics if m.metric_type == 'counter')
    gauge_count = sum(1 for m in metrics if m.metric_type == 'gauge')
    histogram_count = sum(1 for m in metrics if m.metric_type == 'histogram')
    
    print(f"\n  Metric types:")
    print(f"    - Counters: {counter_count}")
    print(f"    - Gauges: {gauge_count}")
    print(f"    - Histograms: {histogram_count}")
    
    # Print discovered metrics
    print(f"\n" + "-" * 70)
    print("DISCOVERED METRICS")
    print("-" * 70)
    for metric in metrics[:50]:  # Limit to first 50 for readability
        type_indicator = {'counter': 'C', 'gauge': 'G', 'histogram': 'H'}.get(metric.metric_type, '?')
        print(f"  [{type_indicator}] {metric.name}")
        if metric.static_labels:
            print(f"      Labels: {metric.static_labels}")
    if len(metrics) > 50:
        print(f"  ... and {len(metrics) - 50} more")
    
    # Generate dashboard title
    if prefixes:
        dashboard_title = ', '.join(prefixes).replace('.', ' ').replace('_', ' ').title() + " Metrics"
    else:
        base_name = os.path.splitext(os.path.basename(go_file))[0]
        dashboard_title = base_name.replace('_', ' ').replace('-', ' ').title() + " Metrics"
    
    # Create dashboard
    print(f"\n" + "-" * 70)
    print("GENERATING DASHBOARD")
    print("-" * 70)
    
    dashboard = create_dashboard(metrics, dashboard_title)
    
    # Count widgets
    total_widgets = sum(len(g['definition']['widgets']) for g in dashboard['widgets'])
    print(f"  Dashboard title: {dashboard_title}")
    print(f"  Groups: {len(dashboard['widgets'])}")
    print(f"  Total widgets: {total_widgets}")
    
    # Write output
    print(f"\n" + "-" * 70)
    print("WRITING OUTPUT")
    print("-" * 70)
    
    with open(output_file, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    print(f"✓ Dashboard written to: {output_file}")
    
    # =========================================================================
    # FINAL VALIDATION
    # =========================================================================
    print(f"\n" + "-" * 70)
    print("FINAL VALIDATION")
    print("-" * 70)
    
    validation_passed = True
    
    # 1. Validate JSON is valid
    json_valid = False
    try:
        with open(output_file, 'r') as f:
            loaded_dashboard = json.load(f)
        json_valid = True
        print(f"✓ JSON validation: PASSED")
    except json.JSONDecodeError as e:
        print(f"✗ JSON validation: FAILED - {e}")
        validation_passed = False
        loaded_dashboard = None
    
    # 2. Count queries in the output dashboard
    output_query_count = 0
    output_widget_count = 0
    
    def count_queries_in_dashboard(dashboard_data):
        """Count all queries in the dashboard."""
        nonlocal output_query_count, output_widget_count
        
        widgets = dashboard_data.get('widgets', [])
        for widget in widgets:
            definition = widget.get('definition', {})
            widget_type = definition.get('type', '')
            
            if widget_type == 'group':
                # Recursively count queries in group widgets
                nested_widgets = definition.get('widgets', [])
                for nested in nested_widgets:
                    nested_def = nested.get('definition', {})
                    output_widget_count += 1
                    for req in nested_def.get('requests', []):
                        output_query_count += len(req.get('queries', []))
            else:
                output_widget_count += 1
                for req in definition.get('requests', []):
                    output_query_count += len(req.get('queries', []))
    
    if loaded_dashboard:
        count_queries_in_dashboard(loaded_dashboard)
    
    # 3. Compare input metrics count with output query count
    input_metric_count = len(metrics)
    
    print(f"\n  Query Count Comparison:")
    print(f"    Input metrics:    {input_metric_count}")
    print(f"    Output queries:   {output_query_count}")
    print(f"    Output widgets:   {output_widget_count}")
    
    if output_query_count == input_metric_count:
        print(f"    ✓ Query count matches input metrics!")
    elif output_query_count > input_metric_count:
        print(f"    ⚠ More queries than input metrics (+{output_query_count - input_metric_count})")
    else:
        print(f"    ✗ MISSING QUERIES: {input_metric_count - output_query_count} metrics not converted")
        validation_passed = False
    
    # 4. Verify all metrics have corresponding queries
    if loaded_dashboard:
        # Extract all metric names from queries
        output_metrics = set()
        
        def extract_metrics_from_dashboard(dashboard_data):
            widgets = dashboard_data.get('widgets', [])
            for widget in widgets:
                definition = widget.get('definition', {})
                if definition.get('type') == 'group':
                    for nested in definition.get('widgets', []):
                        for req in nested.get('definition', {}).get('requests', []):
                            for q in req.get('queries', []):
                                query = q.get('query', '')
                                # Extract metric name from query
                                match = re.search(r'cockroachdb\.([a-zA-Z0-9._]+)\{', query)
                                if match:
                                    output_metrics.add(match.group(1))
                else:
                    for req in definition.get('requests', []):
                        for q in req.get('queries', []):
                            query = q.get('query', '')
                            match = re.search(r'cockroachdb\.([a-zA-Z0-9._]+)\{', query)
                            if match:
                                output_metrics.add(match.group(1))
        
        extract_metrics_from_dashboard(loaded_dashboard)
        
        # Check for missing metrics
        input_metric_names = {m.name for m in metrics}
        missing_metrics = input_metric_names - output_metrics
        extra_metrics = output_metrics - input_metric_names
        
        if missing_metrics:
            print(f"\n    ✗ Missing metrics in output ({len(missing_metrics)}):")
            for m in sorted(missing_metrics)[:10]:
                print(f"        - {m}")
            if len(missing_metrics) > 10:
                print(f"        ... and {len(missing_metrics) - 10} more")
            validation_passed = False
        else:
            print(f"    ✓ All input metrics present in output")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print(f"\n" + "=" * 70)
    print("CONVERSION SUMMARY")
    print("=" * 70)
    
    if validation_passed:
        print("✓ CONVERSION SUCCESSFUL - All validations passed!")
    else:
        print("✗ CONVERSION COMPLETED WITH WARNINGS")
    
    print(f"\n  Input:")
    if prefixes:
        print(f"    - Prefix filter: {', '.join(prefixes)}")
    if go_file:
        print(f"    - Go file: {go_file}")
    if yaml_file:
        print(f"    - YAML file: {yaml_file}")
    
    print(f"\n  Results:")
    print(f"    - Input metrics:  {input_metric_count}")
    print(f"    - Output queries: {output_query_count}")
    print(f"    - Output widgets: {output_widget_count}")
    print(f"    - JSON valid:     {'Yes' if json_valid else 'No'}")
    print(f"    - Output file:    {output_file}")
    
    print("=" * 70)
    
    # Print sample queries
    print(f"\nSample queries generated:")
    for metric in metrics[:5]:
        print(f"  {build_query(metric)}")
    if len(metrics) > 5:
        print(f"  ... and {len(metrics) - 5} more")
    
    return 0 if validation_passed else 1


if __name__ == "__main__":
    sys.exit(main())
