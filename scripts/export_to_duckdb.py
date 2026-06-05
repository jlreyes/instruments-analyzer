#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "duckdb>=1.0.0",
#   "lxml>=5.0.0",
#   "numpy>=1.24.0",
#   "pyarrow>=14.0.0",
# ]
# ///
"""
Export xctrace performance data to DuckDB/Parquet for analysis.

Uses Parquet as the storage format for 10-100x better compression than SQLite.
Creates a DuckDB database that references Parquet files for fast queries.

Usage:
  ./scripts/export_to_duckdb.py traces/recording.trace output.duckdb
"""

import argparse
import io
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

try:
    import duckdb
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"Error: Missing dependency: {e}", file=sys.stderr)
    print("Install with: pip install duckdb pyarrow numpy", file=sys.stderr)
    sys.exit(1)

from lxml import etree as ET


# Schema definitions with PyArrow types for efficient Parquet writing
SCHEMAS = {
    'updates': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('view_name', pa.string(), nullable=True),
            pa.field('description', pa.string(), nullable=True),
            pa.field('update_type', pa.string(), nullable=True),
            pa.field('severity', pa.string(), nullable=True),
            pa.field('module', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('allocations', pa.int32(), nullable=True),
            pa.field('cause_graph_node_id', pa.string(), nullable=True),
            pa.field('root_causes', pa.string(), nullable=True),
            pa.field('view_hierarchy', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'duration_ns', 'severity'],
    },
    'update_groups': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('label', pa.string(), nullable=True),
            pa.field('backtrace_json', pa.string(), nullable=True),
            pa.field('cause_backtrace_json', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'duration_ns'],
    },
    'hitches': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('narrative_description', pa.string(), nullable=True),
            pa.field('is_system', pa.bool_(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'duration_ns', 'swap_id'],
    },
    'hitches_updates': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('surface_id', pa.int32(), nullable=True),
            pa.field('is_hitch', pa.bool_(), nullable=True),
            pa.field('frame_color', pa.string(), nullable=True),
            pa.field('containment_level', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id', 'is_hitch'],
    },
    'hitches_renders': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('surface_id', pa.int32(), nullable=True),
            pa.field('is_hitch', pa.bool_(), nullable=True),
            pa.field('frame_color', pa.string(), nullable=True),
            pa.field('containment_level', pa.int32(), nullable=True),
            pa.field('offscreen_passes', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id', 'is_hitch'],
    },
    'hitches_gpu': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('surface_id', pa.int32(), nullable=True),
            pa.field('is_hitch', pa.bool_(), nullable=True),
            pa.field('frame_color', pa.string(), nullable=True),
            pa.field('containment_level', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id', 'is_hitch'],
    },
    'hitches_frame_lifetimes': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('surface_id', pa.int32(), nullable=True),
            pa.field('frame_color', pa.string(), nullable=True),
            pa.field('layout_qualifier', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id'],
    },
    'hitches_framewait': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('surface_id', pa.int32(), nullable=True),
            pa.field('frame_color', pa.string(), nullable=True),
            pa.field('layout_qualifier', pa.int32(), nullable=True),
            pa.field('swap_label', pa.string(), nullable=True),
            pa.field('display', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id'],
    },
    'time_profile': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.int64()),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('core_id', pa.int32(), nullable=True),
            pa.field('thread_state', pa.string(), nullable=True),
            pa.field('weight_ns', pa.int64(), nullable=True),
            pa.field('backtrace_json', pa.string(), nullable=True),
        ]),
        'indexes': ['time_ns', 'thread_tid'],
    },
    'potential_hangs': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('hang_type', pa.string(), nullable=True),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('process_name', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'duration_ns', 'hang_type'],
    },
    'runloop_intervals': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('interval_type', pa.string(), nullable=True),
            pa.field('interval_id', pa.string(), nullable=True),
            pa.field('nesting_level', pa.int32(), nullable=True),
            pa.field('containment_level', pa.int32(), nullable=True),
            pa.field('mode', pa.string(), nullable=True),
            pa.field('is_main', pa.bool_(), nullable=True),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('runloop_ptr', pa.int64(), nullable=True),
            pa.field('timeout', pa.string(), nullable=True),  # STRING: values can exceed int64
            pa.field('run_result', pa.string(), nullable=True),
            pa.field('label', pa.string(), nullable=True),
            pa.field('color', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'interval_type', 'is_main'],
    },
    'runloop_events': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.int64()),
            pa.field('event_type', pa.string(), nullable=True),
            pa.field('interval_type', pa.string(), nullable=True),
            pa.field('interval_id', pa.string(), nullable=True),
            pa.field('nesting_level', pa.int32(), nullable=True),
            pa.field('mode', pa.string(), nullable=True),
            pa.field('is_main', pa.bool_(), nullable=True),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('runloop_ptr', pa.int64(), nullable=True),
            pa.field('timeout', pa.string(), nullable=True),  # STRING: values can exceed int64
            pa.field('other_arg', pa.string(), nullable=True),  # STRING: values can exceed int64
        ]),
        'indexes': ['time_ns', 'interval_type'],
    },
    'coreanimation_context_intervals': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('phase', pa.string(), nullable=True),
            pa.field('layer_count', pa.int32(), nullable=True),
            pa.field('context_addr', pa.int64(), nullable=True),
        ]),
        'indexes': ['start_ns', 'phase'],
    },
    'coreanimation_layer_intervals': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('phase', pa.string(), nullable=True),
            pa.field('context_addr', pa.int64(), nullable=True),
            pa.field('layer_addr', pa.int64(), nullable=True),
            pa.field('layout_id', pa.int32(), nullable=True),
        ]),
        'indexes': ['start_ns', 'phase'],
    },
    'life_cycle_periods': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.int64()),
            pa.field('duration_ns', pa.int64()),
            pa.field('group_name', pa.string(), nullable=True),
            pa.field('period', pa.string(), nullable=True),
            pa.field('narrative', pa.string(), nullable=True),
        ]),
        'indexes': ['start_ns', 'period'],
    },
    'coreanimation_lifetime_intervals': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.uint64()),  # Use uint64 for timestamps
            pa.field('duration_ns', pa.uint64()),  # Use uint64 for durations
            pa.field('display_id', pa.int32(), nullable=True),
            pa.field('lifetime_id', pa.int32(), nullable=True),
            pa.field('swap_id', pa.int32(), nullable=True),
            pa.field('frame_seed', pa.int32(), nullable=True),
            pa.field('hitch_duration_ns', pa.uint64(), nullable=True),  # Use uint64
            pa.field('acceptable_latency_ns', pa.uint64(), nullable=True),  # Use uint64
            pa.field('hid_latency_ns', pa.uint64(), nullable=True),  # Use uint64
            pa.field('render_start_ns', pa.uint64(), nullable=True),  # Use uint64
            pa.field('render_duration_ns', pa.uint64(), nullable=True),  # Use uint64
            pa.field('layout_qualifier', pa.int32(), nullable=True),
            pa.field('type_label', pa.string(), nullable=True),
            pa.field('narrative', pa.string(), nullable=True),
            pa.field('severity', pa.string(), nullable=True),
            pa.field('color', pa.int32(), nullable=True),
        ]),
        'indexes': ['start_ns', 'swap_id', 'severity'],
    },
    'os_signpost_intervals': {
        # XML column order: start(0), duration(1), layout-qualifier(2), name(3), category(4),
        # subsystem(5), identifier(6), process(7), end-process(8), start-thread(9), end-thread(10),
        # start-message(11), end-message(12), start-backtrace(13), end-backtrace(14),
        # start-emit-location(15), end-emit-location(16), signature(17)
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('start_ns', pa.uint64()),
            pa.field('duration_ns', pa.uint64()),
            pa.field('layout_qualifier', pa.int32(), nullable=True),
            pa.field('name', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('subsystem', pa.string(), nullable=True),
            pa.field('identifier', pa.string(), nullable=True),  # STRING: can be large uint64
            pa.field('start_process_pid', pa.int32(), nullable=True),
            pa.field('end_process_pid', pa.int32(), nullable=True),
            pa.field('start_thread_tid', pa.int32(), nullable=True),
            pa.field('end_thread_tid', pa.int32(), nullable=True),
            pa.field('start_message', pa.string(), nullable=True),
            pa.field('end_message', pa.string(), nullable=True),
            pa.field('start_backtrace_json', pa.string(), nullable=True),
            pa.field('end_backtrace_json', pa.string(), nullable=True),
            pa.field('start_emit_location', pa.string(), nullable=True),
            pa.field('end_emit_location', pa.string(), nullable=True),
            pa.field('signature', pa.string(), nullable=True),  # Combined message
        ]),
        'indexes': ['start_ns', 'name', 'category'],
    },
    'os_signpost': {
        # XML column order: time(0), thread(1), process(2), event-type(3), scope(4),
        # identifier(5), name(6), format-string(7), backtrace(8), subsystem(9),
        # category(10), message(11), emit-location(12)
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.uint64()),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('process_pid', pa.int32(), nullable=True),
            pa.field('event_type', pa.string(), nullable=True),  # Begin/End/Event
            pa.field('scope', pa.string(), nullable=True),  # Process/System
            pa.field('identifier', pa.string(), nullable=True),  # STRING: can be large uint64
            pa.field('name', pa.string(), nullable=True),
            pa.field('format_string', pa.string(), nullable=True),
            pa.field('backtrace_json', pa.string(), nullable=True),
            pa.field('subsystem', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('message', pa.string(), nullable=True),
            pa.field('emit_location', pa.string(), nullable=True),
        ]),
        'indexes': ['time_ns', 'name', 'event_type'],
    },
    'os_signpost_arg': {
        # XML column order: time(0), format-string(1), identifier(2), signpost-name(3),
        # name(4), thread(5), subsystem(6), category(7), value(8)
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.uint64()),
            pa.field('format_string', pa.string(), nullable=True),
            pa.field('identifier', pa.string(), nullable=True),  # STRING: can be large uint64
            pa.field('signpost_name', pa.string(), nullable=True),
            pa.field('arg_name', pa.string(), nullable=True),  # "arg0", "arg1", etc.
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('subsystem', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('value', pa.string(), nullable=True),  # Polymorphic - always STRING
        ]),
        'indexes': ['time_ns', 'identifier', 'arg_name'],
    },
    'os_log': {
        # XML column order: time(0), thread(1), process(2), message-type(3), format-string(4),
        # backtrace(5), subsystem(6), category(7), message(8), emit-location(9)
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.uint64()),
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('process_pid', pa.int32(), nullable=True),
            pa.field('message_type', pa.string(), nullable=True),  # Default/Info/Debug/Error/Fault
            pa.field('format_string', pa.string(), nullable=True),
            pa.field('backtrace_json', pa.string(), nullable=True),
            pa.field('subsystem', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('message', pa.string(), nullable=True),
            pa.field('emit_location', pa.string(), nullable=True),
        ]),
        'indexes': ['time_ns', 'message_type', 'category'],
    },
    'os_log_arg': {
        # XML column order: time(0), format-string(1), name(2), thread(3),
        # subsystem(4), category(5), value(6)
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.uint64()),
            pa.field('format_string', pa.string(), nullable=True),
            pa.field('arg_name', pa.string(), nullable=True),  # "arg0", "arg1", etc.
            pa.field('thread_tid', pa.int32(), nullable=True),
            pa.field('subsystem', pa.string(), nullable=True),
            pa.field('category', pa.string(), nullable=True),
            pa.field('value', pa.string(), nullable=True),  # Polymorphic - always STRING
        ]),
        'indexes': ['time_ns', 'arg_name'],
    },
    'swiftui_causes': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.int64()),
            pa.field('source_event_id', pa.int32(), nullable=True),
            pa.field('source_description', pa.string(), nullable=True),
            pa.field('destination_event_id', pa.int32(), nullable=True),
            pa.field('destination_description', pa.string(), nullable=True),
            pa.field('label', pa.string(), nullable=True),
            pa.field('value_type', pa.string(), nullable=True),
            pa.field('changed_properties', pa.string(), nullable=True),
        ]),
        'indexes': ['time_ns', 'source_event_id', 'destination_event_id'],
    },
    'swiftui_changes': {
        'arrow_schema': pa.schema([
            pa.field('id', pa.int64()),
            pa.field('time_ns', pa.int64()),
            pa.field('change_id', pa.int32(), nullable=True),
            pa.field('description', pa.string(), nullable=True),
            pa.field('backtrace_json', pa.string(), nullable=True),
            pa.field('thread_tid', pa.int32(), nullable=True),
        ]),
        'indexes': ['time_ns', 'change_id'],
    },
}


FRAME_REF_MAP_KEY = '__frame_refs__'
BINARY_REF_MAP_KEY = '__binary_refs__'
BACKTRACE_REF_MAP_KEY = '__backtrace_refs__'


def build_ref_map(xml_source) -> dict:
    """Build map of id -> fmt attribute for reference resolution.

    Instruments also de-duplicates backtrace frames and binaries with id/ref
    attributes. Those are stored in nested maps so parse_backtrace can resolve
    frame refs without changing the fmt ref behavior used by the table parsers.
    """
    ref_map = {
        FRAME_REF_MAP_KEY: {},
        BINARY_REF_MAP_KEY: {},
        BACKTRACE_REF_MAP_KEY: {},
    }
    if hasattr(xml_source, 'seek'):
        xml_source.seek(0)
    for event, elem in ET.iterparse(xml_source, events=['end']):
        if 'id' in elem.attrib and 'fmt' in elem.attrib:
            ref_map[elem.attrib['id']] = elem.attrib['fmt']

        if elem.tag == 'binary' and 'id' in elem.attrib and 'ref' not in elem.attrib:
            binary_data = binary_data_from_element(elem)
            if binary_data:
                ref_map[BINARY_REF_MAP_KEY][elem.attrib['id']] = binary_data

        if elem.tag == 'frame' and 'id' in elem.attrib and 'ref' not in elem.attrib:
            ref_map[FRAME_REF_MAP_KEY][elem.attrib['id']] = frame_data_from_element(elem, ref_map)

        if elem.tag == 'backtrace' and 'id' in elem.attrib and 'ref' not in elem.attrib:
            ref_map[BACKTRACE_REF_MAP_KEY][elem.attrib['id']] = backtrace_data_from_element(elem, ref_map)

        if elem.tag == 'row':
            elem.clear()
    if hasattr(xml_source, 'seek'):
        xml_source.seek(0)
    return ref_map


def resolve_fmt(elem, ref_map: dict) -> str:
    if 'ref' in elem.attrib:
        return ref_map.get(elem.attrib['ref'], '')
    return elem.attrib.get('fmt', elem.text or '')


def parse_duration_ns(elem, ref_map: dict[str, str]) -> int:
    if elem.text and elem.text.strip().isdigit():
        return int(elem.text.strip())
    fmt = resolve_fmt(elem, ref_map)
    if not fmt:
        return 0
    try:
        parts = fmt.split()
        if len(parts) >= 2:
            value = float(parts[0])
            unit = parts[1].lower()
            if unit == 'ns':
                return int(value)
            elif unit in ('µs', 'us'):
                return int(value * 1_000)
            elif unit == 'ms':
                return int(value * 1_000_000)
            elif unit == 's':
                return int(value * 1_000_000_000)
    except (ValueError, IndexError):
        pass
    return 0


def parse_start_ns(elem, ref_map: dict[str, str]) -> int:
    if elem.text and elem.text.strip().isdigit():
        return int(elem.text.strip())
    fmt = resolve_fmt(elem, ref_map)
    if fmt and fmt.replace('.', '').isdigit():
        return int(float(fmt) * 1_000_000_000)
    return 0


def parse_boolean(elem, ref_map: dict[str, str]) -> bool:
    if elem.text and elem.text.strip().isdigit():
        return bool(int(elem.text.strip()))
    fmt = resolve_fmt(elem, ref_map)
    return fmt.lower() in ('yes', 'true', '1')


def parse_uint32(elem, ref_map: dict[str, str]) -> int:
    if elem.text and elem.text.strip().isdigit():
        return int(elem.text.strip())
    fmt = resolve_fmt(elem, ref_map)
    try:
        return int(fmt)
    except ValueError:
        return 0


def parse_uint64(elem, ref_map: dict[str, str]) -> int:
    """Parse uint64 values, returns None if exceeds uint64 max."""
    if elem.text and elem.text.strip().isdigit():
        val = int(elem.text.strip())
    else:
        fmt = resolve_fmt(elem, ref_map)
        try:
            val = int(fmt)
        except ValueError:
            return 0

    # Check if value exceeds uint64 max (2^64 - 1)
    UINT64_MAX = 18446744073709551615
    if val > UINT64_MAX:
        print(f"WARNING: Value {val} exceeds UINT64_MAX, setting to NULL", file=sys.stderr)
        return None  # Value too large for uint64
    return val


def parse_uint64_as_string(elem, ref_map: dict[str, str]) -> str:
    """Parse large integers as strings (no data loss, can be cast to HUGEINT in DuckDB)."""
    if elem.text and elem.text.strip().isdigit():
        return elem.text.strip()
    fmt = resolve_fmt(elem, ref_map)
    try:
        # Validate it's a number
        int(fmt)
        return fmt
    except ValueError:
        return None


def parse_ns_as_string(elem, ref_map: dict[str, str]) -> str:
    """Parse nanosecond values as strings to handle large timestamps - no int conversion."""
    # Get raw text value directly without int conversion
    if elem.text and elem.text.strip().isdigit():
        text_val = elem.text.strip()
        # Sanity check: reject astronomical values (> 1000 years in ns)
        try:
            if len(text_val) > 20:  # Anything > 10^20 ns is ~3170 years, clearly wrong
                return None
        except:
            pass
        return text_val

    # For formatted references, extract the numeric part
    fmt = resolve_fmt(elem, ref_map)
    if not fmt:
        return None

    # If it's already a number string, return it
    if fmt.replace('.', '').replace('-', '').replace(':', '').isdigit():
        # Convert from seconds to nanoseconds if it contains a decimal
        if '.' in fmt and ':' not in fmt:
            try:
                result = str(int(float(fmt) * 1_000_000_000))
                # Sanity check
                if len(result) > 20:
                    return None
                return result
            except (ValueError, OverflowError):
                return None
        # If it has colons, it's a timestamp format - skip for now
        if ':' in fmt:
            return None
        return fmt if len(fmt) <= 20 else None

    # Try to extract just the numeric part from formatted strings like "3.5 s"
    try:
        parts = fmt.split()
        if len(parts) >= 2:
            value = float(parts[0])
            unit = parts[1].lower()
            result_ns = None
            if unit == 'ns':
                result_ns = int(value)
            elif unit in ('µs', 'us'):
                result_ns = int(value * 1_000)
            elif unit == 'ms':
                result_ns = int(value * 1_000_000)
            elif unit == 's':
                result_ns = int(value * 1_000_000_000)
            elif unit in ('min', 'minute', 'minutes'):
                # Reject durations > 1 hour (likely corrupted from Instruments)
                if value > 60:
                    return None
                result_ns = int(value * 60 * 1_000_000_000)

            if result_ns is not None:
                # Sanity check: reject values > 1000 years
                if result_ns > 31536000000000000000:  # 10^18 * 31.536
                    return None
                return str(result_ns)
    except (ValueError, IndexError, OverflowError):
        pass

    return None


def parse_duration_ns_as_string(elem, ref_map: dict[str, str]) -> str:
    """Parse duration values as strings - delegates to parse_ns_as_string."""
    return parse_ns_as_string(elem, ref_map)


def safe_int64(val):
    """Ensure integer fits in int64 range for Parquet, return None if too large or None."""
    if val is None:
        return None
    # int64 is signed 64-bit: -(2^63) to (2^63)-1
    INT64_MAX = 9223372036854775807
    INT64_MIN = -9223372036854775808
    if val > INT64_MAX or val < INT64_MIN:
        return None
    return val


def binary_data_from_element(binary) -> dict:
    """Extract exported binary metadata into the JSON frame field shape."""
    binary_name = binary.attrib.get('name', '')
    binary_path = binary.attrib.get('path', '')
    load_addr = binary.attrib.get('load-addr', '')

    return {
        'binary': binary_name,
        'path': binary_path,
        'load_addr': load_addr,
    }


def collect_binary_info(binary_data: dict, binary_info: dict[str, dict] = None) -> None:
    """Collect binary metadata for symbolication if requested."""
    if binary_info is None:
        return

    binary_name = binary_data.get('binary', '')
    binary_path = binary_data.get('path', '')
    load_addr = binary_data.get('load_addr', '')

    if binary_name and binary_path and load_addr and binary_name not in binary_info:
        binary_info[binary_name] = {
            'path': binary_path,
            'load_addr': load_addr
        }


def resolve_binary_data(binary, ref_map: dict, binary_info: dict[str, dict] = None) -> dict:
    """Resolve a frame's binary element, including Instruments binary refs."""
    if binary is None:
        return {}

    if 'ref' in binary.attrib:
        binary_data = ref_map.get(BINARY_REF_MAP_KEY, {}).get(binary.attrib['ref'])
        if binary_data is not None:
            resolved = dict(binary_data)
            collect_binary_info(resolved, binary_info)
            return resolved
        return {'binary': ref_map.get(binary.attrib['ref'], '')}

    binary_data = binary_data_from_element(binary)
    collect_binary_info(binary_data, binary_info)
    return binary_data


def frame_data_from_element(frame, ref_map: dict, binary_info: dict[str, dict] = None) -> dict:
    """Extract one backtrace frame, resolving its nested binary metadata."""
    frame_data = {
        'name': frame.attrib.get('name', ''),
        'addr': frame.attrib.get('addr', ''),
    }
    frame_data.update(resolve_binary_data(frame.find('binary'), ref_map, binary_info))
    return frame_data


def resolve_frame_data(frame, ref_map: dict, binary_info: dict[str, dict] = None) -> dict:
    """Resolve a frame element, including Instruments frame refs."""
    frame_refs = ref_map.get(FRAME_REF_MAP_KEY, {})

    if 'ref' in frame.attrib:
        frame_data = frame_refs.get(frame.attrib['ref'])
        if frame_data is not None:
            resolved = dict(frame_data)
            collect_binary_info(resolved, binary_info)
            return resolved
        return {'name': '', 'addr': ''}

    frame_data = frame_data_from_element(frame, ref_map, binary_info)
    if 'id' in frame.attrib:
        frame_refs[frame.attrib['id']] = dict(frame_data)
    return frame_data


def backtrace_data_from_element(elem, ref_map: dict, binary_info: dict[str, dict] = None) -> list[dict]:
    """Resolve a backtrace element into an ordered list of frame data."""
    return [
        resolve_frame_data(frame, ref_map, binary_info)
        for frame in elem.findall('.//frame')
    ]


def parse_backtrace(elem, ref_map: dict, binary_info: dict[str, dict] = None) -> str:
    """Parse backtrace XML to JSON, capturing binary info for later symbolication.

    Args:
        elem: The backtrace XML element
        ref_map: Map of ref IDs to values
        binary_info: Optional dict to populate with binary info for symbolication.
                    Keys are binary names, values are dicts with 'path' and 'load_addr'.
    """
    backtrace_refs = ref_map.get(BACKTRACE_REF_MAP_KEY, {})

    if 'ref' in elem.attrib:
        frames = [dict(frame) for frame in backtrace_refs.get(elem.attrib['ref'], [])]
    else:
        frames = backtrace_data_from_element(elem, ref_map, binary_info)
        if 'id' in elem.attrib:
            backtrace_refs[elem.attrib['id']] = [dict(frame) for frame in frames]

    if binary_info is not None:
        for frame in frames:
            collect_binary_info(frame, binary_info)

    return json.dumps(frames) if frames else ''


def symbolicate_address(addr: str, binary_path: str, load_addr: str) -> str:
    """Symbolicate a single address using atos.

    Args:
        addr: The address to symbolicate (hex string like '0x107654000')
        binary_path: Path to the binary
        load_addr: Load address of the binary (hex string)

    Returns:
        Symbolicated name or original address if symbolication fails
    """
    import subprocess
    try:
        result = subprocess.run(
            ['atos', '-o', binary_path, '-l', load_addr, addr],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            symbol = result.stdout.strip()
            # atos returns the address itself if it can't symbolicate
            if symbol and not symbol.startswith('0x'):
                return symbol
    except Exception:
        pass
    return addr


def batch_symbolicate(addrs: list[str], binary_path: str, load_addr: str) -> dict[str, str]:
    """Batch symbolicate multiple addresses using atos.

    Args:
        addrs: List of addresses to symbolicate (hex strings)
        binary_path: Path to the binary
        load_addr: Load address of the binary (hex string)

    Returns:
        Dict mapping original addresses to symbolicated names
    """
    import subprocess
    import os

    if not os.path.exists(binary_path):
        return {}

    results = {}
    try:
        # atos can take multiple addresses on stdin
        input_text = '\n'.join(addrs)
        result = subprocess.run(
            ['atos', '-o', binary_path, '-l', load_addr],
            input=input_text,
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for addr, symbol in zip(addrs, lines):
                symbol = symbol.strip()
                # atos returns the address itself if it can't symbolicate
                if symbol and not symbol.startswith('0x'):
                    results[addr] = symbol
    except Exception as e:
        print(f"Warning: batch symbolication failed for {binary_path}: {e}", file=sys.stderr)
    return results


def extract_polymorphic_value(elem, ref_map: dict[str, str]) -> str:
    """Extract value from polymorphic XML element, returning as string.

    Handles various Instruments XML types: uint64, string, fixed-decimal, address, etc.
    Always returns a string representation to avoid type overflow issues.
    """
    if elem is None:
        return None

    tag = elem.tag
    if tag == 'sentinel':
        return None
    elif tag in ('uint64', 'uint64-hex-lowercase', 'int64', 'uint32', 'int32'):
        # Numeric types - prefer raw text value, fallback to fmt attribute
        if elem.text and elem.text.strip():
            return elem.text.strip()
        return resolve_fmt(elem, ref_map) or None
    elif tag in ('string', 'signpost-name', 'subsystem', 'category', 'narrative-text'):
        return resolve_fmt(elem, ref_map) or None
    elif tag == 'fixed-decimal':
        # Keep decimal representation from fmt attribute
        return resolve_fmt(elem, ref_map) or None
    elif tag == 'address':
        if elem.text and elem.text.strip():
            return elem.text.strip()
        return resolve_fmt(elem, ref_map) or None
    elif tag == 'duration':
        # Duration formatted like "81.79 µs"
        return resolve_fmt(elem, ref_map) or None
    else:
        # Fallback: try fmt attribute, then text
        fmt = resolve_fmt(elem, ref_map)
        if fmt:
            return fmt
        if elem.text and elem.text.strip():
            return elem.text.strip()
        return None


def extract_identifier_as_string(elem, ref_map: dict[str, str]) -> str:
    """Extract os-signpost-identifier as a string to handle large uint64 values.

    These identifiers can be very large (e.g., 17216892719917625070) and overflow int64.
    We store them as strings and let DuckDB cast to HUGEINT if needed for queries.
    """
    if elem is None or elem.tag == 'sentinel':
        return None

    # Try raw text first (the actual number)
    if elem.text and elem.text.strip():
        return elem.text.strip()

    # Fallback to fmt attribute
    fmt = resolve_fmt(elem, ref_map)
    if fmt:
        # fmt might be "OS_SIGNPOST_ID_EXCLUSIVE" or similar - keep as-is
        return fmt

    return None


def export_schema_to_parquet(schema_name: str, trace_path: str, output_dir: str) -> tuple[str, str, int]:
    """Export a schema from trace to Parquet file. Returns (table_name, parquet_path, row_count)."""
    # Map schema names to table names
    table_map = {
        'swiftui-updates': 'updates',
        'swiftui-update-groups': 'update_groups',
        'swiftui-causes': 'swiftui_causes',
        'swiftui-changes': 'swiftui_changes',
        'hitches': 'hitches',
        'hitches-updates': 'hitches_updates',
        'hitches-renders': 'hitches_renders',
        'hitches-gpu': 'hitches_gpu',
        'hitches-frame-lifetimes': 'hitches_frame_lifetimes',
        'hitches-framewait': 'hitches_framewait',
        'time-profile': 'time_profile',
        'potential-hangs': 'potential_hangs',
        'runloop-intervals': 'runloop_intervals',
        'runloop-events': 'runloop_events',
        'coreanimation-context-interval': 'coreanimation_context_intervals',
        'coreanimation-layer-interval': 'coreanimation_layer_intervals',
        'coreanimation-lifetime-interval': 'coreanimation_lifetime_intervals',
        'life-cycle-period': 'life_cycle_periods',
        'os-signpost-interval': 'os_signpost_intervals',
        'os-signpost': 'os_signpost',
        'os-signpost-arg': 'os_signpost_arg',
        'os-log': 'os_log',
        'os-log-arg': 'os_log_arg',
    }
    table_name = table_map.get(schema_name, schema_name.replace('-', '_'))

    cmd = [
        'xcrun', 'xctrace', 'export',
        '--input', trace_path,
        '--xpath', f'/trace-toc/run[@number="1"]/data/table[@schema="{schema_name}"]'
    ]

    print(f"[{schema_name}] Exporting from trace...", file=sys.stderr)
    result = subprocess.run(cmd, capture_output=True, text=False)

    if result.returncode != 0 or len(result.stdout) < 100:
        print(f"[{schema_name}] Empty or failed", file=sys.stderr)
        return (table_name, '', 0)

    print(f"[{schema_name}] Parsing XML ({len(result.stdout) / 1024 / 1024:.1f} MB)...", file=sys.stderr)

    xml_stream = io.BytesIO(result.stdout)
    ref_map = build_ref_map(xml_stream)

    parquet_path = os.path.join(output_dir, f"{table_name}.parquet")
    row_count = 0

    # Route to appropriate parser
    if schema_name == 'swiftui-updates':
        row_count = parse_updates_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'swiftui-update-groups':
        row_count = parse_groups_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'swiftui-causes':
        row_count = parse_swiftui_causes_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'swiftui-changes':
        row_count = parse_swiftui_changes_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'hitches':
        row_count = parse_hitches_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name in ('hitches-updates', 'hitches-gpu'):
        row_count = parse_hitch_variant_to_parquet(xml_stream, ref_map, parquet_path, table_name, has_offscreen=False)
    elif schema_name == 'hitches-renders':
        row_count = parse_hitch_variant_to_parquet(xml_stream, ref_map, parquet_path, table_name, has_offscreen=True)
    elif schema_name in ('hitches-frame-lifetimes', 'hitches-framewait'):
        row_count = parse_frame_timing_to_parquet(xml_stream, ref_map, parquet_path, table_name)
    elif schema_name == 'time-profile':
        row_count = parse_time_profile_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'potential-hangs':
        row_count = parse_potential_hangs_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'runloop-intervals':
        row_count = parse_runloop_intervals_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'runloop-events':
        row_count = parse_runloop_events_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'coreanimation-context-interval':
        row_count = parse_coreanimation_context_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'coreanimation-layer-interval':
        row_count = parse_coreanimation_layer_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'coreanimation-lifetime-interval':
        row_count = parse_coreanimation_lifetime_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'life-cycle-period':
        row_count = parse_life_cycle_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'os-signpost-interval':
        row_count = parse_os_signpost_intervals_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'os-signpost':
        row_count = parse_os_signpost_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'os-signpost-arg':
        row_count = parse_os_signpost_arg_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'os-log':
        row_count = parse_os_log_to_parquet(xml_stream, ref_map, parquet_path)
    elif schema_name == 'os-log-arg':
        row_count = parse_os_log_arg_to_parquet(xml_stream, ref_map, parquet_path)

    if row_count > 0:
        size_mb = os.path.getsize(parquet_path) / 1024 / 1024
        print(f"[{schema_name}] Wrote {row_count:,} rows ({size_mb:.1f} MB)", file=sys.stderr)
    return (table_name, parquet_path, row_count)


def write_parquet(rows: list, schema: pa.Schema, path: str) -> int:
    """Write rows to Parquet file with ZSTD compression using explicit typed arrays."""
    if not rows:
        return 0

    # Transpose rows -> columns
    cols = list(zip(*rows))

    # Build typed arrays explicitly to avoid PyArrow inference bugs
    arrays = []
    for i, field in enumerate(schema):
        vals = list(cols[i])

        if pa.types.is_uint64(field.type):
            # Build uint64 explicitly via numpy to avoid int64 inference overflow
            filled = np.array([0 if v is None else v for v in vals], dtype=np.uint64)
            mask = np.array([v is None for v in vals], dtype=np.bool_)
            if mask.any():
                arrays.append(pa.array(filled, type=pa.uint64(), mask=mask))
            else:
                arrays.append(pa.array(filled, type=pa.uint64()))
        elif pa.types.is_int64(field.type):
            # Build int64 explicitly via numpy
            filled = np.array([0 if v is None else v for v in vals], dtype=np.int64)
            mask = np.array([v is None for v in vals], dtype=np.bool_)
            if mask.any():
                arrays.append(pa.array(filled, type=pa.int64(), mask=mask))
            else:
                arrays.append(pa.array(filled, type=pa.int64()))
        elif pa.types.is_int32(field.type):
            # Build int32 explicitly - clamp values that exceed range
            INT32_MAX = 2147483647
            INT32_MIN = -2147483648
            filled_list = []
            for v in vals:
                if v is None:
                    filled_list.append(0)
                elif v > INT32_MAX or v < INT32_MIN:
                    # Value exceeds int32 range - clamp to None (will be masked)
                    filled_list.append(0)
                else:
                    filled_list.append(v)

            filled = np.array(filled_list, dtype=np.int32)
            mask_list = [v is None or (v is not None and (v > INT32_MAX or v < INT32_MIN)) for v in vals]
            mask = np.array(mask_list, dtype=np.bool_)
            if mask.any():
                arrays.append(pa.array(filled, type=pa.int32(), mask=mask))
            else:
                arrays.append(pa.array(filled, type=pa.int32()))
        elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            # Strings can use direct construction
            arrays.append(pa.array(vals, type=field.type))
        elif pa.types.is_boolean(field.type):
            # Booleans
            arrays.append(pa.array(vals, type=pa.bool_()))
        else:
            # Fallback for other types
            arrays.append(pa.array(vals, type=field.type))

    # Build table from typed arrays (no inference!)
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, path, compression='zstd')
    return table.num_rows


def parse_updates_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse swiftui-updates XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 14:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            allocations = None
            try:
                if children[4].text and children[4].text.strip().isdigit():
                    allocations = int(children[4].text.strip())
            except (ValueError, IndexError):
                pass

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                resolve_fmt(children[9], ref_map) or None,  # view_name
                resolve_fmt(children[5], ref_map) or None,  # description
                resolve_fmt(children[3], ref_map) or None,  # update_type
                resolve_fmt(children[13], ref_map) if len(children) > 13 else None,  # severity
                resolve_fmt(children[8], ref_map) or None,  # module
                resolve_fmt(children[6], ref_map) or None,  # category
                allocations,
                children[14].attrib.get('id') if len(children) > 14 else None,  # cause_graph_node_id
                None,  # root_causes
                resolve_fmt(children[7], ref_map) or None,  # view_hierarchy
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['updates']['arrow_schema'], parquet_path)


def parse_groups_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse swiftui-update-groups XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 5:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            backtrace = None
            cause_backtrace = None
            if children[3].tag == 'backtrace':
                backtrace = parse_backtrace(children[3], ref_map) or None
            if len(children) > 4 and children[4].tag == 'backtrace':
                cause_backtrace = parse_backtrace(children[4], ref_map) or None

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                resolve_fmt(children[2], ref_map) or None,  # label
                backtrace,
                cause_backtrace,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['update_groups']['arrow_schema'], parquet_path)


def parse_hitches_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse hitches XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 7:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            swap_id = parse_uint32(children[4], ref_map)
            rows.append((
                row_id,
                start_ns,
                duration_ns,
                swap_id if swap_id else None,  # swap_id
                resolve_fmt(children[5], ref_map) or None,  # swap_label
                resolve_fmt(children[7], ref_map) if len(children) > 7 else None,  # narrative_description
                parse_boolean(children[3], ref_map),  # is_system
                resolve_fmt(children[6], ref_map) or None,  # display
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['hitches']['arrow_schema'], parquet_path)


def parse_hitch_variant_to_parquet(xml_stream, ref_map: dict, parquet_path: str, table_name: str, has_offscreen: bool) -> int:
    """Parse hitch variant (updates/renders/gpu) XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 8:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            containment_level = parse_uint32(children[7], ref_map)
            is_hitch = containment_level == 1
            swap_id = parse_uint32(children[4], ref_map)
            surface_id = parse_uint32(children[5], ref_map)

            if has_offscreen:
                offscreen = parse_uint32(children[9], ref_map) if len(children) > 9 else None
                rows.append((
                    row_id,
                    start_ns,
                    duration_ns,
                    swap_id if swap_id else None,  # swap_id
                    surface_id if surface_id else None,  # surface_id
                    is_hitch,
                    resolve_fmt(children[6], ref_map) or None,  # frame_color
                    containment_level if containment_level else None,
                    offscreen if offscreen else None,
                    resolve_fmt(children[8], ref_map) if len(children) > 8 else None,  # swap_label
                    resolve_fmt(children[3], ref_map) or None,  # display
                ))
            else:
                rows.append((
                    row_id,
                    start_ns,
                    duration_ns,
                    swap_id if swap_id else None,  # swap_id
                    surface_id if surface_id else None,  # surface_id
                    is_hitch,
                    resolve_fmt(children[6], ref_map) or None,  # frame_color
                    containment_level if containment_level else None,
                    resolve_fmt(children[8], ref_map) if len(children) > 8 else None,  # swap_label
                    resolve_fmt(children[3], ref_map) or None,  # display
                ))

        elem.clear()

    schema_name = 'hitches_renders' if has_offscreen else table_name
    return write_parquet(rows, SCHEMAS[schema_name]['arrow_schema'], parquet_path)


def parse_frame_timing_to_parquet(xml_stream, ref_map: dict, parquet_path: str, table_name: str) -> int:
    """Parse frame timing (lifetimes/framewait) XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 7:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            swap_id = parse_uint32(children[3], ref_map)
            surface_id = parse_uint32(children[4], ref_map)
            layout_qualifier = parse_uint32(children[6], ref_map)
            rows.append((
                row_id,
                start_ns,
                duration_ns,
                swap_id if swap_id else None,  # swap_id
                surface_id if surface_id else None,  # surface_id
                resolve_fmt(children[5], ref_map) or None,  # frame_color
                layout_qualifier if layout_qualifier else None,  # layout_qualifier
                resolve_fmt(children[7], ref_map) if len(children) > 7 else None,  # swap_label
                resolve_fmt(children[2], ref_map) or None,  # display
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS[table_name]['arrow_schema'], parquet_path)


def parse_time_profile_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse time-profile XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 6:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1
            thread_tid = None
            if children[1].tag == 'thread':
                tid_elem = children[1].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map) or None

            core_id = None
            if len(children) > 3 and children[3].tag == 'core':
                core_text = resolve_fmt(children[3], ref_map)
                if core_text:
                    parts = core_text.split()
                    if len(parts) >= 2:
                        try:
                            core_id = int(parts[-1])
                        except ValueError:
                            pass

            backtrace = None
            if len(children) > 6 and children[6].tag == 'backtrace':
                backtrace = parse_backtrace(children[6], ref_map) or None

            weight_ns = parse_duration_ns(children[5], ref_map) if len(children) > 5 else None

            rows.append((
                row_id,
                time_ns,
                thread_tid,
                core_id,
                resolve_fmt(children[4], ref_map) if len(children) > 4 else None,  # thread_state
                weight_ns if weight_ns else None,  # weight_ns
                backtrace,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['time_profile']['arrow_schema'], parquet_path)


def parse_potential_hangs_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse potential-hangs XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 4:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1
            thread_tid = None
            if children[3].tag == 'thread':
                tid_elem = children[3].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map) or None

            process_name = None
            if len(children) > 4 and children[4].tag == 'process':
                name_elem = children[4].find('.//process-name')
                if name_elem is not None:
                    process_name = resolve_fmt(name_elem, ref_map) or None

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                resolve_fmt(children[2], ref_map) or None,  # hang_type
                thread_tid,
                process_name,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['potential_hangs']['arrow_schema'], parquet_path)


def parse_runloop_intervals_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse runloop-intervals XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 8:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            # Extract thread TID
            thread_tid = None
            if len(children) > 8 and children[8].tag == 'thread':
                tid_elem = children[8].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse runloop pointer
            runloop_ptr = None
            if len(children) > 10 and children[10].tag == 'address':
                try:
                    addr_text = children[10].text
                    if addr_text:
                        runloop_ptr = int(addr_text.strip())
                except (ValueError, AttributeError):
                    pass

            # Parse timeout (store as string because values can exceed int64)
            timeout = None
            if len(children) > 11 and children[11].tag == 'uint64':
                timeout_val = children[11].text
                if timeout_val:
                    timeout = timeout_val.strip()

            # Parse run_result
            run_result = None
            if len(children) > 12 and children[12].tag != 'sentinel':
                run_result = resolve_fmt(children[12], ref_map) or None

            # Parse label
            label = None
            if len(children) > 16 and children[16].tag == 'string':
                label = resolve_fmt(children[16], ref_map) or None

            # Parse color
            color = None
            if len(children) > 17 and children[17].tag != 'sentinel':
                color = resolve_fmt(children[17], ref_map) or None

            interval_type = resolve_fmt(children[2], ref_map) if children[2].tag != 'sentinel' else None
            interval_id = resolve_fmt(children[3], ref_map) if len(children) > 3 and children[3].tag != 'sentinel' else None
            nesting_level = parse_uint32(children[4], ref_map) if len(children) > 4 else None
            containment_level = parse_uint32(children[5], ref_map) if len(children) > 5 else None
            mode = resolve_fmt(children[6], ref_map) if len(children) > 6 else None
            is_main = parse_boolean(children[7], ref_map) if len(children) > 7 else None

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                interval_type or None,
                interval_id or None,
                nesting_level,
                containment_level,
                mode or None,
                is_main,
                thread_tid,
                runloop_ptr,
                timeout,
                run_result,
                label,
                color,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['runloop_intervals']['arrow_schema'], parquet_path)


def parse_runloop_events_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse runloop-events XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 8:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Extract thread TID
            thread_tid = None
            if len(children) > 8 and children[8].tag == 'thread':
                tid_elem = children[8].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse address
            runloop_ptr = None
            if len(children) > 9 and children[9].tag == 'address':
                try:
                    addr_text = children[9].text
                    if addr_text:
                        runloop_ptr = int(addr_text.strip())
                except (ValueError, AttributeError):
                    pass

            # Parse timeout and other_arg as strings (values can exceed int64)
            timeout = None
            if len(children) > 10:
                timeout_val = children[10].text
                if timeout_val:
                    timeout = timeout_val.strip()

            other_arg = None
            if len(children) > 11:
                other_val = children[11].text
                if other_val:
                    other_arg = other_val.strip()

            event_type = resolve_fmt(children[3], ref_map) if len(children) > 3 else None
            interval_type = resolve_fmt(children[2], ref_map) if children[2].tag != 'sentinel' else None
            interval_id = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None
            nesting_level = parse_uint32(children[5], ref_map) if len(children) > 5 else None
            mode = resolve_fmt(children[6], ref_map) if len(children) > 6 else None
            is_main = parse_boolean(children[7], ref_map) if len(children) > 7 else None

            rows.append((
                row_id,
                time_ns,
                event_type or None,
                interval_type or None,
                interval_id or None,
                nesting_level,
                mode or None,
                is_main,
                thread_tid,
                runloop_ptr,
                timeout,
                other_arg,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['runloop_events']['arrow_schema'], parquet_path)


def parse_coreanimation_context_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse coreanimation-context-interval XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 6:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            # Extract thread TID
            thread_tid = None
            if len(children) > 3 and children[3].tag == 'thread':
                tid_elem = children[3].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            phase = resolve_fmt(children[4], ref_map) if len(children) > 4 else None

            # Parse layer count
            layer_count = None
            if len(children) > 5:
                count_text = children[5].text
                if count_text and count_text.strip().isdigit():
                    layer_count = int(count_text.strip())

            # Parse context address
            context_addr = None
            if len(children) > 6 and children[6].tag == 'address':
                try:
                    addr_text = children[6].text
                    if addr_text:
                        context_addr = int(addr_text.strip())
                except (ValueError, AttributeError):
                    pass

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                thread_tid,
                phase or None,
                layer_count,
                context_addr,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['coreanimation_context_intervals']['arrow_schema'], parquet_path)


def parse_coreanimation_layer_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse coreanimation-layer-interval XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 6:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            # Extract thread TID
            thread_tid = None
            if len(children) > 3 and children[3].tag == 'thread':
                tid_elem = children[3].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            phase = resolve_fmt(children[4], ref_map) if len(children) > 4 else None

            # Parse context address
            context_addr = None
            if len(children) > 5 and children[5].tag == 'address':
                try:
                    addr_text = children[5].text
                    if addr_text:
                        context_addr = int(addr_text.strip())
                except (ValueError, AttributeError):
                    pass

            # Parse layer address
            layer_addr = None
            if len(children) > 7 and children[7].tag == 'address':
                try:
                    addr_text = children[7].text
                    if addr_text:
                        layer_addr = int(addr_text.strip())
                except (ValueError, AttributeError):
                    pass

            # Parse layout ID
            layout_id = None
            if len(children) > 8:
                layout_id = parse_uint32(children[8], ref_map)

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                thread_tid,
                phase or None,
                context_addr,
                layer_addr,
                layout_id,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['coreanimation_layer_intervals']['arrow_schema'], parquet_path)


def parse_life_cycle_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse life-cycle-period XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 6:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[3], ref_map) if len(children) > 3 else 0

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            group_name = resolve_fmt(children[1], ref_map) if len(children) > 1 else None
            period = resolve_fmt(children[5], ref_map) if len(children) > 5 else None

            # Extract narrative text
            narrative = None
            if len(children) > 6:
                narrative_elem = children[6]
                if narrative_elem.tag == 'narrative':
                    narrative_text = narrative_elem.attrib.get('fmt', '')
                    if not narrative_text:
                        parts = []
                        for child in narrative_elem:
                            if child.tag == 'narrative-text':
                                parts.append(resolve_fmt(child, ref_map))
                            elif child.tag == 'duration':
                                parts.append(resolve_fmt(child, ref_map))
                            elif child.tag == 'app-period':
                                parts.append(resolve_fmt(child, ref_map))
                        narrative_text = ''.join(parts)
                    narrative = narrative_text or None

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                group_name or None,
                period or None,
                narrative,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['life_cycle_periods']['arrow_schema'], parquet_path)


def parse_coreanimation_lifetime_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse coreanimation-lifetime-interval XML to Parquet.

    XML row structure (16 elements):
        0: start-time
        1: duration
        2: display-id (uint32, sentinel 0xFFFFFFFF → NULL)
        3: lifetime-id (uint32) - Frame number
        4: swap-id (uint32, sentinel 0xFFFFFFFF → NULL)
        5: frame-seed (uint32, sentinel 0xFFFFFFFF → NULL)
        6: hitch-duration (duration or sentinel)
        7: acceptable-latency (duration or sentinel)
        8: hid-latency (duration or sentinel)
        9: render-start (start-time or sentinel)
        10: render-duration (duration or sentinel)
        11: layout-qualifier (layout-id)
        12: type-label (string or sentinel)
        13: narrative (formatted-label)
        14: severity (event-concept)
        15: color (render-buffer-depth or sentinel)
    """
    rows = []
    row_id = 0
    SENTINEL_UINT32 = 4294967295  # 0xFFFFFFFF

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 15:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            # Parse uint32 fields with sentinel handling (0xFFFFFFFF → NULL)
            display_id = parse_uint32(children[2], ref_map) if len(children) > 2 else None
            if display_id == SENTINEL_UINT32:
                display_id = None

            lifetime_id = parse_uint32(children[3], ref_map) if len(children) > 3 else None

            swap_id = parse_uint32(children[4], ref_map) if len(children) > 4 else None
            if swap_id == SENTINEL_UINT32:
                swap_id = None

            frame_seed = parse_uint32(children[5], ref_map) if len(children) > 5 else None
            if frame_seed == SENTINEL_UINT32:
                frame_seed = None

            # Parse optional duration fields (sentinel elements → NULL)
            hitch_duration_ns = None
            if len(children) > 6 and children[6].tag != 'sentinel':
                hitch_duration_ns = parse_duration_ns(children[6], ref_map)

            acceptable_latency_ns = None
            if len(children) > 7 and children[7].tag != 'sentinel':
                acceptable_latency_ns = parse_duration_ns(children[7], ref_map)

            hid_latency_ns = None
            if len(children) > 8 and children[8].tag != 'sentinel':
                hid_latency_ns = parse_duration_ns(children[8], ref_map)

            render_start_ns = None
            if len(children) > 9 and children[9].tag != 'sentinel':
                render_start_ns = parse_start_ns(children[9], ref_map)

            render_duration_ns = None
            if len(children) > 10 and children[10].tag != 'sentinel':
                render_duration_ns = parse_duration_ns(children[10], ref_map)

            # Parse layout qualifier
            layout_qualifier = None
            if len(children) > 11:
                layout_qualifier = parse_uint32(children[11], ref_map)

            # Parse type label (string or sentinel)
            type_label = None
            if len(children) > 12 and children[12].tag != 'sentinel':
                type_label = resolve_fmt(children[12], ref_map) or None

            # Parse narrative (formatted-label with complex structure)
            narrative = None
            if len(children) > 13:
                narrative_elem = children[13]
                if narrative_elem.tag == 'formatted-label':
                    # Try fmt attribute first
                    narrative_text = narrative_elem.attrib.get('fmt', '')
                    if not narrative_text:
                        # Concatenate child elements
                        parts = []
                        for child in narrative_elem:
                            if child.tag in ('gpu-frame-number', 'narrative-text', 'duration'):
                                parts.append(resolve_fmt(child, ref_map))
                        narrative_text = ''.join(parts)
                    narrative = narrative_text or None

            # Parse severity (event-concept)
            severity = None
            if len(children) > 14:
                severity = resolve_fmt(children[14], ref_map) or None

            # Parse color (render-buffer-depth or sentinel)
            color = None
            if len(children) > 15 and children[15].tag != 'sentinel':
                color = parse_uint32(children[15], ref_map)

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                display_id,
                lifetime_id,
                swap_id,
                frame_seed,
                hitch_duration_ns,
                acceptable_latency_ns,
                hid_latency_ns,
                render_start_ns,
                render_duration_ns,
                layout_qualifier,
                type_label,
                narrative,
                severity,
                color,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['coreanimation_lifetime_intervals']['arrow_schema'], parquet_path)


def parse_swiftui_causes_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse swiftui-causes XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 5:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            source_event_id = parse_uint32(children[1], ref_map) if len(children) > 1 else None
            destination_event_id = parse_uint32(children[3], ref_map) if len(children) > 3 else None

            # Extract source metadata description
            source_description = None
            if len(children) > 2 and children[2].tag == 'metadata':
                source_meta = children[2]
                first_string = source_meta.find('string')
                if first_string is not None:
                    source_description = resolve_fmt(first_string, ref_map) or None
                else:
                    source_description = source_meta.attrib.get('fmt', '') or None

            # Extract destination metadata description
            destination_description = None
            if len(children) > 4 and children[4].tag == 'metadata':
                dest_meta = children[4]
                first_string = dest_meta.find('string')
                if first_string is not None:
                    destination_description = resolve_fmt(first_string, ref_map) or None
                else:
                    destination_description = dest_meta.attrib.get('fmt', '') or None

            label = resolve_fmt(children[5], ref_map) if len(children) > 5 and children[5].tag != 'sentinel' else None
            value_type = resolve_fmt(children[6], ref_map) if len(children) > 6 and children[6].tag != 'sentinel' else None
            changed_properties = resolve_fmt(children[7], ref_map) if len(children) > 7 and children[7].tag != 'sentinel' else None

            rows.append((
                row_id,
                time_ns,
                source_event_id,
                source_description,
                destination_event_id,
                destination_description,
                label or None,
                value_type or None,
                changed_properties or None,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['swiftui_causes']['arrow_schema'], parquet_path)


def parse_swiftui_changes_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse swiftui-changes XML to Parquet."""
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 3:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Extract thread TID
            thread_tid = None
            if len(children) > 5 and children[5].tag == 'thread':
                tid_elem = children[5].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            change_id = parse_uint32(children[1], ref_map) if len(children) > 1 else None
            description = resolve_fmt(children[2], ref_map) if len(children) > 2 else None

            # Parse backtrace if present
            backtrace_json = None
            if len(children) > 3 and children[3].tag == 'backtrace':
                backtrace_json = parse_backtrace(children[3], ref_map) or None

            rows.append((
                row_id,
                time_ns,
                change_id,
                description or None,
                backtrace_json,
                thread_tid,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['swiftui_changes']['arrow_schema'], parquet_path)


def parse_os_signpost_intervals_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse os-signpost-interval XML to Parquet.

    XML column order:
        0: start (start-time)
        1: duration
        2: layout-qualifier (layout-id)
        3: name (signpost-name)
        4: category
        5: subsystem
        6: identifier (os-signpost-identifier - can be large uint64)
        7: process (start process)
        8: end-process
        9: start-thread
        10: end-thread
        11: start-message (os-log-metadata or sentinel)
        12: end-message (os-log-metadata or sentinel)
        13: start-backtrace (sentinel usually)
        14: end-backtrace (sentinel usually)
        15: start-emit-location (return-location)
        16: end-emit-location (return-location)
        17: signature (os-log-metadata - combined message)
    """
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 7:
            elem.clear()
            continue

        start_ns = parse_start_ns(children[0], ref_map)
        duration_ns = parse_duration_ns(children[1], ref_map)

        if duration_ns > 0 or start_ns > 0:
            row_id += 1

            # Parse layout-qualifier (index 2)
            layout_qualifier = parse_uint32(children[2], ref_map) if len(children) > 2 and children[2].tag != 'sentinel' else None

            # Parse signpost identity (indices 3-6)
            name = resolve_fmt(children[3], ref_map) if len(children) > 3 and children[3].tag != 'sentinel' else None
            category = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None
            subsystem = resolve_fmt(children[5], ref_map) if len(children) > 5 and children[5].tag != 'sentinel' else None

            # Parse identifier (index 6) - can be large uint64, store as string
            identifier = extract_identifier_as_string(children[6], ref_map) if len(children) > 6 else None

            # Parse start process (index 7)
            start_process_pid = None
            if len(children) > 7 and children[7].tag == 'process':
                pid_elem = children[7].find('.//pid')
                if pid_elem is not None:
                    start_process_pid = parse_uint32(pid_elem, ref_map)

            # Parse end process (index 8) - may be same ref as start process
            end_process_pid = None
            if len(children) > 8 and children[8].tag == 'process':
                pid_elem = children[8].find('.//pid')
                if pid_elem is not None:
                    end_process_pid = parse_uint32(pid_elem, ref_map)

            # Parse start thread (index 9)
            start_thread_tid = None
            if len(children) > 9 and children[9].tag == 'thread':
                tid_elem = children[9].find('.//tid')
                if tid_elem is not None:
                    start_thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse end thread (index 10)
            end_thread_tid = None
            if len(children) > 10 and children[10].tag == 'thread':
                tid_elem = children[10].find('.//tid')
                if tid_elem is not None:
                    end_thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse start-message (index 11) - os-log-metadata or sentinel
            start_message = None
            if len(children) > 11 and children[11].tag not in ('sentinel',):
                start_message = resolve_fmt(children[11], ref_map) or None

            # Parse end-message (index 12) - os-log-metadata or sentinel
            end_message = None
            if len(children) > 12 and children[12].tag not in ('sentinel',):
                end_message = resolve_fmt(children[12], ref_map) or None

            # Parse start-backtrace (index 13) - usually sentinel
            start_backtrace_json = None
            if len(children) > 13 and children[13].tag == 'backtrace':
                start_backtrace_json = parse_backtrace(children[13], ref_map) or None
            elif len(children) > 13 and children[13].tag == 'text-backtrace':
                start_backtrace_json = resolve_fmt(children[13], ref_map) or None

            # Parse end-backtrace (index 14) - usually sentinel
            end_backtrace_json = None
            if len(children) > 14 and children[14].tag == 'backtrace':
                end_backtrace_json = parse_backtrace(children[14], ref_map) or None
            elif len(children) > 14 and children[14].tag == 'text-backtrace':
                end_backtrace_json = resolve_fmt(children[14], ref_map) or None

            # Parse start-emit-location (index 15)
            start_emit_location = None
            if len(children) > 15 and children[15].tag == 'return-location':
                start_emit_location = resolve_fmt(children[15], ref_map) or None

            # Parse end-emit-location (index 16)
            end_emit_location = None
            if len(children) > 16 and children[16].tag == 'return-location':
                end_emit_location = resolve_fmt(children[16], ref_map) or None

            # Parse signature (index 17) - combined message
            signature = None
            if len(children) > 17 and children[17].tag not in ('sentinel',):
                signature = resolve_fmt(children[17], ref_map) or None

            rows.append((
                row_id,
                start_ns,
                duration_ns,
                layout_qualifier,
                name,
                category,
                subsystem,
                identifier,
                start_process_pid,
                end_process_pid,
                start_thread_tid,
                end_thread_tid,
                start_message,
                end_message,
                start_backtrace_json,
                end_backtrace_json,
                start_emit_location,
                end_emit_location,
                signature,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['os_signpost_intervals']['arrow_schema'], parquet_path)


def parse_os_signpost_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse os-signpost (point events) XML to Parquet.

    XML column order:
        0: time (event-time)
        1: thread
        2: process
        3: event-type (Begin/End/Event)
        4: scope (Process/System)
        5: identifier (os-signpost-identifier - can be large uint64)
        6: name (signpost-name)
        7: format-string (nullable/sentinel)
        8: backtrace (text-backtrace, nullable/sentinel)
        9: subsystem
        10: category
        11: message (os-log-metadata, nullable/sentinel)
        12: emit-location (return-location)
    """
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 7:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Parse thread (index 1)
            thread_tid = None
            if len(children) > 1 and children[1].tag == 'thread':
                tid_elem = children[1].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse process (index 2)
            process_pid = None
            if len(children) > 2 and children[2].tag == 'process':
                pid_elem = children[2].find('.//pid')
                if pid_elem is not None:
                    process_pid = parse_uint32(pid_elem, ref_map)

            # Parse event-type (index 3) - Begin/End/Event
            event_type = resolve_fmt(children[3], ref_map) if len(children) > 3 and children[3].tag != 'sentinel' else None

            # Parse scope (index 4) - Process/System
            scope = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None

            # Parse identifier (index 5) - can be large uint64, store as string
            identifier = extract_identifier_as_string(children[5], ref_map) if len(children) > 5 else None

            # Parse name (index 6) - signpost-name
            name = resolve_fmt(children[6], ref_map) if len(children) > 6 and children[6].tag != 'sentinel' else None

            # Parse format-string (index 7) - nullable
            format_string = None
            if len(children) > 7 and children[7].tag != 'sentinel':
                format_string = resolve_fmt(children[7], ref_map) or None

            # Parse backtrace (index 8) - nullable
            backtrace_json = None
            if len(children) > 8 and children[8].tag == 'text-backtrace':
                backtrace_json = resolve_fmt(children[8], ref_map) or None
            elif len(children) > 8 and children[8].tag == 'backtrace':
                backtrace_json = parse_backtrace(children[8], ref_map) or None

            # Parse subsystem (index 9)
            subsystem = resolve_fmt(children[9], ref_map) if len(children) > 9 and children[9].tag != 'sentinel' else None

            # Parse category (index 10)
            category = resolve_fmt(children[10], ref_map) if len(children) > 10 and children[10].tag != 'sentinel' else None

            # Parse message (index 11) - os-log-metadata, nullable
            message = None
            if len(children) > 11 and children[11].tag not in ('sentinel',):
                message = resolve_fmt(children[11], ref_map) or None

            # Parse emit-location (index 12)
            emit_location = None
            if len(children) > 12 and children[12].tag == 'return-location':
                emit_location = resolve_fmt(children[12], ref_map) or None

            rows.append((
                row_id,
                time_ns,
                thread_tid,
                process_pid,
                event_type,
                scope,
                identifier,
                name,
                format_string,
                backtrace_json,
                subsystem,
                category,
                message,
                emit_location,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['os_signpost']['arrow_schema'], parquet_path)


def parse_os_signpost_arg_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse os-signpost-arg (flattened signpost arguments) XML to Parquet.

    XML column order:
        0: time (event-time)
        1: format-string
        2: identifier (os-signpost-identifier - can be large uint64)
        3: signpost-name
        4: name (arg name like "arg0", "arg1", "transaction_seed")
        5: thread
        6: subsystem
        7: category
        8: value (polymorphic - can be uint64, string, fixed-decimal, etc.)
    """
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 5:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Parse format-string (index 1)
            format_string = resolve_fmt(children[1], ref_map) if len(children) > 1 and children[1].tag != 'sentinel' else None

            # Parse identifier (index 2) - can be large uint64, store as string
            identifier = extract_identifier_as_string(children[2], ref_map) if len(children) > 2 else None

            # Parse signpost-name (index 3)
            signpost_name = resolve_fmt(children[3], ref_map) if len(children) > 3 and children[3].tag != 'sentinel' else None

            # Parse arg name (index 4) - "arg0", "arg1", etc.
            arg_name = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None

            # Parse thread (index 5)
            thread_tid = None
            if len(children) > 5 and children[5].tag == 'thread':
                tid_elem = children[5].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse subsystem (index 6)
            subsystem = resolve_fmt(children[6], ref_map) if len(children) > 6 and children[6].tag != 'sentinel' else None

            # Parse category (index 7)
            category = resolve_fmt(children[7], ref_map) if len(children) > 7 and children[7].tag != 'sentinel' else None

            # Parse value (index 8) - polymorphic type, always store as string
            value = None
            if len(children) > 8:
                value = extract_polymorphic_value(children[8], ref_map)

            rows.append((
                row_id,
                time_ns,
                format_string,
                identifier,
                signpost_name,
                arg_name,
                thread_tid,
                subsystem,
                category,
                value,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['os_signpost_arg']['arrow_schema'], parquet_path)


def parse_os_log_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse os-log XML to Parquet.

    XML column order:
        0: time (event-time)
        1: thread
        2: process
        3: message-type (Default/Info/Debug/Error/Fault)
        4: format-string
        5: backtrace (sentinel usually)
        6: subsystem
        7: category
        8: message (os-log-metadata)
        9: emit-location (return-location)
    """
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 5:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Parse thread (index 1)
            thread_tid = None
            if len(children) > 1 and children[1].tag == 'thread':
                tid_elem = children[1].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse process (index 2)
            process_pid = None
            if len(children) > 2 and children[2].tag == 'process':
                pid_elem = children[2].find('.//pid')
                if pid_elem is not None:
                    process_pid = parse_uint32(pid_elem, ref_map)

            # Parse message-type (index 3) - Default/Info/Debug/Error/Fault
            message_type = resolve_fmt(children[3], ref_map) if len(children) > 3 and children[3].tag != 'sentinel' else None

            # Parse format-string (index 4)
            format_string = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None

            # Parse backtrace (index 5) - usually sentinel
            backtrace_json = None
            if len(children) > 5 and children[5].tag == 'backtrace':
                backtrace_json = parse_backtrace(children[5], ref_map) or None
            elif len(children) > 5 and children[5].tag == 'text-backtrace':
                backtrace_json = resolve_fmt(children[5], ref_map) or None

            # Parse subsystem (index 6)
            subsystem = resolve_fmt(children[6], ref_map) if len(children) > 6 and children[6].tag != 'sentinel' else None

            # Parse category (index 7)
            category = resolve_fmt(children[7], ref_map) if len(children) > 7 and children[7].tag != 'sentinel' else None

            # Parse message (index 8) - os-log-metadata
            message = None
            if len(children) > 8 and children[8].tag not in ('sentinel',):
                message = resolve_fmt(children[8], ref_map) or None

            # Parse emit-location (index 9)
            emit_location = None
            if len(children) > 9 and children[9].tag == 'return-location':
                emit_location = resolve_fmt(children[9], ref_map) or None

            rows.append((
                row_id,
                time_ns,
                thread_tid,
                process_pid,
                message_type,
                format_string,
                backtrace_json,
                subsystem,
                category,
                message,
                emit_location,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['os_log']['arrow_schema'], parquet_path)


def parse_os_log_arg_to_parquet(xml_stream, ref_map: dict, parquet_path: str) -> int:
    """Parse os-log-arg (flattened log arguments) XML to Parquet.

    XML column order:
        0: time (event-time)
        1: format-string
        2: name (arg name like "arg0", "arg1")
        3: thread
        4: subsystem
        5: category
        6: value (polymorphic - can be uint64, string, fixed-decimal, etc.)
    """
    rows = []
    row_id = 0

    for event, elem in ET.iterparse(xml_stream, events=['end']):
        if elem.tag != 'row':
            continue

        children = list(elem)
        if len(children) < 3:
            elem.clear()
            continue

        time_ns = parse_start_ns(children[0], ref_map)
        if time_ns > 0:
            row_id += 1

            # Parse format-string (index 1)
            format_string = resolve_fmt(children[1], ref_map) if len(children) > 1 and children[1].tag != 'sentinel' else None

            # Parse arg name (index 2) - "arg0", "arg1", etc.
            arg_name = resolve_fmt(children[2], ref_map) if len(children) > 2 and children[2].tag != 'sentinel' else None

            # Parse thread (index 3)
            thread_tid = None
            if len(children) > 3 and children[3].tag == 'thread':
                tid_elem = children[3].find('.//tid')
                if tid_elem is not None:
                    thread_tid = parse_uint32(tid_elem, ref_map)

            # Parse subsystem (index 4)
            subsystem = resolve_fmt(children[4], ref_map) if len(children) > 4 and children[4].tag != 'sentinel' else None

            # Parse category (index 5)
            category = resolve_fmt(children[5], ref_map) if len(children) > 5 and children[5].tag != 'sentinel' else None

            # Parse value (index 6) - polymorphic type, always store as string
            value = None
            if len(children) > 6:
                value = extract_polymorphic_value(children[6], ref_map)

            rows.append((
                row_id,
                time_ns,
                format_string,
                arg_name,
                thread_tid,
                subsystem,
                category,
                value,
            ))

        elem.clear()

    return write_parquet(rows, SCHEMAS['os_log_arg']['arrow_schema'], parquet_path)


def export_from_trace(trace_path: str, output_dir: str) -> dict[str, tuple[str, int]]:
    """Export all performance data from a .trace file to Parquet files."""
    results = {}  # table_name -> (parquet_path, row_count)

    schema_names = [
        'swiftui-updates',
        'swiftui-update-groups',
        'swiftui-causes',
        'swiftui-changes',
        'hitches',
        'hitches-updates',
        'hitches-renders',
        'hitches-gpu',
        'hitches-frame-lifetimes',
        'hitches-framewait',
        'time-profile',
        'potential-hangs',
        'runloop-intervals',
        'runloop-events',
        'coreanimation-context-interval',
        'coreanimation-layer-interval',
        'coreanimation-lifetime-interval',
        'life-cycle-period',
        'os-signpost-interval',
        'os-signpost',
        'os-signpost-arg',
        'os-log',
        'os-log-arg',
    ]

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Exporting {len(schema_names)} schemas to Parquet (parallel)...", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    max_workers = min(os.cpu_count() or 4, len(schema_names))
    print(f"Using {max_workers} parallel workers", file=sys.stderr)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(export_schema_to_parquet, schema_name, trace_path, output_dir): schema_name
            for schema_name in schema_names
        }

        for future in as_completed(futures):
            table_name, parquet_path, row_count = future.result()
            if parquet_path and row_count > 0:
                results[table_name] = (parquet_path, row_count)

    return results


def create_duckdb_views(conn: duckdb.DuckDBPyConnection, parquet_dir: str, tables: dict):
    """Create DuckDB views that reference Parquet files."""
    for table_name, (parquet_path, _) in tables.items():
        # Create view pointing to Parquet file
        conn.execute(f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

    # Create metadata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            created_at VARCHAR,
            created_by VARCHAR
        )
    """)


def main():
    parser = argparse.ArgumentParser(
        description='Export xctrace performance data to DuckDB/Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('trace', help='Input .trace file')
    parser.add_argument('output', help='Output DuckDB database path')

    args = parser.parse_args()

    trace_path = Path(args.trace)
    db_path = Path(args.output)

    if not trace_path.exists():
        print(f"Error: Trace file not found: {trace_path}", file=sys.stderr)
        sys.exit(1)

    # Create output directory for Parquet files (same name as db, without extension)
    # Use parents=True to create intermediate directories (e.g., traces/recording/)
    parquet_dir = db_path.parent / db_path.stem
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Remove existing DB
    if db_path.exists():
        db_path.unlink()

    print(f"Exporting {trace_path}...", file=sys.stderr)
    print(f"Parquet files: {parquet_dir}/", file=sys.stderr)
    print(f"DuckDB views:  {db_path}", file=sys.stderr)

    # Export to Parquet
    tables = export_from_trace(str(trace_path), str(parquet_dir))

    # Create DuckDB with views pointing to Parquet files
    print(f"\nCreating DuckDB database with views...", file=sys.stderr)
    conn = duckdb.connect(str(db_path))
    create_duckdb_views(conn, str(parquet_dir), tables)
    conn.execute("INSERT INTO metadata VALUES ('trace_file', ?)", (str(trace_path),))
    conn.execute("INSERT INTO metadata VALUES ('parquet_dir', ?)", (str(parquet_dir),))
    conn.close()

    # Summary
    total_rows = sum(count for _, count in tables.values())
    total_size = sum(os.path.getsize(path) for path, _ in tables.values()) / 1024 / 1024
    db_size = db_path.stat().st_size / 1024

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Export complete!", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Parquet files: {parquet_dir}/ ({total_size:.1f} MB total)", file=sys.stderr)
    print(f"DuckDB database: {db_path} ({db_size:.1f} KB)", file=sys.stderr)
    print(f"\nRows by table:", file=sys.stderr)
    for table, (path, count) in sorted(tables.items()):
        size = os.path.getsize(path) / 1024 / 1024
        print(f"  - {table}: {count:,} ({size:.1f} MB)", file=sys.stderr)
    print(f"\nTotal: {total_rows:,} rows", file=sys.stderr)

    print(f"\nUsage:", file=sys.stderr)
    print(f"  duckdb {db_path}", file=sys.stderr)
    print(f"  > SELECT * FROM updates ORDER BY duration_ns DESC LIMIT 10;", file=sys.stderr)


if __name__ == '__main__':
    main()
