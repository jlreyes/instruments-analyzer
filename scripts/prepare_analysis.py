#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "duckdb>=1.0.0",
# ]
# ///
"""
Prepare canonical analysis views and analyze frames for frame-first workflow.

Two modes:
1. Default mode: Creates persistent views for frame budget and canonical frames table
2. Frame analysis mode: Deep dive into a specific frame with cascade analysis

Usage:
  # Default mode - create views and print summary
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb

  # Frame analysis mode - analyze a specific frame with cascade context
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb --swap-id 166871
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb --swap-id 166871 --context-frames 10

Examples:
  # Infer budget from trace data (default)
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb

  # Override budget for 120fps analysis
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb --budget-ms 8.33

  # Analyze a dropped frame with cascade context
  ./scripts/prepare_analysis.py traces/recording/analysis.duckdb --swap-id 166871
"""

import argparse
import math
import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError as e:
    print(f"Error: Missing dependency: {e}", file=sys.stderr)
    print("Install with: pip install duckdb", file=sys.stderr)
    sys.exit(1)


class AnalysisUnavailable(Exception):
    """The trace has no usable frame observations; no smoothness claim is possible."""


def table_exists(conn, name):
    return conn.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?',
                        [name]).fetchone()[0] > 0


def export_status(conn, table):
    if not table_exists(conn, '_export_status'):
        return None
    return conn.execute('SELECT status, error FROM _export_status WHERE table_name = ?',
                        [table]).fetchone()


def require_frames(conn):
    source = 'hitches_frame_lifetimes'
    status = export_status(conn, source)
    if status and status[0] in ('failed', 'unsupported'):
        raise ValueError(f'{source}: {status[0]}: {status[1] or "export unavailable"}')
    if status and status[0] in ('empty', 'not_recorded'):
        raise AnalysisUnavailable(f'{source}: {status[0]}; frame metrics unavailable')
    if not table_exists(conn, source):
        if status and status[0] == 'exported':
            raise ValueError(f'{source}: exported status but source table is missing')
        raise AnalysisUnavailable(f'{source}: missing source and export status; reason unknown')
    count, invalid = conn.execute('''SELECT COUNT(*), COUNT(*) FILTER (WHERE
        start_ns IS NULL OR duration_ns IS NULL OR start_ns < 0 OR duration_ns <= 0)
        FROM hitches_frame_lifetimes''').fetchone()
    if invalid:
        raise ValueError(f'{source}: {invalid} invalid frame timing rows')
    if not count:
        raise AnalysisUnavailable(f'{source}: empty; frame metrics unavailable')


def infer_budget(conn, override_budget_ms=None):
    """
    Infer frame budget from coreanimation_lifetime_intervals.acceptable_latency_ns.

    Args:
        conn: DuckDB connection
        override_budget_ms: Optional manual budget override

    Returns:
        tuple: (budget_ms, source) where source is 'override', 'inferred', or 'fallback'
    """
    if override_budget_ms is not None:
        if not math.isfinite(override_budget_ms) or override_budget_ms <= 0:
            raise ValueError('--budget-ms must be finite and positive')
        return (override_budget_ms, 'override')

    try:
        result = conn.execute("""
            SELECT
              quantile_cont(acceptable_latency_ns, 0.5)/1e6 AS p50_budget_ms,
              quantile_cont(acceptable_latency_ns, 0.9)/1e6 AS p90_budget_ms,
              COUNT(*) AS n
            FROM coreanimation_lifetime_intervals
            WHERE acceptable_latency_ns > 0
        """).fetchone()

        if result and result[0] is not None and result[2] > 0:
            return (result[0], 'inferred')
    except Exception as e:
        print(f"Warning: Could not infer budget from coreanimation_lifetime_intervals: {e}", file=sys.stderr)

    # Fallback to 60fps
    return (16.67, 'fallback')


def create_views(conn, budget_ms, start_s=None, end_s=None):
    """
    Persist the selected budget and half-open event-start window for later queries.

    Args:
        conn: DuckDB connection
        budget_ms: Frame budget in milliseconds
    """
    bounds = []
    if start_s is not None:
        bounds.append(f'start_ns >= {int(start_s * 1e9)}')
    if end_s is not None:
        bounds.append(f'start_ns < {int(end_s * 1e9)}')
    where = ' WHERE ' + ' AND '.join(bounds) if bounds else ''
    if not conn.execute('SELECT COUNT(*) FROM hitches_frame_lifetimes' + where).fetchone()[0]:
        raise AnalysisUnavailable('selected event-start window is empty; frame metrics unavailable')
    conn.execute(f"""
        CREATE OR REPLACE VIEW _budget AS
        SELECT {budget_ms}::DOUBLE AS budget_ms;
    """)

    # Create frames view from hitches_frame_lifetimes (has clean start/duration timing)
    conn.execute(f"""
        CREATE OR REPLACE VIEW frames AS
        WITH f AS (
          SELECT
            swap_id,
            start_ns,
            (start_ns + duration_ns) AS end_ns,
            duration_ns/1e6 AS frame_ms,
            frame_color,
            display,
            surface_id,
            (SELECT budget_ms FROM _budget) AS budget_ms,
            GREATEST(0, CEIL((duration_ns/1e6) / (SELECT budget_ms FROM _budget)) - 1) AS missed_frames
          FROM hitches_frame_lifetimes {where}
        )
        SELECT
          *,
          CASE
            WHEN missed_frames = 0 THEN 'OK'
            WHEN missed_frames = 1 THEN 'Low'
            WHEN missed_frames BETWEEN 2 AND 3 THEN 'Medium'
            WHEN missed_frames BETWEEN 4 AND 7 THEN 'High'
            ELSE 'Extreme'
          END AS bucket
        FROM f;
    """)
    return 'hitches_frame_lifetimes'


def print_summary(conn, budget_ms, budget_source, frames_source):
    """Print summary statistics about the prepared views."""
    try:
        stats = conn.execute("""
            SELECT
              COUNT(*) AS total_frames,
              SUM(CASE WHEN bucket != 'OK' THEN 1 ELSE 0 END) AS dropped_frames,
              SUM(CASE WHEN bucket = 'Low' THEN 1 ELSE 0 END) AS low,
              SUM(CASE WHEN bucket = 'Medium' THEN 1 ELSE 0 END) AS medium,
              SUM(CASE WHEN bucket = 'High' THEN 1 ELSE 0 END) AS high,
              SUM(CASE WHEN bucket = 'Extreme' THEN 1 ELSE 0 END) AS extreme
            FROM frames;
        """).fetchone()

        total_frames, dropped_frames, low, medium, high, extreme = stats
        drop_pct = (dropped_frames / total_frames * 100) if total_frames > 0 else 0

        budget_desc = {
            'override': f'{budget_ms}ms (user override)',
            'inferred': f'{budget_ms:.2f}ms (inferred from p50 acceptable_latency_ns)',
            'fallback': f'{budget_ms}ms (fallback, 60fps default)'
        }

        print(f"Budget: {budget_desc[budget_source]}")
        print(f"Total frames: {total_frames:,}")
        print(f"Dropped frames: {dropped_frames:,} ({drop_pct:.1f}%)")
        print(f"  Low: {low:,}")
        print(f"  Medium: {medium:,}")
        print(f"  High: {high:,}")
        print(f"  Extreme: {extreme:,}")
        print()
        print("Created persistent views:")
        print(f"  - _budget (budget_ms: DOUBLE)")
        print(f"  - frames (from {frames_source})")
        print()
        print("Views are available when reopening this database; rerun preparation to replace the budget/window.")
        print("Use DuckDB CLI or API to query: SELECT * FROM frames LIMIT 10")

    except Exception as e:
        print(f"Error: Could not generate summary: {e}", file=sys.stderr)
        sys.exit(1)


# =============================================================================
# Frame Analysis Mode (Cascade Analysis)
# =============================================================================

def get_target_frame(conn, swap_id):
    """Get the target frame details."""
    result = conn.execute("""
        SELECT
            swap_id,
            start_ns,
            end_ns,
            frame_ms,
            budget_ms,
            missed_frames,
            bucket,
            frame_color,
            display,
            surface_id
        FROM frames
        WHERE swap_id = ?
    """, [swap_id]).fetchall()

    if len(result) > 1:
        raise ValueError(f'ambiguous swap_id {swap_id}; query frames by display/surface/start_ns')
    result = result[0] if result else None

    if not result:
        return None

    return {
        'swap_id': result[0],
        'start_ns': result[1],
        'end_ns': result[2],
        'frame_ms': result[3],
        'budget_ms': result[4],
        'missed_frames': result[5],
        'bucket': result[6],
        'frame_color': result[7],
        'display': result[8],
        'surface_id': result[9],
    }


def get_preceding_frames(conn, target_frame, context_frames, budget_ms):
    """Get preceding frames for cascade analysis."""
    results = conn.execute("""
        WITH preceding AS (
            SELECT
                swap_id,
                start_ns,
                end_ns,
                frame_ms,
                missed_frames,
                bucket,
                frame_color,
                ROW_NUMBER() OVER (ORDER BY start_ns DESC) as row_num
            FROM frames
            WHERE start_ns < ?
              AND display IS NOT DISTINCT FROM ?
              AND surface_id IS NOT DISTINCT FROM ?
        )
        SELECT *
        FROM preceding
        WHERE row_num <= ?
        ORDER BY start_ns ASC
    """, [target_frame['start_ns'], target_frame['display'], target_frame['surface_id'],
           context_frames]).fetchall()

    frames = []
    for i, r in enumerate(results):
        position = -(len(results) - i)
        over_budget = r[3] > budget_ms  # frame_ms > budget_ms
        frames.append({
            'swap_id': r[0],
            'start_ns': r[1],
            'end_ns': r[2],
            'frame_ms': r[3],
            'missed_frames': r[4],
            'bucket': r[5],
            'frame_color': r[6],
            'position': position,
            'over_budget': over_budget,
        })

    return frames


def identify_root_cause(frames, target_frame, budget_ms):
    """
    Select the first over-budget frame as a temporal investigation heuristic.

    The root cause is the FIRST frame that exceeded budget in the sequence
    leading up to the target hitch. If no preceding frame is over budget,
    the target itself is the root cause.
    """
    for frame in frames:
        if frame['over_budget']:
            return frame

    # Target itself is the root cause
    return {
        **target_frame,
        'position': 'TARGET',
        'over_budget': target_frame['frame_ms'] > budget_ms,
    }


def get_signposts_for_frame(conn, start_ns, end_ns, limit=30):
    """Get os_signpost_intervals overlapping a frame window, sorted by duration."""
    if not table_exists(conn, 'os_signpost_intervals'):
        return None
    results = conn.execute("""
        SELECT
            start_ns,
            start_ns/1e9 as start_s,
            duration_ns/1e6 as duration_ms,
            name,
            category,
            subsystem,
            start_message
        FROM os_signpost_intervals
        WHERE start_ns < ?
          AND (start_ns + duration_ns) > ?
        ORDER BY duration_ns DESC
        LIMIT ?
    """, [end_ns, start_ns, limit]).fetchall()

    return [{
        'start_ns': r[0],
        'start_s': r[1],
        'duration_ms': r[2],
        'name': r[3],
        'category': r[4],
        'subsystem': r[5],
        'message': r[6],
    } for r in results]


def get_logs_for_frame(conn, start_ns, end_ns, limit=20):
    """Get os_log messages during a frame window."""
    if not table_exists(conn, 'os_log'):
        return None
    results = conn.execute("""
        SELECT
            time_ns,
            time_ns/1e9 as time_s,
            subsystem,
            category,
            message_type,
            message
        FROM os_log
        WHERE time_ns >= ? AND time_ns < ?
        ORDER BY time_ns
        LIMIT ?
    """, [start_ns, end_ns, limit]).fetchall()

    return [{
        'time_ns': r[0],
        'time_s': r[1],
        'subsystem': r[2],
        'category': r[3],
        'message_type': r[4],
        'message': r[5],
    } for r in results]


def get_hitch_info(conn, swap_id):
    """Get hitch information from the hitches table if available."""
    try:
        result = conn.execute("""
            SELECT
                duration_ns/1e6 as hitch_ms,
                narrative_description
            FROM hitches
            WHERE swap_id = ?
        """, [swap_id]).fetchone()

        if result:
            return {
                'hitch_ms': result[0],
                'narrative': result[1],
            }
    except Exception:
        pass
    return None


def format_frame_analysis(target_frame, preceding_frames, root_cause, signposts, logs, hitch_info):
    """Format the cascade analysis as markdown for reports."""
    lines = []
    budget_ms = target_frame['budget_ms']

    # Header
    time_s = target_frame['start_ns'] / 1e9
    lines.append(f"### Frame {target_frame['swap_id']} at {time_s:.3f}s - Cascade Analysis")
    lines.append("")

    # Target frame info
    over_budget = target_frame['frame_ms'] > budget_ms
    status = f"OVER BUDGET ({target_frame['bucket']})" if over_budget else "OK"
    lines.append(f"**Target Frame:** {target_frame['frame_ms']:.2f}ms ({status}, budget: {budget_ms:.2f}ms)")

    if hitch_info:
        lines.append(f"**Hitch:** {hitch_info['hitch_ms']:.2f}ms - {hitch_info['narrative']}")
    lines.append("")

    # Preceding frames table
    lines.append(f"**Preceding Frames ({len(preceding_frames)} before):**")
    lines.append("| Frame | swap_id | Start (s) | Duration (ms) | Status |")
    lines.append("|-------|---------|-----------|---------------|--------|")

    for frame in preceding_frames:
        pos = frame['position']
        status = f"OVER BUDGET ({frame['bucket']})" if frame['over_budget'] else "OK"
        if root_cause and frame['swap_id'] == root_cause['swap_id'] and root_cause.get('position') != 'TARGET':
            status += " **INVESTIGATION CANDIDATE**"
        lines.append(f"| {pos} | {frame['swap_id']} | {frame['start_ns']/1e9:.3f} | {frame['frame_ms']:.2f} | {status} |")

    # Target row
    target_status = f"OVER BUDGET ({target_frame['bucket']})" if over_budget else 'OK'
    if root_cause and root_cause.get('position') == 'TARGET':
        target_status += " **INVESTIGATION CANDIDATE**"
    lines.append(f"| TARGET | {target_frame['swap_id']} | {target_frame['start_ns']/1e9:.3f} | {target_frame['frame_ms']:.2f} | {target_status} |")
    lines.append("")

    # Root cause analysis
    lines.append("**Temporal heuristic (not proof of causality):**")
    lines.append("Context is restricted to the target display and surface. Unknown keys may combine streams.")
    if root_cause and root_cause.get('position') != 'TARGET':
        lines.append(f"- Candidate: Frame {root_cause['position']} (swap_id: {root_cause['swap_id']}) took {root_cause['frame_ms']:.2f}ms")
        lines.append("- A cascade is possible; corroborate with frame phases, signposts, and CPU stacks.")
        context_label = f"candidate frame {root_cause['swap_id']}"
    else:
        lines.append("- No preceding over-budget frame in this context; investigate the target. This does not establish a root cause.")
        context_label = "target frame"
    lines.append("")

    # Signposts context
    lines.append(f"**Context (Signposts during {context_label}, sorted by duration):**")
    if signposts is None:
        lines.append("*Signposts unavailable: source table was not exported; inspect _export_status.*")
    elif signposts:
        lines.append("| Duration (ms) | Name | Category | Subsystem |")
        lines.append("|---------------|------|----------|-----------|")
        for sp in signposts[:15]:
            name = sp['name'] or '-'
            category = sp['category'] or '-'
            subsystem = sp['subsystem'] or '-'
            lines.append(f"| {sp['duration_ms']:.3f} | {name} | {category} | {subsystem} |")
    else:
        lines.append("*No signposts found during this frame*")
    lines.append("")

    # Logs context
    lines.append(f"**Context (Logs during {context_label}):**")
    if logs is None:
        lines.append("*Logs unavailable: source table was not exported; inspect _export_status.*")
    elif logs:
        lines.append("| Time (s) | Subsystem | Category | Message |")
        lines.append("|----------|-----------|----------|---------|")
        for log in logs[:10]:
            subsystem = log['subsystem'] or '-'
            category = log['category'] or '-'
            message = (log['message'] or '-')[:60]
            lines.append(f"| {log['time_s']:.3f} | {subsystem} | {category} | {message} |")
    else:
        lines.append("*No logs found during this frame*")
    lines.append("")

    return "\n".join(lines)


def analyze_frame(conn, swap_id, context_frames, budget_ms):
    """Perform cascade analysis on a specific frame."""
    # Get target frame
    target_frame = get_target_frame(conn, swap_id)
    if not target_frame:
        print(f"Error: Frame with swap_id {swap_id} not found", file=sys.stderr)
        sys.exit(1)

    # Get preceding frames
    preceding_frames = get_preceding_frames(conn, target_frame, context_frames, budget_ms)

    # Identify root cause
    root_cause = identify_root_cause(preceding_frames, target_frame, budget_ms)

    # Get context for the root cause frame (or target if it's the root cause)
    if root_cause and root_cause.get('position') != 'TARGET':
        context_start = root_cause['start_ns']
        context_end = root_cause['end_ns']
    else:
        context_start = target_frame['start_ns']
        context_end = target_frame['end_ns']

    signposts = get_signposts_for_frame(conn, context_start, context_end)
    logs = get_logs_for_frame(conn, context_start, context_end)
    hitch_info = get_hitch_info(conn, swap_id)

    # Output markdown
    markdown = format_frame_analysis(target_frame, preceding_frames, root_cause, signposts, logs, hitch_info)
    print(markdown)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Prepare canonical analysis views and analyze frames',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default mode - create views and print summary
  %(prog)s traces/recording/analysis.duckdb

  # Override budget for 120fps analysis
  %(prog)s traces/recording/analysis.duckdb --budget-ms 8.33

  # Analyze a specific frame with cascade context
  %(prog)s traces/recording/analysis.duckdb --swap-id 166871

  # Analyze with more context frames
  %(prog)s traces/recording/analysis.duckdb --swap-id 166871 --context-frames 10
        """
    )
    parser.add_argument('database', type=Path, help='Path to DuckDB database')
    parser.add_argument('--budget-ms', type=float, default=None,
                       help='Override frame budget in milliseconds (default: infer from trace)')
    parser.add_argument('--swap-id', type=int, default=None,
                       help='Analyze a specific frame by swap_id (enables frame analysis mode)')
    parser.add_argument('--context-frames', type=int, default=5,
                       help='Number of preceding frames to include in cascade analysis (default: 5)')

    parser.add_argument('--start-s', type=float, help='Inclusive frame start time in seconds')
    parser.add_argument('--end-s', type=float, help='Exclusive frame start time in seconds')
    args = parser.parse_args()
    if args.context_frames < 0:
        parser.error('--context-frames must be nonnegative')
    for value in (args.start_s, args.end_s):
        if value is not None and (not math.isfinite(value) or value < 0):
            parser.error('window bounds must be finite and nonnegative')
    if args.start_s is not None and args.end_s is not None and args.start_s >= args.end_s:
        parser.error('--start-s must precede --end-s')

    # Check database exists
    if not args.database.exists():
        print(f"Error: Database not found: {args.database}", file=sys.stderr)
        print(f"Run export_to_duckdb.py first to create the database.", file=sys.stderr)
        sys.exit(1)

    # Resolve once before changing cwd for legacy relative Parquet views.
    database = args.database.resolve()
    old_cwd = os.getcwd()
    conn = None
    exit_code = 0
    try:
        conn = duckdb.connect(str(database))
        if table_exists(conn, 'metadata'):
            result = conn.execute("SELECT value FROM metadata WHERE key = 'parquet_dir'").fetchone()
            if result:
                parquet_dir = Path(result[0])
                for base in (database.parent, *database.parents):
                    if (base / parquet_dir).exists():
                        conn.close()
                        os.chdir(base)
                        conn = duckdb.connect(str(database))
                        break
        # Invalidate previous derived results even when the new selection is unavailable.
        conn.execute('DROP VIEW IF EXISTS frames')
        conn.execute('DROP VIEW IF EXISTS _budget')
        require_frames(conn)
        budget_ms, budget_source = infer_budget(conn, args.budget_ms)
        frames_source = create_views(conn, budget_ms, args.start_s, args.end_s)
        if args.swap_id is not None:
            analyze_frame(conn, args.swap_id, args.context_frames, budget_ms)
        else:
            print_summary(conn, budget_ms, budget_source, frames_source)
    except AnalysisUnavailable as error:
        print(f'Frame analysis unavailable: {error}. No FPS or smoothness conclusion.', file=sys.stderr)
        exit_code = 2
    except (ValueError, duckdb.Error) as error:
        print(f'Error: {error}', file=sys.stderr)
        exit_code = 1
    finally:
        if conn is not None:
            conn.close()
        os.chdir(old_cwd)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
