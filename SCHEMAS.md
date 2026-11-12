# DuckDB Schema Reference

After exporting trace data with `export_to_duckdb.py`, query using DuckDB SQL or any tool that supports Parquet files.

## Data Storage

- **Parquet files**: Each table is stored as a compressed `.parquet` file (ZSTD compression)
- **DuckDB database**: Contains views that reference the Parquet files
- **Nullable fields**: Most columns support NULL values (except id, start_ns, duration_ns, time_ns)

## Export status

`_export_status` contains one row for each inventoried schema and each known schema absent from the run. The same information is written to `export-status.json` in the per-export Parquet directory, whose path is stored in `metadata` under the key `parquet_dir`.

| Column | Meaning |
| --- | --- |
| `schema_name` | Instruments schema identifier |
| `table_name` | SQL table name; NULL for unknown schemas |
| `status` | `exported`, `empty`, `not_recorded`, `unsupported`, or `failed` |
| `row_count` | Number of successfully exported rows |
| `error` | Failure or unsupported-schema detail, when available |

`empty` means a successful zero-row export; `not_recorded` means the schema was absent from the selected run. `unsupported` means no parser is available; `failed` means export or parsing failed. Failed exports exit nonzero and leave partial data inspectable. Do not interpret an absent view as zero events without checking status. Legacy databases lack this manifest, so the reason for missing data may be unknown.

Frame metrics require frame observations. A trace can declare frame schemas yet contain no frame rows; without frame data, do not claim zero dropped frames, an FPS, or smooth rendering. `prepare_analysis.py` persists `_budget` and `frames` after successful preparation; see [preparation and time windows](SKILL.md#step-4-prepare-derived-views-for-frame-analysis).

## Tables

### `updates` — Individual SwiftUI Updates

Records of SwiftUI body evaluations, representable updates, and other work across modules. To count app body evaluations, filter `module`, `update_type`, `category`, and a stated timestamp window. Stop/flush can emit rows after the advertised recording duration. A half-open filter (`start_ns >= start AND start_ns < end`) selects event starts; an overlap filter selects events differently. Durations may nest or overlap, so their sum is not wall time.

| Column                | Type   | Description                                                              |
| --------------------- | ------ | ------------------------------------------------------------------------ |
| `id`                  | INT64  | Auto-increment primary key                                               |
| `start_ns`            | INT64  | Timestamp (nanoseconds from trace start)                                 |
| `duration_ns`         | INT64  | Time spent (nanoseconds)                                                 |
| `view_name`           | STRING | View name (often "[unknown view: created before tracing started?]")      |
| `description`         | STRING | **Best column for identifying views** — contains view body accessor info |
| `update_type`         | STRING | "View Body Updates", "Representable Updates", "Other Updates"            |
| `severity`            | STRING | "Very Low", "Low", "Moderate", "High", "Very High"                       |
| `module`              | STRING | Module name (often "<unknown>" for system frameworks)                    |
| `category`            | STRING | "Update" or "Creation"                                                   |
| `allocations`         | INT32  | Memory allocations during update                                         |
| `cause_graph_node_id` | STRING | Link to cause graph (for traversal)                                      |
| `root_causes`         | STRING | Root cause info (nullable)                                               |
| `view_hierarchy`      | STRING | Full hierarchy path (arrow-separated view chain)                         |

### `update_groups` — Transaction Groups

Batched SwiftUI work. Long groups indicate complex update cascades.

| Column                 | Type   | Description                                     |
| ---------------------- | ------ | ----------------------------------------------- |
| `id`                   | INT64  | Auto-increment primary key                      |
| `start_ns`             | INT64  | Group start (nanoseconds)                       |
| `duration_ns`          | INT64  | Total time (nanoseconds)                        |
| `label`                | STRING | "Transaction", "Transaction for onChange", etc. |
| `backtrace_json`       | STRING | JSON array of stack frames                      |
| `cause_backtrace_json` | STRING | JSON array of cause stack frames                |

### `hitches` — Animation Hitches (Main)

Frame drops and stutters detected by Instruments.

| Column                  | Type   | Description                                                  |
| ----------------------- | ------ | ------------------------------------------------------------ |
| `id`                    | INT64  | Auto-increment primary key                                   |
| `start_ns`              | INT64  | Timestamp (nanoseconds from trace start)                     |
| `duration_ns`           | INT64  | Hitch duration (nanoseconds)                                 |
| `swap_id`               | INT32  | Frame swap identifier (use to correlate across hitch tables) |
| `swap_label`            | STRING | Swap label (hex format)                                      |
| `narrative_description` | STRING | Description (e.g., "Potentially expensive app update(s)")    |
| `is_system`             | BOOL   | Whether hitch is system-attributed                           |
| `display`               | STRING | Display identifier                                           |

### `hitches_updates` — UI Update Hitches

Hitches correlated with UI update work. Use `is_hitch=true` to find actual hitches.

| Column              | Type   | Description                                      |
| ------------------- | ------ | ------------------------------------------------ |
| `id`                | INT64  | Auto-increment primary key                       |
| `start_ns`          | INT64  | Timestamp (nanoseconds)                          |
| `duration_ns`       | INT64  | Duration (nanoseconds)                           |
| `swap_id`           | INT32  | Frame swap ID (correlate with other tables)      |
| `surface_id`        | INT32  | UI surface ID (9, 11, 13 for different surfaces) |
| `is_hitch`          | BOOL   | True if containment_level=1 (actual hitch)       |
| `frame_color`       | STRING | Visual indicator (Teal, Orange, Brown, Red)      |
| `containment_level` | INT32  | 0=normal, 1=hitch                                |
| `swap_label`        | STRING | Hex label                                        |
| `display`           | STRING | Display identifier                               |

### `hitches_renders` — Rendering Pipeline Hitches

GPU rendering work and hitches.

| Column              | Type   | Description                       |
| ------------------- | ------ | --------------------------------- |
| `id`                | INT64  | Auto-increment primary key        |
| `start_ns`          | INT64  | Render start (nanoseconds)        |
| `duration_ns`       | INT64  | Render duration (nanoseconds)     |
| `swap_id`           | INT32  | Frame swap ID                     |
| `surface_id`        | INT32  | Surface ID                        |
| `is_hitch`          | BOOL   | True if hitch occurred            |
| `frame_color`       | STRING | Visual indicator                  |
| `containment_level` | INT32  | 0=normal, 1=hitch                 |
| `offscreen_passes`  | INT32  | Number of offscreen render passes |
| `swap_label`        | STRING | Hex label                         |
| `display`           | STRING | Display identifier                |

### `hitches_gpu` — GPU Work Hitches

GPU-specific performance data.

| Column              | Type   | Description                  |
| ------------------- | ------ | ---------------------------- |
| `id`                | INT64  | Auto-increment primary key   |
| `start_ns`          | INT64  | GPU work start (nanoseconds) |
| `duration_ns`       | INT64  | GPU duration (nanoseconds)   |
| `swap_id`           | INT32  | Frame swap ID                |
| `surface_id`        | INT32  | Surface ID                   |
| `is_hitch`          | BOOL   | True if hitch occurred       |
| `frame_color`       | STRING | Visual indicator             |
| `containment_level` | INT32  | 0=normal, 1=hitch            |
| `swap_label`        | STRING | Hex label                    |
| `display`           | STRING | Display identifier           |

### `hitches_frame_lifetimes` — Total Frame Durations

Complete frame lifetime from start to present.

| Column             | Type   | Description                    |
| ------------------ | ------ | ------------------------------ |
| `id`               | INT64  | Auto-increment primary key     |
| `start_ns`         | INT64  | Frame start (nanoseconds)      |
| `duration_ns`      | INT64  | Total frame time (nanoseconds) |
| `swap_id`          | INT32  | Frame swap ID                  |
| `surface_id`       | INT32  | Surface ID                     |
| `frame_color`      | STRING | Visual indicator               |
| `layout_qualifier` | INT32  | Layout phase identifier (0-4)  |
| `swap_label`       | STRING | Hex label                      |
| `display`          | STRING | Display identifier             |

### `hitches_framewait` — Frame Wait Times

Time spent waiting for frame presentation.

| Column             | Type   | Description                   |
| ------------------ | ------ | ----------------------------- |
| `id`               | INT64  | Auto-increment primary key    |
| `start_ns`         | INT64  | Wait start (nanoseconds)      |
| `duration_ns`      | INT64  | Wait duration (nanoseconds)   |
| `swap_id`          | INT32  | Frame swap ID                 |
| `surface_id`       | INT32  | Surface ID                    |
| `frame_color`      | STRING | Visual indicator              |
| `layout_qualifier` | INT32  | Layout phase identifier (0-4) |
| `swap_label`       | STRING | Hex label                     |
| `display`          | STRING | Display identifier            |

### `time_profile` — CPU Profiling Samples

CPU sampling data with backtraces. Use the recorded `weight_ns` values rather than assuming a fixed sampling interval. `SUM(weight_ns)/1e6` is sampled CPU weight in milliseconds, not measured elapsed wall time. Scope by timestamp and thread before attributing work.

| Column           | Type   | Description                                           |
| ---------------- | ------ | ----------------------------------------------------- |
| `id`             | INT64  | Auto-increment primary key                            |
| `time_ns`        | INT64  | Sample timestamp (nanoseconds)                        |
| `thread_tid`     | INT32  | Thread ID                                             |
| `core_id`        | INT32  | CPU core number                                       |
| `thread_state`   | STRING | Thread state (e.g., "Running")                        |
| `weight_ns`      | INT64  | Sample weight (typically 100000 = 100µs)              |
| `backtrace_json` | STRING | JSON array of stack frames (function names, binaries) |

### `cpu_profile` — CPU Profiler Cycle Samples

CPU Profiler samples use the same columns as `time_profile`, except `weight_cycles` replaces `weight_ns`. `weight_cycles` measures hardware cycles, not nanoseconds; do not convert it to milliseconds or combine it with `time_profile` weights. The exporter keeps these sources separate and creates no compatibility alias.

### `potential_hangs` — Hang Detection

Potential hang interval records. A count can include overlapping or related intervals and does not necessarily count distinct hang episodes. A NULL `process_name` means the process is unknown; it does not mean the target app was absent.

| Column         | Type   | Description                                                                |
| -------------- | ------ | -------------------------------------------------------------------------- |
| `id`           | INT64  | Auto-increment primary key                                                 |
| `start_ns`     | INT64  | Hang start (nanoseconds)                                                   |
| `duration_ns`  | INT64  | Hang duration (nanoseconds)                                                |
| `hang_type`    | STRING | Type: "Potential Interaction Delay", "Brief Unresponsiveness", "Microhang" |
| `thread_tid`   | INT32  | Thread ID where hang occurred                                              |
| `process_name` | STRING | Process name                                                               |

### `runloop_intervals` — RunLoop Activity Intervals

Tracks RunLoop activity including runs, iterations, and waiting periods.

| Column              | Type   | Description                                                         |
| ------------------- | ------ | ------------------------------------------------------------------- |
| `id`                | INT64  | Auto-increment primary key                                          |
| `start_ns`          | INT64  | Interval start (nanoseconds)                                        |
| `duration_ns`       | INT64  | Interval duration (nanoseconds)                                     |
| `interval_type`     | STRING | "Runloop Run", "Individual Iteration", "Waiting For Events", "Busy" |
| `interval_id`       | STRING | Sentinel identifier for nested intervals                            |
| `nesting_level`     | INT32  | Nesting depth of runloop calls                                      |
| `containment_level` | INT32  | Containment hierarchy (3-5 typical range)                           |
| `mode`              | STRING | "kCFRunLoopDefaultMode", etc.                                       |
| `is_main`           | BOOL   | Whether this is the main runloop                                    |
| `thread_tid`        | INT32  | Thread ID                                                           |
| `runloop_ptr`       | INT64  | RunLoop pointer address                                             |
| `timeout`           | STRING | Timeout value (stored as string because values can exceed int64)    |
| `run_result`        | STRING | "Handled Source", etc.                                              |
| `label`             | STRING | Human-readable label                                                |
| `color`             | STRING | Visual indicator (Info, Blue, etc.)                                 |

### `runloop_events` — RunLoop Point Events

Point-in-time runloop events (START/END markers for intervals).

| Column          | Type   | Description                                                            |
| --------------- | ------ | ---------------------------------------------------------------------- |
| `id`            | INT64  | Auto-increment primary key                                             |
| `time_ns`       | INT64  | Event timestamp (nanoseconds)                                          |
| `event_type`    | STRING | "START" or "END"                                                       |
| `interval_type` | STRING | Type of interval starting/ending                                       |
| `interval_id`   | STRING | Sentinel identifier                                                    |
| `nesting_level` | INT32  | Nesting depth                                                          |
| `mode`          | STRING | RunLoop mode                                                           |
| `is_main`       | BOOL   | Whether main runloop                                                   |
| `thread_tid`    | INT32  | Thread ID                                                              |
| `runloop_ptr`   | INT64  | RunLoop pointer address                                                |
| `timeout`       | STRING | Timeout value (stored as string because values can exceed int64)       |
| `other_arg`     | STRING | Additional argument (stored as string because values can exceed int64) |

### `coreanimation_context_intervals` — Core Animation Context Work

Core Animation rendering context phases (Layout, Display, Prepare, Commit).

| Column         | Type   | Description                              |
| -------------- | ------ | ---------------------------------------- |
| `id`           | INT64  | Auto-increment primary key               |
| `start_ns`     | INT64  | Phase start (nanoseconds)                |
| `duration_ns`  | INT64  | Phase duration (nanoseconds)             |
| `thread_tid`   | INT32  | Thread ID                                |
| `phase`        | STRING | "Layout", "Display", "Prepare", "Commit" |
| `layer_count`  | INT32  | Number of layers affected                |
| `context_addr` | INT64  | CA context address                       |

### `coreanimation_layer_intervals` — Core Animation Layer Work

Per-layer Core Animation work tracking individual layer operations.

| Column         | Type   | Description                    |
| -------------- | ------ | ------------------------------ |
| `id`           | INT64  | Auto-increment primary key     |
| `start_ns`     | INT64  | Phase start (nanoseconds)      |
| `duration_ns`  | INT64  | Phase duration (nanoseconds)   |
| `thread_tid`   | INT32  | Thread ID                      |
| `phase`        | STRING | "Layout", "Display", "Prepare" |
| `context_addr` | INT64  | CA context address             |
| `layer_addr`   | INT64  | Specific layer address         |
| `layout_id`    | INT32  | Layout qualifier (0-4)         |

### `life_cycle_periods` — App Lifecycle Phases

Application lifecycle transitions and durations.

| Column        | Type   | Description                                                         |
| ------------- | ------ | ------------------------------------------------------------------- |
| `id`          | INT64  | Auto-increment primary key                                          |
| `start_ns`    | INT64  | Period start (nanoseconds)                                          |
| `duration_ns` | INT64  | Period duration (nanoseconds)                                       |
| `group_name`  | STRING | "States"                                                            |
| `period`      | STRING | "Initializing - Process Creation", "Foreground", "Background", etc. |
| `narrative`   | STRING | Human-readable description of the period                            |

### `swiftui_causes` — SwiftUI Causality Graph

Edges in the SwiftUI dependency graph showing what caused what to update.

| Column                    | Type   | Description                   |
| ------------------------- | ------ | ----------------------------- |
| `id`                      | INT64  | Auto-increment primary key    |
| `time_ns`                 | INT64  | Event timestamp (nanoseconds) |
| `source_event_id`         | INT32  | Source node ID in the graph   |
| `source_description`      | STRING | Source node description       |
| `destination_event_id`    | INT32  | Destination node ID           |
| `destination_description` | STRING | Destination node description  |
| `label`                   | STRING | "Update", "Creation"          |
| `value_type`              | STRING | Type of value that changed    |
| `changed_properties`      | STRING | Properties that changed       |

### `swiftui_changes` — SwiftUI Change Events

Change events in the SwiftUI graph with optional backtraces.

| Column           | Type   | Description                           |
| ---------------- | ------ | ------------------------------------- |
| `id`             | INT64  | Auto-increment primary key            |
| `time_ns`        | INT64  | Event timestamp (nanoseconds)         |
| `change_id`      | INT32  | Change identifier                     |
| `description`    | STRING | What changed (e.g., "External: Time") |
| `backtrace_json` | STRING | JSON array of stack frames (optional) |
| `thread_tid`     | INT32  | Thread ID                             |

---

## Time Units

- All timestamps and durations are in **nanoseconds**
- `start_ns` is relative to trace start (not wall clock)
- Convert to milliseconds: `duration_ns / 1000000.0`

**Frame budgets**:

- 60 fps = 16.6ms per frame (~16,600,000 ns)
- 120 fps = 8.3ms per frame (~8,300,000 ns)

---

## SQL Queries (DuckDB)

### Find slow updates

```sql
-- Updates > 5ms
SELECT id, description, duration_ns / 1e6 AS ms, severity
FROM updates
WHERE duration_ns > 5000000
ORDER BY duration_ns DESC
LIMIT 50;
```

### Filter by time range

```sql
-- Updates between 1.2s and 1.5s of the trace
SELECT id, description, duration_ns / 1e6 AS ms
FROM updates
WHERE start_ns BETWEEN 1200000000 AND 1500000000
ORDER BY duration_ns DESC;
```

### Find culprit views (by description)

```sql
-- Views consuming the most time
SELECT
    description,
    COUNT(*) AS count,
    SUM(duration_ns) / 1e6 AS total_ms,
    MAX(duration_ns) / 1e6 AS max_ms,
    AVG(duration_ns) / 1e6 AS avg_ms
FROM updates
WHERE description LIKE '%ViewBodyAccessor<%'
GROUP BY description
ORDER BY total_ms DESC
LIMIT 20;
```

### Detect invalidation storms

```sql
-- 16ms windows with many updates = potential frame drops
WITH windows AS (
    SELECT
        (start_ns / 16000000) AS window_id,
        MIN(start_ns) / 1e9 AS window_start_s,
        COUNT(*) AS update_count,
        SUM(duration_ns) / 1e6 AS total_ms
    FROM updates
    GROUP BY window_id
)
SELECT * FROM windows
WHERE update_count > 50 OR total_ms > 16
ORDER BY total_ms DESC;
```

### Find hitches in time range

```sql
-- All hitches over 30ms
SELECT start_ns/1e9 as time_s, duration_ns/1e6 as ms, narrative_description
FROM hitches
WHERE duration_ns > 30000000
ORDER BY duration_ns DESC;
```

### Correlate frame data across tables

```sql
-- Get complete picture for a specific frame (by swap_id)
SELECT 'update' as phase, duration_ns/1e6 as ms, is_hitch
FROM hitches_updates WHERE swap_id = 1848099

UNION ALL
SELECT 'render', duration_ns/1e6, is_hitch
FROM hitches_renders WHERE swap_id = 1848099

UNION ALL
SELECT 'gpu', duration_ns/1e6, is_hitch
FROM hitches_gpu WHERE swap_id = 1848099

UNION ALL
SELECT 'lifetime', duration_ns/1e6, NULL
FROM hitches_frame_lifetimes WHERE swap_id = 1848099;
```

### RunLoop activity queries

```sql
-- Find long runloop waits (main thread blocked)
SELECT
  start_ns/1e9 as time_s,
  duration_ns/1e6 as ms,
  interval_type,
  mode
FROM runloop_intervals
WHERE is_main = true
  AND interval_type = 'Waiting For Events'
  AND duration_ns > 16000000
ORDER BY duration_ns DESC
LIMIT 20;
```

### Core Animation queries

```sql
-- CA phase breakdown
SELECT
  phase,
  COUNT(*) as count,
  SUM(duration_ns)/1e6 as total_ms,
  AVG(duration_ns)/1e6 as avg_ms,
  MAX(duration_ns)/1e6 as max_ms
FROM coreanimation_context_intervals
GROUP BY phase
ORDER BY total_ms DESC;
```

### SwiftUI causality queries

```sql
-- Most common update causes
SELECT
  source_description,
  COUNT(*) as update_count
FROM swiftui_causes
WHERE label = 'Update'
GROUP BY source_description
ORDER BY update_count DESC
LIMIT 20;
```

### App lifecycle queries

```sql
-- Launch time breakdown
SELECT
  period,
  duration_ns/1e6 as ms,
  narrative
FROM life_cycle_periods
WHERE period LIKE 'Launching%' OR period LIKE 'Initializing%'
ORDER BY start_ns;
```

### Find hangs

```sql
-- Hangs by type
SELECT
  hang_type,
  COUNT(*) as count,
  AVG(duration_ns)/1e6 as avg_ms,
  MAX(duration_ns)/1e6 as max_ms
FROM potential_hangs
GROUP BY hang_type
ORDER BY max_ms DESC;
```

### `os_signpost_intervals` — OS Signpost Intervals

Signpost intervals (begin/end pairs) from os_signpost framework. Used for tracking performance spans.

| Column                 | Type   | Description                                             |
| ---------------------- | ------ | ------------------------------------------------------- |
| `id`                   | INT64  | Auto-increment primary key                              |
| `start_ns`             | UINT64 | Interval start (nanoseconds from trace start)           |
| `duration_ns`          | UINT64 | Interval duration (nanoseconds)                         |
| `layout_qualifier`     | INT32  | Layout qualifier                                        |
| `name`                 | STRING | Signpost name                                           |
| `category`             | STRING | Category (e.g., "UpdateCycle", "Transaction")           |
| `subsystem`            | STRING | Subsystem (e.g., "com.apple.AppKit")                    |
| `identifier`           | STRING | Signpost identifier (stored as STRING for large uint64) |
| `start_process_pid`    | INT32  | Start process PID                                       |
| `end_process_pid`      | INT32  | End process PID                                         |
| `start_thread_tid`     | INT32  | Start thread ID                                         |
| `end_thread_tid`       | INT32  | End thread ID                                           |
| `start_message`        | STRING | Start message (os-log-metadata)                         |
| `end_message`          | STRING | End message (os-log-metadata)                           |
| `start_backtrace_json` | STRING | Start backtrace (JSON)                                  |
| `end_backtrace_json`   | STRING | End backtrace (JSON)                                    |
| `start_emit_location`  | STRING | Start emit location                                     |
| `end_emit_location`    | STRING | End emit location                                       |
| `signature`            | STRING | Combined message signature                              |

### `os_signpost` — OS Signpost Point Events

Individual signpost events (Begin, End, or Event markers).

| Column           | Type   | Description                                             |
| ---------------- | ------ | ------------------------------------------------------- |
| `id`             | INT64  | Auto-increment primary key                              |
| `time_ns`        | UINT64 | Event timestamp (nanoseconds from trace start)          |
| `thread_tid`     | INT32  | Thread ID                                               |
| `process_pid`    | INT32  | Process PID                                             |
| `event_type`     | STRING | "Begin", "End", or "Event"                              |
| `scope`          | STRING | "Process" or "System"                                   |
| `identifier`     | STRING | Signpost identifier (stored as STRING for large uint64) |
| `name`           | STRING | Signpost name                                           |
| `format_string`  | STRING | Format string                                           |
| `backtrace_json` | STRING | Backtrace (JSON)                                        |
| `subsystem`      | STRING | Subsystem                                               |
| `category`       | STRING | Category                                                |
| `message`        | STRING | Message (os-log-metadata)                               |
| `emit_location`  | STRING | Emit location                                           |

### `os_signpost_arg` — OS Signpost Arguments (Flattened)

Flattened signpost arguments. Each row is one argument from a signpost event.

| Column          | Type   | Description                                             |
| --------------- | ------ | ------------------------------------------------------- |
| `id`            | INT64  | Auto-increment primary key                              |
| `time_ns`       | UINT64 | Event timestamp (nanoseconds)                           |
| `format_string` | STRING | Format string                                           |
| `identifier`    | STRING | Signpost identifier (stored as STRING for large uint64) |
| `signpost_name` | STRING | Signpost name                                           |
| `arg_name`      | STRING | Argument name (e.g., "arg0", "arg1", "seed")            |
| `thread_tid`    | INT32  | Thread ID                                               |
| `subsystem`     | STRING | Subsystem                                               |
| `category`      | STRING | Category                                                |
| `value`         | STRING | Argument value (polymorphic types stored as STRING)     |

### `os_log` — OS Log Messages

Log messages from os_log framework.

| Column           | Type   | Description                                  |
| ---------------- | ------ | -------------------------------------------- |
| `id`             | INT64  | Auto-increment primary key                   |
| `time_ns`        | UINT64 | Log timestamp (nanoseconds from trace start) |
| `thread_tid`     | INT32  | Thread ID                                    |
| `process_pid`    | INT32  | Process PID                                  |
| `message_type`   | STRING | "Default", "Info", "Debug", "Error", "Fault" |
| `format_string`  | STRING | Format string                                |
| `backtrace_json` | STRING | Backtrace (JSON)                             |
| `subsystem`      | STRING | Subsystem                                    |
| `category`       | STRING | Category                                     |
| `message`        | STRING | Formatted message                            |
| `emit_location`  | STRING | Emit location                                |

### `os_log_arg` — OS Log Arguments (Flattened)

Flattened log arguments. Each row is one argument from a log message.

| Column          | Type   | Description                                         |
| --------------- | ------ | --------------------------------------------------- |
| `id`            | INT64  | Auto-increment primary key                          |
| `time_ns`       | UINT64 | Log timestamp (nanoseconds)                         |
| `format_string` | STRING | Format string                                       |
| `arg_name`      | STRING | Argument name (e.g., "arg0", "arg1")                |
| `thread_tid`    | INT32  | Thread ID                                           |
| `subsystem`     | STRING | Subsystem                                           |
| `category`      | STRING | Category                                            |
| `value`         | STRING | Argument value (polymorphic types stored as STRING) |

### `coreanimation_lifetime_intervals` — CoreAnimation Frame Lifetimes

CoreAnimation frame lifetime tracking for hitch analysis.

| Column                  | Type   | Description                  |
| ----------------------- | ------ | ---------------------------- |
| `id`                    | INT64  | Auto-increment primary key   |
| `start_ns`              | UINT64 | Frame start (nanoseconds)    |
| `duration_ns`           | UINT64 | Frame duration (nanoseconds) |
| `display_id`            | INT32  | Display identifier           |
| `lifetime_id`           | INT32  | Frame number / lifetime ID   |
| `swap_id`               | INT32  | Swap ID for correlation      |
| `frame_seed`            | INT32  | Frame seed                   |
| `hitch_duration_ns`     | UINT64 | Duration of hitch (if any)   |
| `acceptable_latency_ns` | UINT64 | Acceptable latency threshold |
| `hid_latency_ns`        | UINT64 | HID latency                  |
| `render_start_ns`       | UINT64 | Render start timestamp       |
| `render_duration_ns`    | UINT64 | Render duration              |
| `layout_qualifier`      | INT32  | Layout qualifier             |
| `type_label`            | STRING | Type label                   |
| `narrative`             | STRING | Human-readable narrative     |
| `severity`              | STRING | Severity level               |
| `color`                 | INT32  | Color code                   |

---

## Direct Parquet Access

You can also query Parquet files directly without the DuckDB database:

```sql
-- Query Parquet file directly
SELECT * FROM read_parquet('output/updates.parquet')
WHERE duration_ns > 5000000
ORDER BY duration_ns DESC
LIMIT 10;
```

Or use Python with PyArrow/Pandas:

```python
import pyarrow.parquet as pq

# Read entire table
table = pq.read_table('output/updates.parquet')
df = table.to_pandas()

# Read with filtering (predicate pushdown)
table = pq.read_table(
    'output/updates.parquet',
    filters=[('duration_ns', '>', 5000000)]
)
```
