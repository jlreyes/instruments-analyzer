---
name: instruments-analyzer
description: Gives AI agents programmatic access to Apple Instruments trace data by exporting .trace files to DuckDB. Covers recording traces, exporting to DuckDB, exploring exported tables, and running analysis scripts.
---

# Instruments Analyzer

This tool gives you programmatic access to Apple Instruments trace data. Instruments is normally a GUI tool — this tool bridges the gap by exporting `.trace` files into DuckDB, where you can query them with SQL.

---

## When to use this tool

- You have (or want to record) an Instruments `.trace` file
- You want to analyze performance data: CPU profiling, hitches, hangs, signposts, Core Animation, SwiftUI updates, RunLoop activity, etc.
- You want root-cause analysis backed by frame-level or event-level evidence

**For scroll & animation jank diagnosis**: See [scroll_and_animation.md](scroll_and_animation.md) — a frame-first workflow for isolating interaction windows, ranking dropped frames, cascade analysis, per-frame attribution, and producing a prioritized fix plan.

---

## Workflow overview

1. **Record** a trace (or use an existing one)
2. **Export** the trace to DuckDB
3. **Explore** the exported tables
4. **Prepare** derived views (optional, for frame-level analysis)
5. **Analyze** using SQL queries against the DuckDB database

---

## Step 1: Record a trace

Use `xctrace` to record from the command line:

```bash
# Attach to a running app
xcrun xctrace record --template 'SwiftUI' --time-limit 20s \
  --output ./traces/recording.trace \
  --attach AppName --no-prompt

# Or launch the app
xcrun xctrace record --template 'SwiftUI' --time-limit 20s \
  --output ./traces/recording.trace \
  --launch /path/to/App.app --no-prompt
```

You can also use the included `PerfDebugging.tracetemplate` in Instruments.

### Choosing a template

- **SwiftUI**: SwiftUI view updates, hitches, Core Animation, signposts
- **Time Profiler**: CPU sampling with backtraces
- **Animation Hitches**: Frame lifetimes, hitch detection
- **Custom**: Combine instruments as needed

Use `xcrun xctrace list templates` to see available templates.

---

## Step 2: Export to DuckDB

The export script converts an Instruments `.trace` file into a DuckDB database with Parquet backing:

```bash
./scripts/export_to_duckdb.py traces/recording.trace traces/recording/analysis.duckdb
```

The script:
- Uses `uv run` via shebang — run it directly (not with `python3`)
- Requires [uv](https://github.com/astral-sh/uv)
- Creates any missing parent directories for the output path
- Exports each Instruments table as a compressed Parquet file
- Creates a DuckDB database with views referencing the Parquet files

Check `_export_status` before interpreting a missing table. `exported` means rows were exported; `empty` means the recorded schema exported successfully with zero rows; `not_recorded` means the schema was absent. Diagnose `unsupported` and `failed` exports. A different template or longer recording may provide missing observations, but will not fix a parser failure. See [export status reference](SCHEMAS.md#export-status).

---

## Step 3: Explore the exported tables

After export, connect to the DuckDB database and explore what's available:

```sql
-- List all tables/views
SHOW TABLES;

-- Check availability before querying optional tables
SELECT schema_name, table_name, status, row_count, error FROM _export_status;

-- Check row counts only for tables marked exported
SELECT 'updates' AS tbl, COUNT(*) AS rows FROM updates
UNION ALL SELECT 'hitches', COUNT(*) FROM hitches
UNION ALL SELECT 'time_profile', COUNT(*) FROM time_profile
UNION ALL SELECT 'os_signpost_intervals', COUNT(*) FROM os_signpost_intervals
UNION ALL SELECT 'runloop_intervals', COUNT(*) FROM runloop_intervals
UNION ALL SELECT 'potential_hangs', COUNT(*) FROM potential_hangs;
```

### Key tables

| Table | What it contains |
|-------|-----------------|
| `updates` | SwiftUI body, representable, and other update records |
| `update_groups` | Batched SwiftUI transaction groups |
| `hitches` | Detected animation hitches (frame drops) |
| `hitches_frame_lifetimes` | Complete frame lifetime data |
| `hitches_updates` / `hitches_renders` / `hitches_gpu` / `hitches_framewait` | Per-phase frame data |
| `time_profile` | CPU sampling with backtraces |
| `cpu_profile` | CPU Profiler samples with hardware cycle weights (`weight_cycles`) |
| `os_signpost_intervals` | Signpost intervals (begin/end pairs) |
| `os_signpost` | Signpost point events |
| `os_log` | Log messages from os_log |
| `runloop_intervals` | RunLoop activity (main thread scheduling) |
| `coreanimation_context_intervals` | CA rendering phases (Layout, Display, Prepare, Commit) |
| `coreanimation_lifetime_intervals` | CA frame lifetimes with acceptable latency thresholds |
| `potential_hangs` | Potential hang interval records (not necessarily distinct episodes) |
| `life_cycle_periods` | App lifecycle transitions |
| `swiftui_causes` | SwiftUI dependency/causality graph |
| `swiftui_changes` | SwiftUI change events with backtraces |

Full schema reference: [SCHEMAS.md](SCHEMAS.md)

### Common exploration queries

```sql
-- Signpost overview (what instrumentation exists)
SELECT
  name, category, subsystem,
  COUNT(*) AS n,
  MAX(CAST(duration_ns AS BIGINT))/1e6 AS max_ms
FROM os_signpost_intervals
WHERE name IS NOT NULL
GROUP BY 1,2,3
ORDER BY max_ms DESC
LIMIT 50;
```

```sql
-- Worst hitches
SELECT start_ns/1e9 AS time_s, duration_ns/1e6 AS ms, narrative_description
FROM hitches
ORDER BY duration_ns DESC
LIMIT 20;
```

```sql
-- Heaviest CPU backtraces
SELECT
  COUNT(*) AS samples,
  SUM(weight_ns)/1e6 AS approx_ms,
  backtrace_json
FROM time_profile
WHERE backtrace_json IS NOT NULL
GROUP BY backtrace_json
ORDER BY approx_ms DESC
LIMIT 10;
```

```sql
-- Hang summary
SELECT hang_type, COUNT(*) AS count, MAX(duration_ns)/1e6 AS max_ms
FROM potential_hangs
GROUP BY hang_type
ORDER BY max_ms DESC;
```

---

## Step 4: Prepare derived views (for frame analysis)

The `prepare_analysis.py` script creates analysis-ready views on top of the raw exported data:

```bash
./scripts/prepare_analysis.py traces/recording/analysis.duckdb
```

This creates persistent `_budget` and `frames` views from `hitches_frame_lifetimes` with:
- Computed `missed_frames` count
- Severity buckets (Low / Medium / High / Extreme)
- Inferred frame budget from `coreanimation_lifetime_intervals.acceptable_latency_ns`
- Falls back to 60fps (16.67ms) if unavailable

Override for 120fps displays:

```bash
./scripts/prepare_analysis.py traces/recording/analysis.duckdb --budget-ms 8.33
```

Select a recording or interaction window explicitly when needed:

```bash
./scripts/prepare_analysis.py traces/recording/analysis.duckdb --start-s 0 --end-s 20
```

The window selects frames by start time: `--start-s` is inclusive and `--end-s` is exclusive. It does not select frames by overlap. Without bounds, preparation uses all stored rows, including stop/flush rows after the advertised recording duration. A second run replaces the saved budget and window. Failed or unavailable preparation clears the previous derived views.

Exit 2 means frame observations are unavailable because they are missing, empty, or outside the selection. Exit 1 means invalid inputs, incompatible data, or an export failure. Without frame data, do not claim an FPS, dropped-frame percentage, or smoothness. CPU, SwiftUI, signpost, and potential-hang evidence can still be analyzed independently.

### Frame-specific cascade analysis

Analyze a specific frame with surrounding context:

```bash
./scripts/prepare_analysis.py traces/recording/analysis.duckdb --swap-id 166871
./scripts/prepare_analysis.py traces/recording/analysis.duckdb --swap-id 166871 --context-frames 10
```

This outputs:
- Target frame details
- Preceding frames with budget status
- First over-budget frame as an investigation candidate, using the same display and surface
- Signposts and logs during the candidate frame, with unavailable sources identified

An earlier over-budget frame is a candidate, not proof that it caused a later frame to miss its deadline. Corroborate it with phase timing and stacks. Unknown display or surface keys can combine streams. Duplicate `swap_id` values are rejected as ambiguous; query `frames` with display, surface, and start time instead.

---

## Scope SwiftUI counts

`COUNT(*) FROM updates` counts exported update records across modules and update types. To count app body records, select the app module, `update_type = 'View Body Updates'`, category, and a stated time window. Creation and update are distinct categories. Group by `update_type` and `category` to inspect the records:

```sql
SELECT update_type, category, COUNT(*) AS records
FROM updates
WHERE module = 'YourApp' AND start_ns >= 0 AND start_ns < 20000000000
GROUP BY update_type, category;
```

Nested or overlapping update durations must not be summed and presented as elapsed wall time. Unknown module/process fields mean unknown attribution, not proof the app was absent.

## Time units

`time_profile.weight_ns` is sampled CPU weight in nanoseconds. `cpu_profile.weight_cycles` is hardware cycles, not elapsed time. Keep the two sources and their units separate; the exporter creates no compatibility alias and their weights must not be combined.

- All timestamps and durations are in **nanoseconds**
- `start_ns` is relative to trace start (not wall clock)
- Convert: `duration_ns / 1e6` for milliseconds, `start_ns / 1e9` for seconds
- `os_signpost_*` timestamps are strings — cast to BIGINT when comparing

## Use-case companion resources

- [scroll_and_animation.md](scroll_and_animation.md) — SwiftUI scroll and animation jank diagnosis
