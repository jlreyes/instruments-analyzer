# Instruments Analyzer

A tool that gives AI agents programmatic access to Apple Instruments trace data. Instruments is normally a GUI-only tool — this tool bridges the gap by exporting `.trace` files into DuckDB, where they can be queried with SQL.

## What it does

Given an Instruments `.trace` file, this tool will:

1. **Export** the trace to DuckDB + Parquet (via `export_to_duckdb.py`)
2. **Explore** the exported tables — CPU profiling, hitches, hangs, signposts, Core Animation, SwiftUI updates, RunLoop activity, and more
3. **Prepare** derived views for frame-level analysis (via `prepare_analysis.py`)
4. **Analyze** performance data using SQL queries with full schema knowledge

## Install

Requires macOS with Xcode, [uv](https://github.com/astral-sh/uv), and [DuckDB](https://duckdb.org):

```bash
brew install duckdb
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### As an agent skill

```bash
# Claude Code
claude install-skill jlreyes/instruments-analyzer

# Any agent (via skills.sh)
npx skills add jlreyes/instruments-analyzer
```

### Manual

Clone and copy the skill files into your project's `.claude/skills/` directory:

```bash
git clone https://github.com/jlreyes/instruments-analyzer.git
cp -r instruments-analyzer/. your-project/.claude/skills/instruments-analyzer/
```

### Standalone (for use with any AI agent or CLI)

```bash
git clone https://github.com/jlreyes/instruments-analyzer.git
cd instruments-analyzer
```

## Usage

### 1. Record a trace

```bash
xcrun xctrace record --template 'SwiftUI' --time-limit 20s \
  --output ./traces/recording.trace \
  --attach YourApp --no-prompt
```

Or use the included `PerfDebugging.tracetemplate` in Instruments.

### 2. Export to DuckDB

```bash
./scripts/export_to_duckdb.py traces/recording.trace traces/recording/analysis.duckdb
```

### 3. Analyze with your tool of choice

```bash
# Use with Claude Code
# Analyze the performance trace in traces/recording/analysis.duckdb.

# Or query directly with DuckDB
duckdb traces/recording/analysis.duckdb
SELECT * FROM hitches ORDER BY duration_ns DESC LIMIT 10;
```

## Tests

The repository includes lightweight unit tests for parser behavior that can run
without Xcode or an Instruments trace:

```bash
uv run --with duckdb --with pyarrow --with numpy --with lxml python -m unittest discover -s tests
```

These tests cover XML parsing edge cases in the exporter, including a captured
`xcrun xctrace export` time-profile fixture. End-to-end trace export still
requires macOS with Xcode because it shells out to `xcrun xctrace`.

## What's included

| File | Description |
|------|-------------|
| `SKILL.md` | The skill prompt — Instruments → DuckDB workflow |
| `SCHEMAS.md` | Full DuckDB schema reference for all exported tables |
| `scripts/export_to_duckdb.py` | Exports Instruments `.trace` to DuckDB + Parquet |
| `scripts/prepare_analysis.py` | Creates analysis views, frame summaries, cascade analysis |
| `PerfDebugging.tracetemplate` | Instruments template for recording traces |
| `scroll_and_animation.md` | Jank diagnosis workflow — interaction windowing, cascade analysis, per-frame attribution, root cause clustering, and prioritized fix plans. |

## License

MIT
