# Profiling & Table Generation (WIP)

Notes on generating benchmark tables and flamegraphs. Table structure is still being finalized.

## Generating benchmark tables

The `data/generate_checkpoint_tables.py` script creates two Delta tables designed to
benchmark the `scan_metadata` pipeline with controlled log structure and stats configuration.

Prerequisites: `pip install pyarrow`

```bash
cd benchmarks/data
python3 generate_checkpoint_tables.py
```

This produces two tables under `workloads/benchmarks/`:

| Table | Description |
|-------|-------------|
| `10k_checkpoint_with_parsed_stats` | Checkpoint includes `add.stats_parsed` (struct column). Table config has `delta.checkpoint.writeStatsAsStruct=true`. |
| `10k_checkpoint_no_parsed_stats` | Checkpoint has only `add.stats` (JSON string). No `writeStatsAsStruct` config. |

Both tables have the same layout:
- Versions 0-9: 10 JSON commits, 10k adds each (100k total)
- Version 10: Checkpoint consolidating all 100k adds
- Versions 11-20: 10 JSON commits, 100 new adds each (1k more)
- Total at latest version: 101k add actions

After regenerating tables, delete `data/workloads/.done` to force re-extraction on the
next benchmark run, or just run benchmarks directly (the generated tables take precedence
over the tarball when the directory already exists).

## Read specs with predicates

```json
{
  "type": "read",
  "predicate": "id = 100",
  "include_stats": true
}
```

The `predicate` field accepts SQL expressions (parsed via `sqlparser`). The `include_stats`
field controls whether `include_all_stats_columns()` is called on the scan builder.

## Generating flamegraphs

Criterion's profiling adds overhead that obscures the actual workload. Use the `run_once`
binary with `perf` + `inferno` for clean, interactive SVG flamegraphs.

### Prerequisites

```bash
# perf (Linux only)
sudo apt install linux-tools-common linux-tools-$(uname -r)

# inferno (Rust flamegraph tools)
cargo install inferno
```

### Steps

```bash
# 1. Build the run_once binary in release mode
cargo build -p delta_kernel_benchmarks --release --bin run_once

# 2. Record a profile (run 100 iterations in-process for sufficient samples)
perf record -g --call-graph dwarf -F 4999 \
  -o perf_profile.data \
  -- ./target/release/run_once "with_parsed_stats/read_highly_selective" 100

# 3. Generate an interactive SVG flamegraph
perf script -i perf_profile.data 2>/dev/null \
  | inferno-collapse-perf 2>/dev/null \
  | inferno-flamegraph > flamegraph.svg
```

The `run_once` binary takes two arguments:
- A filter string matching against `<table_name>/<spec_name>` (runs first match)
- An optional iteration count (default 1; use 50-100 for profiling)

Open the SVG in a browser for interactive exploration (click to zoom, hover for details).
To find how long a function takes, compare its width to the total width -- each sample
represents `1 / frequency` seconds (e.g., at `-F 4999`, each sample is ~0.2ms).

### Available workload names

```bash
# List all workloads
./target/release/run_once ""
```
