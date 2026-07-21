# Delta Kernel Benchmarking

This crate contains benchmarking infrastructure for Delta Kernel using Criterion and JSON workload specs. It is separate from the `kernel` crate to keep benchmark-specific code and dependencies out of the core library.

## Running benchmarks

```bash
# run all benchmarks
cargo bench -p delta_kernel_benchmarks

# run a specific bench binary
cargo bench -p delta_kernel_benchmarks --bench workload_bench

# filter to benchmarks whose name contains a substring (Criterion substring matching)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "some_name"

# profile a benchmark and generate a flamegraph
cargo install samply
samply record cargo bench -p delta_kernel_benchmarks --bench workload_bench "some_name"
```

### Filtering benchmarks

#### By benchmark name

Benchmark names use the human-readable table name and spec file name. Read benchmarks and
registered snapshot-construction benchmarks also include the harness config name. Unregistered
snapshot-construction benchmarks retain the unsuffixed name:

```
{table_name}/{spec_file_name}               # unregistered snapshot construction
{table_name}/{spec_file_name}/{config_name} # read or registered snapshot construction
```

- `{table_name}` — the human-readable `name` from the table's `tableInfo.json`
- `{spec_file_name}` — the spec filename without its `.json` extension (the `case_name`)
- `{config_name}` — the harness config from
  [`bench-registry.json`](#registry-bench-registryjson), such as `serial`, `fresh`, or `from199`

Examples:
```
crcVeryStale/snapshotLatest/fresh
v1Checkpoint/readMetadataLatest/serial
v2Checkpoint/readMetadataLatest/parallel2
```

The filter argument is a regular expression, so you can create patterns to target the benchmarks that you want:

```bash
# all benchmarks for a specific table name
cargo bench -p delta_kernel_benchmarks --bench workload_bench "crcVeryStale"

# all benchmarks for either of two tables (| for OR)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "crcVeryStale|crcLatest"

# all snapshot-construction benchmarks (the snapshotLatest spec)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "snapshotLatest"

# snapshot-construction workloads for a specific table (.* to AND two parts of the name)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "crcVeryStale.*snapshotLatest"

# profile a specific benchmark with samply
samply record cargo bench -p delta_kernel_benchmarks --bench workload_bench \
  "crcVeryStale/snapshotLatest/fresh"
```

#### By tag (`BENCH_TAGS`)

Set the `BENCH_TAGS` environment variable to a comma-separated list of tags to run only tables whose `tags` field (in `tableInfo.json`) contains at least one matching tag. If `BENCH_TAGS` is unset or empty, all tables are loaded and benchmarked.

```bash
# run only tables tagged "base"
BENCH_TAGS=base cargo bench -p delta_kernel_benchmarks
```

Built-in tags (with current table assignments - for the most up-to-date table assignments, run benchmarks locally and inspect `benchmarks/workloads/benchmarks/<any-existing-table-name>/tableInfo.json` to learn about the existing tables):
- **`base`** — a base set of tables run in CI
  - Tables: `101kAdds1kCommitsSinceChkpt1Chkpt`
- **`commit-size-scaling`** — tables for comparing how log replay time scales with the number of actions in the log; all are single-commit tables with varying action counts (100, 1k, 10k, 100k, 1M)
  - Tables: `100Adds0Chkpts`, `1kAdds0Chkpts`, `10kAdds0Chkpts`, `100kAdds0Chkpts`, `1MAddsNoData0Chkpts`
- **`checkpoint-reads-by-type`** — tables for comparing checkpoint reading performance for different kinds of checkpointing
  - Tables: `10kAdds0CommitsSinceChkpt1Chkpt`, `10kAdds0CommitsSinceChkpt1V2Chkpt`
- **`v2-checkpoint`** — tables with v2 checkpoints
  - Tables: `10kAdds0CommitsSinceChkpt1V2Chkpt`
- **`crc-optimization`** — tables for comparing how CRC files affect log replay timing; designed to isolate the effect of CRC files at different versions relative to the checkpoint and latest version
  - Tables: `101kAdds1kCommitsSinceChkpt1Chkpt`, `20kAdds100CommitsSinceChkpt1Chkpt0CommitsSinceCrc`, `20kAdds100CommitsSinceChkpt1Chkpt50CommitsSinceCrc`, `20kAdds100CommitsSinceChkpt1ChkptNoCrc`
- **`time-travel-optimization`** — tables with multiple specs or specs not at the latest version, useful for benchmarking snapshot construction at historical versions
  - Tables: `101kAdds1kCommitsSinceChkpt1Chkpt`, `200kAdds0CommitsSinceChkpt2Chkpts0CommitsSinceCrc`
- **`listing-optimization`** — table for benchmarking log listing efficiency (e.g. `list_from()` call patterns); useful for features that optimize how the delta log directory is scanned
  - Tables: `200kAdds0CommitsSinceChkpt2Chkpts0CommitsSinceCrc`
- **`metadata-only`** — tables with no actual data files, useful for isolating log metadata processing overhead
  - Tables: `1MAddsNoData0Chkpts`


You can also add custom tags to the `tags` field of any local `tableInfo.json` to group tables relevant to your work, then pass that tag via `BENCH_TAGS` without modifying any code:

```bash
BENCH_TAGS=my-feature cargo bench -p delta_kernel_benchmarks

# run all tables tagged either "base" or "my-feature"
BENCH_TAGS=base,my-feature cargo bench -p delta_kernel_benchmarks
```

### Running benchmarking on a PR

Benchmarks run automatically on every push to a non-draft PR with `BENCH_TAGS=base`, no filter.
Results are posted as a single PR comment that updates in place on each push, so the latest
comment always reflects the latest commit. The comment is posted by a companion
`workflow_run`-triggered workflow ([benchmark-post-comment.yml](../.github/workflows/benchmark-post-comment.yml)),
so the bench job itself runs with a read-only token even on fork PRs. To bench a draft PR or
override the tags or filter, post a `/bench` comment as documented below -- it updates the
same comment that the auto-trigger uses.

The bench job fails if any benchmark is at least 15% slower than the base branch. The result
comment is still posted so you can see which benchmark regressed. To merge anyway (for an
expected or noise-driven regression), add the `ignore-benchmark-failure` label to the PR.

To trigger benchmarks on a pull request manually, post a comment using the following syntax:

```
/bench [--tags <comma separated list of tags>] [--filter <regex>]
```

- `--tags` sets `BENCH_TAGS` (comma-separated), controlling which table groupings run.
- `--filter` is a single-token Criterion regex matched against benchmark names.
- Both flags are optional and independent; they can be given in any order.
- When both are specified, they apply as AND: only benchmarks from tables that match the tag filter AND whose name matches the regex are run.
- Running just `/bench` (with no flags) defaults to `BENCH_TAGS=base`. If neither flag is parsed, the same default applies.

Examples:
```
/bench                                                  # BENCH_TAGS=base, all benchmark names
/bench --tags base,my-tag                               # BENCH_TAGS=base,my-tag, all benchmark names
/bench --filter snapshotLatest                          # no BENCH_TAGS set, only snapshot-construction benchmarks
/bench --tags base --filter crcVeryStale.*snapshotLatest  # only snapshot-construction benchmarks from tables tagged "base"
/bench --filter crcVeryStale|crcLatest                    # no BENCH_TAGS set, OR two table names
```

See [By tag (`BENCH_TAGS`)](#by-tag-bench_tags) for how tags work and [By benchmark name](#by-benchmark-name) for regex pattern examples. Results are posted automatically as a PR comment, comparing the PR branch against the base branch.
CI timings are noisy and tend to run higher than on dedicated hardware, but proportional differences between branches are a rough signal for performance changes.

## Registry (`bench-registry.json`)

[`bench-registry.json`](bench-registry.json) maps read and snapshot-construction benchmarks to the
harness configs they run. Each registered workload expands into one Criterion benchmark per
config, and the config `name` becomes the trailing path segment of the benchmark name (see
[By benchmark name](#by-benchmark-name)). Unregistered snapshot-construction workloads retain
their existing unsuffixed name and fresh snapshot behavior.

The registry is a checked-in file under the crate root, separate from the spec files: the
workload archive (`benchmarks/workloads/`) is downloaded at build time and gitignored, so configs
that belong with the source live here and reference benchmarks by name.

```json
{
  "crc1Col200Commits1ChkptCrc195": {
    "snapshotLatest": [
      {
        "name": "fresh",
        "snapshotBuilder": "for"
      },
      {
        "name": "from199",
        "snapshotBuilder": { "from": { "version": 199 } }
      }
    ]
  },
  "10kAdds0CommitsSinceChkpt1V2Chkpt": {
    "readMetadataLatest": [
      { "name": "serial",    "parallelScan": "disabled" },
      { "name": "parallel2", "parallelScan": { "enabled": { "numThreads": 2 } } }
    ]
  }
}
```

- The file nests config lists by table directory name then case name
  (`{ table: { case: [configs] } }`).
- A read benchmark whose `(table, case)` key is absent from the registry falls back to the built-in
  serial config. An unlisted snapshot-construction benchmark uses fresh construction without
  changing its benchmark name. An explicit but empty config list is rejected at load rather than
  silently dropping the benchmark.
- Config `name`s within a list must be unique and non-empty (they form the benchmark name's last
  segment). A list cannot mix read and snapshot-construction configs.

Config-field value forms:

- `parallelScan` (read configs): `"disabled"` or `{ "enabled": { "numThreads": <n> } }` — serde
  externally-tagged, so the fieldless variant is a bare string and the parameterized variant a
  single-key object.
- `snapshotBuilder` (snapshot-construction configs): `"for"` builds from the table root, while
  `{ "from": { "version": <n> } }` prebuilds snapshot version `n` outside the timed loop and uses
  `Snapshot::builder_from` during each iteration.
- Catalog-managed snapshot benchmarks currently support only `"snapshotBuilder": "for"`.

## Workload data layout

Each table lives in its own subdirectory under `benchmarks/workloads/benchmarks/`:

```
benchmarks/workloads/
├── benchmarks/
│   └── <table_name>/
│       ├── tableInfo.json        # describes the table (name, schema, protocol, etc.)
│       ├── delta/                # Delta table data (if no explicit tablePath)
│       └── specs/
│           └── <case_name>.json  # one file per benchmark operation
└── tests/                        # reserved for future test workloads (currently empty)
```

## Loading workloads

Workloads are downloaded from the DAT GitHub release and extracted to `benchmarks/workloads/` automatically by `build.rs` when the crate is built. A `.done` marker file is written on success to skip re-downloading on subsequent builds. To force a fresh download, delete `benchmarks/workloads/.done`.

Workloads are discovered automatically by path. `load_all_workloads()` scans every subdirectory of `benchmarks/workloads/benchmarks/`, loading `tableInfo.json` and every spec file under `specs/`. The subdirectory name becomes the registry table key, the human-readable `name` from `tableInfo.json` becomes the benchmark identifier's first segment, and the spec filename (without extension) becomes the `case_name`.

## Current benchmarking workloads

There is no single exhaustive list of all benchmark tables and their contents maintained in this README, as this can change over time. The [built-in tags](#by-tag-bench_tags) section includes a list of table names grouped by tag, but this is non-exhaustive and subject to change. To explore which tables exist and what each one contains, run benchmarks locally and inspect the `tableInfo.json` file in each table's directory under `benchmarks/workloads/benchmarks/`.

## Adding a new table locally

### Local tables

To benchmark against a local Delta table:

1. Extract the workload archive if you haven't already — the simplest way is to run any benchmark once, which auto-extracts it:
   ```bash
   cargo bench -p delta_kernel_benchmarks --bench workload_bench
   ```
2. Create a directory for the new table under `benchmarks/workloads/benchmarks/`:
   ```
   benchmarks/data/workloads/benchmarks/<tableName>/
   ├── tableInfo.json       # see TableInfo section below for required fields
   ├── delta/               # Delta table files (_delta_log/, parquet data, etc.)
   └── specs/
       └── <case_name>.json # one or more spec files describing operations to benchmark
   ```
3. Run benchmarks — the new table is discovered automatically (you can filter by table name — see [By benchmark name](#by-benchmark-name)):
   ```bash
   cargo bench -p delta_kernel_benchmarks --bench workload_bench "<table_name>"
   ```

### Remote tables (S3 / UC)

Remote tables are benchmarked via `KERNEL_BENCH_WORKLOAD_DIR`, which points to a directory of
table configs outside of the workload archive. Each subdirectory has the same layout as local
tables (`tableInfo.json` + `specs/`), but no `delta/` directory is needed.

There are two types of remote tables, determined by the `tableInfo.json` fields:

- **S3 tables** — set `tablePath` to the S3 URL (e.g. `s3://bucket/path`). Requires `AWS_*`
  env vars for credentials.
- **UC tables** — set `catalogInfo` with a `tableName` field (e.g. `catalog.schema.table`).
  Credentials are vended via UC at runtime (`UC_WORKSPACE` / `UC_TOKEN` env vars). The
  benchmark harness automatically detects catalog-managed tables (via UC properties) and
  uses the appropriate snapshot loading path.

Example `tableInfo.json` for a UC table:
```json
{
  "name": "my_uc_table",
  "description": "A UC-managed table",
  "catalogInfo": {"tableName": "catalog.schema.table"},
  "schema": {"type": "struct", "fields": [...]},
  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
  "logInfo": {"numAddFiles": 100, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 100},
  "properties": {},
  "dataLayout": {},
  "tags": []
}
```

Example:
```bash
KERNEL_BENCH_WORKLOAD_DIR=/path/to/my/tables \
  UC_WORKSPACE=https://my-workspace.cloud.databricks.com UC_TOKEN=... AWS_REGION=us-west-2 \
  cargo bench --bench workload_bench
```

## Entities

### `TableInfo`

Deserialized from `tableInfo.json`. Captures a human label (`name`) and `description`, Delta schema and protocol, log statistics (`logInfo`), physical data layout, table properties, and benchmark tags. The `name` field becomes the benchmark identifier's table segment, while the registry table key comes from the table's directory name. See [`workloads/src/models.rs`](../workloads/src/models.rs) for field-level documentation.

#### Example

```json
{
  "name": "myTable",
  "description": "A basic table with two append writes.",
  "schema": {"type": "struct", "fields": [
    {"name": "id", "type": "long", "nullable": true, "metadata": {}}
  ]},
  "protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": [], "writerFeatures": []},
  "logInfo": {
    "numAddFiles": 10,
    "numRemoveFiles": 0,
    "sizeInBytes": 4096,
    "numCommits": 2,
    "numActions": 12
  },
  "properties": {},
  "dataLayout": {},
  "tags": ["base"]
}
```

### `Spec`

Deserialized from a JSON file in a table's `specs/` directory. Describes a single operation to benchmark (what to do, e.g. read at version 3). Two variants are supported:

- **`Read`** — scan a table at an optional version (defaults to latest). A single `Read` spec expands into one benchmark per `ReadOperation` × `ReadConfig` combination — every relevant operation and parallelism mode is benchmarked. Currently only `ReadMetadata` is implemented; `ReadData` is not yet supported.
- **`SnapshotConstruction`** — measure the cost of building a `Snapshot` from scratch at an optional version (defaults to latest)

Read specs:
```json
{
  "type": "read"
}
```
Or with a specific version:

```json
{
  "type": "read",
  "version": 0
}
```

With a predicate for data skipping (SQL WHERE clause syntax):

```json
{
  "type": "read",
  "predicate": "id < 500 AND value > 10"
}
```

The `predicate` field accepts a SQL WHERE clause expression that is parsed into a kernel `Predicate` and passed to the scan builder. See [`workloads/src/predicate_parser.rs`](../workloads/src/predicate_parser.rs) for the full list of supported SQL features.

Snapshot construction specs:
```json
{
  "type": "snapshotConstruction"
}
```
Or with a specific version:

```json
{
  "type": "snapshotConstruction",
  "version": 0
}
```

### `Workload`

The concrete unit of work that gets benchmarked. Assembled when loading workloads by pairing a `Spec` (the operation) with a `TableInfo` (the table, whose directory determines `TableInfo::registry_table_key()`) and a `case_name`. A `Spec` file on its own solely describes an operation without context of the table it is performed on; when combined with a table, it becomes a `Workload`. A single table therefore produces multiple workloads, one for each spec file in its `specs/` directory.

### Harness configs

`ReadConfig` specifies whether metadata scanning runs serially or in parallel and how many threads
to use. `SnapshotConstructionConfig` selects fresh or previous-snapshot construction. Multiple
configs can be applied to the same workload to compare modes. Which configs run for a given
benchmark is determined by the
[registry](#registry-bench-registryjson).

### `WorkloadRunner`

Owns all pre-built state for a workload so that `execute()` measures only the target operation.
Read runners carry the selected `ReadConfig`. Snapshot-construction runners either construct from
the table root or reuse a configured base snapshot prepared outside the timed loop.


## Source Layout

The workload spec data types (`TableInfo`, `Spec`, `Workload`, …) live in the shared
[`delta_kernel_workloads`](../workloads/) crate, since the spec types are also used by the
`acceptance` crate. The harness config types and registry that map benchmarks to them are
benchmark-specific and live in this crate.

| File | Purpose |
|------|---------|
| `../workloads/src/models.rs` | Workload spec types: `TableInfo`, `Spec`, `Workload`, `ReadOperation` |
| `../workloads/src/predicate_parser.rs` | SQL WHERE clause to kernel `Predicate` parser |
| `src/registry.rs` | Read and snapshot-construction harness config types plus `BenchRegistry` |
| `bench-registry.json` | Checked-in registry mapping benchmarks to their harness configs |
| `src/runners.rs` | `WorkloadRunner` trait and implementations: `ReadMetadataRunner`, `SnapshotConstructionRunner` |
| `src/utils.rs` | Workload loading: deserializes workloads from the extracted data directory |
| `benches/workload_bench.rs` | Criterion entry point — loads workloads + registry, builds runners, drives benchmarks |
| `build.rs` | Downloads and extracts benchmark workloads from the DAT GitHub release at build time |
