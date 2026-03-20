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

Benchmark names follow a hierarchical path structure assembled from the table name, the spec file name, the operation, and (for `Read` workloads) the read config name:

```
{table_name}/{spec_file_name}/{operation}/{config_name}
```

- `{table_name}` — the `name` field from `tableInfo.json`
- `{spec_file_name}` — the spec filename without its `.json` extension (the `case_name`)
- `{operation}` — `snapshotConstruction` or `readMetadata`
- `{config_name}` — only present for `Read` workloads; e.g. `serial`, `parallel2`, `parallel4`

All path components use camelCase to match the JSON keys used throughout the workload spec format.

Examples:
```
101kAdds1000CommitsSinceChkpt1Chkpt/snapshotLatest/snapshotConstruction
101kAdds1000CommitsSinceChkpt1Chkpt/snapshotLatest/readMetadata/serial
10kAdds0CommitsSinceChkpt1V2Chkpt/snapshotLatest/readMetadata/parallel2
```

The filter argument is a regular expression, so you can create patterns to target the benchmarks that you want:

```bash
# all benchmarks for a specific table name
cargo bench -p delta_kernel_benchmarks --bench workload_bench "101kAdds1000CommitsSinceChkpt1Chkpt"

# all benchmarks for either of two tables (| for OR)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "101kAdds1000CommitsSinceChkpt1Chkpt|10kAdds0Chkpts"

# all snapshotConstruction benchmarks
cargo bench -p delta_kernel_benchmarks --bench workload_bench "snapshotConstruction"

# snapshotConstruction workloads for a specific table (.* to AND two parts of the name)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "101kAdds1000CommitsSinceChkpt1Chkpt.*snapshotConstruction"

# profile a specific benchmark with samply
samply record cargo bench -p delta_kernel_benchmarks --bench workload_bench "101kAdds1000CommitsSinceChkpt1Chkpt/snapshotLatest/snapshotConstruction"
```

#### By tag (`BENCH_TAGS`)

Set the `BENCH_TAGS` environment variable to a comma-separated list of tags to run only tables whose `tags` field (in `tableInfo.json`) contains at least one matching tag. If `BENCH_TAGS` is unset or empty, all tables are loaded and benchmarked.

```bash
# run only tables tagged "base"
BENCH_TAGS=base cargo bench -p delta_kernel_benchmarks
```

Built-in tags:
- **`base`** — a base set of tables run in CI

You can also add custom tags to any `tableInfo.json` to group tables relevant to your work, then pass that tag via `BENCH_TAGS` without modifying any code:

```bash
BENCH_TAGS=my-feature cargo bench -p delta_kernel_benchmarks

# run all tables tagged either "base" or "my-feature"
BENCH_TAGS=base,my-feature cargo bench -p delta_kernel_benchmarks
```

### Running benchmarking on a PR

To trigger benchmarks on a pull request, post a comment on the PR with one of the following:

- `/bench` — runs benchmarks with `BENCH_TAGS=base`
- `/bench <tags>` — runs benchmarks with `BENCH_TAGS=<tags>` (e.g. `bench base,tag1`)

See [By tag (`BENCH_TAGS`)](#by-tag-bench_tags) for details on how tags work. Results are posted automatically as a PR comment, comparing the PR branch against the base branch.

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

Workloads are discovered automatically by path. `load_all_workloads()` scans every subdirectory of `benchmarks/workloads/benchmarks/`, loading `tableInfo.json` and every spec file under `specs/`. The spec filename (without extension) becomes the `case_name`.

## Adding a new table

To benchmark against a custom Delta table:

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

## Entities

### `TableInfo`

Deserialized from `tableInfo.json`. Captures the table's identity (`name`, `description`), Delta schema and protocol, log statistics (`logInfo`), physical data layout, table properties, and benchmark tags. See [`src/models.rs`](src/models.rs) for field-level documentation.

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

The `predicate` field accepts a SQL WHERE clause expression that is parsed into a kernel `Predicate` and passed to the scan builder. See [`src/predicate_parser.rs`](src/predicate_parser.rs) for the full list of supported SQL features.

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

The concrete unit of work that gets benchmarked. Assembled when loading workloads by pairing a `Spec` (the operation) with a `TableInfo` (the table) and a `case_name`. A `Spec` file on its own solely describes an operation without context of the table it is performed on; when combined with a table, it becomes a `Workload`. A single table therefore produces multiple workloads, one for each spec file in its `specs/` directory.

### `ReadConfig`

Specifies runtime parameters for `Read` workloads that are not part of the spec JSON — currently whether to scan serially or in parallel, and how many threads to use. Multiple configs can be applied to the same workload to compare modes. By default all workloads run serial log replay; workloads with sidecar files additionally run parallel configs to benchmark parallel scanning.

### `WorkloadRunner`

Owns all pre-built state for a workload (e.g. a pre-constructed `Snapshot`) so that `execute()` measures only the target operation. Each runner corresponds to one `Workload` plus whatever additional configuration that workload type requires — `Read` workloads take a `ReadConfig`, while `SnapshotConstruction` workloads require no extra configuration.


## Source Layout

| File | Purpose |
|------|---------|
| `src/models.rs` | Data types: `TableInfo`, `Spec`, `Workload`, `ReadConfig`, `ReadOperation` |
| `src/predicate_parser.rs` | SQL WHERE clause to kernel `Predicate` parser |
| `src/runners.rs` | `WorkloadRunner` trait and implementations: `ReadMetadataRunner`, `SnapshotConstructionRunner` |
| `src/utils.rs` | Workload loading: deserializes workloads from the extracted data directory |
| `benches/workload_bench.rs` | Criterion entry point — loads workloads, builds runners, drives benchmarks |
| `build.rs` | Downloads and extracts benchmark workloads from the DAT GitHub release at build time |

