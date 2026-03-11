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

Benchmark names follow a hierarchical path structure assembled from the Criterion group name, the table name, the spec file name, the operation, and (for `Read` workloads) the read config name:

```
workloadBenchmarks/{table_name}/{spec_file_name}/{operation}/{config_name}
```

- `workloadBenchmarks` — the Criterion benchmark group (always this literal string)
- `{table_name}` — the `name` field from `tableInfo.json`
- `{spec_file_name}` — the spec filename without its `.json` extension (the `case_name`)
- `{operation}` — `snapshotConstruction` or `readMetadata`
- `{config_name}` — only present for `Read` workloads; e.g. `serial`, `parallel2`, `parallel4`

All path components use camelCase to match the JSON keys used throughout the workload spec format.

Examples:
```
workloadBenchmarks/checkpointV91009Versions/snapshotLatest/snapshotConstruction
workloadBenchmarks/checkpointV91009Versions/snapshotLatest/readMetadata/serial
workloadBenchmarks/checkpointV91009Versions/snapshotLatest/readMetadata/parallel4
```

The filter argument is a regular expression, so you can create patterns to target the benchmarks that you want:

```bash
# all benchmarks for a specific table name
cargo bench -p delta_kernel_benchmarks --bench workload_bench "checkpointV91009Versions"

# all benchmarks for either of two tables (| for OR)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "checkpointV91009Versions|10Adds"

# snapshotConstruction workloads for a specific table (.* to AND two parts of the name)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "checkpointV91009Versions.*snapshotConstruction"

# profile a specific benchmark with samply
samply record cargo bench -p delta_kernel_benchmarks --bench workload_bench "workloadBenchmarks/checkpointV91009Versions/snapshotLatest/snapshotConstruction"
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

## Workload data layout

Each table lives in its own subdirectory under `benchmarks/data/workloads/benchmarks/`:

```
benchmarks/data/workloads/
├── benchmarks/
│   └── <table_name>/
│       ├── tableInfo.json        # describes the table (name, schema, protocol, etc.)
│       ├── delta/                # Delta table data (if no explicit tablePath)
│       └── specs/
│           └── <case_name>.json  # one file per benchmark operation
└── tests/                        # reserved for future test workloads (currently empty)
```

## Loading workloads

Workloads are loaded from `benchmarks/data/workloads.tar.gz`. On first run the tarball is extracted to `benchmarks/data/workloads/` and a `.done` file is written (to `benchmarks/data/workloads/`) to skip re-extraction on subsequent runs. To pick up changes to the tarball, delete the `.done` file.

Workloads are discovered automatically by path. `load_all_workloads()` scans every subdirectory of `benchmarks/data/workloads/benchmarks/`, loading `tableInfo.json` and every spec file under `specs/`. The spec filename (without extension) becomes the `case_name`.

## Adding a new table

To benchmark against a custom Delta table:

1. Extract the workload archive if you haven't already — the simplest way is to run any benchmark once, which auto-extracts it:
   ```bash
   cargo bench -p delta_kernel_benchmarks --bench workload_bench
   ```
2. Create a directory for the new table under `benchmarks/data/workloads/benchmarks/`:
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

If you want to commit this change and add it to the `workloads.tar.gz` archive:
```bash
cd benchmarks/data/workloads
tar -czf ../workloads.tar.gz .
```
Then commit the updated archive and delete the `.done` file so it is re-extracted on the next run.

## Entities

### `TableInfo`

Deserialized from `tableInfo.json`. Describes the Delta table being benchmarked. All JSON keys use camelCase. All fields are required except `tablePath`.

| Field | Type | Required | Description |
|-------|------|:--------:|-------------|
| `name` | String | yes | Short identifier used as a path component in the benchmark name (e.g. `100Adds0Chkpts`) |
| `description` | String | yes | Human-readable description of the table |
| `tablePath` | String (URL) | no | Explicit URL to the table, for remote tables (e.g. S3) or absolute local paths. If absent, the table is assumed to be in the `delta/` subdirectory next to `tableInfo.json` |
| `schema` | Object | yes | Schema at the latest version, deserialized into `delta_kernel::schema::Schema`. Uses Delta protocol JSON format: `{"type": "struct", "fields": [...]}` |
| `protocol` | Object | yes | Delta protocol requirements at the latest version, deserialized into `delta_kernel::actions::Protocol`. Format: `{"minReaderVersion": N, "minWriterVersion": N, ...}` |
| `logInfo` | Object | yes | Log-level statistics giving a quick overview of the table without requiring a full log replay; see [`logInfo`](#loginfo) below |
| `properties` | Object | yes | Delta table properties from the `metadata` action (string key-value pairs). Use `{}` if none. Example: `{"delta.enableDeletionVector": "true", "delta.columnMapping.mode": "none"}` |
| `dataLayout` | Object | yes | Physical data organization; see [`dataLayout`](#datalayout) below |
| `tags` | Array[String] | yes | Tags for filtering benchmarks via `BENCH_TAGS` (see [By tag](#by-tag-bench_tags)). Use `[]` if none. Built-in tag: `base` (run in CI) |

#### `logInfo`

| Field | Type | Required | Description |
|-------|------|:--------:|-------------|
| `numAddFiles` | u64 | yes | Number of active `add` file actions (live data files in the table) |
| `numRemoveFiles` | u64 | yes | Number of `remove` file actions in the log |
| `sizeInBytes` | u64 | yes | Total on-disk size of all data files in bytes |
| `numCommits` | u64 | yes | Number of commits (JSON log files) in the table history |
| `numActions` | u64 | yes | Total number of actions across all commits |
| `lastCheckpointVersion` | Option\<u64\> | no | Version of the most recent checkpoint, if any |
| `lastCrcVersion` | Option\<u64\> | no | Version of the most recent CRC (version checksum) file, if any |
| `numParallelCheckpointFiles` | Option\<u32\> | no | Number of parquet part files in the most recent multi-part checkpoint, if any |

#### `dataLayout`

Describes how data is physically organized in the table:

| Value | Description |
|-------|-------------|
| `{}` | No special organization (unpartitioned, unclustered) |
| `{"numPartitionColumns": N, "numDistinctPartitions": M}` | Partitioned table with `N` partition columns and `M` distinct partition values. Two tables with the same `N` can differ significantly in cardinality (e.g. 1 partition column with 100 distinct values vs. 10000), so both are tracked |
| `{"numClusteringColumns": N}` | Clustered table with `N` clustering columns |

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
| `src/runners.rs` | `WorkloadRunner` trait and implementations: `ReadMetadataRunner`, `SnapshotConstructionRunner` |
| `src/utils.rs` | Workload loading: extracts the tarball and deserializes all workloads |
| `benches/workload_bench.rs` | Criterion entry point — loads workloads, builds runners, drives benchmarks |
