# Delta Kernel Benchmarking

This crate contains benchmarking infrastructure for Delta Kernel using Criterion and JSON workload specs. It is separate from the `kernel` crate to keep benchmark-specific code and dependencies out of the core library.

## Running benchmarks
```bash
# run all benchmarks
cargo bench -p delta_kernel_benchmarks

# run a specific bench binary
cargo bench -p delta_kernel_benchmarks --bench workload_bench

# filter to benchmarks whose name contains a substring (Criterion substring matching)
cargo bench -p delta_kernel_benchmarks --bench workload_bench "some_table_name"
```

## Entities

### `TableInfo`

Deserialized from a `table_info.json` file. Describes a Delta table and includes its name, an optional human-readable description, and either an explicit `table_path` (for remote tables) or a local path (`delta/` subdirectory at the same directory level as `table_info.json`). Note that `table_path` is mainly intended for remote tables (e.g. S3), but support for remote tables is not yet implemented; all current workloads are under `delta/` as described.

```json
{
  "name": "basic_append",
  "description": "A basic table with two append writes.",
}
```

### `Spec`

Deserialized from a JSON file in a table's `specs/` directory. Describes a single operation to benchmark against a table. Two variants are supported:

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
  "type": "snapshot_construction"
}
```
Or with a specific version:

```json
{
  "type": "snapshot_construction",
  "version": 0
}
```

### `Workload`

Includes `TableInfo`, `case_name`, and `Spec` — the unit of work that gets benchmarked. A single table can have multiple workloads (one per spec file).

### `ReadConfig`

Specifies runtime parameters for `Read` workloads that are not part of the spec JSON — currently whether to scan serially or in parallel, and how many threads to use. Multiple configs can be applied to the same workload to compare modes. By default all workloads run serial log replay; workloads with sidecar files additionally run parallel configs to benchmark parallel scanning.

### `WorkloadRunner`

Owns all pre-built state for a workload (e.g. a pre-constructed `Snapshot`) so that `execute()` measures only the target operation. Each runner corresponds to one `Workload` plus whatever additional configuration that workload type requires — `Read` workloads take a `ReadConfig`, while `SnapshotConstruction` workloads require no extra configuration.


## Workload Data

Workloads are loaded from `benchmarks/data/workloads.tar.gz`. On first run the tarball is extracted to `benchmarks/data/workloads/` and a `.done` file is written to skip re-extraction on subsequent runs. To pick up changes to the tarball, delete the `.done` file.

Workloads are discovered automatically by path — there is no explicit registry. `load_all_workloads()` scans every subdirectory of `workloads/benchmarks/`, and for each one loads `table_info.json` and every spec file found under `specs/`. Each `(table, spec file)` pair becomes one `Workload`, with the spec filename (without extension) used as the `case_name`.

The expected layout inside the archive is:

```
workloads/
├── benchmarks/
│   └── <table_name>/
│       ├── table_info.json
│       ├── delta/              # Delta table (if no explicit table_path)
│       └── specs/
│           └── <case_name>.json
└── tests/                      # reserved for future test workloads (currently empty)
```

## Source Layout

| File | Purpose |
|------|---------|
| `src/models.rs` | Data types: `TableInfo`, `Spec`, `Workload`, `ReadConfig`, `ReadOperation` |
| `src/runners.rs` | `WorkloadRunner` trait and implementations: `ReadMetadataRunner`, `SnapshotConstructionRunner` |
| `src/utils.rs` | Workload loading: extracts the tarball and deserializes all workloads |
| `benches/workload_bench.rs` | Criterion entry point — loads workloads, builds runners, drives benchmarks |
