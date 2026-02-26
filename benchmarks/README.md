# delta-kernel-rs Benchmarks

This crate contains the workload-driven benchmarking framework for delta-kernel-rs. It is separate from the `kernel` crate to keep benchmark-specific code and dependencies out of the core library.

Benchmarks are run with [Criterion](https://github.com/bheisler/criterion.rs). All required features are declared in this crate's `Cargo.toml`, so no extra flags are needed:

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

Deserialized from a `table_info.json` file. Describes a Delta table — its name, an optional human-readable description, and either an explicit `table_path` (for remote tables) or a convention-based local path (`delta/` subdirectory alongside `table_info.json`).

### `Spec`

Deserialized from a JSON file in a table's `specs/` directory. Describes a single operation to benchmark against a table. Two variants are supported:

- **`Read`** — scan a table at an optional version (defaults to latest)
- **`SnapshotConstruction`** — measure the cost of building a `Snapshot` from scratch at an optional version (defaults to latest)

### `Workload`

A `(TableInfo, case_name, Spec)` triple — the unit of work that gets benchmarked. A single table can have multiple workloads (one per spec file).

### `ReadConfig`

Specifies runtime parameters for `Read` workloads that are not part of the spec JSON — currently whether to scan serially or in parallel, and how many threads to use. Multiple configs can be applied to the same workload to compare modes.

### `WorkloadRunner`

Owns all pre-built state for a workload (e.g. a pre-constructed `Snapshot`) so that `execute()` measures only the target operation. Each runner corresponds to one `Workload` plus whatever additional configuration that workload type requires — `Read` workloads take a `ReadConfig`, while `SnapshotConstruction` workloads require no extra configuration.

## Entity Relationships

```
TableInfo (table_info.json)
    └── + Spec (<case_name>.json) + case_name  →  Workload
                                                    ├── [Read]                + ReadConfig  →  WorkloadRunner
                                                    └── [SnapshotConstruction]              →  WorkloadRunner
                                                                                                 └── .execute()  (benchmarked)
```

## Workload Data

Workloads are loaded from `kernel/tests/data/workloads.tar.gz`. On first run the tarball is extracted to `kernel/tests/data/workloads/`. The expected layout inside the archive is:

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
