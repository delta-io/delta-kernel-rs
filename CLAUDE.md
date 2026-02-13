# CLAUDE.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the Delta protocol so connectors can read and write Delta tables without understanding protocol internals. Kernel never does I/O directly -- it defines _what_ to do via its APIs (`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

Current capabilities: table reads with predicates, data skipping, deletion vectors, change data feed, checkpoints (V1 & V2), log compaction, blind append writes, table creation (including clustered tables), and catalog-managed table support.

## Build & Test Commands

```bash
# Build
cargo build --workspace --all-features

# Run all tests (prefer nextest over cargo test)
cargo nextest run --workspace --all-features

# Run tests for a specific crate
cargo nextest run -p delta_kernel --all-features

# Run a single test in a specific crate (fastest -- only compiles that crate)
cargo nextest run -p delta_kernel --lib --all-features test_name_here

# Run a test by name, searching all crates (slow -- compiles everything)
cargo nextest run --workspace --all-features test_name_here

# Format and lint (always run after code changes)
cargo fmt && cargo clippy --benches --tests --all-features -- -D warnings

# Quick pre-push check (mimics CI)
cargo fmt && cargo clippy --benches --tests --all-features -- -D warnings && cargo nextest run --workspace --all-features
```

### Crate Names for `-p` Flag

| Crate | Directory | Description |
|-------|-----------|-------------|
| `delta_kernel` | `kernel/` | Core library |
| `delta_kernel_ffi` | `ffi/` | C/C++ FFI bindings |
| `delta_kernel_derive` | `derive-macros/` | Proc macros |
| `acceptance` | `acceptance/` | Acceptance tests (DAT) |
| `test_utils` | `test-utils/` | Shared test utilities |
| `feature_tests` | `feature-tests/` | Feature flag tests |
| `uc-catalog` | `uc-catalog/` | Unity Catalog integration (UCCatalog, UCCommitter) |
| `uc-client` | `uc-client/` | Unity Catalog REST client |

### Feature Flags

- `default-engine` / `default-engine-rustls` / `default-engine-native-tls` -- async Arrow/Tokio engine (pick one TLS backend)
- `arrow`, `arrow-56`, `arrow-57` -- Arrow version selection
- `arrow-conversion`, `arrow-expression` -- Arrow interop (auto-enabled by default engine)
- `catalog-managed` -- catalog-managed table support (experimental)
- `clustered-table` -- clustered table write support (experimental)
- `internal-api` -- unstable APIs like `parallel_scan_metadata`
- `test-utils`, `integration-test` -- development only

## Architecture at a Glance

**Snapshot** is the entry point for everything -- an immutable view of a table at a version. From it you build a `Scan` (reads) or `Transaction` (writes).

**Read path:** `Snapshot` -> `ScanBuilder` -> `Scan` -> data. Three execution paths: `execute()` (simple), `scan_metadata()` (advanced/distributed), `parallel_scan_metadata()` (two-phase distributed log replay).

**Write path:** `Snapshot` -> `Transaction` -> `commit()`. Kernel provides `WriteContext`, assembles commit actions, enforces protocol compliance, delegates atomic commit to a `Committer`.

**Engine trait:** four handlers (`StorageHandler`, `JsonHandler`, `ParquetHandler`, `EvaluationHandler`). `DefaultEngine` lives in `kernel/src/engine/default/`.

**EngineData:** opaque columnar data interface. IMPORTANT: never access `EngineData` columns directly -- always use the visitor pattern (`visit_rows` with typed `GetData` accessors).

## Testing

- Include unit tests for new functionality.
- Include integration tests when feasible.
- Consider how the feature interacts with Delta table features (see Protocol TLDR below).
- Consider write paths: normal commits, checkpointing, CRC (version checksum) files, log compaction files.
- Consider read paths: loading a snapshot from scratch at latest version, at a specific version (time travel), and updating from an existing snapshot.

## Protocol TLDR

The Delta protocol spec (`~/delta/PROTOCOL.md`) is the source of truth. Key concepts:

- **Actions** -- atomic units of a transaction: Metadata, Add File, Remove File, Add CDC File, Protocol, CommitInfo, Domain Metadata, Sidecar, Checkpoint Metadata
- **Log structure** -- JSON commit files, checkpoints (V1 parquet, V2 multi-part), log compaction files, version checksum (CRC) files, `_last_checkpoint`
- **Protocol versioning** -- (readerVersion, writerVersion) pair. At (3, 7) switches to explicit table features via `readerFeatures`/`writerFeatures` arrays. Features cannot be removed once added.
- **Data skipping** -- per-file column statistics (min, max, null count, row count) with tight/wide bounds
- **Schemas** -- JSON serialization format for StructType/StructField/DataType

**Table features** (21 total, R=reader, W=writer):

- W: `appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`, `allowColumnDefaults`, `changeDataFeed`, `identityColumns`, `rowTracking`, `domainMetadata`, `icebergCompatV1`, `icebergCompatV2`, `clustering`, `inCommitTimestamp`
- R+W: `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`, `variantType`, `variantType-preview`, `typeWidening`

## Code Style / Documentation

- MUST include doc comments for all public functions, structs, enums, and methods.
- MUST document function parameters, return values, and errors.
- Keep comments up-to-date with code changes.
- Include examples in doc comments for complex functions only.
- NEVER use emoji or unicode in comments that emulates emoji (e.g. `→`, `✓`, `✗`). Use ASCII equivalents (`->`, `=>`, etc.) instead.

## Pull Requests

**Title:** use conventional commit format, lowercase after prefix, no period at the end.
Examples: `feat: add checkpoint stream support`, `fix: handle empty log segment`, `refactor: extract common log replay logic`

**Description:** follow the template in `.github/PULL_REQUEST_TEMPLATE.md`. Error on the side of simplicity -- don't list every change. Focus on key API changes, functionality, and data flow. Keep it concise.

## Deep Context

Read these when relevant to the task at hand:
- `claude/architecture.md` -- kernel architecture: snapshot loading, read/write paths, engine trait system, EngineData, key modules, catalog-managed tables
- `claude/references.md` -- Java kernel and Spark reference files for cross-checking implementations
- Always cross-check protocol behavior against `~/delta/PROTOCOL.md`

**Keeping docs current:** After major module changes or new capabilities, update `claude/architecture.md` and this file to reflect the new state. If you notice stale types, traits, file paths, or APIs in these docs, inform the user so they can be updated.
