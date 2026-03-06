# CLAUDE.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the
Delta protocol so connectors can read and write Delta tables without understanding protocol
internals. Kernel never does I/O directly -- it defines _what_ to do via its APIs
(`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

Current capabilities: table reads with predicates, data skipping, deletion vectors, change
data feed, checkpoints (V1 & V2), log compaction, blind append writes, table creation
(including clustered tables), and catalog-managed table support.

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
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings

# Quick pre-push check (mimics CI)
cargo fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo nextest run --workspace --all-features
```

### Crate Names for `-p` Flag

| Crate                  | Directory       | Description                                    |
|------------------------|-----------------|------------------------------------------------|
| `delta_kernel`         | `kernel/`       | Core library                                   |
| `delta_kernel_ffi`     | `ffi/`          | C/C++ FFI bindings                             |
| `delta_kernel_derive`  | `derive-macros/`| Proc macros                                    |
| `acceptance`           | `acceptance/`   | Acceptance tests (DAT)                         |
| `test_utils`           | `test-utils/`   | Shared test utilities                          |
| `feature_tests`        | `feature-tests/`| Feature flag tests                             |
| `uc-catalog`           | `uc-catalog/`   | Unity Catalog integration (UCCatalog, UCCommitter) |
| `uc-client`            | `uc-client/`    | Unity Catalog REST client                      |

### Feature Flags

- `default-engine` / `default-engine-rustls` / `default-engine-native-tls` -- async
  Arrow/Tokio engine (pick one TLS backend)
- `arrow`, `arrow-XX`, `arrow-YY` -- Arrow version selection (kernel tracks the latest two
  major Arrow releases; `arrow` defaults to latest). Kernel itself does not depend on Arrow,
  but default-engine does.
- `arrow-conversion`, `arrow-expression` -- Arrow interop (auto-enabled by default engine)
- `catalog-managed` -- catalog-managed table support (experimental)
- `clustered-table` -- clustered table write support (experimental)
- `internal-api` -- unstable APIs like `parallel_scan_metadata`. Items are marked with the
  `#[internal_api]` proc macro attribute.
- `test-utils`, `integration-test` -- development only

## Architecture at a Glance

**Snapshot** is the entry point for everything -- an immutable view of a table at a specific
version. From it you build a `Scan` (reads) or `Transaction` (writes).

**Read path:** `Snapshot` -> `ScanBuilder` -> `Scan` -> data. Three execution paths:
`execute()` (simple), `scan_metadata()` (advanced/distributed),
`parallel_scan_metadata()` (two-phase distributed log replay).

**Write path:** `Snapshot` -> `Transaction` -> `commit()`. Kernel provides `WriteContext`,
assembles commit actions, enforces protocol compliance, delegates atomic commit to a
`Committer`.

**Engine trait:** four handlers (`StorageHandler`, `JsonHandler`, `ParquetHandler`,
`EvaluationHandler`). `DefaultEngine` lives in `kernel/src/engine/default/`.

**EngineData:** opaque columnar data interface. IMPORTANT: never access `EngineData` columns
directly -- always use the visitor pattern (`visit_rows` with typed `GetData` accessors).

## Testing

- **Unit tests** test internal APIs and module internals. It is fine to use public APIs
  like `create_table` in a unit test as setup (e.g. to create a table for testing reads,
  writes, or state loading).
- **Integration tests** exercise only public APIs end-to-end.
- Consider how the feature interacts with Delta table features (see Protocol TLDR below).
- Consider write paths: normal commits, checkpointing, CRC files, log compaction files.
- Consider read paths: loading a snapshot from scratch at latest version, at a specific
  version (time travel), and updating from an existing snapshot.
- Consider table state: only deltas (`00.json` to `0N.json`), after a checkpoint, after
  a CRC (`0N.crc`) file, after log compaction, etc.
- Prefer descriptive test names over doc comments. Encode the scenario and expected
  behavior in the test name. Only add a test doc comment when the intent is too
  verbose or complex to express succinctly in the name.
- Use `rstest` to parameterize tests that share the same logic but differ in setup
  or inputs. Prefer `#[case]` over duplicating test functions.
- Reuse helpers from `test_utils` instead of writing custom ones when possible.

## Protocol TLDR

The [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)
is the source of truth. Key concepts:

- **Actions** -- atomic units of a transaction: Metadata, Add File, Remove File, Add CDC
  File, Protocol, CommitInfo, Domain Metadata, Sidecar, Checkpoint Metadata
- **Log structure** -- JSON commit files, checkpoints (V1 parquet, V2 multi-part), log
  compaction files, version checksum (CRC) files, `_last_checkpoint`
- **Protocol versioning** -- (readerVersion, writerVersion) pair. At (3, 7) switches to
  explicit table features via `readerFeatures`/`writerFeatures` arrays. Features cannot be
  removed once added.
- **Data skipping** -- per-file column statistics (min, max, null count, row count) with
  tight/wide bounds
- **Schemas** -- JSON serialization format for StructType/StructField/DataType
- **Stats and partition values** -- per-file column statistics (min, max, nullCount) and
  partition values are stored as JSON strings in Add file actions. The stats JSON structure
  mirrors the table schema. See the protocol spec sections on "Per-file Statistics" and
  "Partition Value Serialization" for the exact formats.

**Table features**:

- Writer: `appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`,
  `allowColumnDefaults`, `changeDataFeed`, `identityColumns`, `rowTracking`,
  `domainMetadata`, `icebergCompatV1`, `icebergCompatV2`, `clustering`,
  `inCommitTimestamp`
- Reader + writer: `columnMapping`, `deletionVectors`, `timestampNtz`,
  `v2Checkpoint`, `vacuumProtocolCheck`, `variantType`, `variantType-preview`,
  `typeWidening`

Keep this list updated when new protocol features are added to kernel.

## Common Gotchas

- **EngineData is opaque:** Never downcast to `ArrowEngineData` or any concrete type
  in production code (ok in tests). Never assume one batch per file -- always iterate.
- **Column mapping:** Physical column names can differ from logical names. Always use
  the schema from `Snapshot::schema()` for user data columns. Metadata/system schema
  column names (defined by the protocol) are not subject to column mapping.

## Code Style / Documentation

- MUST include doc comments for all public functions, structs, enums, and methods.
- MUST document function parameters, return values, and errors.
- Keep comments up-to-date with code changes.
- Include examples in doc comments for complex functions only.
- NEVER use emoji or unicode in comments that emulates emoji (e.g. special arrows,
  checkmarks). Use ASCII equivalents (`->`, `=>`, etc.) instead.
- Comments should be concise and non-repetitive -- find the right place to say it once.
- Comments should never include temporal references -- only refer to current code and
  design, not past iterations.
- Doc comments focus on "what" (contract with caller) more than "how" (implementation),
  unless the "how" meaningfully impacts the "what".
- Code comments state intent and explain "why" -- don't restate what the code self-documents.
- Place `use` imports at the top of the file (for non-test code) or at the top of the
  `mod tests` block (for test code) -- never inside function bodies.

## Pull Requests

**Title:** use conventional commit format, lowercase after prefix, no period at the end.
Allowed types: `feat`, `fix`, `refactor`, `chore`, `docs`, `perf`, `test`, `ci`.
If the pull request contains a breaking change, the type must have a `!` suffix.
Examples: `feat: add checkpoint stream support`, `fix: handle empty log segment`,
`refactor: extract common log replay logic`
Breaking change examples: `feat!: make_physical takes column mapping and sets parquet field ids`,
`chore!: remove the arrow-55 feature`

**Description:** follow the template in `.github/PULL_REQUEST_TEMPLATE.md`. Error on the
side of simplicity -- don't list every change. Focus on key API changes, functionality,
and data flow. Keep it concise.

## Deep Context

Read these when relevant to the task at hand:
- `CLAUDE/architecture.md` -- kernel architecture: snapshot loading, read/write paths,
  engine trait system, EngineData, key modules, catalog-managed tables
- Always cross-check protocol behavior against the
  [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)

**Keeping docs current:** If you notice anything inaccurate in these docs -- renamed
structs, traits, functions, modules, crates, APIs, stale data flows, wrong file paths --
inform the user so they can be updated. After major changes, update this file,
`CLAUDE/architecture.md`, `ffi/CLAUDE.md`, and any relevant `<crate>/CLAUDE.md` files.
