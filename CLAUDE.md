# CLAUDE.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the
Delta protocol so connectors can read and write Delta tables without understanding protocol
internals. Kernel never does I/O directly -- it defines _what_ to do via its APIs
(`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

Current capabilities: table reads with predicates, data skipping, deletion vectors, change
data feed, checkpoints (V1 & V2), log compaction (disabled, #2337), blind append writes, table creation
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

# Format, lint, and doc check (always run after code changes)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo doc --workspace --all-features --no-deps

# Workspace no-default-features lint for crates that depend on kernel's Arrow APIs
cargo clippy --workspace --no-default-features --features arrow \
  --exclude delta_kernel --exclude delta_kernel_ffi --exclude delta_kernel_derive --exclude delta_kernel_ffi_macros -- -D warnings

# Quick pre-push check (mimics CI)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo doc --workspace --all-features --no-deps \
  && cargo nextest run --workspace --all-features
```

### Crate Names for `-p` Flag

| Crate                                    | Directory                                  | Description                                             |
|------------------------------------------|--------------------------------------------|---------------------------------------------------------|
| `delta_kernel`                           | `kernel/`                                  | Core library                                            |
| `delta_kernel_default_engine`            | `default-engine/`                          | Default Arrow/Tokio `Engine` implementation             |
| `delta_kernel_ffi`                       | `ffi/`                                     | C/C++ FFI bindings                                      |
| `delta_kernel_derive`                    | `derive-macros/`                           | Proc macros                                             |
| `acceptance`                             | `acceptance/`                              | Acceptance tests (DAT)                                  |
| `test_utils`                             | `test-utils/`                              | Shared test utilities                                   |
| `feature_tests`                          | `feature-tests/`                           | Feature flag tests                                      |
| `delta-kernel-unity-catalog`             | `delta-kernel-unity-catalog/`              | Unity Catalog integration (UCKernelClient, UCCommitter) |
| `unity-catalog-delta-client-api`         | `unity-catalog-delta-client-api/`          | Unity Catalog client traits and shared models           |
| `unity-catalog-delta-rest-client`        | `unity-catalog-delta-rest-client/`         | Unity Catalog REST client                               |

### Feature Flags

- `default-engine-rustls` / `default-engine-native-tls` -- backwards-compat no-op shims on the
  kernel crate. Consumers should depend on `delta_kernel_default_engine` directly and pick a TLS
  backend there (`rustls` or `native-tls`).
- `arrow`, `arrow-XX`, `arrow-YY` -- Arrow version selection (kernel tracks the latest two
  major Arrow releases; `arrow` defaults to latest). Kernel itself does not depend on Arrow,
  but the default engine does.
- `arrow-conversion`, `arrow-expression` -- Arrow interop (auto-enabled by `default-engine-base`)
- `prettyprint` -- enables Arrow pretty-print helpers (primarily test/example oriented)
- `clustered-table` -- clustered table write support (experimental)
- `internal-api` -- unstable APIs like `parallel_scan_metadata`. Items are marked with the
  `#[internal_api]` proc macro attribute.
- `test-utils`, `integration-test` -- development only (`test-utils` enables `prettyprint`)

## Architecture at a Glance

**Snapshot** is the entry point for everything -- an immutable view of a table at a specific
version. From it you build a `Scan` (reads) or `Transaction` (writes).

**Read path:** `Snapshot` -> `ScanBuilder` -> `Scan` -> data. Three execution paths:
`execute()` (simple), `scan_metadata()` (advanced/distributed),
`parallel_scan_metadata()` (two-phase distributed log replay).

**Write path:** `Snapshot` -> `Transaction` -> `commit()`. Kernel provides `WriteContext`
(via `partitioned_write_context` or `unpartitioned_write_context`), assembles commit
actions, enforces protocol compliance, delegates atomic commit to a `Committer`.

**Engine trait:** five handlers (`StorageHandler`, `JsonHandler`, `ParquetHandler`,
`EvaluationHandler`, optional `MetricsReporter`). `DefaultEngine` lives in
`kernel/src/engine/default/`.

**EngineData:** opaque columnar data interface. IMPORTANT: never access `EngineData` columns
directly -- always use the visitor pattern (`visit_rows` with typed `GetData` accessors).

## Testing

- **Unit tests** test internal APIs and module internals. It is fine to use public APIs
  like `create_table` in a unit test as setup (e.g. to create a table for testing reads,
  writes, or state loading).
- **Integration tests** exercise only public APIs end-to-end. See `kernel/tests/README.md`
  for a catalog of available test tables (schema, protocol, features, and which tests use
  them). Consult it before creating new test data to avoid duplication.
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
  or inputs. Prefer `#[case]` over duplicating test functions. When parameters are
  independent and form a cartesian product, prefer `#[values]` over enumerating
  every combination with `#[case]`.
- Actively look for rstest consolidation opportunities: when writing multiple tests
  that share the same setup/flow and differ only in configuration and expected
  outcome, write one parameterized rstest instead of separate functions. Also check
  whether a new test duplicates the flow of an existing nearby test and should be
  merged into it as a new `#[case]`. A common pattern is toggling a feature (e.g.
  column mapping on/off) and asserting success vs. error.
- Reuse helpers from `test_utils` and the integration-test fixtures instead of writing
  custom ones when possible. See **Common test helpers** below for a curated starter list.
- **Committing in tests:** Use `txn.commit(engine)?.unwrap_committed()` to assert a
  successful commit and get the `CommittedTransaction`. Do NOT use `match` + `panic!`
  for this -- `unwrap_committed()` provides a clear error message on failure. Available
  under `#[cfg(test)]` and the `test-utils` feature.
- **Prefer snapshot/public API assertions over reading raw commit JSON.** Only read raw
  commit JSON when the data is inaccessible via public API (e.g., system domain metadata
  is blocked by `get_domain_metadata`). For commit JSON reads, use `read_actions_from_commit`
  from `test_utils` -- do NOT write local helpers that duplicate this.
- **`add_commit` and table setup in tests:** `add_commit` takes a `table_root` string and
  resolves it to an absolute object-store path. The `table_root` must be a proper URL string
  with a trailing slash (e.g. `"memory:///"`, `"file:///tmp/my_table/"`). Avoid using the
  `Url` type directly -- most test helpers and kernel APIs accept `impl AsRef<str>`, so pass
  URL strings instead. When using local storage, use an un-prefixed store
  (`LocalFileSystem::new()`) with a `file:///` URL string. Do NOT use
  `LocalFileSystem::new_with_prefix()` with `add_commit` -- the prefix causes double-nesting
  because `add_commit` already resolves the full path from the URL. For in-memory tests, use
  `InMemory::new()` with `"memory:///"`. Always use the same `table_root` URL string for
  both `add_commit` (writing log files) and `snapshot`/`Snapshot::try_new` (reading the
  table). Always include a trailing slash in directory URLs to ensure correct path joining.

### Common test helpers

Before writing a custom helper, check this curated list and the locations below.
This list is non-exhaustive -- when in doubt, browse the source files directly
(`test-utils/src/lib.rs`, `kernel/tests/integration/common/`,
`kernel/tests/integration/<topic>/mod.rs`).

**Arrow construction (from `delta_kernel::arrow`)**

- `arrow::array::new_null_array(&arrow_type, n)` -- Arrow array of `n` nulls of any Arrow
  type. Prefer this over per-type `Int32Array::from(vec![None as Option<i32>])` builders.
- `engine::arrow_conversion::TryIntoArrow`:
  `(&kernel_data_type).try_into_arrow()` for `DataType`,
  `(&kernel_struct_type).try_into_arrow()` for `StructType` -> Arrow `Schema`.

**Engine + table setup (from `test_utils`)**

- `test_table_setup()` / `test_table_setup_mt()` -- engine + temp table path. Use the `_mt`
  variant under `#[tokio::test(flavor = "multi_thread")]`.
- `engine_store_setup(name, opts)` -- returns `(store, engine, table_location)` when a test
  needs direct object-store access.
- `setup_test_tables(...)` -- multiple pre-built tables for read/scan tests.

**Table creation in tests**

- Prefer the kernel `create_table` builder
  (`delta_kernel::transaction::create_table::create_table`). It exercises the same path
  connectors use and auto-derives the protocol from the schema and feature flags.
- `test_utils::create_table` (a JSON helper that hand-rolls protocol + metadata) is older
  but still needed when the kernel builder cannot enable a particular feature combination.

**Schema fixtures**

- `test_utils`: `nested_schema`, `schema_with_type`, `nested_schema_with_type`,
  `multi_schema_with_type`, `top_level_ntz_schema` / `nested_ntz_schema` /
  `multiple_ntz_schema`, `top_level_variant_schema` / `nested_variant_schema` /
  `multiple_variant_schema`.
- `kernel/tests/integration/create_table/mod.rs`: `simple_schema`, `partition_test_schema`.

**Commit + read helpers (from `test_utils`)**

- `add_commit`, `add_staged_commit` -- write a JSON commit at a given version.
- `read_actions_from_commit` -- read raw JSON actions from a specific commit. Use this
  instead of hand-rolled `serde_json` parsing.
- `test_read` -- full-scan read of a table; use for round-trip assertions.
- `into_record_batch` -- convert `Box<dyn EngineData>` to Arrow `RecordBatch`.

**Assertion helpers (from `test_utils`)**

- `assert_schema_has_field(schema, &["a", "b"])` -- assert a (possibly nested) field path.
- `assert_result_error_with_message(result, "needle")` -- assert an error contains a
  substring.

**If a name here doesn't match what's in code:** the list may have drifted from a rename.
Run `rg '^pub (fn|async fn)' test-utils/src/lib.rs` to discover the current public surface,
and update this section in your PR. The same pattern works for
`kernel/tests/integration/common/write_utils.rs`.

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
  `domainMetadata`, `icebergCompatV1`, `icebergCompatV2`, `icebergCompatV3`,
  `clustering`, `inCommitTimestamp`
- Reader + writer: `catalogManaged`, `catalogOwned-preview`, `columnMapping`,
  `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`,
  `variantType`, `variantType-preview`, `typeWidening`

Keep this list updated when new protocol features are added to kernel.

## Common Gotchas

- **EngineData is opaque:** Never downcast to `ArrowEngineData` or any concrete type
  in production code (ok in tests). Never assume one batch per file -- always iterate.
- **Column mapping:** Physical column names can differ from logical names. Always use
  the schema from `Snapshot::schema()` for user data columns. Metadata/system schema
  column names (defined by the protocol) are not subject to column mapping.
- **Transforms:** Generic recursive schema and expression transform traits and helpers
  are in `kernel/src/transforms/`.
- **Tracing layer callbacks must not emit tracing events directly:** Calling `warn!()` or
  any tracing macro inside a `tracing_subscriber::Layer` callback (`on_event`, `on_record`,
  `on_close`) while holding a span's `extensions_mut()` write lock will re-enter the layer
  and deadlock on the same lock. In `on_new_span`, no extension lock is held during
  `attrs.record()`, so direct `warn!()` is safe there. In `on_event` and `on_record`, store
  warnings in a `pending_warnings: Vec<String>` field on the visitor, take them out after
  the extensions block closes, and emit via `warn!()` only then. See
  `kernel/src/metrics/reporter.rs` for the canonical pattern.

## Code Style / Documentation

- Line width is 100 characters. Wrap comments and string literals at 100, not 80.
- Use `==` as a visual section divider in comments (e.g. `// === Helpers ===` or
  `// ============`).
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
- Prefer `==` over `matches!` for simple single-variant enum comparisons. `matches!` is
  for patterns with bindings or guards. For example: `self == Variant` not
  `matches!(self, Variant)`.
- Prefer `StructField::nullable` / `StructField::not_null` over
  `StructField::new(name, type, bool)` when nullability is known at compile time.
  Reserve `StructField::new` for cases where nullability is a runtime value.
- NEVER panic in production code -- use errors instead. Panicking
  (including `unwrap()`, `expect()`, `panic!()`, `unreachable!()`, etc) is acceptable in test code only.

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

### CI Jobs and Github Actions

**Supply chain security:** every `cargo` command in CI that resolves dependencies MUST use
`--locked` to enforce the committed `Cargo.lock`. This prevents CI from silently picking up
a newer (potentially compromised) transitive dependency. If `Cargo.lock` is out of sync with
`Cargo.toml`, the build fails immediately, forcing dependency changes to be explicit and
reviewable. See the top-level comment in `build.yml` for full rationale. Commands exempt from
`--locked`: `cargo +nightly fmt` (no dep resolution), `cargo msrv verify/show` (wrapper tool),
`cargo miri setup` (tooling setup).

Ensure that when writing any github action you are considering safety including thinking of
and mitigating common attack vectors such expression injection and pull request target attacks.

Example:
```yaml
# The code below is vulnerable to expression injection
run: |
    echo "Comment: ${{ github.event.comment.body }}"

# To mitigate instead use environment variables
env:
    COMMENT_BODY: ${{ github.event.comment.body }}
run: |
    echo "Comment: $COMMENT_BODY"
```

## Deep Context

Read these when relevant to the task at hand:
- `CLAUDE/architecture.md` -- kernel architecture: snapshot loading, read/write paths,
  engine trait system, EngineData, key modules, catalog-managed tables
- `docs/user-guide/CLAUDE.md` -- writing standards for the mdBook user guide
- Always cross-check protocol behavior against the
  [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)

**Keeping docs current:** If you notice anything inaccurate in these docs -- renamed
structs, traits, functions, modules, crates, APIs, stale data flows, wrong file paths --
inform the user so they can be updated. After major changes, update this file,
`CLAUDE/architecture.md`, `ffi/CLAUDE.md`, and any relevant `<crate>/CLAUDE.md` files.
