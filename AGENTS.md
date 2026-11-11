# AGENTS.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the
Delta protocol so connectors can read and write Delta tables without understanding protocol
internals. Kernel never does I/O directly: it defines _what_ to do via its APIs
(`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

Current capabilities include table reads with predicates, data skipping, deletion vectors,
change data feed, incremental scans (`incremental_scan_builder`) and commit ranges, checkpoints
(V1 & V2), version checksums, blind appends, file removals, writes to tables with generated
columns (the connector computes and verifies the values after `Transaction::ack_generated_columns`),
table creation (including clustered tables), limited schema alteration, and catalog-managed tables.
Log compaction remains disabled (#2337).

## Documentation Ownership

Choose where to document something by the reader's task and the scope of the explanation:

- **Rustdoc** is authoritative for public API behavior and usage. Crate and module docs explain
  how APIs work together and how to choose between them. Type and trait docs explain purpose,
  lifecycle, and usage; function and method docs explain the caller's contract. Keep enough
  context for readers to use the API without first reading the guide. Follow the
  [comment and documentation style](#comment--doc-style), including its guidance on errors and
  doctests.
- **The user guide** provides introductions, longer examples, and broader connector workflows
  that would make everyday API navigation harder if included in rustdoc. Keep enough context to
  follow each page, and link to rustdoc for API details. Follow the
  [guide maintenance policy](docs/user-guide/README.md#maintenance).
- **Source comments** explain implementation invariants, pitfalls, and low-level design decisions
  next to the code they concern.
- **Agent docs** explain repository workflows, test helpers, code conventions, source navigation,
  and internal design considerations spanning modules. Link to source comments for implementation
  details.

Give each contract, inventory, or dependency setup example one authoritative location. Summarize
what readers need for their task and link to that location instead of maintaining another copy.
Use crate manifests for package and feature inventories, and the Delta protocol spec for protocol
rules. An explanation involving several APIs can still belong in module or type rustdoc; neither
the number of APIs nor the length alone determines its home.

For example, document how to use `Snapshot::checkpoint` and its caller requirements in the method's
rustdoc. Explain how the checkpoint APIs work together in module docs. Use the guide for a
connector's maintenance workflow, including scheduling and operational choices, with links to the
API details.

- **Good:** Explain when to choose `Scan::execute` or `Scan::scan_metadata` in the scan module's
  rustdoc. Use the guide to develop a distributed connector workflow using those APIs.
- **Bad:** Replace the module explanation with "See the user guide" because it discusses multiple
  APIs.

## Build & Test Commands

> **`datafusion-executor` and `integration-tests` are separate workspaces.** Root `--workspace`
> commands do not include them. For `datafusion_executor` commands, see
> `datafusion-executor/CLAUDE.md`. `integration-tests/test-all-arrow-versions.sh` tests each
> supported Arrow version.

```bash
# Build
cargo build --workspace --all-features

# Run all tests (prefer nextest over cargo test)
cargo nextest run --workspace --all-features

# Run tests for a specific crate
cargo nextest run -p delta_kernel --all-features

# Run a single test in a specific crate (fastest: only compiles that crate)
cargo nextest run -p delta_kernel --lib --all-features test_name_here

# Run a test by name, searching all crates (slow: compiles everything)
cargo nextest run --workspace --all-features test_name_here

# Format, lint, and doc check (always run after code changes)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo doc --workspace --all-features --no-deps

# Split no-default-features CI checks (cargo aliases from .cargo/config.toml)
cargo clippy-no-default-kernel-dependents
cargo check-no-default-kernel
cargo check-no-default-engine
cargo clippy-no-default-kernel-leaves

# Download the latest upstream catalog and record its commit and hash.
cargo xtask update-delta-error-catalog

# Generate DeltaErrorCondition from the local catalog and selected-conditions.json.
# --check fails if stale, writing nothing.
cargo xtask generate-delta-error-conditions [--check]

# Quick pre-push check (mimics CI)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo doc --workspace --all-features --no-deps \
  && cargo nextest run --workspace --all-features \
  && cargo test --workspace --all-features --doc
```

### Crate Names for `-p` Flag

| Crate | Directory | Description |
|---|---|---|
| `delta_kernel` | `kernel/` | Core library |
| `delta_kernel_default_engine` | `default-engine/` | Default Arrow/Tokio `Engine` implementation |
| `delta_kernel_default_engine_test_utils` | `default-engine/test-utils/` | Default-engine test utilities |
| `delta_kernel_ffi` | `ffi/` | C/C++ FFI bindings |
| `delta_kernel_ffi_macros` | `ffi-proc-macros/` | FFI proc macros |
| `delta_kernel_derive` | `derive-macros/` | Proc macros |
| `acceptance` | `acceptance/` | Acceptance tests (DAT) |
| `test_utils` | `test-utils/` | Shared test utilities |
| `delta_kernel_workloads` | `workloads/` | Shared workload spec types + SQL predicate parser |
| `delta_kernel_benchmarks` | `benchmarks/` | Workload benchmarks |
| `feature_tests` | `feature-tests/` | Feature flag tests |
| `mem-test` | `mem-test/` | Memory-usage test executable |
| `delta-kernel-unity-catalog` | `delta-kernel-unity-catalog/` | Unity Catalog integration (UCCommitter, snapshot + create-table helpers) |
| `unity-catalog-delta-client-api` | `unity-catalog-delta-client-api/` | Transport-agnostic UC client traits + wire models |
| `unity-catalog-delta-rest-client` | `unity-catalog-delta-rest-client/` | REST/HTTP client for the Unity Catalog Delta Tables API |
| `xtask` | `xtask/` | Dev tasks (regenerates checked-in code from pinned inputs) |

Packages under `kernel/examples/` are also workspace members. Use the package name from the
example's `Cargo.toml` with `-p`.

### Feature Flags

`[features]` in `kernel/Cargo.toml` is the authoritative feature list. Each feature's comment says
what it enables and, where relevant, what kernel blocks without it. Read those comments before
using, adding, or removing a feature. Cautions agents commonly need:

- TLS backend features (`rustls`, `native-tls`) live on `delta_kernel_default_engine`, not on
  `delta_kernel`.
- `internal-api` makes `#[internal_api]` items public. Treat those items as unstable.
- `*-in-dev` features gate experimental, in-progress support.
- `--all-features` builds against the newest supported Arrow version.

## Architecture at a Glance

`Snapshot` is the entry point for an existing table: an immutable view at one version. Reads go
`Snapshot` -> `ScanBuilder` -> `Scan`, then `execute()`, `scan_metadata()`, or
`parallel_scan_metadata()`. Writes go `Snapshot` -> `UpdateTableTransactionBuilder` ->
`Transaction` -> `commit()`, which delegates the atomic commit to a `Committer`; new tables use
`CreateTableTransactionBuilder`. The `Engine` trait supplies `StorageHandler`, `JsonHandler`,
`ParquetHandler`, and `EvaluationHandler`, plus an optional `PlanExecutor` under
`declarative-plans`. `DefaultEngine` lives in `default-engine/src/`. Metrics use tracing layers,
not an engine handler.

Read `CLAUDE/architecture.md` before changing snapshot loading, log replay, scans, transactions,
committers, engine handlers, `EngineData`, or catalog-managed tables. It explains the builder
types, execution modes, write steps, and data flow.

Rules that span modules:

- Snapshot metadata getters that may read storage belong on `SnapshotReader`
  (`snapshot.reader()`). In-memory getters stay on `Snapshot`.
- Operation names that incremental CRC replay accepts (`FileStatsPolicy` in
  `kernel/src/crc/file_stats.rs`) must be rejected for `UpdateTableOperation::Custom`. Add new
  operations as typed `UpdateTableOperation` variants, so name reservation is automatic, and mirror
  each new variant in the FFI.

## Testing

- **Unit tests** test internal APIs and module internals. It is fine to use public APIs
  like `create_table` in a unit test as setup (e.g. to create a table for testing reads,
  writes, or state loading).
- **Test module layout:** keep short unit-test modules inline in the implementation file.
  When tests grow large enough to warrant a separate file, use `module/mod.rs` for the
  implementation and `module/tests.rs` for the tests. Avoid `module.rs` alongside a
  `module/` directory containing only `tests.rs`.
- **Integration tests** exercise only public APIs end-to-end. See `kernel/tests/README.md`
  for a catalog of available test tables (schema, protocol, features, and which tests use
  them). Consult it before creating new test data to avoid duplication.
- **Consider `TestTableBuilder` (`test_utils::table_builder`)** when a test needs a multi-version
  table in a specific state: data across many commits, checkpoints, CRC files, a stale or missing
  `_last_checkpoint` hint, or post-cleanup logs. Read `CLAUDE/testing.md` for when to choose it
  over `create_table`, the `build_snapshot!` macro, and the `default_sweep` template.
- Consider how the feature interacts with Delta table features (see Protocol TLDR below).
- Consider write paths: normal commits, checkpointing, and CRC files.
- When adding cloud-storage functionality to an engine, such as writing JSON files, make sure to
  test it against S3, Azure, and GCS.
- Consider read paths: loading a snapshot from scratch at latest version, at a specific
  version (time travel), and updating from an existing snapshot.
- Consider table state: only versioned JSON commits, after a checkpoint, and after a version
  checksum (`.crc`) file.
- Prefer descriptive test names over doc comments. Encode the scenario and expected
  behavior in the test name. Only add a test doc comment when the intent is too
  verbose or complex to express succinctly in the name.
- Use `rstest` when tests share logic and differ only in setup, inputs, or expected outcome.
  Prefer `#[case]` over duplicate test functions. When parameters are independent and form a
  cartesian product, prefer `#[values]` over enumerating every combination with `#[case]`. Before
  adding a test, check whether a nearby test already has the same flow and should gain a new
  `#[case]` instead. A common pattern is toggling a feature, such as column mapping, and asserting
  success or error.
- Reuse helpers from `test_utils` and the integration-test fixtures instead of writing custom ones.
  Before writing a helper, check the curated list in `CLAUDE/testing.md`.
- **Committing in tests:** Use `txn.commit(engine)?.unwrap_committed()` to assert a
  successful commit and get the `CommittedTransaction`. When you only need the resulting
  snapshot, use `txn.commit(engine)?.unwrap_post_commit_snapshot()` to get the
  `SnapshotRef` directly. Do NOT use `match` + `panic!` for either: both helpers provide a clear
  error message on failure. Available under `#[cfg(test)]` and the `test-utils` feature.
- **Prefer snapshot/public API assertions over reading raw commit JSON.** Only read raw
  commit JSON when the data is inaccessible via public API (e.g., system domain metadata
  is blocked by `get_domain_metadata`). For commit JSON reads, use `read_actions_from_commit`
  from `test_utils`: do NOT write local helpers that duplicate this.
- **`add_commit` and table setup in tests:** `add_commit` takes a `table_root` string and
  resolves it to an absolute object-store path. The `table_root` must be a proper URL string
  with a trailing slash (e.g. `"memory:///"`, `"file:///tmp/my_table/"`). Avoid using the
  `Url` type directly: most test helpers and kernel APIs accept `impl AsRef<str>`, so pass
  URL strings instead. When using local storage, use an un-prefixed store
  (`LocalFileSystem::new()`) with a `file:///` URL string. Do NOT use
  `LocalFileSystem::new_with_prefix()` with `add_commit`: `add_commit` already resolves the full
  path from the URL, so the prefix causes double-nesting. For in-memory tests, use
  `InMemory::new()` with `"memory:///"`. ALWAYS use the same `table_root` URL string for both
  `add_commit` (writing log files) and `Snapshot::builder_for` (reading the table). ALWAYS include
  a trailing slash in directory URLs to ensure correct path joining.

## Protocol TLDR

The [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)
is the source of truth. Key concepts:

- **Actions**: records in commits and checkpoints: Metadata, Add File, Remove File, Add CDC
  File, Protocol, CommitInfo, SetTransaction, Domain Metadata, Sidecar, Checkpoint Metadata,
  and the feature-gated adaptive metadata `checkpoint` action
- **Log structure**: JSON commit files, checkpoints (V1 parquet, V2 multi-part), log
  compaction files, version checksum (CRC) files, `_last_checkpoint`
- **Protocol versioning**: `(readerVersion, writerVersion)` pair. Reader version 3 requires
  `readerFeatures`; writer version 7 requires `writerFeatures`. Follow each feature's rules for
  activation, dependencies, and any permitted removal.
- **Data skipping**: per-file column statistics (min, max, null count, row count) with
  tight/wide bounds
- **Schemas**: JSON serialization format for StructType/StructField/DataType
- **Stats and partition values**: per-file statistics are stored as a JSON-encoded string in
  the Add action's `stats` field. `partitionValues` is a JSON map from column names to serialized
  string or null values. The stats structure mirrors the table schema. See the protocol spec
  sections on "Per-file Statistics" and "Partition Value Serialization" for the exact formats.

**Table features**:

- Writer: `allowColumnDefaults`, `appendOnly`, `changeDataFeed`, `checkConstraints`,
  `checkpointProtection`, `clustering`, `collations`, `collations-preview`, `domainMetadata`,
  `generatedColumns`, `icebergCompatV1`, `icebergCompatV2`, `icebergCompatV3`,
  `icebergNativeV4-preview`, `identityColumns`, `inCommitTimestamp`, `invariants`,
  `materializePartitionColumns`, `rowTracking`
- Reader + writer: `adaptiveMetadata-preview`, `catalogManaged`, `catalogOwned-preview`,
  `columnMapping`, `deletionVectors`, `fileType`, `fileType-preview`, `geospatial`,
  `timestampNtz`, `typeWidening`, `typeWidening-preview`, `v2Checkpoint`, `vacuumProtocolCheck`,
  `variantShredding`, `variantShredding-preview`, `variantType`, `variantType-preview`

Keep this list updated when new protocol features are added to kernel.

## Common Gotchas

- **EngineData is opaque:** NEVER access its columns directly: ALWAYS use the visitor pattern
  (`visit_rows` with typed `GetData` accessors). NEVER downcast to `ArrowEngineData` or any
  concrete type in production code (ok in tests). NEVER assume one batch per file: ALWAYS iterate.
- **Column mapping:** Physical column names can differ from logical names. ALWAYS use
  the schema from `Snapshot::schema()` for user data columns. Metadata/system schema
  column names (defined by the protocol) are not subject to column mapping.
- **Transforms:** Generic recursive schema and expression transform traits and helpers
  are in `kernel/src/transforms/`.
- **Tracing layer callbacks must not emit tracing events, such as `warn!()`, while holding a span's
  `extensions_mut()` lock:** the event re-enters the layer and deadlocks on the same lock. Collect
  warnings and emit them after the lock is released. `drain_into_visitor` in
  `kernel/src/metrics/reporter.rs` shows the pattern, and its doc comment explains which callbacks
  may warn directly.
- **Keep tests with process-global state safe under concurrency:** `cargo test` runs tests as
  parallel threads in one test binary, while nextest normally runs each test in a separate
  process. Tests for global tracing subscribers and callbacks must not share capture buffers with
  thread-local dispatch tests or assume no other thread can emit an event.
- **Generated code:** files starting with ``//! Generated by `cargo xtask ...` `` are never edited by
  hand. Change the generator in `xtask/` or its inputs and rerun the command.
- **Tests that call `snapshot.checkpoint()` need a multi-threaded runtime:** use
  `test_table_setup_mt()` under `#[tokio::test(flavor = "multi_thread")]`. Checkpointing issues
  nested `block_on` calls that deadlock on a single-threaded runtime or `TokioBackgroundExecutor`.

## Code Style

- For FFI module organization, follow the [source layout guidance](ffi/AGENTS.md#source-layout).
- Line width is 100 characters. Wrap comments and string literals at 100, not 80.
- Place `use` imports at the top of the file (for non-test code) or at the top of the
  `mod tests` block (for test code): never inside function bodies.
- Prefer `==` over `matches!` for simple single-variant enum comparisons. `matches!` is
  for patterns with bindings or guards. For example: `self == Variant` not
  `matches!(self, Variant)`.
- Prefer `#[repr(C)]` enums for closed FFI choice sets instead of integer aliases and constants.
  Add `cbindgen:prefix-with-name=true` so generated variants remain unambiguous. Use an integer
  discriminator only when unknown values are intentionally recoverable, and validate them at the
  boundary. Invalid enum tags are undefined behavior, so unsafe FFI APIs must require valid tags.
- Prefer `StructField::nullable` / `StructField::not_null` over
  `StructField::new(name, type, bool)` when nullability is known at compile time.
  Reserve `StructField::new` for cases where nullability is a runtime value.
- Leverage `impl Into<DataType>` to avoid `DataType::Struct/Array/Map(Box::new(...))`
  boilerplate. `StructType`, `ArrayType`, and `MapType` all implement `Into<DataType>`,
  and constructors like `StructField::new`/`nullable`/`not_null`, `ArrayType::new`, and
  `MapType::new` accept `impl Into<DataType>`. So:
  - When passing to a parameter that accepts `impl Into<DataType>`, pass the container
    type directly: `StructField::nullable("a", ArrayType::new(DataType::INTEGER, true))`; do NOT
    wrap in `DataType::from(...)` or `.into()` (redundant at best, and an ambiguous-type compile
    error at worst).
  - When a concrete `DataType` value is actually required (e.g. a `DataType`-typed
    binding/field, a `[DataType]`/`Vec<DataType>` element, or a `&DataType` argument),
    prefer `DataType::from(ArrayType::new(...))` over
    `DataType::Array(Box::new(ArrayType::new(...)))`.
- Kernel APIs use `KernelResult<T, E = KernelError>`, which defaults to `KernelError` and permits
  an explicit error type. `Result<T>` is reserved for the top-level `Error` envelope. Return it
  from functions that can produce `Error::Delta` and from every function that propagates their
  errors. `?` converts a `KernelError` into `Error::Kernel`; never convert an `Error` back into a
  `KernelError`. `?` applies one conversion, so a function returning `Result` converts foreign
  errors through `KernelError` first (`.map_err(KernelError::from)?`).
- Prefer the `KernelResultIterator<T>` marker trait over spelling
  `Iterator<Item = KernelResult<T>> + Send`, including in public APIs. Use
  `impl KernelResultIterator<T>` for unboxed streams and `Box<dyn KernelResultIterator<T>>`
  (`'static`) or `Box<dyn KernelResultIterator<T> + '_>` (borrowed) at dyn/storage boundaries.
- Prefer the `lit` / `null_lit` constructors over `Expression::literal(...)` / `lit(Scalar::Null(...))`
  when building expressions inline. They take `impl Into<Scalar>` and `impl Into<DataType>`,
  respectively. Prefer `Predicate::TRUE` / `FALSE` / `NULL` for predicates whose value is statically
  known, reserving `Predicate::literal(b)` for runtime `bool` values.
- Prefer the `col!` macro and `lit(value)` constructor over `Expression::column(...)` /
  `Expression::literal(...)` when building expressions inline. `col!` uses the same
  compile-time segment rules as `column_name!` (string literals split on `.`; constants are
  single simple segments). Use `Expression::column([...])` for runtime or non-simple names.
  (`column_expr!` is a doc-hidden compatibility alias of `col!`.)
- Prefer the `schema!` / `schema_ref!` macros for inline declarative schema literals,
  `lazy_schema_ref!` for `LazyLock<SchemaRef>` statics, and `try_schema!` when names of
  interpolated fields might collide. For Delta log action schemas, reuse the canonical
  `*_FIELD` and `LOG_*_SCHEMA` statics from `actions` instead of re-declaring
  `StructField::nullable(ACTION_NAME, Action::to_schema())` or projecting from
  `get_commit_schema()`. Prefer `StructType::try_new` or schema builder/patch APIs for complex
  data-dependent schema manipulation.
- NEVER panic in production code: use errors instead. Panicking
  (including `unwrap()`, `expect()`, `panic!()`, `unreachable!()`, etc) is acceptable in test
  code only.
- Order a file so the most important APIs and impls come first; put private helper functions
  toward the bottom. Within that, order by visibility: `pub` first, then `pub(crate)`, then
  private. A reader scanning top to bottom should hit the public surface before the private
  plumbing. (Order-sensitive items like `macro_rules!` used within the file are exempt: they
  must precede their use.)

## Comment & Doc Style

### Doc comments

- MUST include doc comments for all public modules, types, traits, macros, functions, and methods;
  see below for what to put in them.
- Start with one concise sentence describing the caller-facing behavior. Add semantics and usage
  details that help callers use the API correctly: preconditions, side effects, ownership, or edge
  cases. Ask what the caller would do differently after reading each detail.
- Rustdoc already displays the signature and links to types. Explain parameter meanings and
  relationships in prose when needed; don't restate types or mention every parameter by rote.
  `# Arguments`, `# Returns`, and `# Errors` sections are optional, not a checklist to fill in.
- Document a function's errors only when the information changes how callers use or recover from
  the API, is an intended part of its public contract, and is not already covered by the error
  type's documentation. Don't enumerate propagated implementation errors or promise an exhaustive
  set of failures.
- Keep safety obligations explicit in `# Safety` sections for unsafe APIs, including relevant
  ownership and lifetime requirements.
- Type and module docs explain the abstraction, how to use it, and its relationships with other
  abstractions. Don't inventory members that rustdoc already lists.
- Keep implementation rationale and maintainer commentary in code comments unless callers need
  the information to use the API.
- Add small, runnable doctests for important entry points when they clarify usage or invariants.
  Scale their number and size to the API's complexity and importance; prefer focused examples over
  a monolithic one. An invariant tested by a doctest need not have a duplicate unit test. Don't add
  examples just to have them.
- Use meaningful assertions in runnable doctests to check the behavior they teach. Use `no_run`
  when execution requires external resources and `compile_fail` to demonstrate invalid usage.

For example, this summary from `ColumnName::parent` explains when the result is absent:

```rust
/// Returns the parent of this column name, or `None` if this is a top-level column.
pub fn parent(&self) -> Option<ColumnName>
```

A separate "Returns an `Option<ColumnName>`" section would add nothing to the signature.

### Code comments

- Code comments state intent and explain "why": don't restate what the code self-documents.
- Comments earn their place only for hidden invariants, real-bug workarounds, or
  constraints the reader can't see from the code itself.
- Don't enumerate what grep can answer. Lists like `// Used by a, b, c` rot the
  moment `d` lands. Describe the shape; let the reader grep.
- Use `==` as a visual section divider in comments (e.g. `// === Helpers ===` or
  `// ============`).

### Shared conventions

- Be succinct. No verbose AI-slop comments. With well-written and well-named code,
  verbose comments are worse than none.
- Say each thing once, in the right place: don't repeat the same idea across
  doc comment and inline comment.
- No stale-prone anchors in durable docs or source comments: counts ("the 10
  variants", "5-arm match"), line numbers, or enumeration lists. Describe the
  shape; let the reader grep.
- Comments MUST NOT include temporal references: only refer to current code and
  design, not past iterations.
- Keep comments up-to-date with code changes.
- NEVER use emoji or unicode in comments that emulates emoji (e.g. special arrows,
  checkmarks). Use ASCII equivalents (`->`, `=>`, etc.) instead.

## Pull Requests

**Title:** use conventional commit format, lowercase after prefix, no period at the end.
Allowed types: `feat`, `fix`, `refactor`, `chore`, `docs`, `perf`, `test`, `ci`.
If the pull request contains a breaking change, the type must have a `!` suffix.
Examples: `feat: add checkpoint stream support`, `fix: handle empty log segment`,
`refactor: extract common log replay logic`
Breaking change examples: `feat!: make_physical takes column mapping and sets parquet field ids`,
`chore!: remove the arrow-55 feature`

**Description:** follow the template in `.github/PULL_REQUEST_TEMPLATE.md`. Err on the
side of simplicity: don't list every change. Focus on key API changes, functionality,
and data flow. Keep it concise.

## Deep Context

Read the file that matches your task.

| File | Read when |
|---|---|
| `CLAUDE/architecture.md` | Before changing snapshot loading, log replay, scans, transactions, committers, engine handlers, `EngineData`, or catalog-managed tables |
| `CLAUDE/testing.md` | Before writing a test helper, building a test table, or choosing test setup |
| `kernel/tests/README.md` | Before choosing an existing test table or adding test data |
| `kernel/Cargo.toml` `[features]` | Before using, adding, or removing a feature flag |
| `kernel/src/metrics/README.md` | Before changing metric events or reporters |
| `ffi/AGENTS.md` | Before any change under `ffi/` |
| `datafusion-executor/CLAUDE.md` | Before any change in the `datafusion-executor` workspace |
| `.github/CLAUDE.md` | Before any change under `.github/` |
| `docs/user-guide/CLAUDE.md` | Before any user-guide edit |

Always cross-check protocol behavior against the
[Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md).

**Keeping docs current:** Update the authoritative documentation for a changed fact, along with
affected examples and cross-references. Use [Documentation Ownership](#documentation-ownership)
to choose where the explanation belongs. Do not mirror a change across every agent file.
If you notice stale names, paths, or data flows in docs outside the task, tell the user.
