# AGENTS.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the
Delta protocol so connectors can read and write Delta tables without understanding protocol
internals. Kernel never does I/O directly: it defines _what_ to do via its APIs
(`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

## Documentation Ownership

Keep each fact in the documentation surface whose audience needs it:

- **Rustdoc** owns public API contracts: parameters, return values, errors, safety requirements,
  and focused examples. Update rustdoc in the same PR as a public API change.
- **The user guide** owns connector workflows and explanations that span APIs. It links to rustdoc
  for exact signatures and method-level contracts. Update the relevant guide page in the same PR
  as a user-visible workflow change; release audits are a backstop, not the update cadence.
- **Agent docs** own repository workflows, test helpers, code conventions, source navigation, and
  hard-to-discover implementation gotchas. Do not copy API summaries, capability lists, crate
  inventories, feature inventories, or protocol reference material into agent docs.

Before adding documentation, find its owner and link there from other surfaces. If the destination
does not answer the question, improve the owner instead of copying an incomplete answer elsewhere.

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

# Quick pre-push check (mimics CI)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && cargo doc --workspace --all-features --no-deps \
  && cargo nextest run --workspace --all-features
```

Use `cargo metadata --no-deps` to discover current workspace package names. Read each crate's
`Cargo.toml` for its feature flags; the manifests are the source of truth.

## Testing

- **Unit tests** test internal APIs and module internals. It is fine to use public APIs
  like `create_table` in a unit test as setup (e.g. to create a table for testing reads,
  writes, or state loading).
- **Integration tests** exercise only public APIs end-to-end. See `kernel/tests/README.md`
  for a catalog of available test tables (schema, protocol, features, and which tests use
  them). Consult it before creating new test data to avoid duplication.
- **Consider `TestTableBuilder` (`test_utils::table_builder`) to build the table under test.**
  Unlike `create_table`, which builds a single create transaction with a given set of features and
  data layout, the builder *builds up* a multi-version table: data files written across many
  commits, plus checkpoints, CRC files, a stale/missing `_last_checkpoint` hint, or post-cleanup
  logs. So when a test needs a populated table history in a specific state, the builder is often a
  good fit, composing `LogState`, `FeatureSet`, `DataLayoutConfig`, and `TableConfig` through the
  real kernel write path so the table is protocol-correct by construction. Load a snapshot at any
  `VersionTarget` with the `build_snapshot!` macro. For coverage across many table states, the
  `default_sweep` cross-product template
  (`LogState x FeatureSet x (DataLayoutConfig, TableConfig) x VersionTarget`: data layout and table
  config are bundled into one axis to avoid a cartesian explosion), or a per-axis template with
  your own `#[values]`, can help; see `kernel/tests/integration/cross_product/mod.rs`. Drop to
  lower-level setup like `test_table_setup` (or hand-rolled `add_commit` / `LocalMockTable`) only
  when necessary: e.g. for states the builder cannot express, such as corrupt or malformed logs.
- Consider how the feature interacts with Delta table features. Cross-check behavior against the
  Delta protocol spec.
- Consider write paths: normal commits, checkpointing, CRC files, log compaction files.
- When adding cloud-storage functionality to an engine, such as writing JSON files, make sure to
  test it against S3, Azure, and GCS.
- Consider read paths: loading a snapshot from scratch at latest version, at a specific
  version (time travel), and updating from an existing snapshot.
- Consider table state: only versioned JSON commits, after a checkpoint, after a version
  checksum (`.crc`) file, after log compaction, etc.
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

### Common Test Helpers

Before writing a custom helper, check this curated list and the locations below.
This list is non-exhaustive: when in doubt, browse the source files directly
(`test-utils/src/lib.rs`, `kernel/tests/integration/common/`,
`kernel/tests/integration/<topic>/mod.rs`).

**Arrow construction (from `delta_kernel::arrow`)**

- `arrow::array::new_null_array(&arrow_type, n)`: Arrow array of `n` nulls of any Arrow
  type. Prefer this over per-type `Int32Array::from(vec![None as Option<i32>])` builders.
- `engine::arrow_conversion::TryIntoArrow`:
  `(&kernel_data_type).try_into_arrow()` for `DataType`,
  `(&kernel_struct_type).try_into_arrow()` for `StructType` -> Arrow `Schema`.

**Engine + table setup (from `test_utils`)**

- `test_table_setup()` / `test_table_setup_mt()`: engine + temp table path. Use the `_mt`
  variant under `#[tokio::test(flavor = "multi_thread")]`. Required whenever a test calls
  `snapshot.checkpoint()`: it issues nested `block_on` calls that deadlock on a single-threaded
  runtime / `TokioBackgroundExecutor`.
- `engine_store_setup(table_name, local_directory)`: returns
  `(store, engine, table_location)` when a test needs direct object-store access.
- `setup_test_tables(...)`: multiple pre-built tables for read/scan tests.

**Table creation in tests**

- `test_utils::table_builder::TestTableBuilder` (and the `test_table(...)` shorthand): worth
  considering for a table in a specific state: composes `LogState`, `FeatureSet`,
  `DataLayoutConfig`, and `TableConfig` through the real write path. Pair with the
  `build_snapshot!` macro and, for broad coverage, the `default_sweep` template. See the Testing
  section above.
- Prefer the kernel `create_table` builder
  (`delta_kernel::transaction::create_table::create_table`) when you need a single bespoke
  table rather than the builder's composed states. It exercises the same path connectors use
  and auto-derives the protocol from the schema and feature flags.
- `test_utils::create_table` (a JSON helper that hand-rolls protocol + metadata) is older
  but still needed when the kernel builder cannot enable a particular feature combination.

**Schema fixtures**

- `test_utils`: `nested_schema`, `schema_with_type`, `nested_schema_with_type`,
  `multi_schema_with_type`, `top_level_ntz_schema` / `nested_ntz_schema` /
  `multiple_ntz_schema`, `top_level_variant_schema` / `nested_variant_schema` /
  `multiple_variant_schema`.
- `kernel/tests/integration/create_table/mod.rs`: `simple_schema`, `partition_test_schema`.

**Commit + read helpers (from `test_utils`)**

- `add_commit`, `add_staged_commit`: write a JSON commit at a given version.
- `read_actions_from_commit`: read raw JSON actions from a specific local-file commit. Use
  this instead of hand-rolled `serde_json` parsing.
- `test_read`: full-scan read of a table; use for round-trip assertions.
- `into_record_batch`: convert `Box<dyn EngineData>` to Arrow `RecordBatch`.

**Assertion helpers (from `test_utils`)**

- `assert_schema_has_field(schema, &["a".into(), "b".into()])`: assert a (possibly nested)
  field path.
- `assert_result_error_with_message(result, "needle")`: assert an error contains a
  substring.

**If a name here doesn't match what's in code:** the list may have drifted from a rename.
Run `rg '^pub (fn|async fn)' test-utils/src/lib.rs` to discover the current public surface,
and update this section in your PR. The same pattern works for
`kernel/tests/integration/common/write_utils.rs`.

## Common Gotchas

- **EngineData is opaque:** NEVER downcast to `ArrowEngineData` or any concrete type
  in production code (ok in tests). NEVER assume one batch per file: ALWAYS iterate.
- **Column mapping:** Physical column names can differ from logical names. ALWAYS use
  the schema from `Snapshot::schema()` for user data columns. Metadata/system schema
  column names (defined by the protocol) are not subject to column mapping.
- **Transforms:** Generic recursive schema and expression transform traits and helpers
  are in `kernel/src/transforms/`.
- **Tracing layer callbacks must not emit tracing events directly:** Calling `warn!()` or
  any tracing macro inside a `tracing_subscriber::Layer` callback (`on_event`, `on_record`,
  `on_close`) while holding a span's `extensions_mut()` write lock will re-enter the layer
  and deadlock on the same lock. In `on_new_span`, no extension lock is held during
  `attrs.record()`, so direct `warn!()` is safe there. In `on_record`, store warnings in a
  `pending_warnings: Vec<String>` field on the visitor, take them out after the extensions
  block closes, and emit via `warn!()` only then. (`on_event`'s visitor does no
  warning-eligible work, so it may run under the lock directly.) See
  `kernel/src/metrics/reporter.rs` for the canonical pattern.
- **Keep tests with process-global state safe under concurrency:** `cargo test` runs tests as
  parallel threads in one test binary, while nextest normally runs each test in a separate
  process. Tests for global tracing subscribers and callbacks must not share capture buffers with
  thread-local dispatch tests or assume no other thread can emit an event.

## Code Style

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
- Prefer the `DeltaResultIterator<'a, T>` / `DeltaResultIteratorStatic<T>` aliases over
  hand-rolled `Box<dyn Iterator<Item = DeltaResult<T>> + Send (+ 'a)>`.
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

- MUST include doc comments for all public functions, structs, enums, and methods.
- MUST document function parameters, return values, and errors.
- Doc comments focus on "what" (contract with caller) more than "how" (implementation),
  unless the "how" meaningfully impacts the "what".
- Code comments state intent and explain "why": don't restate what the code self-documents.
- Be succinct. No verbose AI-slop comments. With well-written and well-named code,
  verbose comments are worse than none.
- Say each thing once, in the right place: don't repeat the same idea across
  doc comment and inline comment.
- Comments earn their place only for hidden invariants, real-bug workarounds, or
  constraints the reader can't see from the code itself.
- Don't enumerate what grep can answer. Lists like `// Used by a, b, c` rot the
  moment `d` lands. Describe the shape; let the reader grep.
- No stale-prone anchors in durable docs or source comments: counts ("the 10
  variants", "5-arm match"), line numbers, or enumeration lists. Describe the
  shape; let the reader grep.
- Comments MUST NOT include temporal references: only refer to current code and
  design, not past iterations.
- Keep comments up-to-date with code changes.
- Include examples in doc comments for complex functions only.
- Use `==` as a visual section divider in comments (e.g. `// === Helpers ===` or
  `// ============`).
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

Read these when relevant to the task at hand:

- `CLAUDE/architecture.md`: task-oriented source navigation for kernel internals
- `docs/user-guide/CLAUDE.md`: writing standards for the mdBook user guide
- [Delta Kernel user guide](https://docs.delta.io/kernel/rust/): connector workflows and
  cross-API explanations
- [delta_kernel rustdoc](https://docs.rs/delta_kernel/latest/delta_kernel/): public API contracts
- Always cross-check protocol behavior against the
  [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)

Update only the documentation surface that owns a changed fact. Do not mirror a change across every
agent file. When a cross-reference becomes stale, fix the link or its authoritative destination.
