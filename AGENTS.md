# AGENTS.md

## Project Overview

Delta-kernel-rs is a Rust library for building Delta Lake connectors. It encapsulates the
Delta protocol so connectors can read and write Delta tables without understanding protocol
internals. Kernel never does I/O directly: it defines _what_ to do via its APIs
(`Snapshot`, `Scan`, `Transaction`) and delegates _how_ to the `Engine` trait.

## Read Before You Start

Before you change code, read the file that matches your task. Most of these files are not loaded
automatically.

| Task | Read |
|---|---|
| Any change under `ffi/` | `ffi/AGENTS.md` |
| Any change under `.github/` | `.github/CLAUDE.md` |
| Writing or updating a PR description | `AGENTS/pr_description.md` |
| Any user-guide edit | `docs/user-guide/CLAUDE.md` |
| Writing or changing a test, test helper, or test table | `AGENTS/testing.md` |
| Adding test data under `kernel/tests/data/` | `kernel/tests/README.md` |
| Finding the module that owns a behavior | `AGENTS/architecture.md` |
| Changing metric events, reporters, or tracing layers | `kernel/src/metrics/README.md` |
| Changing Delta error conditions or their generated code | `kernel/error_catalog/README.md` |

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

> **`integration-tests` is a separate workspace.** Root `--workspace` commands do not
> include it. `integration-tests/test-all-arrow-versions.sh` tests each supported Arrow version.

```bash
# Build
cargo build --workspace --all-features

# Run all tests (prefer nextest over cargo test)
cargo nextest run --workspace --all-features

# Run doctests
cargo test --workspace --all-features --doc

# Run tests for a specific crate
cargo nextest run -p delta_kernel --all-features

# Run a single test in a specific crate (fastest: only compiles that crate)
cargo nextest run -p delta_kernel --lib --all-features test_name_here

# Run a test by name, searching all crates (slow: compiles everything)
cargo nextest run --workspace --all-features test_name_here

# Format, lint, and doc check (always run after code changes)
cargo +nightly fmt \
  && cargo clippy --workspace --benches --tests --all-features -- -D warnings \
  && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

# Split no-default-features CI checks (cargo aliases from .cargo/config.toml)
cargo clippy-no-default-kernel-dependents
cargo check-no-default-kernel
cargo check-no-default-engine
cargo clippy-no-default-kernel-leaves
```

Before pushing, run the format, lint, and doc check, then all tests and the doctests. CI runs more
than that: it passes `--locked`, runs the no-default-features aliases above, verifies the MSRV, and
runs the `ffi` tests under Miri. `.github/workflows/build.yml` is authoritative.

Use `cargo metadata --no-deps` to discover current workspace package names. Read each crate's
`Cargo.toml` for its feature flags; the manifests are the source of truth.

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
  table in a specific state: data files across many commits, checkpoints, CRC files, a stale or
  missing `_last_checkpoint` hint, or post-cleanup logs.
- Consider how the feature interacts with Delta table features. Cross-check behavior against the
  Delta protocol spec.
- Consider write paths: normal commits, checkpointing, and CRC files. Log compaction writes are
  disabled (#2337), but kernel reads tables that contain compaction files.
- When adding cloud-storage functionality to an engine, such as writing JSON files, make sure to
  test it against S3, Azure, and GCS.
- Consider read paths: loading a snapshot from scratch at latest version, at a specific
  version (time travel), and updating from an existing snapshot.
- Consider table state: only versioned JSON commits, after a checkpoint, after a version
  checksum (`.crc`) file, after log compaction, etc.
- Prefer descriptive test names over doc comments. Encode the scenario and expected
  behavior in the test name. Only add a test doc comment when the intent is too
  verbose or complex to express succinctly in the name.
- Use `rstest` when tests share the same logic and differ only in setup, inputs, or expected
  outcome: write one parameterized test with `#[case]` instead of separate functions. When
  parameters are independent and form a cartesian product, use `#[values]` instead of enumerating
  every combination. Before adding a test, check whether a nearby test has the same flow and
  should gain a new `#[case]`. A common pattern is toggling a feature (e.g. column mapping on/off)
  and asserting success vs. error.
- Reuse helpers from `test_utils` and the integration-test fixtures instead of writing custom
  ones, such as `test_table_setup`, `TestTableBuilder`, `add_commit`, `read_actions_from_commit`,
  and `assert_result_error_with_message`. `AGENTS/testing.md` has the full catalog.
- **Committing in tests:** Use `txn.commit(engine)?.unwrap_committed()` to assert a
  successful commit and get the `CommittedTransaction`. When you only need the resulting
  snapshot, use `txn.commit(engine)?.unwrap_post_commit_snapshot()` to get the
  `SnapshotRef` directly. Do NOT use `match` + `panic!` for either: both helpers provide a clear
  error message on failure. Available under `#[cfg(test)]` and the `test-utils` feature.
- **Prefer snapshot/public API assertions over reading raw commit JSON.** Only read raw
  commit JSON when the data is inaccessible via public API (e.g., system domain metadata
  is blocked by `get_domain_metadata`). For commit JSON reads, use `read_actions_from_commit`
  from `test_utils`: do NOT write local helpers that duplicate this.
- **`add_commit`:** pass the same `table_root` URL string, with a trailing slash, to `add_commit`
  and `Snapshot::builder_for`. Do NOT use `LocalFileSystem::new_with_prefix()` with `add_commit`:
  the prefix double-nests the path.

## Common Gotchas

- **EngineData is opaque:** inspect rows through the visitor APIs. NEVER downcast to
  `ArrowEngineData` or any concrete type in production code (ok in tests). NEVER assume one batch
  per file: ALWAYS iterate.
- **Column mapping:** Physical column names can differ from logical names. ALWAYS use
  the schema from `Snapshot::schema()` for user data columns. Metadata/system schema
  column names (defined by the protocol) are not subject to column mapping.
- **Transforms:** Generic recursive schema and expression transform traits and helpers
  are in `kernel/src/transforms/`.
- **Tracing layer callbacks must not emit tracing events while holding a span's
  `extensions_mut()` lock:** the emitted event re-enters the layer and deadlocks on the same
  lock. Read `kernel/src/metrics/README.md` before changing a layer.
- **Keep tests with process-global state safe under concurrency:** `cargo test` runs tests as
  parallel threads in one test binary, while nextest normally runs each test in a separate
  process. Tests for global tracing subscribers and callbacks must not share capture buffers with
  thread-local dispatch tests or assume no other thread can emit an event.
- **Tests that call `snapshot.checkpoint()` need a multi-threaded runtime:** use
  `test_table_setup_mt()` under `#[tokio::test(flavor = "multi_thread")]`. Checkpointing issues
  nested `block_on` calls that deadlock on a single-threaded runtime or `TokioBackgroundExecutor`.
- **Generated code:** files starting with ``//! Generated by `cargo xtask ...` `` are never edited by
  hand. Change the generator in `xtask/` or its inputs and rerun the command.

## Code Style

- For FFI module organization, follow the [source layout guidance](ffi/AGENTS.md#source-layout).
- Line width is 100 characters. Wrap comments and string literals at 100, not 80.
- Place `use` imports at the top of the file (for non-test code) or at the top of the
  `mod tests` block (for test code): never inside function bodies.
- Prefer `==` over `matches!` for simple single-variant enum comparisons. `matches!` is
  for patterns with bindings or guards. For example: `self == Variant` not
  `matches!(self, Variant)`.
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
- When matching a `KernelError` to classify a failure, match on `error.without_backtrace()` so
  `Backtraced` wrappers do not change the classification. This borrows the underlying error.
  Preserve the original `error`, including its backtrace wrappers, when propagating it or attaching
  it as a source to another error.
- Prefer the `KernelResultIterator<T>` marker trait over spelling
  `Iterator<Item = KernelResult<T>> + Send`, including in public APIs. Use
  `impl KernelResultIterator<T>` for unboxed streams and `Box<dyn KernelResultIterator<T>>`
  (`'static`) or `Box<dyn KernelResultIterator<T> + '_>` (borrowed) at dyn/storage boundaries.
- When building expressions inline, prefer `lit(value)` and `null_lit(data_type)` over
  `Expression::literal(...)` and `lit(Scalar::Null(...))`, and the `col!` macro over
  `Expression::column(...)`. `lit` takes `impl Into<Scalar>` and `null_lit` takes
  `impl Into<DataType>`. `col!` uses the same compile-time segment rules as `column_name!`
  (string literals split on `.`; constants are single simple segments); use
  `Expression::column([...])` for runtime or non-simple names. (`column_expr!` is a doc-hidden
  compatibility alias of `col!`.)
- Prefer `Predicate::TRUE` / `FALSE` / `NULL` for predicates whose value is statically known,
  reserving `Predicate::literal(b)` for runtime `bool` values.
- Prefer the `schema!` / `schema_ref!` macros for inline declarative schema literals,
  `lazy_schema_ref!` for `LazyLock<SchemaRef>` statics, and `try_schema!` when names of
  interpolated fields might collide. Outside the kernel crate, these macros require the
  `internal-api` feature. For Delta log action schemas, reuse the canonical `*_FIELD` and
  `LOG_*_SCHEMA` statics from `actions` instead of re-declaring
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

**Title:** use conventional commit format, lowercase after prefix, no period at the end, and at
most 72 characters after the prefix (CI rejects longer titles).
Allowed types: `feat`, `fix`, `refactor`, `chore`, `docs`, `perf`, `test`, `ci`.
If the pull request contains a breaking change, the type must have a `!` suffix. CI adds the
`breaking-change` label when its semver check finds one, and a labeled PR fails title validation
without the `!`.
Examples: `feat: add checkpoint stream support`, `fix: handle empty log segment`,
`refactor: extract common log replay logic`
Breaking change examples: `feat!: make_physical takes column mapping and sets parquet field ids`,
`chore!: remove the arrow-55 feature`

**Description:** follow the headings in `.github/PULL_REQUEST_TEMPLATE.md`. The rules for each
section, worked examples, and the recipe agents follow when drafting a description are in
`AGENTS/pr_description.md`.

## Deep Context

Read these when relevant to the task at hand:

- [Delta Kernel user guide](https://docs.delta.io/kernel/rust/): introductions and connector workflows
- [delta_kernel rustdoc](https://docs.rs/delta_kernel/latest/delta_kernel/): API explanations,
  contracts, and executable examples
- Always cross-check protocol behavior against the
  [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)
- When changing anything under `kernel/src/content_tree/`, cross-check the behavior against the V4
  sections of the
  [Iceberg spec](https://raw.githubusercontent.com/apache/iceberg/refs/heads/main/format/spec.md)
  and the
  [Delta Iceberg V4 metadata RFC](https://raw.githubusercontent.com/delta-io/delta/refs/heads/master/protocol_rfcs/iceberg-v4-metadata.md).
  Flag any divergence between the code and either document.

**Keeping docs current:** Update the authoritative documentation for a changed fact, along with
affected examples and cross-references. Use [Documentation Ownership](#documentation-ownership)
to choose where the explanation belongs. Do not mirror a change across every agent file.
If you notice stale names, paths, or data flows in docs outside the task, tell the user.
