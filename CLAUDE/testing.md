# Testing reference

Curated test setup and helper guidance for delta-kernel-rs. Testing rules live in `AGENTS.md`;
this file is the catalog to check before writing a helper or building a test table.

## Building the table under test

**Consider `TestTableBuilder` (`test_utils::table_builder`) to build the table under test.**
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

## Common test helpers

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

**Handler access and overrides (from `delta_kernel::engine`, with `test-utils`)**

- `PanicEngine`: assert that an operation requests no required engine handler.
- `DelegatingEngine`: override selected handlers while delegating the rest to an inner engine.
  See the types' rustdoc for override and executor selection behavior.

**Engine + table setup (from `test_utils`)**

- `test_table_setup()` / `test_table_setup_mt()`: engine + temp table path. Use the `_mt`
  variant under `#[tokio::test(flavor = "multi_thread")]` whenever a test calls
  `snapshot.checkpoint()`; see Common Gotchas in `AGENTS.md`.
- `engine_store_setup(table_name, local_directory)`: returns
  `(store, engine, table_location)` when a test needs direct object-store access.
- `setup_test_tables(...)`: multiple pre-built tables for read/scan tests.

**Table creation in tests**

- `test_utils::table_builder::TestTableBuilder` (and the `test_table(...)` shorthand): worth
  considering for a table in a specific state: composes `LogState`, `FeatureSet`,
  `DataLayoutConfig`, and `TableConfig` through the real write path. Pair with the
  `build_snapshot!` macro and, for broad coverage, the `default_sweep` template. See
  [Building the table under test](#building-the-table-under-test).
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
