# Integration test guidelines

Scope: `kernel/tests/**`. This file holds the mechanics for writing kernel integration
tests -- table setup, helper catalog, and commit/read patterns. The universal testing
philosophy (unit vs integration, rstest consolidation, naming) lives in the root
`CLAUDE.md`; this file is the source of truth for *how* to set up and assert against tables.

Integration tests exercise only public APIs end-to-end. See `kernel/tests/README.md` for a
catalog of available test tables (schema, protocol, features, and which tests use them).
Consult it before creating new test data to avoid duplication.

## Table setup

- **`add_commit` and table setup:** `add_commit` takes a `table_root` string and resolves it
  to an absolute object-store path. The `table_root` must be a proper URL string with a
  trailing slash (e.g. `"memory:///"`, `"file:///tmp/my_table/"`). Avoid using the `Url` type
  directly -- most test helpers and kernel APIs accept `impl AsRef<str>`, so pass URL strings
  instead. When using local storage, use an un-prefixed store (`LocalFileSystem::new()`) with
  a `file:///` URL string. Do NOT use `LocalFileSystem::new_with_prefix()` with `add_commit`
  -- the prefix causes double-nesting because `add_commit` already resolves the full path from
  the URL. For in-memory tests, use `InMemory::new()` with `"memory:///"`. ALWAYS use the same
  `table_root` URL string for both `add_commit` (writing log files) and
  `snapshot`/`Snapshot::try_new` (reading the table). ALWAYS include a trailing slash in
  directory URLs to ensure correct path joining.

## Asserting against commits

- **Committing in tests:** Use `txn.commit(engine)?.unwrap_committed()` to assert a successful
  commit and get the `CommittedTransaction`. When you only need the resulting snapshot, use
  `txn.commit(engine)?.unwrap_post_commit_snapshot()` to get the `SnapshotRef` directly. Do NOT
  use `match` + `panic!` for either -- they provide a clear error message on failure. Available
  under `#[cfg(test)]` and the `test-utils` feature.
- **Prefer snapshot/public API assertions over reading raw commit JSON.** Only read raw commit
  JSON when the data is inaccessible via public API (e.g., system domain metadata is blocked by
  `get_domain_metadata`). For commit JSON reads, use `read_actions_from_commit` from
  `test_utils` -- do NOT write local helpers that duplicate this.

## Common test helpers

Before writing a custom helper, check this curated list and the locations below. This list is
non-exhaustive -- when in doubt, browse the source files directly (`test-utils/src/lib.rs`,
`kernel/tests/integration/common/`, `kernel/tests/integration/<topic>/mod.rs`).

**Arrow construction (from `delta_kernel::arrow`)**

- `arrow::array::new_null_array(&arrow_type, n)` -- Arrow array of `n` nulls of any Arrow
  type. Prefer this over per-type `Int32Array::from(vec![None as Option<i32>])` builders.
- `engine::arrow_conversion::TryIntoArrow`:
  `(&kernel_data_type).try_into_arrow()` for `DataType`,
  `(&kernel_struct_type).try_into_arrow()` for `StructType` -> Arrow `Schema`.

**Engine + table setup (from `test_utils`)**

- `test_table_setup()` / `test_table_setup_mt()` -- engine + temp table path. Use the `_mt`
  variant under `#[tokio::test(flavor = "multi_thread")]`. Required whenever a test calls
  `snapshot.checkpoint()`: it issues nested `block_on` calls that deadlock on a single-threaded
  runtime / `TokioBackgroundExecutor`.
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
