# Integration test guidelines

This file is the source of truth for *how* to set up and assert against tables in kernel
integration tests: table setup, commit/read patterns, and where the shared helpers live. (The
universal testing philosophy -- unit vs integration, rstest consolidation, naming -- is in the
root `CLAUDE.md`.)

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
  JSON when the data is genuinely inaccessible otherwise (e.g. the public `get_domain_metadata`
  rejects system `delta.*` domains; `get_domain_metadata_internal` can read them but is gated
  behind `internal-api`). For commit JSON reads, use `read_actions_from_commit` from `test_utils`
  -- do NOT write local helpers that duplicate this.

## Common test helpers

Before writing a custom helper, look for an existing one -- the shared utilities already cover
most setup, commit, read, and assertion needs, and re-rolling them is one of the most common
review comments on test PRs. Read the source rather than relying on a name list here, which would
drift as helpers are added or renamed:

- `test-utils/src/lib.rs` -- engine/table setup (`test_table_setup` and its `_mt` variant),
  commit and read helpers (`add_commit`, `read_actions_from_commit`, `test_read`,
  `into_record_batch`), schema fixtures, and assertion helpers. Run
  `rg '^pub (fn|async fn)' test-utils/src/lib.rs` for the current surface.
- `kernel/tests/integration/common/` (notably `write_utils.rs`) -- integration-test fixtures and
  write helpers.
- `kernel/tests/integration/<topic>/mod.rs` -- topic-local fixtures (e.g. `simple_schema`,
  `partition_test_schema` in `create_table/mod.rs`).
- `delta_kernel::arrow` -- Arrow construction helpers such as `new_null_array`; prefer these over
  hand-rolled per-type array builders. (The `TryIntoArrow` conversions live separately, in
  `delta_kernel::engine::arrow_conversion`.)

Two judgment calls worth stating outright, since grep won't tell you which to pick:

- Prefer the kernel `create_table` builder (`delta_kernel::transaction::create_table::create_table`)
  over the older hand-rolled `test_utils::create_table` JSON helper -- reach for the JSON helper
  only when the builder can't express the feature combination you need.
- Use the `_mt` setup variant under `#[tokio::test(flavor = "multi_thread")]` whenever a test hits
  a nested-`block_on` path such as `snapshot.checkpoint()` (see `default-engine/CLAUDE.md` for why
  a single-threaded runtime deadlocks there).
