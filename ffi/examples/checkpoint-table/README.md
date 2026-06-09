# checkpoint-table

C FFI example for the checkpoint write surface. Demonstrates the opaque
`FfiCheckpointBuilder` family (`checkpoint_builder_for`,
`checkpoint_builder_set_v2_no_sidecar`, `checkpoint_builder_set_v2_with_sidecars`,
`checkpoint_builder_build`, `free_checkpoint_builder`) and the discriminated-union
return type `FfiCheckpointWriteResult` against a pre-existing table.

## Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
$ ./checkpoint_table /path/to/existing/table <sub-flow>
```

The `<sub-flow>` argument selects one of three demos:

- `inline` — no setter; the kernel auto-picks V1 or V2 from the table's protocol
  features and writes an inline checkpoint with no sidecars.
- `v2_no_sidecar` — `checkpoint_builder_set_v2_no_sidecar` requests a V2 manifest
  with all file actions inlined (still no sidecar files). Requires the table to
  support the `v2Checkpoint` reader/writer feature.
- `v2_with_sidecars` — `checkpoint_builder_set_v2_with_sidecars` requests a V2
  manifest that emits sidecar parquet files. The hint parameter caps how many
  file actions can land in a single sidecar (the example passes `2` so multiple
  sidecars are emitted even for the small fixture).

## Test harness

The paired `ffi/tests/checkpoint-table-testing/run_test.sh` seeds a temporary
table by copying one of the kernel-bundled fixtures
(`kernel/tests/data/app-txn-no-checkpoint/` for `inline`,
`kernel/tests/data/v2-parquet-sidecars-struct-stats-only/` for the V2 sub-flows
with all pre-existing checkpoint artifacts and `_last_checkpoint` scrubbed), runs
this binary against it, and diffs the output against the matching
`expected-data/<sub-flow>.expected` file.

There is no dependency on the `create-table` example binary: the fixture-copy
strategy produces a deterministic seed corpus for all three sub-flows without
any prior cargo invocation beyond the FFI cdylib build.

## Companion: checksum builder

The `FfiChecksumBuilder` family (added in the same PR as the checkpoint
builder) is exercised by Rust in-file tests against the FFI lib; no C ctest
ships with this PR per the `FfiSnapshotBuilder` (PR #2255) precedent of
introducing a new FFI surface family without a paired C example.
