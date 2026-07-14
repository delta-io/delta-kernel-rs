# checkpoint-table

C FFI example for the checkpoint write surface. Demonstrates the
`checkpoint_snapshot(snapshot, engine, spec)` function and its
`FfiCheckpointSpec` discriminated union (`V1` / `V2NoSidecar` /
`V2WithSidecar { file_actions_per_sidecar_hint: OptionalValue<usize> }`) plus the
`FfiCheckpointWriteResult` return type (`Written` / `AlreadyExists`,
each carrying an owned `Handle<SharedSnapshot>`).

## Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
$ ./checkpoint_table /path/to/existing/table <sub-flow>
```

The `<sub-flow>` argument selects one of three demos:

- `default` -- pass `spec = NULL`. The kernel auto-picks V1 or V2 from the
  table's protocol features and writes a single-file checkpoint with no sidecars.
- `v2_no_sidecar` -- `FfiCheckpointSpec::V2NoSidecar` requests a V2 manifest
  with all file actions inlined (no sidecar files). Requires the table to
  declare the `v2Checkpoint` reader/writer feature.
- `v2_with_sidecars` -- `FfiCheckpointSpec::V2WithSidecar` with
  `file_actions_per_sidecar_hint = Some(2)` requests a V2 manifest that emits
  sidecar parquet files. The hint is the suggested upper bound of file actions
  per sidecar parquet (the example passes `2` so multiple sidecars are emitted
  even for the small fixture); pass `None` to use the kernel default.
  Requires the table to declare the `v2Checkpoint` feature.

## Test harness

The paired `ffi/tests/checkpoint-table-testing/run_test.sh` seeds a temporary
table by copying one of the kernel-bundled fixtures
(`kernel/tests/data/app-txn-no-checkpoint/` for `default`,
`kernel/tests/data/v2-parquet-sidecars-struct-stats-only/` for the V2 sub-flows
with all pre-existing checkpoint artifacts and `_last_checkpoint` scrubbed),
runs this binary against it, and diffs the output against the matching
`expected-data/<sub-flow>.expected` file.