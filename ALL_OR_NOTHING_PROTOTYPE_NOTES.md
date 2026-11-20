# All-or-nothing V2 checkpoint reuse prototype

Branch: `stack/incremental-v2-checkpoint-all-or-nothing`, based on fetched
`upstream/main` at `1748070c0c84ac203661964f7579317b9c0e8e90` (2026-11-20).

## Contract

`incremental-checkpoints-in-dev` exposes two explicit modes. The ordinary
`ReuseSidecarsAllOrNothing` mode reuses every base sidecar only when the tail has
no Add, Remove, metadata, or protocol action. The separate
`ReuseSidecarsAssumeFreshAddsForBenchmark` mode also admits Add-only tails with
`dataChange=true`. **Its caller assumes and independently verifies that every
tail Add path was freshly created after the base and is absent from all base
sidecars.** Kernel cannot establish this from `commitInfo`, `isBlindAppend`,
operation names, or UUID-looking paths. Both modes fall back to a full V2
checkpoint on any tail Remove, metadata/protocol change, `dataChange=false` Add,
or missing base proof. The benchmark mode is unsuitable for production tables.

The base root must contain one matching `checkpointMetadata` action, no inline
file actions, and at least one sidecar reference. Its protocol-defined tags
`sidecarNumActions`, `sidecarSizeInBytes`, `numOfAddFiles`, and
`sidecarFileSchema` must be present and consistent with its references and the
target output schema. Because a valid V2 sidecar contains only Add and Remove
actions, `sidecarNumActions == numOfAddFiles` proves there are no inherited
Remove tombstones to expire. A valid base and truthful tags are prerequisites;
this is not a checkpoint repair or authentication mechanism. In development
builds, the full sidecar writer emits these tags after it has written new
sidecars and knows their exact counts and size.

On reuse success, Kernel reads the JSON tail twice and the base root once. It
does **not** open, read, inspect footers, or HEAD inherited sidecars. It writes
new sidecars for benchmark-mode tail Adds and writes a new root with inherited
references, preserving their tags including null-valued entries. `_last_checkpoint`
totals include inherited Add rows and sidecar bytes; output metrics count only
new Parquet writes. Eligibility uses O(root sidecars) memory, with no table-sized
file-path set. Runtime is O(tail actions + root rows + newly written actions).

## Production blocker and benchmark scope

The Delta protocol permits an Add to replace an existing logical file's
metadata without a Remove. Current Zerobus writes UUID-named files with ordinary
object-store `put`; neither its naming nor writer-supplied commit fields enforce
freshness against arbitrary writers or PO. A production Add-tail no-read path
needs authoritative writer admission and an enforced create-only path contract,
or an equivalent independently verifiable disjointness proof. The benchmark
harness can validate its isolated fixture *outside* the timed checkpoint call by
scanning base sidecars and checking tail Add paths against every base path.

Concurrent metadata cleanup remains outside this prototype's production
guarantee. A sidecar can be deleted between reading the base root and publishing
the new reference unless a retention/coordination contract protects it. The
protocol's young-sidecar grace period is not a general long-term pin.

Compare full checkpointing with this all-or-nothing build under identical
append-only fixtures, varying inherited sidecar count, data-file count, and
tail commit count. Record checkpoint latency, new Parquet bytes, sidecars
written/reused, inherited-sidecar reads, and memory. Selective reuse is a later
experiment. The tests use
`CARGO_TARGET_DIR=/home/ryan.liao/.cache/bazel/kernel-all-or-nothing-target`
to keep generated artifacts off the nearly full home volume. The isolated
UC/S3 benchmark uses disposable staging tables and an external fresh-path
oracle; it does not establish a production writer guarantee.

## Local validation on the corrected base

- `cargo test -p delta_kernel --features incremental-checkpoints-in-dev --lib checkpoint::incremental --no-fail-fast`: 19 passed.
- `cargo test -p delta_kernel_ffi --features tracing,delta_kernel/incremental-checkpoints-in-dev --lib ffi_metrics::tests --no-fail-fast`: 92 passed.
- `cargo test -p delta_kernel_ffi --features tracing --lib ffi_metrics::tests --no-fail-fast`: 92 passed.
- `cargo clippy -p delta_kernel -p delta_kernel_ffi --all-targets --features delta_kernel/incremental-checkpoints-in-dev,delta_kernel_ffi/tracing -- -D warnings`: passed.
- `cargo +nightly fmt --all --check` and `git diff --check`: passed.

Every Cargo command above used the `CARGO_TARGET_DIR` shown earlier.
