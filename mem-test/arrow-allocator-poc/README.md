# Local Arrow fallible-API experiment

Both published Arrow and these patched dependencies allocate through the installed Kernel global
Rust allocator. No Arrow allocator registration, buffer allocator identity, or `allocator-api2`
container is needed. The separate workspace tests fallible dependency paths on Rust 1.95.

The patch adds fallible growth and propagation for JSON decoder scratch/output, null and empty
arrays, selected filtering/aggregation paths, and participating Parquet readers. Scratch vectors
are standard `Vec` values using `try_reserve`. Memory-error paths avoid allocating diagnostic
strings; the Parquet allocation error is a unit variant. Other public infallible dependency APIs
retain their allocation-error behavior. This is an experiment in recovery coverage, rather than a
whole-query recovery guarantee.

`prepare.py` applies the patch to disposable copies of cached Arrow 60.0.0 sources. It preserves the
Cargo registry and the normal Kernel dependency configuration.

From the Kernel repository root, choose a new output directory:

```sh
python3 mem-test/arrow-allocator-poc/prepare.py --output /tmp/kernel-arrow-poc

CARGO_TARGET_DIR=/tmp/kernel-arrow-poc/target CARGO_PROFILE_RELEASE_DEBUG=1 \
  cargo +1.95.0 build --offline \
  --manifest-path mem-test/arrow-allocator-poc/Cargo.toml \
  --config /tmp/kernel-arrow-poc/patch.toml --release

python3 mem-test/arrow-allocator-poc/verify.py \
  --binaries /tmp/kernel-arrow-poc/target/release --record-infallible-gaps \
  --output /tmp/kernel-arrow-poc/allocation-survey.json

python3 mem-test/metadata/run_matrix.py \
  --binary /tmp/kernel-arrow-poc/target/release/rss_lifecycle --record-infallible-gaps \
  --output /tmp/kernel-arrow-poc/lifecycle-matrix.json
```

The first build updates the ignored local lockfile; subsequent builds can add `--locked`.
Allocation checks include standard-vector ownership, Arrow buffer growth/shrink, imported vectors,
JSON decoding, cross-thread destruction, and the Arrow C Data release callback. Fault probes deny
global allocations, so they also expose ordinary dependency allocations outside the converted
fallible paths. The survey records allocation aborts separately; omitting
`--record-infallible-gaps` makes them fail the run. Unexpected errors still fail either mode.

The lifecycle includes local reference-engine snapshot discovery, checkpoint resolution, query and
metadata-plan construction, encoding, opaque write-relation staging, write-state encoding, and
owner release. It excludes terminal metadata-plan execution and declarative commit. Global
tracking includes retained caller diagnostics, so the old per-container cleanup baseline does not
apply. Rust and C tests check ownership and exact charge release separately.

Use unprofiled workers for enforcement tests. DHAT profiles must disable enforcement because
profiler bookkeeping uses the same process allocator. The [measurement notes](../metadata/DECLARATIVE_RSS.md)
separate live requested Rust bytes, in-flight reservations, and process RSS.

[Current global-hook validation](PROCESS_GLOBAL_VALIDATION.json) records the implementation and
its remaining recovery gaps. [Earlier stable-backend validation](GLOBAL_ALLOC_VALIDATION.json)
used an explicit per-container `GlobalAlloc` backend and `allocator-api2`. The
[original results](RESULTS.json) used Rust 1.100's standard-library allocator API. Their absolute
wide-tail profiles remain historical measurements with their original hashes.
