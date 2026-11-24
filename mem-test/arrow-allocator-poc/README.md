# Local Arrow allocator experiment

This separate workspace uses stable Rust 1.100 and Kernel's `memory-budget` feature. A local Arrow
60 patch lets selected buffer payloads and parser scratch/output use the explicit Kernel
`std::alloc::Allocator`. Normal Kernel builds retain the 1.95 MSRV and published dependencies.
No nightly compiler API, `allocator-api2`, or process-global rejecting allocator is used.

The patch propagates fallible errors through participating JSON decoder scratch/output, null and
empty arrays, selected filtering/aggregation paths, and Parquet readers. It retains the allocator
in owned buffers so final destruction and Arrow C Data release callbacks free through the original
allocator, including on another thread. Imported ordinary vectors keep their original allocator.
Array/schema objects, reference-counting storage, and other ordinary dependency allocations remain
outside the explicit budget; ordinary process allocations are never denied by that budget.

`prepare.py` applies the patch to disposable cached-source copies. It preserves the Cargo registry
and the normal Kernel dependency configuration. From the repository root, choose a new directory:

```sh
python3 mem-test/arrow-allocator-poc/prepare.py --output /tmp/kernel-arrow-poc

CARGO_TARGET_DIR=/tmp/kernel-arrow-poc/target CARGO_PROFILE_RELEASE_DEBUG=1 \
  cargo +1.100.0 build --offline \
  --manifest-path mem-test/arrow-allocator-poc/Cargo.toml \
  --config /tmp/kernel-arrow-poc/patch.toml --release

python3 mem-test/arrow-allocator-poc/verify.py \
  --binaries /tmp/kernel-arrow-poc/target/release \
  --output /tmp/kernel-arrow-poc/allocation-checks.json

python3 mem-test/metadata/run_matrix.py \
  --binary /tmp/kernel-arrow-poc/target/release/rss_lifecycle \
  --output /tmp/kernel-arrow-poc/lifecycle-matrix.json
```

The first build updates the ignored local lockfile; subsequent builds can add `--locked`.
Ownership checks exercise buffer growth/shrink, imported and allocator-backed vectors, JSON
parsing, cross-thread destruction, and Arrow C Data release. Fault probes reject participating
allocations and require typed errors, cleanup, and successful retry. Aborts fail the survey;
there is no mode that counts them as successful recovery.

The measured lifecycle includes local reference-engine snapshot discovery, checkpoint resolution,
query/schema and metadata-plan construction, encoding, opaque write-relation staging, write-state
encoding, and final release. It excludes terminal metadata-plan execution and declarative commit.
Rust heap and RSS also include the reference engine and harness, so they are not Kernel-only byte
attribution. [Measurement notes](../metadata/DECLARATIVE_RSS.md) explain that boundary.

[Current validation](ALLOCATOR_API_VALIDATION.json) records the split MSRV and explicit backend.
[Global-hook results](PROCESS_GLOBAL_VALIDATION.json) are historical: that rejected design could
abort ordinary infallible allocations throughout the host process. The other result files preserve
earlier explicit-backend measurements and their original source/binary provenance; their old
coverage fractions are not fresh measurements of this revision.
