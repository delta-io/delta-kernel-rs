# Local Arrow allocator experiment

This separate Cargo workspace connects participating Arrow storage to Kernel's existing shared
`GlobalAlloc` backend on Rust 1.95. A stable `allocator-api2` adapter provides the fallible
containers; no standard-library allocator API is required. Tracking is always enabled after
registration. Kernel's existing limit controls enforcement; no request scopes or builder budgets
are introduced.

`prepare.py` applies the patch to disposable copies of cached Arrow 60.0.0 sources. It preserves the
Cargo registry and the normal Kernel dependency configuration. The benchmark entry points install
the allocator once, then run the same lifecycle harness used by the normal build.

The patch covers mutable buffer allocation/growth/release, JSON tape and field-position scratch,
JSON string/primitive/boolean output storage, list/map offsets, and validity buffers. Parquet's
participating bitmap and list-offset paths propagate allocation errors, including through its
map reader and conversion to Arrow errors. Each allocation retains its allocator identity until
release. Ordinary imported Rust vectors retain their original allocator and remain outside this
limit, including ordinary vectors created inside Arrow compute kernels.

Null/empty array construction and the participating filter/aggregate paths propagate storage
allocation failures. Kernel uses Arrow's existing fallible cast entry point for null construction,
keeping the same implementation for the supported Arrow versions. Stats parsing propagates memory
errors instead of treating them as malformed statistics. The reference executor releases a node's
inputs after their final consumer and reuses arrays whose nested null masks are already correct.

Arrow array objects, `ArrayData` trees, schema objects, and other ordinary dependency containers
still have allocations outside this allocator. Public infallible Arrow operations also retain
infallible allocation paths. Passing these checks does not establish a whole-Arrow or process-RSS
cap. The [stable-backend validation](GLOBAL_ALLOC_VALIDATION.json) records Rust 1.95 checks and
simultaneous-peak attribution for the `GlobalAlloc` implementation. The [historical results](RESULTS.json)
retain measurements from the Rust 1.100 `std::alloc::Allocator` implementation. Their `tail_memory`
section includes the exact wide-tail fixture with full/narrow projections and
a wide filter. It separates bytes passing through the cap at the Rust heap peak, measured bytes
outside the cap, the allocator's own peak counter, and RSS from a separate unprofiled worker.

From the Kernel repository root, choose a new output directory:

```sh
python3 mem-test/arrow-allocator-poc/prepare.py --output /tmp/kernel-arrow-poc

CARGO_TARGET_DIR=/tmp/kernel-arrow-poc/target CARGO_PROFILE_RELEASE_DEBUG=1 \
  cargo +1.95.0 build --offline \
  --manifest-path mem-test/arrow-allocator-poc/Cargo.toml \
  --config /tmp/kernel-arrow-poc/patch.toml --release

python3 mem-test/arrow-allocator-poc/verify.py \
  --binaries /tmp/kernel-arrow-poc/target/release \
  --output /tmp/kernel-arrow-poc/allocation-checks.json

python3 mem-test/metadata/run_matrix.py \
  --binary /tmp/kernel-arrow-poc/target/release/rss_lifecycle \
  --output /tmp/kernel-arrow-poc/lifecycle-matrix.json

python3 mem-test/arrow-allocator-poc/profile.py \
  --binaries /tmp/kernel-arrow-poc/target/release \
  --output /tmp/kernel-arrow-poc/heaps
```

The first build updates the ignored local lockfile from the repository's dependency versions;
subsequent builds can add `--locked`. Tests use fresh processes because the allocator and limit
are shared within a library instance. `verify.py` checks ownership, failed growth, cross-thread
release, and recovery. Its C Data export check keeps the allocation charged until the exported
array's C release callback runs on another thread. It rejects each participating allocation in turn
across JSON shapes and an actual checkpoint-backed declarative lifecycle, checking typed errors and
cleanup. The matrix adds table/query variation and concurrent callers sharing the same cap.

The measured lifecycle includes snapshot discovery, checkpoint resolution, query and metadata-plan
construction, wire encoding, opaque write-relation staging, and final release. The local reference
engine executes discovery operations. Terminal metadata-plan execution and declarative commit are
outside the measurement. Heap figures are requested Rust bytes, rather than RSS. Timing samples
from these correctness checks are diagnostic.

For an exact wide-tail heap profile, use `mem-test/metadata/run_heap.py` with the experimental
`rss_lifecycle` and `kernel_lifecycle` binaries, `--mode declarative --cap 0`, and the
`--fixture-options` and `--query` JSON objects recorded in `tail_memory`. This uses the same
profiler entry point as `profile.py`; fixture preparation stays outside the measurement.
