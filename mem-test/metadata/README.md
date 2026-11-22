# Shared Kernel allocator POC

Selected metadata storage always uses `memory::KERNEL_ALLOCATOR`, an explicit stable Rust 1.100
`Allocator`. Kernel does not install a process `GlobalAlloc`. Tracking starts at the first allocation;
the default limit is `None`. There are no budget handles, TLS policy scopes, or builder setters.

```rust
use delta_kernel::memory::KERNEL_ALLOCATOR;

let existing = KERNEL_ALLOCATOR.usage();
KERNEL_ALLOCATOR.set_limit(Some(existing.reserved + 256 * 1024 * 1024))?;
// Ordinary snapshot, scan, and transaction calls use the shared allocator.
KERNEL_ALLOCATOR.set_limit(None)?; // Tracking continues.
# Ok::<(), delta_kernel::memory::MemoryError>(())
```

The C API exposes `kernel_memory_set_limit(OptionalValueusize)` and `kernel_memory_usage(out)`.
The first configuration call initializes and counts fixed canonical schemas before enabling a cap.
A successful update covers existing allocations and concurrent reservations. A value below current
usage returns false and preserves the old limit. Clearing the limit preserves all counters.
Limits and counters belong to the loaded Kernel library. Destruction and the final refcount release
return the charge on whichever thread frees the allocation, including for retained FFI plan bytes.

This caps participating requested allocation bytes, including overlapping buffers during growth.
It does not cap RSS, malloc overhead, parser scratch, standard Arc control blocks, or external
engine/Arrow/Parquet allocations. Infallible Rust collection operations retain Rust's allocation-error
behavior; Kernel's participating fallible paths reserve capacity before growing.

## Driver lifecycle

The declarative workload loads a snapshot, resolves checkpoint metadata, builds and encodes a scan
plan without executing its terminal action rows, stages opaque write relations, and encodes write
state. The reference engine's snapshot discovery and decoding allocations are included in process
measurements. `commit_declarative` is unsupported on this base; successful driver planning does not
claim an end-to-end declarative commit.

The generated tail table has 10,000 top-level columns, 30,000 schema fields, 11,301,270 schema-JSON
bytes, 1,000 initial files, one checkpoint, and 10,000 tail commits. Schema width, stats payload size,
indexed columns, parsed-stats checkpoints, limits, and concurrency are configurable. Table generation
runs in a separate process.

## Run

For the cross-product checks, phase failures, and recovery assertions, use
[the local lifecycle matrix](LIFECYCLE_CHECKS.md).
The separate [Arrow allocator experiment](../arrow-allocator-poc/README.md) tests dependency
storage and error propagation without changing the normal Kernel dependency configuration.

From the repository root:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo +1.100.0 build --locked --release -p mem-test --features declarative-benchmark \
  --example rss_lifecycle --example kernel_lifecycle

python3 mem-test/metadata/run_rss.py \
  --binary target/release/examples/rss_lifecycle --mode declarative \
  --width 10000 --initial 1000 --tail 10000 --stats-columns -1 \
  --caps 0 512 128 64 --concurrency 1 --warmup --output /tmp/kernel-wide-tail.json

python3 mem-test/metadata/run_rss.py \
  --binary target/release/examples/rss_lifecycle --mode declarative \
  --width 1000 --initial 100 --tail 1000 --stats-columns -1 --parsed-stats --stats-bytes 4096 \
  --caps 0 256 64 --concurrency 1 4 --warmup --output /tmp/kernel-concurrent.json

python3 mem-test/metadata/run_heap.py \
  --prepare-binary target/release/examples/rss_lifecycle \
  --heap-binary target/release/examples/kernel_lifecycle --mode declarative \
  --width 10000 --initial 1000 --tail 10000 --stats-columns -1 --cap 512 \
  --phases complete --output /tmp/kernel-heap

python3 mem-test/metadata/run_ffi_lifecycle.py
```

A CLI cap of `0` disables enforcement; the C/Rust API can explicitly set a zero-byte limit.
`--warmup` loads matching small tables without enforcement first, applies the requested limit,
and checks that retained usage returns to the warmed baseline. Fixed canonical schema caches remain
charged after query owners are dropped.
`KERNEL_DECLARATIVE_STATS=json|struct|both` selects the scan stats representation.

`run_heap.py` attributes live allocations at DHAT's simultaneous heap peak. The RSS worker also
tracks requested Rust heap bytes without DHAT. On macOS, `KERNEL_MEMORY_VM_MAPS=/tmp/kernel-vm` adds
self-inspection of physical footprint, malloc pages, and resident stacks. See
[the measurement notes](DECLARATIVE_RSS.md) for interpreting those metrics.

The POC raises the workspace minimum to Rust 1.100, changes schema ownership payloads, and returns
allocator-owning `Bytes` from write-state encoding. The C `KernelOwnedBytes` record has an opaque
owner word; rebuild connector bindings from the matching generated header.
