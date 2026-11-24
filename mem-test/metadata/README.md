# Global Rust allocator POC

Kernel exposes one stable `GlobalAlloc` implementation. Standard `Vec`, `String`, `Box`, `Arc`,
hash tables, and Rust-owned Arrow storage use it when installed. Containers carry no allocator
instance, and there are no operation budgets or thread-local allocation policies. Rust 1.95 is
sufficient; `allocator-api2` is not used.

Rust applications install the hook once:

```rust
use delta_kernel::memory::{KERNEL_ALLOCATOR, KernelAllocator};

#[global_allocator]
static ALLOCATOR: KernelAllocator = KernelAllocator::new();

let existing = KERNEL_ALLOCATOR.usage();
KERNEL_ALLOCATOR.set_limit(Some(existing.reserved + 256 * 1024 * 1024))?;
// Existing Snapshot / Scan / Transaction calls use the shared limit.
KERNEL_ALLOCATOR.set_limit(None)?;
```

The FFI crate installs it through its default `global-allocator` feature. The existing
`kernel_memory_set_limit` and `kernel_memory_usage` functions configure and observe the same
state. Rust embedding applications with their own allocator must disable that feature and install
`KernelAllocator::with_backing(...)` around their backing allocator. `alloc-tracking` is a
compatibility alias for installing the hook.

Tracking starts with Rust allocation, and enforcement defaults to disabled. Reservations use a
CAS loop; frees use an atomic subtraction. An allocation-free reader/writer gate coordinates limit
changes and coherent snapshots with concurrent reservations. The backing allocator runs outside
the gate. Reallocation reserves a full replacement while the old block is charged, then delegates
to the backing allocator so it can resize in place. Reservation peaks can therefore exceed the
actual live heap peak. Failed backing calls roll back their reservations.

Charges follow the actual allocation until deallocation, including across retained snapshot
references, calls, threads, and C release callbacks. This tracks Rust array/schema objects,
`ArrayData` child vectors, buffer ownership objects, and ordinary strings that bypassed the
per-container experiment. Direct libc/C++ allocations, memory mappings, allocator overhead,
retained allocator pages, and stacks are outside this requested-byte limit.

## Recoverable failures

`Vec::try_reserve`, `String::try_reserve`, and other fallible allocation APIs return errors when
the global allocator returns null. Kernel's container helpers use standard containers and map
those errors to its memory error. Its fallible box helper uses the process allocation functions.
JSON output records a failed reservation and discards subsequent output before returning the saved
error; this avoids allocating serde_json's boxed I/O error. FFI memory errors use static message
bytes, so reporting them needs no additional Rust allocation.

Recovery remains incomplete. Infallible paths such as `Arc::new`, ordinary `Box::new`, and
unconverted dependency calls can abort when denied. Installing the hook makes those allocations
tracked and limited; it does not turn their callers into fallible APIs. Pressure surveys record
these aborts separately from successful memory-error returns.

## Local checks

The [lifecycle matrix](LIFECYCLE_CHECKS.md) varies schema shape, logs/actions/statistics, query
shape, declarative plan shape, and concurrency. Both published dependencies and the
[Arrow fallible-API experiment](../arrow-allocator-poc/README.md) use the same installed allocator.
The dependency patch changes fallibility; it is unnecessary for routing allocations to the hook.

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo +1.95.0 build --locked --release -p mem-test \
  --features declarative-benchmark --example rss_lifecycle --example kernel_lifecycle

python3 mem-test/metadata/run_matrix.py \
  --binary target/release/examples/rss_lifecycle --record-infallible-gaps \
  --output /tmp/kernel-global-matrix.json

python3 mem-test/metadata/run_ffi_lifecycle.py
```

Omit `--record-infallible-gaps` to make any pressure-process abort fail the run. Unlimited and
fit-budget failures always fail the run. The C harness exercises the generated ABI, zero-headroom
write-state encoding failure and retry, and foreign-thread byte release under a full budget.
Rust tests also race standard-container reservations and limit changes across threads.

[Current validation](../arrow-allocator-poc/PROCESS_GLOBAL_VALIDATION.json) separates normal runs,
recoverable pressure failures, and abort gaps. The older result files preserve their original
compiler, source, and binary hashes. They describe the historical per-container implementation.
RSS and physical-footprint interpretation is covered in [the measurement notes](DECLARATIVE_RSS.md).
Use unprofiled RSS workers for limit experiments; run DHAT profiles with enforcement disabled,
since profiler bookkeeping also uses the process allocator.
