# Explicit allocator budget

Kernel's normal build keeps its stable Rust 1.95 MSRV. The internal `memory-budget` feature
requires stable Rust 1.100 and selects `std::alloc::Allocator` for participating metadata storage.
The FFI package declares Rust 1.100 and enables that feature. Neither configuration needs nightly.
Cargo has no per-feature MSRV metadata, so the feature requirement is documented and checked by
Kernel's build script. Kernel with all features enabled requires 1.100.

Private storage aliases and constructors select standard containers on 1.95 or allocator-backed
containers with the feature. Schema, log, statistics, and declarative-plan logic is shared.
The allocator is explicit; Kernel does not install a process-global limiting allocator.

## Configure

With `memory-budget` enabled, Rust callers use one shared instance:

```rust
use delta_kernel::memory::KERNEL_ALLOCATOR;

KERNEL_ALLOCATOR.set_limit(Some(256 * 1024 * 1024))?;
// Existing Snapshot / Scan / Transaction calls use participating storage.
let usage = KERNEL_ALLOCATOR.usage();
KERNEL_ALLOCATOR.set_limit(None)?;
```

Enforcement defaults to disabled. FFI callers use `kernel_memory_set_limit` and
`kernel_memory_usage`; no allocator installation or operation scope is required. Charges follow
backing allocations until final release, including retained schema references and C release
callbacks on another thread. The optional FFI `alloc-tracking` feature installs a separate,
tracking-only process allocator for diagnostic native-byte getters; it never rejects allocations.

Reservations use atomic compare-and-swap; frees use atomic subtraction. An allocation-free atomic
reader/writer gate coordinates configuration, snapshots, and peak resets. Backing allocations run
outside the gate. Growth reserves only `new_size - old_size`, and shrinking releases its reduction
only after success. Failed backing operations roll back reservations and preserve old storage.
Allocator-internal copying can still require transient memory outside this requested-byte budget.

## Fallibility and coverage

Converted storage paths use fallible growth and return `MemoryError`. The fallible box helper uses
`Allocator::allocate` and transfers initialized storage to a Box with the same allocator;
`Box::try_new_in` is not needed. JSON output and FFI memory-error reporting avoid allocating an
additional budgeted diagnostic after failure.

The explicit allocator does not reject ordinary Rust allocations in dependencies, callers,
reference-engine objects, or error formatting. Infallible constructors and cloning on participating
allocator-backed storage still have Rust's allocation-error behavior: this feature does not make
every infallible Rust API recoverable. The lifecycle checks exercise converted fallible paths;
process aborts fail those checks.

The budget covers participating requested storage, not all native memory or RSS. Rust allocations
outside participating containers, Arrow array/schema ownership objects, foreign engine buffers,
direct libc/C++ allocations, mappings, stacks, and allocator-retained pages require separate
measurement. The [Arrow experiment](../arrow-allocator-poc/README.md) extends participation to
selected buffer payloads and parser scratch storage without installing a global rejection hook.

## Local checks

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo +1.100.0 build --locked --release -p mem-test \
  --features declarative-benchmark --example rss_lifecycle --example kernel_lifecycle

python3 mem-test/metadata/run_matrix.py \
  --binary target/release/examples/rss_lifecycle --output /tmp/kernel-allocator-matrix.json

python3 mem-test/metadata/run_ffi_lifecycle.py
```

The [matrix](LIFECYCLE_CHECKS.md) varies schema shape, logs/actions/statistics, query and plan shape,
and concurrency. Tracking-only baseline runs measure successful live Rust allocations through a
separate process allocator. Fresh constrained workers limit only participating Kernel storage.
Each returned allocation error must be typed; owners must release their charges, and clearing the
limit must allow retry. Aborts, leaks, limit overruns, and unexpected failures fail the runner.

[Current validation](../arrow-allocator-poc/ALLOCATOR_API_VALIDATION.json) records the feature/MSRV
split and strict recovery checks. `PROCESS_GLOBAL_VALIDATION.json` preserves the rejected global
limiting-hook experiment; its recoverability gaps and coverage claims do not describe this backend.
Other historical result files retain their original compiler, source, and executable hashes.
[Rust heap and RSS notes](DECLARATIVE_RSS.md) distinguish requested storage from resident pages.
