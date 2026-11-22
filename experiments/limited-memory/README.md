# Shared allocation budget POC

This experiment uses stable Rust 1.100 allocator-aware storage to reject participating Kernel
allocations before a shared limit is exceeded. Enable `limited-memory` and configure the budget
once, before any other Kernel work. Existing snapshot, scan, transaction and release calls inherit
the shared policy; callers do not need operation scopes or per-builder setters.

```rust
use delta_kernel::{Snapshot, engine::sync::SyncEngine, memory::MemoryBudget};

let budget = MemoryBudget::install_global(256 * 1024 * 1024)?;
let snapshot = Snapshot::builder_for("file:///my/table/").build(&SyncEngine::new())?;
drop(snapshot);
assert_eq!(budget.usage().reserved, 0);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The C entry points are `kernel_memory_budget_install_global` and `memory_budget_global_usage`.
Allocation exhaustion returns `MemoryBudgetError`. Allocator ownership survives retained objects,
FFI returns, Rust reference-counted clones and cross-thread release. The implementation does not
use allocation-error hooks, allocator panics or nightly allocator features.

This is a POC cap on converted requested allocations. Standard Arc nodes, names and URL payloads,
footer protobuf decoding, some infallible transforms/containers and foreign engine buffers remain
outside it. It does not establish a whole-process or RSS cap.

## Declarative driver lifecycle

[The lifecycle report](DECLARATIVE_RSS.md) describes the driver scope, stress matrix, measurement
boundaries and remaining allocation gaps. Metadata scan plans are built and encoded without
executing their terminal action rows in Kernel. Snapshot discovery uses the local reference
executor and is explicitly reported separately.

The exact tail fixture has 10,000 top-level columns, 30,000 schema fields, 11,301,270 schema-JSON
bytes, 1,000 initial files, one checkpoint and 10,000 tail commits. The runner also varies stats
width, JSON payload size, parsed-stats checkpoints, shared budget and query concurrency. Fixtures
are synthetic and created in a separate process before measurement.

## Build and run

From the repository root:

```sh
export KERNEL_MEMORY_TARGET="$PWD/target/limited-memory-poc"
cargo +1.100.0 build --offline --locked --release \
  --manifest-path experiments/limited-memory/Cargo.toml --features declarative-benchmark \
  --example rss_lifecycle --example kernel_lifecycle --target-dir "$KERNEL_MEMORY_TARGET"

python3 experiments/limited-memory/run_rss.py \
  --binary "$KERNEL_MEMORY_TARGET/release/examples/rss_lifecycle" \
  --mode declarative --width 10000 --initial 1000 --tail 10000 --stats-columns -1 \
  --caps 512 128 64 --concurrency 1 \
  --output /tmp/kernel-wide-tail.json

cargo +1.100.0 nextest run --offline --locked \
  --manifest-path experiments/limited-memory/Cargo.toml --features declarative-benchmark \
  --target-dir "$KERNEL_MEMORY_TARGET"

python3 experiments/limited-memory/run_ffi_lifecycle.py
```

Remove `--offline` when dependencies have not yet been downloaded. The experiment has its own
workspace and lockfile. `declarative-benchmark` enables vendored protoc. Without that feature,
`kernel_lifecycle` provides an imperative diagnostic baseline.

Use `--stats-columns -1 --parsed-stats --stats-bytes 4096 --concurrency 1 4` for structured-stats
and concurrency cases. `KERNEL_DECLARATIVE_STATS=json|struct|both` selects the returned stats
representation. `KERNEL_DECLARATIVE_RELATIONS` controls staged opaque write relations.

`run_heap.py` and `summarize_heap.py` attribute allocations at a simultaneous heap peak. See the
lifecycle report for isolated planning profiles and macOS self-VM diagnostics. Raw DHAT profiles
are ignored; the checked-in declarative receipts contain summary measurements.

## Compatibility and validation limits

With `limited-memory`, public schema enum payloads use allocator-aware ownership and MetadataValue
has an additional string variant. `WriteState::encode` returns allocator-owning Bytes. The
declarative `KernelOwnedBytes` C record gains an opaque owner word: connectors must update their
bindings to the matching generated header and feature configuration.

Mainline declarative commit is unsupported. The benchmark covers transaction preparation and
write-state encoding; its results explicitly report that it has not committed declaratively.
Linux driver/cgroup OOM validation remains outstanding.

Observed macOS RSS includes reusable malloc pages. In the exact tail diagnostic, cleanup left
about 492 MiB RSS but only 24 MiB physical footprint; about 448 MiB was marked reusable and stack
residency during planning was only 160 KiB. Reusable pages must not be counted as extra live
Kernel allocations or used to infer an OOM from raw RSS alone.
