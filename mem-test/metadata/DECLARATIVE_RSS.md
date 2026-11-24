# Declarative driver memory measurements

Measure snapshot discovery and schema parsing, checkpoint resolution, scan planning, plan
serialization, transaction preparation with opaque relations, write-state encoding, and release.
Terminal metadata-plan execution and full declarative commit are outside this benchmark. The
local reference engine adds its parsing, Arrow objects, and buffers to process measurements.

Kernel's explicit allocator counts participating storage independently of a tracking-only global
allocator. Only the explicit allocator can reject requests. The global tracker never enforces a
limit, so ordinary dependency or diagnostic allocations are not denied by Kernel's budget.

## Metrics

- **Kernel reservations** (`kernel_live` / `kernel_peak`): participating requested bytes, including
  pending allocations and growth deltas. Shrink reductions apply after success. These counters do
  not charge a hypothetical full replacement during `realloc`; allocator-internal copy transients
  remain outside this limit.
- **Live Rust heap**: successful requested layouts observed by the diagnostic global allocator.
  This includes Kernel, the local reference engine, and the caller/harness. It excludes direct
  libc/C++ allocations and mappings that bypass that global hook.
- **DHAT global peak**: allocations live at one simultaneous requested-heap peak. Compare Kernel
  counters and allocation stacks at that same instant. Do not divide independent peak counters
  and call the result coverage. Profile with enforcement disabled.
- **RSS**: resident process pages, including allocator caches, reusable pages, code, and stacks.
- **macOS physical footprint**: the OS pressure charge, which can differ substantially from RSS
  when resident allocator pages are reusable.

Live Rust bytes and Kernel charges must be compared at the same observation. Requests in flight
can temporarily make reservations exceed materialized participating storage. The tracking-only
global hook measures successful public allocation calls; it cannot observe every transient inside
system `realloc`, or prove that no native allocations bypass the hook.

Freed reusable malloc pages can remain resident and serve later allocations. Their residency does
not imply untracked live storage or an equal additional OOM risk. Conversely, moving reallocation
can copy old data before freeing it; zero swap does not make that transient free. Requested-byte
budgets do not establish a hard RSS or OOM-pressure bound.

## Cases and boundaries

The exact tail fixture contains 10,000 top-level columns / 30,000 schema fields, 11,301,270 schema-
JSON bytes, 1,000 initial files, one checkpoint, and 10,000 tail commits. Also vary statistics,
action sizes/counts, parsed-stats checkpoints, query shape, and concurrent requests sharing a cap.

Use fresh processes for baseline and constrained runs, with fixture preparation/copies outside
measurement. Verify typed errors, exact participating-owner release, successful retry, and absence
of unintended commits. Process aborts fail the checks. Include sample counts with latency results;
small correctness samples do not establish performance percentiles.

[Current explicit-backend validation](../arrow-allocator-poc/ALLOCATOR_API_VALIDATION.json) and
[lifecycle checks](LIFECYCLE_CHECKS.md) document the tested compiler/feature combinations. Historical
results retain their original provenance. In particular, the removed process-global limiting hook
and its coverage observations are not measurements of the explicit allocator.
