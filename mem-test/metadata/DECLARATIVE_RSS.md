# Declarative driver memory measurements

Measure the lifecycle that retains driver metadata: snapshot discovery and schema parsing,
checkpoint resolution, scan planning, plan serialization, transaction preparation with opaque
relations, and write-state encoding. Executor execution and full declarative commit are outside
this benchmark. The local reference engine used for snapshot discovery adds its own parsing and
Arrow buffers to process measurements.

The installed global Rust allocator tracks standard collections, dependency objects, and Rust-owned
Arrow buffers even when enforcement is disabled. Query objects, returned wire bytes, and shared
schema text retain their charge until final release. Fixed canonical caches remain charged.
Global counters also include caller diagnostics and harness storage; exact payload ownership and
release are checked separately by the Rust and generated-C tests.

## Metrics

- **Global Rust reservations** (`kernel_live` / `kernel_peak` in receipts): requested bytes through
  the installed `KernelAllocator`, including requests in flight. Reallocation reserves a complete
  replacement before invoking the backing allocator, so its peak can conservatively exceed the
  actual live heap peak even when the backing allocator grows in place.
- **Live Rust heap**: requested layouts observed independently by the benchmark's inner diagnostic
  allocator. These count actual successful backing requests and reallocation size changes. They
  exclude direct libc/C++ allocations and memory mappings.
- **DHAT global peak**: live allocations at the profiler's single simultaneous heap peak. A profile
  started after snapshot creation excludes earlier live allocations. Run with enforcement disabled:
  profiler bookkeeping also allocates through the process hook.
- **RSS**: resident process pages, including allocator caches, reusable pages, code, and stacks.
- **macOS physical footprint**: the OS pressure charge. Reusable malloc pages can remain resident
  while carrying a much smaller physical-footprint charge.

RSS and live requested heap bytes are different measurements. In particular, freed reusable malloc
pages can remain in RSS and serve later allocations. Their residency is not evidence of untracked
live allocations or of an equal additional OOM risk. Do not subtract independent high-water marks
to infer simultaneous overhead.

## Cases

The exact tail fixture contains 10,000 top-level columns / 30,000 schema fields, 11,301,270 bytes of
schema JSON, 1,000 initial files, one checkpoint, and 10,000 tail commits. Include all stats columns
and structured output to exercise derived-schema and plan-wire storage. Also vary action stats
payload size and parsed-stats checkpoints, and run a smaller fixture with concurrent requests
sharing the same limit.

Use a fresh process for each table/limit/concurrency combination. Generate and copy fixtures before
measurement. Compare unlimited tracking against configured limits, verify errors do not publish a
commit, and check typed memory errors separately from allocation aborts. The global matrix retains
caller diagnostic data after releasing query owners; use the ownership tests for exact release
assertions. Keep latency, live-heap, and OS-memory measurements together; the allocator counter alone does not
establish total native-memory coverage.

## Measured results

The [local lifecycle matrix](LIFECYCLE_CHECKS.md#validated-results) records simultaneous-peak heap
attribution across table, query, and generated plan shapes, together with pressure, recovery,
concurrency, and exact wide-tail checks.
[Current validation](../arrow-allocator-poc/PROCESS_GLOBAL_VALIDATION.json) records the installed
hook and recovery gaps. The [historical receipts](LIFECYCLE_RESULTS.json) describe the earlier
per-container accounting and preserve their original executable hashes and boundaries. Those
coverage fractions are not measurements of the global hook.
