# Declarative driver memory measurements

Measure the lifecycle that retains driver metadata: snapshot discovery and schema parsing,
checkpoint resolution, scan planning, plan serialization, transaction preparation with opaque
relations, and write-state encoding. Executor execution and full declarative commit are outside
this benchmark. The local reference engine used for snapshot discovery adds its own parsing and
Arrow buffers to process measurements.

The shared allocator tracks participating backing allocations even when enforcement is disabled.
Historical peaks include temporary old-plus-new capacity during growth. Query objects, returned
wire bytes, and reference-counted schema text retain their charge until their final release.
Fixed canonical schema caches remain charged after all query owners are dropped; compare repeated
workloads against a matching warmed baseline when checking leaks.

## Metrics

- **Kernel requested bytes**: reservations made through `KERNEL_ALLOCATOR`. A configured limit
  bounds this counter, including allocations in flight. It excludes storage using other allocators.
- **Live Rust heap**: requested layouts observed by the benchmark executable's diagnostic
  `GlobalAlloc`. Kernel itself does not install that allocator.
- **DHAT global peak**: live allocations at the profiler's single simultaneous heap peak. Attribution
  by allocation stack estimates the share using Kernel's allocator. A profile started after snapshot
  creation excludes earlier live allocations and cannot establish whole-lifecycle coverage.
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
commit, and verify query allocations return to the warmed baseline after joining workers. Keep
latency, live-heap, and OS-memory measurements together; the allocator counter alone does not
establish total native-memory coverage.

## Measured results

The [local lifecycle matrix](LIFECYCLE_CHECKS.md#validated-results) records simultaneous-peak heap
attribution across table, query, and generated plan shapes, together with pressure, recovery,
concurrency, and exact wide-tail checks. Its [compact receipts](LIFECYCLE_RESULTS.json) record the
manifests, executable hash, and measurement boundaries. Coverage varies substantially by workload;
a schema-heavy profile cannot establish coverage for an Arrow-heavy one.
