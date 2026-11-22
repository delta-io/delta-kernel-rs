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

These local macOS runs use Rust 1.100 release builds, structured scan statistics, and a warmed
baseline. They measure the declarative driver lifecycle described above. Numbers are MiB unless
stated otherwise; independent high-water marks in the table need not occur together.

| Exact tail limit | Outcome | Kernel reservation peak | Rust requested-heap peak | RSS peak |
| --- | --- | ---: | ---: | ---: |
| Disabled | Success | 221.29 | 250.85 | 504.83 |
| 512 | Success | 221.29 | 250.85 | 501.59 |
| 128 | Allocation error | 128.00 | 243.61 | 371.17 |
| 64 | Allocation error | 64.00 | 243.61 | 323.45 |

Every run returned to the 159,388-byte warmed Kernel baseline. The low-limit runs still have large
Rust peaks because snapshot discovery in the reference engine uses allocations outside this
allocator. The successful metadata-plan wire output is 12,140,985 bytes; encoded write state is
34,422,173 bytes.

For the parsed-stats case, use 1,000 top-level columns / 3,000 fields, 200,918 schema-JSON bytes,
100 initial files, one checkpoint, 1,000 tail commits, 4,096-byte action-stat payloads, and all
indexed columns. One worker succeeds with disabled, 256 MiB, and 64 MiB limits, with a 24.15 MiB
Kernel peak and a 27.66 MiB Rust peak. Four unlimited workers peak at 84.16 MiB through the Kernel
allocator and 96.49 MiB of Rust heap; all also succeed under 256 MiB. Under a shared 64 MiB limit,
two workers succeeded and two returned allocation errors, with the Kernel peak bounded at 64 MiB.
The completion split depends on scheduling. All runs returned to the same warmed baseline.

A separate full-lifecycle DHAT run starts after canonical initialization and before snapshot
loading. At its single simultaneous Rust heap peak, 231,885,521 of 262,784,536 requested bytes
(88.24%) were allocated through `KernelAllocator`. Remaining live groups included schema
structures (22,631,122 bytes), other allocations (4,505,718), log descriptors/URLs (3,520,739), and
metadata strings (241,436). This is live Rust-heap attribution, not RSS or foreign-memory coverage.

Run `python3 mem-test/metadata/run_exhaustion.py --help` for fresh-process failure sweeps. The
validated sweep includes 43 limit cases, with 32 configured between lifecycle stages; each checks
that failures propagate, query allocations are released, and no commit is published. The generated
C ABI harness also retains plan bytes across other handle destruction, frees them on another
pthread, and verifies repeated successful imperative diagnostic commits reach a stable retained
baseline. Imperative commit checks do not extend declarative commit support.
