# Declarative driver memory

The target is the Kernel driver lifecycle used by a declarative connector. Imperative
SyncEngine receipts in this directory are baseline diagnostics. Their allocation coverage
percentages do not establish coverage of the declarative path.

| Phase | Kernel storage that can raise driver RSS | Stress dimensions |
| --- | --- | --- |
| Discovery | Log descriptors, URLs, filenames, checkpoint/sidecar lists; P&M plan IR and encoding | Tail length, path length, sidecars, independent snapshots |
| P&M ingestion | Raw schema JSON alongside parsed logical/physical schemas, metadata and names | Top-level/nested field counts, metadata bytes, schema replacement |
| Checkpoint inspection | Returned footer protobuf, decoded and cached checkpoint/leaf schemas, structured stats | Wide `stats_parsed`, sidecars, checkpoint/tail schema differences |
| Metadata-plan construction | Stats read/output schemas, JSON parse schemas, projection/pruning expressions, file constants, plan nodes and copies | Indexed/requested stats width, predicate width/nesting, tail file count |
| FFI handoff | IR, budgeted wire bytes and host copies during callback; unbudgeted mode also builds a protobuf mirror | Repeated schemas, encoded size, concurrent encodes and delayed releases |
| Declarative write preparation | Effective schema, statistics paths, write state/encoding, relation IDs/schemas, validation plans | Schema/stats width, staged-relation count, overlapping read/write owners |
| Cleanup/error/retry | Shared snapshot/plan owners retained across downcalls and threads | Shared versus independent snapshots, cancellation and cross-thread frees |

With an active budget, operation encoding borrows the IR and writes to allocator-backed bytes
without a protobuf mirror. Length prefixes are compacted in place. Temporary prefix space and
old-plus-new buffer growth are charged before backing allocation. The owner survives FFI return;
`free_kernel_bytes` drops that owner rather than reconstructing a globally allocated Vec. A final
Rust clone can keep the allocation charged after the FFI object is freed.

The `limited-memory` POC adds an opaque owner word to the declarative `KernelOwnedBytes` ABI. Build
connectors against the generated header from the same feature configuration and Kernel build.

Ordinary declarative metadata scans execute actions, JSON stats parsing, file-key deduplication
and replay in the engine. When Kernel returns a plan and the engine consumes the rows directly,
action row count and payload size need not scale Kernel's retained heap. Engine buffers still on
the driver contribute to process RSS and need separate accounting. The Rust reference executor's
Parquet decoding buffers belong to that engine category.

Exceptions matter: CRC `allFiles` materializes Rust Add actions, and a literal-row plan can copy
their large stats strings into scalars and protobufs. P&M, domain metadata, checkpoint schemas
and validation results consumed by Kernel also cross the boundary. Declarative mode alone does
not make every large action an engine-only allocation.

## Workloads

- Exact tail case: 10,000 top-level columns, 30,000 schema fields, 11,301,270 schema-JSON bytes,
  1,000 initial files, one checkpoint and 10,000 tail commits.
- Same schema with 0/32/all indexed stats columns; JSON-only, structured-only and both outputs.
- Narrow/wide checkpoint `stats_parsed`, including stats schemas that differ from the current
  table schema. Vary requested-stat, projection and predicate widths independently.
- Small/large action and stats payloads at fixed width/count. Compare engine-owned relations
  with CRC `allFiles` / literal-row inputs materialized by Kernel.
- Long tails at fixed width; many actions/files at fixed tail length. Count plan file descriptors
  as well as terminal nodes.
- Declarative append preparation with few/many staged relations. Rows per relation stay opaque;
  relation-handle count is a separate dimension.
- Concurrency 1/4/16 with shared and independent snapshots, overlapping encodes and delayed frees.

Record phase RSS, requested Rust heap, participating budget usage, cleanup retention, schema
fields, plan nodes/wire bytes and elapsed time. Prepare fixtures in another process. Report pure
Kernel planning windows separately from mock-engine execution. Requested heap coverage is not
RSS coverage.

## Local harness and limits

Build with `--features declarative-benchmark`, then use `run_rss.py --mode declarative`.
Snapshot P&M uses a real PlanExecutor. The wrapper also performs the IR/protobuf conversion used
by FfiPlanExecutor. Its local executor is an engine stand-in, not Spark/DBSQL.

Checkpoint information is resolved before scan-plan construction. A callback counter asserts
that building and serializing the returned metadata plan makes no further engine calls. Only
single-query runs emit per-window heap peaks; concurrent runs use aggregate counters.

`--stats-columns -1 --parsed-stats` creates all-column stats and a structured-stats checkpoint.
`--stats-bytes` varies stats payload size. `KERNEL_DECLARATIVE_STATS=json|struct|both` selects
requested stats; `KERNEL_DECLARATIVE_RELATIONS` varies staged relation handles.

Mainline `013fe8bcd8429d0d74664f4b60e5ad10989656e4` returns Unsupported from
`commit_declarative`. The harness covers staging and write-state encoding and reports this
limitation. The plan-based writes branch is required to measure declarative commit end to end.

The shared allocator participates in decoded schema storage, retained listing buffers, derived
stats schemas, schema copies made by metadata projection/aggregation, operation wire encoding,
and write-state output. Audit other plan/expression storage, footer results and literal rows
independently. Infallible transforms and protobuf decoding buffers do not become limited merely
because a snapshot has an allocator.
Opaque FFI byte buffers retain the charge through their final release. Budget exhaustion is a
typed error; this does not rely on a panic, unwinding across C, or a request-lifetime reservation.

The Rust FFI lifetime test frees the FFI byte object while a Rust clone remains, then observes
release after dropping the final clone. The C harness holds a returned declarative plan after
releasing all scan, snapshot, transaction and engine handles, checks that bytes remain charged,
and calls `free_kernel_bytes` on another pthread. Participating live bytes then reach zero.

Validation includes 17,887 passing Kernel/FFI nextest cases (30 skipped), 32 passing experiment
cases, protobuf equivalence checks against prost, the C/pthread lifecycle harness, workspace
all-feature clippy and rustdoc, and the split no-default-feature checks. Rust 1.100 is used for
compilation and execution; nightly is used only for the repository's formatter.

## Measurements

The exact tail fixture with all columns indexed and structured stats requested has these
fresh-process observations (MiB):

| Observation | Before bounded planning/encoding | With bounded planning/encoding |
| --- | ---: | ---: |
| Whole-run requested Rust heap peak | 383.26 | 282.80 |
| Whole-run RSS peak | 720.03 | 583.56 |
| Participating reservation peak | 90.28 | 238.53 |
| 128 MiB participating cap | Completes despite unbounded planning allocations | Returns allocation error |
| Participating bytes after cleanup | 0 | 0 |

The successful bounded run uses a 512 MiB cap. A 64 MiB cap also returns a typed error without
publication. The 128 MiB rejection still reaches a 243.44 MiB requested Rust heap peak during the
mock-engine-inclusive snapshot work: this is evidence that the whole heap remains uncapped.

An isolated DHAT run starts after scan-state construction, excluding earlier live allocations
and mock-engine snapshot execution. At that window's single simultaneous heap peak, participating
allocations account for 164,660,072 of 194,899,355 requested bytes (**84.5%**). This percentage is
specific to allocations made after that profiling boundary. It is not a whole-lifecycle coverage
percentage or an RSS coverage percentage. Remaining large sites include standard Arc nodes, names,
URLs, scalar vectors, and other unconverted storage. Footer protobuf decoding is also unconverted.

### macOS RSS attribution

Self-inspection of VM regions and malloc zones attributes most cleanup RSS retention to reusable
malloc pages. Raw RSS includes those pages and must not be interpreted as live Kernel memory or
as unreclaimable OOM pressure. The physical-footprint metric excludes reclaimable pages and much
shared mapped memory. This is specific to Darwin accounting; production Linux driver validation
requires its own allocator and cgroup measurements.

The same wide-tail fixture with a 512 MiB budget produced these adjacent observations (MiB):

| Observation | Plan wire live | After owners and worker threads released |
| --- | ---: | ---: |
| Requested live Rust heap | 274.80 | 0.08 |
| RSS | 532.53 | 492.22 |
| macOS physical footprint | 277.72 | 23.81 |
| Malloc small-region reusable pages | 169.77 | 447.70 |
| Resident stack pages | 0.16 | 0.06 |

The run's lifetime physical-footprint peak was 328.45 MiB, versus 583.41 MiB peak RSS. These are
separate high-water marks and must not be subtracted to calculate simultaneous overhead. Stack
reservations were about 68 MiB during planning, but only 160 KiB of stack pages were resident.
Virtual reservations do not account for the RSS gap.

The allocator charges active requested allocations. Freeing storage releases its charge even if
malloc keeps reusable resident pages. Reusing those pages for a new allocation charges the new
live allocation without necessarily increasing physical memory use.

To record these diagnostics, set `KERNEL_MEMORY_VM_MAPS` to an output directory for a single-worker
run. Snapshots use the process's own Mach VM counters, `malloc_zone_statistics`, and
`proc_pid_rusage`, so external debugger attachment is unnecessary. Region resident sums may
include aliases and must not be added as if they were disjoint process RSS. Diagnostic snapshots
add overhead and should be kept separate from performance runs.

Receipts: [diagnostic lifecycle](results/declarative/wide-tail-vmmap.json),
[planning VM counters](results/declarative/vmmap/declarative_metadata_plan_proto_live_no_execution.json),
and [cleanup VM counters](results/declarative/vmmap/after_threads_joined.json).

Receipts: [whole lifecycle](results/declarative/wide-tail-encoded.json),
[isolated allocation attribution](results/declarative/plan-encoded/complete-heap.json), and
[parsed-stats concurrency](results/declarative/parsed-stats-concurrent.json).
The concurrency fixture uses 1,000 top-level columns, 1,000 tail commits and a parsed-stats
checkpoint. Four independent queries finish under a shared 256 MiB cap; under 64 MiB some fail
cleanly while others finish. All runs release participating bytes to zero.

To reproduce the isolated profile after building both examples:

```sh
python3 experiments/limited-memory/run_heap.py \
  --prepare-binary "$KERNEL_MEMORY_TARGET/release/examples/rss_lifecycle" \
  --heap-binary "$KERNEL_MEMORY_TARGET/release/examples/kernel_lifecycle" \
  --mode declarative --width 10000 --initial 1000 --tail 10000 --stats-columns -1 \
  --cap 512 --phases complete --profile-from-stage declarative_scan_state_built \
  --output experiments/limited-memory/results/declarative/plan-profile
```
