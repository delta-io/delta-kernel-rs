# Local allocator lifecycle checks

The matrix checks whether the shared allocator behaves consistently across table and query shapes.
It measures declarative driver work: snapshot loading, checkpoint discovery, query/schema
construction, metadata-plan construction and encoding, opaque write-relation staging, write-state
encoding, and final ownership release. It never executes the returned terminal metadata plan or
commits a declarative transaction.

The local reference engine does execute the discovery operations that Kernel needs to build a
snapshot. Those operations allocate Arrow buffers. They are included in process measurements and
remain outside the shared allocator's cap.

## Workload dimensions

[The matrix manifest](lifecycle_matrix.json) crosses compatible table and query profiles rather
than changing labels on a fixed plan. Each successful operation reports its generated operator
counts, DAG depth, maximum input fan-in, JSON/Parquet source counts, and wire lengths.

| Dimension | Cases |
| --- | --- |
| Schema | Flat primitives, nested structs, deep structs, arrays/maps, wide schemas, large field metadata |
| Log | JSON-only, checkpoint-only, checkpoint plus tail, long tails, hundreds of actions per commit, add/remove churn |
| Statistics | Sparse/dense nested bounds and null counts, large action JSON, JSON checkpoints, structured-only checkpoints |
| Query | Full/narrow projection; range, wide AND/OR, IN lists, null checks, nested predicates, large literals, partition filters, constant-false filters |
| Output | No stats, JSON, structured stats, selected stats columns, both representations, multiple opaque write relations |
| Lifetime | Repeated calls in one process, concurrent requests sharing one limit, cross-thread output release, retry after clearing a limit |

[The stress manifest](lifecycle_stress.json) adds the exact 10,000-column / 30,000-field table with
11,301,270 schema-JSON bytes, 1,000 initial files, one checkpoint, and 10,000 tail commits.

## Run

From the repository root:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo +1.95.0 build --locked --release -p mem-test \
  --features declarative-benchmark --example rss_lifecycle --example kernel_lifecycle

python3 mem-test/metadata/run_matrix.py \
  --binary target/release/examples/rss_lifecycle --output /tmp/kernel-matrix.json \
  --failure-stages declarative_snapshot_ready_mock_engine_included declarative_query_built \
  declarative_scan_state_built declarative_metadata_plan_built_no_execution \
  declarative_metadata_plan_proto_live_no_execution declarative_transaction_relations_staged

python3 mem-test/metadata/run_matrix.py \
  --binary target/release/examples/rss_lifecycle --manifest mem-test/metadata/lifecycle_stress.json \
  --concurrency 1 --output /tmp/kernel-stress.json
```

Use `--tables` / `--queries` to select named profiles. `--concurrency` defaults to `1 4` and
`--repetitions` defaults to `2`. Table preparation runs in separate processes. Each measured process
warms a matching smaller table before enabling its requested limit. Read-only workers share a
temporary fixture; the runner checks that planning leaves its transaction-log files unchanged.

For each compatible pair and concurrency, the runner checks unlimited tracking, a generous limit,
a pressure limit derived from measured usage, and zero additional headroom. Optional phase checks
enable a zero-headroom limit after the named phase, using one worker and one measured operation.
Phases absent from a constant-false scan are omitted. A phase check can succeed when later phases
need less memory than was already retained at the injection point.

## Assertions and receipts

The runner fails on a process abort, timeout, unexpected error, observed limit overrun, retained
query allocations, or failed recovery. Allocation failures must be typed memory errors with a
recorded rejected reservation. Every process clears the limit and retries successfully. Serial
iterations must each return to the warmed baseline; concurrent workers must do so after joining.
Canonical caches remain charged and are not treated as leaks.

Unlimited and generous-limit runs must succeed, capture the expected snapshot version, and encode
write state. Constant-false filters must produce no metadata plan; other profiles must produce a
nonempty plan. Rust validates topological ordering and verifies that building/encoding the
returned metadata plan makes no engine execution calls. Encoded outputs are released on another
thread after their originating plan/transaction owners are dropped.

The output JSON contains compact summaries and failures. Adjacent `.receipts.jsonl` contains one
full receipt per line, referenced by `receipt_line`. Both are local artifacts; they are not checked
into the repository. Memory counters are requested bytes. Historical Kernel peaks include warmup,
so active-limit checks use live observations and account for the earlier historical peak.

Latency summaries include their sample count. Small default samples are diagnostic; p99.9 is left
unset unless a configuration has at least 1,000 operations. Timing comparisons need matching
machines, build settings, concurrency, and repetition counts. Harness records and reference-engine
work are included in the executable's Rust-heap measurements.

## Attribute uncovered memory

Use a full-lifecycle DHAT run to distinguish Kernel allocations from Arrow/parser and other
allocations at the same simultaneous heap peak. `run_heap.py --fixture-options` and `--query`
accept the corresponding JSON objects from the manifests. Run declarative profiles with
`--mode declarative --phases complete`; the profiler starts after fixed canonical initialization
and before query/snapshot construction. A cap of `0` disables enforcement.

The matrix establishes behavior for the participating allocator. It cannot establish a cap on
whole-process native memory. A high coverage fraction on a wide-schema case does not establish
coverage for a large-action or Arrow-heavy case. See [the measurement notes](DECLARATIVE_RSS.md)
for the difference between requested heap bytes, RSS, and physical footprint.

## Validated results

[Stable-backend validation](../arrow-allocator-poc/GLOBAL_ALLOC_VALIDATION.json) records the Rust
1.95 `GlobalAlloc` checks, including the matrix, exact wide-tail stress case, and C lifecycle.

[The original results](LIFECYCLE_RESULTS.json) record the manifest and executable hashes, platform,
check counts, and simultaneous-peak heap attribution for the published-dependency baseline build.
The measurements below used the historical Rust 1.100 `std::alloc::Allocator` implementation on
macOS arm64. The [Arrow allocator experiment](../arrow-allocator-poc/README.md) records separate
measurements with its dependency storage changes and reference-executor retention improvements.

- The matrix crossed 169 compatible pairs at concurrency 1 and 4: 2,354 fresh-process checks and
  7,762 measured operations. These included 3,995 typed allocation errors and 2,354 successful
  retries after clearing the limit. Every cleanup and active-limit check passed.
- The exact wide-tail fixture added 21 process checks and 33 operations, including 19 typed
  allocation errors. Every process recovered and returned to its warmed baseline.
- Operator/source signatures varied across the generated plans; the signature includes source
  file counts as well as operator counts, depth, and fan-in.

Full-lifecycle DHAT profiles show why this is not a whole-lifecycle native-memory cap:

| Table / query | Requested Rust heap at peak (MiB) | Through Kernel allocator at that same peak |
| --- | ---: | ---: |
| Wide schema / all columns | 23.83 | 87.57% |
| Deep schema / nested predicate | 2.89 | 82.15% |
| Dense structured statistics / both outputs | 7.46 | 88.75% |
| Many actions / range filter | 2.10 | 18.25% |
| Large action statistics / large IN | 1.28 | 10.93% |
| Long tail / wide OR | 22.13 | 1.94% |

In the long-tail profile, Arrow-related buffers, arrays, schemas, and decoder state accounted for
about 97% of the simultaneous peak. Tracking only Arrow's buffer payloads would still omit its
object graphs and parser vectors. The attribution categories are allocation-stack diagnostics;
they do not prove ownership or enforcement boundaries. Reference-engine allocations are included.

The checks exposed aborts from infallible schema cloning, physical/projection transformations,
protocol/metadata plan schemas, replay-key construction, and filtered log-tail construction.
Those participating paths now propagate allocation errors. Public infallible Rust operations and
unconverted dependency allocations still retain their ordinary allocation-error behavior.
