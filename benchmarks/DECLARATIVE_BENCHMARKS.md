# DAT benchmarks for declarative metadata plans

## Context

Kernel's DAT infrastructure has two consumers. `acceptance` checks results against a
Spark-generated corpus; `benchmarks` times operations with Criterion. The benchmark
runner loads a table/version and parses its predicate during setup. Each read iteration
builds a scan with `StatsOptions::all_struct()`, calls `scan_metadata` (or its parallel
API), and drains the results. It does not read table data files.

The declarative counterpart already exists:
[`Scan::declarative_metadata_scan_plan`](https://github.com/delta-io/delta-kernel-rs/blob/7b43fd2f/kernel/src/scan/mod.rs#L1467)
returns an optional plan producing live `add` actions. The caller executes that plan
through `PlanExecutor::execute_op(Operation::QueryPlan(plan))`, then drains `into_data()`.
`None` is a successful empty scan. Planning alone does not execute the metadata query.

There is an execution constraint: current main removed the DataFusion executor
([#3990](https://github.com/delta-io/delta-kernel-rs/pull/3990)). Its in-repo
[`SyncPlanExecutor`](https://github.com/delta-io/delta-kernel-rs/blob/7b43fd2f/kernel/src/engine/sync/plan.rs#L1)
is a test executor that materializes every operator's output. It supports local files
and in-memory stores; cloud stores requiring a Tokio reactor are unsupported.
`DefaultEngine` supplies the existing imperative implementation, not a production
query executor for these plans.

## Problem and goal

Declarative planning and execution have no counterparts in the DAT benchmark matrix.
A change can improve imperative replay while increasing plan construction cost or the
work expressed by a plan without a corresponding performance signal.

**Reuse DAT tables, versions, predicates, Criterion, and CI reporting to measure both
paths.** Start with metadata scans. Snapshot construction, table-data reads, transaction
plans, checkpoint writes, and production connector execution are separate follow-ups.

## Proposal sketch

```text
Published DAT benchmark archive, pinned by SHA-256
  -> TableInfo + ReadSpec (table, version, predicate)
  -> bench-registry.json selects execution configurations
  -> setup: engine + snapshot + parsed predicate
  -> timed iteration:
       imperative: build Scan -> scan_metadata -> drain
       declarative: build Scan -> build Plan -> execute QueryPlan -> drain
       planner: build Scan -> build Plan -> drop Plan
  -> Criterion samples + existing PR comparison
```

The prototype adds a defaulted `metadataExecution` field to `ReadConfig`. Omitted fields
keep existing behavior and benchmark names. Configurations are explicit registry entries:

| Configuration | Timed operation | Execution engine |
|---|---|---|
| `serial`, `parallel2` | Existing imperative metadata scan | DefaultEngine |
| `imperativeSync` | Imperative metadata scan | SyncEngine |
| `declarativeSync` | Plan construction and complete execution | SyncPlanExecutor |
| `declarativePlan` | Plan construction and destruction | SyncEngine for discovery |

The synchronous imperative run controls for the engine change when assessing the
synchronous declarative run. These numbers characterize Kernel plus the reference
executor; they do not predict Reyden or another production connector's performance.
Output representations also differ: imperative results include selection vectors and
file constants, while declarative results contain live `add` structs. Consumer-side
normalization is excluded from all timings.

The prototype registers these configurations for the published `v1Checkpoint` and
`v2Checkpoint` cases, including the latter's sidecars. Other workloads retain their
existing configurations. Both tables already have the `base` tag, so the current CI
script discovers the additional benchmarks without workflow changes.

Each runner owns a separate preloaded snapshot. Criterion warm-up makes this a warm
snapshot/cache measurement, including repeated scan construction, planning, query
execution where applicable, and destruction. Planner discovery can perform I/O on first
use; it is not a promise of zero I/O. Cold snapshot-plus-plan latency needs its own case.
No plan or query result is reused across timed iterations. Every result and terminal
error is consumed; an execution failure fails the benchmark.

The sync engine does not emit the storage metrics used by `CountingReporter`. Its I/O
profile is explicitly marked unavailable. Existing DefaultEngine I/O summaries remain
available. Instrumenting executor reads is a follow-up; zero counters would be misleading.

## Design Decisions

### Design Decision 1: Reuse workload specs or introduce declarative DAT specs?

Both APIs perform the same metadata operation for the same table version and predicate.
The choice belongs to Kernel's execution harness.

**[PREFERRED] Option 1: Extend the Kernel read configuration.**

- 🟢 Both paths consume the same bytes and predicate.
- 🟢 Existing Criterion names and CI comparison infrastructure remain usable.
- 🟡 Configuration names must identify the executor and measured stage.

**Option 2: Add a declarative spec or serialized-plan artifact to DAT.**

- 🟢 Useful later for portable executor conformance or serialization benchmarks.
- 🔴 Duplicates semantic workloads and couples the corpus to experimental plan IR.
- 🔴 Requires artifact versioning before the initial benchmark can run.

Use configuration for this proposal. Plan serialization is a separate operation and
should have a separate benchmark when it becomes relevant.

### Design Decision 2: What can execute the plan in OSS CI?

**[PREFERRED] Option 1: Start with planner timing and an explicitly named sync reference.**

- 🟢 Runs now using code and public fixtures already in the repository.
- 🟢 Provides a matched imperative sync comparison and exercises full query consumption.
- 🔴 Eager intermediate allocations can dominate execution and memory use.
- 🔴 Cannot benchmark the private/cloud tables used by production connectors.

**Option 2: Add or restore a production query executor to OSS Kernel.**

- 🟢 Measures streaming, optimization, concurrency, and realistic execution behavior.
- 🔴 Requires ownership of an executor, dependencies, and support for the required IR.
- 🔴 Adds an implementation project beyond wiring up benchmark coverage.

The reference executor is sufficient to prototype the harness and measure planner
regressions. Add a production executor as another configuration once one has an owner;
keep connector-specific performance coverage in the connector's environment.

### Design Decision 3: When should these numbers block PRs?

**[PREFERRED] Option 1: Evaluate a draft PR, then select stable gating cases.**

- 🟢 Allows inspection of runtime, noise, and report presentation before adoption.
- 🟡 The prototype uses the existing benchmark job and its 15% threshold. New names
  have no base measurement on the introducing PR, so their initial ratios are unavailable.
- 🔴 After merging the harness, matching names become subject to that existing gate.
  A separately advisory rollout would need an explicit gate policy change.

**Option 2: Gate all declarative workloads immediately.**

- 🟢 Broad coverage from the first subsequent PR.
- 🔴 Risks making reference-executor costs and runner noise merge blockers before calibration.

Use the draft to decide which configurations should join the required benchmark set.
CI compares the same name across commits. Comparing `imperativeSync` with
`declarativeSync` within one commit answers a different question and requires reading
their absolute timings or a separate comparison view.

## Artifacts and workload expansion

**No DAT upload is needed for this prototype.** `benchmarks/build.rs` already pins
`v0.0.5-preview/v0.0.5_benchmark_workloads.tar.gz`, SHA-256
`ddac5359eca42e7ec65b4a7cfc6f4bc1d629d9c204ba714ec15e4ae830c37ba2`.
A fresh verified download contains v1/v2 checkpoints, CRC freshness cases, partitioned
and clustered predicates, and wide JSON/structured statistics tables. These provide
several next experiments without publishing data.

For a workload absent from that corpus, generate synthetic tables and specs in DAT,
validate their expected results, and package the benchmark loader's layout:
`benchmarks/<table>/tableInfo.json`, `delta/`, and `specs/*.json`. Publish a new versioned
**benchmark corpus archive** and update its release tag, asset name, and SHA-256 in
`benchmarks/build.rs`. A generator JAR alone does not update Kernel CI's data.
Do not replace an existing release asset in place.

DAT's modern acceptance generator uses `table_info.json`, richer expected-result specs,
and potentially grouped output. It is not interchangeable with the benchmark corpus
without a conversion/loader change. Acceptance's `DELTA_ACCEPTANCE_WORKLOADS_PATH` is
also distinct from benchmarking's `KERNEL_BENCH_WORKLOAD_DIR`. The latter can point at
local synthetic benchmark fixtures for iteration before a release.

## Validation and follow-up work

The prototype checks live-file identity against imperative scans for full scans,
partition predicates, data skipping, empty predicates, and time travel. It rejects
cloud tables for sync configurations before credential lookup and rejects combining
these configurations with imperative scan parallelism. Existing Kernel declarative
parity tests cover normalized metadata fields, statistics, and checkpoint variants.

Next steps, in order:

1. Smoke-run every registered configuration and inspect Criterion output on a draft PR.
2. Add configurations for the existing partitioned, clustered, and wide-statistics cases.
3. Expand declarative acceptance validation against DAT expectations, including removes,
   re-adds, DVs, column mapping, and structured-statistics edge cases, outside timed loops.
4. Add cold snapshot-plus-plan cases and, if useful, execution-only cases with explicit
   treatment of plan cloning, executor preparation, and retained state.
5. Integrate a production executor and connector benchmarks; measure peak memory and
   executor I/O before scaling the eager reference to very large logs.

Local smoke command (functional execution, not a performance measurement):

```bash
cargo bench --locked -p delta_kernel_benchmarks --bench workload_bench \
  --profile dev -- --test 'v1Checkpoint|v2Checkpoint'
```

For an optimized local measurement, omit `--profile dev --test` and pass the same filter.
On a draft PR, the existing `/bench --tags base --archive` command runs the benchmark
workflow and preserves raw Criterion output. The initial comparison has no base values
for the newly introduced configurations; their PR timings are displayed with Base and
Change marked unavailable. The prototype fixes the formatter to preserve that distinction.
