# stack/fsr cleanup plan

Working plan to ruthlessly simplify the stack/fsr branch. Reduces the branch diff against main from **+38,876 LOC** to **~+14,500 LOC** (~63% reduction) without feature loss, plus structural wins: engine refocused as a thin DataFusion adapter, custom Execs collapsed, sinks moved to executor dispatch, vendored crate replaced by a git fork, coroutine framework on a maintained crate, tests parameterized via rstest, engine internals on `EngineError`, no plan-tree-shape brittleness in tests.

## Goals & constraints

| Goal | Measure |
|---|---|
| Cut ~24k LOC from the branch diff without losing features | `git diff main...stack/fsr \| wc -l` |
| Restore the abstraction boundary: engine knows DataFusion, kernel knows Delta | Zero references to Delta protocol concepts in `delta-kernel-datafusion-engine/src/` |
| Replace hand-rolled mechanics with maintained library code | Coroutine framework on `genawaiter2`; `RecordBatch::try_new` for nullability; stock DataFusion ops where possible |
| Use DataFusion's full primitives | `SessionContext::register_batch` / `read_table` for plan→plan dataflow; `TableProvider` for row-producing leaves; `LogicalPlanBuilder` helpers like `with_column_renamed` before hand-built projections |
| Engine internals on `EngineError`; translation to `DeltaError` only at the public boundary | One translation per public entry point, not one per internal call |
| Keep tests green at every step | `cargo nextest run --workspace --all-features` on each merge |

**Out of scope:** vendor crate body (managed via fork); redesigning the kernel IR; non-additive kernel public API changes.

## Total task tally — 103 substantive items, 1 done

| Phase | Items | LOC win |
|---|---|---:|
| **A** — pure deletions | A1, A2 (fork), A4, A5 | **~−19,400** |
| **B** — engine quality | B1, B2, B3c, **~~B5~~** (DONE), B6, B8, B9, B10, B11, B12, B13, B14, B15, B16 | ~−1,835 (~−170 realized via B5) |
| **C** — framework simplification | C1, Clone strip, parallelism follow-on | ~−740 |
| **D** — Delta details into kernel | D1, D3, D4, D5, D6 | ~−620 |
| **EC** — engine architecture/code quality | EC1–EC4, EC5+6, EC7–EC12, EC15–EC30 (27 items) | ~−1,015 / +50 doc |
| **KC** — kernel plans code quality | KC1–KC15 | ~−250 / +18 doc |
| **F** — fluent plan construction | F1, F2, F4–F10 | ~−35 (qualitative win) |
| **T** — tests | T1–T26 | ~−2,720 |
| **Total** | **103 tasks** (1 done) | **~−26,615 LOC** (~68% of branch diff) |

**Recent branch work landed:** B5 (DONE), B6 dispatch (struct removal pending), B10 partial (RowIndex moved via `window_plan(row_number())`), B15 partial (join_key_to_physical uses `kernel_expr_to_df`), B16 partial (typed `DvKind`/`DvRef` + `SinkType::Results(Option<SchemaRef>)`), KC8 partial (`resolve_checkpoint_shape_for_scan` extracted as free fn), KC9 partial (semantic dedup-key fixes).

**Recent branch work regressed:** EC7 (`Result<Option<LogicalPlan>>` pattern propagated to ~10 sites in new `compile/logical.rs`), EC15 (4 fresh manual projection helpers), EC16 (ordinal+union dance now in three places).

---

## Phase A — pure deletions (highest LOC, lowest risk)

### A1 — Shrink `delta-error-codes.json` to 12 entries
**File:** `kernel/src/plans/errors/delta-error-codes.json`. Trim to the 12 entries matching declared `DeltaErrorCode` variants; the parity test (`errors/mod.rs:455-481`) keeps working unchanged.
**LOC:** ~−3,548.

### A2 — Switch vendor to git fork; delete `vendor/datafusion-datasource-parquet/`
Replace the entire vendored crate (15.6k LOC) with a `[patch.crates-io]` git entry pointing at an org-owned fork of `apache/datafusion`. The ~155 LOC of real changes live as commits on a `delta-field-id` branch; everything else stays in upstream where it belongs. Supersedes A2a/A2b.
**LOC:** ~−15,606.

### A4 — Delete single-phase SM adapters
`kernel/src/plans/state_machines/df/{insert.rs, checkpoint_write.rs, mod.rs}` — trivial coroutine wrappers that yield one plan and finish.
**LOC:** ~−108.

### A5 — Delete unused `OperationType` enum
`kernel/src/plans/state_machines/framework/{operation_type.rs, mod.rs}` — zero callers; intended for tracing that was never wired.
**LOC:** ~−18.

---

## Phase B — engine quality

### B1 — `ContextDelta` / `ContextEngine` extension traits + error-conversion sweep
**Files:** `delta-kernel-datafusion-engine/src/error.rs` + ~80 call sites. Add two parallel traits: `ContextDelta` for code that produces `DeltaError` (kernel-facing boundary), `ContextEngine` for engine-internal code (producing `EngineError`). Adds `arrow_schema_of()` helper for the kernel→arrow schema conversion. Coordinates with EC8.
**LOC:** ~−250.

### B2 — Lightweight `map_batch_stream` utility
New `exec/util.rs` (~40 LOC) per-batch transform wrapper that absorbs `ExecutionPlan` Stream/poll boilerplate. Only two callers (`KernelAssertExec`, `NullabilityValidationExec`).

```rust
// New helper in delta-kernel-datafusion-engine/src/exec/util.rs:
pub fn map_batch_stream<S, F>(
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    state: S,
    transform: F,
) -> SendableRecordBatchStream
where
    S: Send + 'static,
    F: FnMut(&mut S, RecordBatch) -> Result<RecordBatch, DataFusionError> + Send + 'static,
{
    Box::pin(MapBatchStream { inner, schema, state, transform })
}

// Caller use (replaces the entire KernelAssertStream struct + Stream impl + poll_next):
fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
    let inner = self.child.execute(partition, ctx)?;
    let evaluator = self.evaluator.clone();
    Ok(map_batch_stream(
        inner,
        self.schema.clone(),
        0usize,   // row counter for error messages
        move |row_counter, batch| { /* assert logic */ Ok(batch) },
    ))
}
```
No trait, no `Clone` bound, no `output_schema`/`finish` callbacks — neither remaining caller needs them. The entire `KernelAssertStream` struct + its `Stream`/`RecordBatchStream` impls go away.

**LOC:** ~−190.

### B3c — Fix `expand_projection_columns`; remove disabled optimizer rules
**Files:** `compile/mod.rs:362-402`, `executor.rs:127-128`. Rewrite the IR generator to emit clean projection chains; delete the two `remove_optimizer_rule` calls. Future DataFusion optimizer improvements reach engine plans; B9's drop-`__order` projection folds naturally.
**LOC:** ~−5, real correctness/composability win.

### B5 — `LiteralExec` → `MemorySourceConfig` — **DONE** (in branch work)
Completed: `exec/literal.rs` collapsed 193→55 LOC; `build_literal_exec` uses `MemorySourceConfig::try_new_exec` at line 11-24. Free function exported via `exec/mod.rs:7` and called from `compile/mod.rs:148`.
**Realized LOC:** ~−170.

### B6 — Delete `RelationSinkExec` struct + re-exports — **DISPATCH DONE; STRUCT REMOVAL PENDING**
Dispatch is done: `compile/mod.rs:99-101` no longer wraps in `RelationSinkExec`; `executor.rs:257-272, 348-353, 407-412, 632-661, 743-748` collect partitions then call `register_relation_batches`. The struct is structurally dead but still exported (`exec/mod.rs:9`, `exec/shape/mod.rs:16`) and has its own `with_new_children` invocation site. Remaining: delete `exec/shape/relation_sink.rs`, remove re-exports, and confer with EC28 (dual-materialization cleanup) on whether registry vs MemTable is the canonical sink.

Original framing for posterity (move to executor dispatch via `SessionContext::register_batch`):

```rust
// BEFORE (engine plan tree includes RelationSinkExec):
let plan = compile_tree(root)?;
let wrapped = Arc::new(RelationSinkExec::new(plan, handle.id, registry.clone()));
collect(wrapped).await?;   // RelationSinkExec's Stream impl registers on drain

// AFTER (executor dispatches by SinkType; tree has no sink Exec):
let plan = compile_tree(root)?;
let batches = physical_plan_collect(plan, &ctx).await?;
ctx.register_batch(&handle.name(), concat_batches(batches)?)?;   // EC10
```
**LOC:** ~−170. Plus unlocks parallel plan execution.

### B8 — Delete `KernelConsumeByKdfExec`; move to executor dispatch
Same shape as B6. Drain tree, feed batches through `consumer.apply()`, call `consumer.finish()` on stream end, the dispatch returns the `FinishedHandle` inline. Deletes:
- `KernelConsumeByKdfExec` itself
- `executor.rs::kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>` field
- `take_last_kdf_finished()` public method
- `clear_kdf_harvest_slot()` and its 5 call sites (defensive scaffolding for executor-scoped state that should be execution-scoped)
- The `kdf_harvest_slot` field threaded through `CompileContext`

After B8 the finished handle is produced by the dispatch arm and either pushed into `PhaseState` (the kernel's intended path) or returned to the caller. No side-channel slot.

**LOC:** ~−260 + ~−40 (slot plumbing).

### B9 — Compile `OrderedUnion` to stock DataFusion ops
For each child `i`: prepend `ProjectionExec` adding `__order: Int64 = i` + `CoalescePartitionsExec` if needed → `UnionExec` → `SortPreservingMergeExec` keyed by `__order` ascending → `ProjectionExec` drops `__order`. Streaming-equivalent to the custom Exec.

```text
Compile shape for Union{ordered: true, children: [c0, c1, c2]}:

  c0 → CoalescePartitions → Project(*, lit 0 as __order) ─┐
  c1 → CoalescePartitions → Project(*, lit 1 as __order) ─┼→ Union → SortPreservingMerge(__order asc) → Project(*, drop __order)
  c2 → CoalescePartitions → Project(*, lit 2 as __order) ─┘
```
Since each input has a constant `__order`, `SortPreservingMergeExec` drains them in order without materializing — same back-pressure profile as the custom Exec.
**LOC:** ~−90.

### B10 — Delete `RowIndexExec`; reject JSON+row-index symmetrically
Parquet uses native `RowNumber` virtual columns via the fork; JSON+row-index already rejected at load-sink level. Add symmetric rejection at scan compile time; delete the synthetic counter.
**LOC:** ~−145.

### B11 — Simplify `expr_translator`
Replace `coalesce_chain` (lines 252-263) with `datafusion_functions::core::expr_fn::coalesce`; delete the primitive match in `typed_null_to_df` (lines 311-345) in favor of `TryIntoArrow + ScalarValue::try_from`.
**LOC:** ~−40.

### B12 — Consolidate executor drive methods + `execute_phase_operation*` triplication
Two parallel 3-way duplications in `executor.rs`:

1. **Public layer** — three `drive_coroutine_sm*` methods (lines 416, 438, 465) differ only in result handling.
2. **Private layer** — three `execute_phase_operation*` methods (lines 303, 350, 686), each 40-55 LOC, byte-identical for `Relation`/`Load`/`ConsumeByKdf` branches; differ only in what to do with the `Results` stream (drain / collect into Vec / forward to FnMut).

Consolidate both layers to one function each, parameterized by a results handler:
```rust
enum ResultsHandler<'a> {
    Drain,
    Collect(&'a mut Vec<RecordBatch>),
    Stream(&'a mut dyn FnMut(RecordBatch) -> Result<(), DeltaError>),
}
```
Each `drive_coroutine_sm_*` becomes a one-line wrapper choosing its handler. **Must land before EC26** — otherwise composition logic gets duplicated three times.

**LOC:** ~−100 across both layers.

### B13 — Delete `KernelLoadSinkExec`; move to executor dispatch
Same principle as B6 (`RelationSinkExec`) and B8 (`KernelConsumeByKdfExec`): sinks live above the plan tree. `LoadSink` terminates a plan and produces a named relation; the file reads + DV apply + passthrough broadcast aren't transformations *in* the plan tree — they're what the sink does to materialize that relation.

Executor `SinkType::Load` dispatch: drain compiled tree (action rows + file metadata) → per row: extract path/size/file_constants, read file via kernel handler (or DataFusion `ParquetSource`, see EC14), apply DV mask, broadcast `file_constants` → concat → `ctx.register_batch(handle.name(), materialized)`.

```rust
// Executor dispatch shape (delta-kernel-datafusion-engine/src/executor.rs):
match &plan.sink.sink_type {
    SinkType::Load(load_sink) => {
        // 1. Compile the tree (which produces action rows with file metadata) — no LoadSink Exec
        let stream = self.compile_and_execute(&plan.root, &ctx).await?;

        // 2. Per upstream batch: read each referenced file, apply DV, broadcast file_constants
        let mut materialized = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            for row in 0..batch.num_rows() {
                let path = extract_path(&batch, row, &load_sink.file_meta)?;
                let dv_desc = extract_dv_descriptor(&batch, row, &load_sink.dv_ref)?;
                let file_batches = read_file_via_kernel(&self.engine, &path, &load_sink.file_type)?;
                for fb in file_batches {
                    let masked = apply_dv_mask(fb, &dv_desc)?;
                    let broadcasted = broadcast_passthrough(masked, &batch, row, &load_sink.passthrough_columns)?;
                    materialized.push(broadcasted);
                }
            }
        }

        // 3. Register as named relation (EC10's pattern)
        let concatenated = concat_batches(&load_sink.output_relation.schema_arrow()?, &materialized)?;
        self.session_ctx.register_batch(&load_sink.output_relation.name(), concatenated)?;
    }
    // ...
}
```

Settles the earlier "Load is a transform" confusion. Industry survey confirmed no DataFusion-ecosystem project puts this pattern in the plan tree — because it doesn't belong there.

**LOC:** ~−520 (engine sheds ~670; executor gains ~150).

### B14 — Partitioned write: parallelism + file rolling + Add-action metadata
Post-D5, `KernelPartitionedWriteExec` needs three things together. Pattern derived from delta-rs `PartitionWriter` + iceberg-rust `FanoutWriter`; concrete source pointers in the task description.

**Plan shape (declarative, optimizer-honest):**
```rust
impl ExecutionPlan for KernelPartitionedWriteExec {
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(partition_col_exprs)]
    }
    // No required_input_ordering — fanout writer handles per-tuple routing internally
}
```
DataFusion's `EnforceDistribution` optimizer rule auto-inserts `RepartitionExec(Hash(...))` upstream. **Same trait pattern as `HashJoinExec`** — uses the optimizer instead of fighting it. If upstream is already hash-partitioned the right way, `EnforceDistribution` skips the insertion (free).

**Hash collisions are correctness-safe.** Two distinct partition tuples may land in the same task. The task's fanout writer has `HashMap<PartitionTuple, PartitionWriter>` — each tuple gets its own writer pointing at its own directory. Hash collisions cost parallelism, not correctness:
```
table_root/
  dt=foo/part-00-{taskUuid}.parquet  ← writer for foo (in task 0)
  dt=bar/part-00-{taskUuid}.parquet  ← writer for bar (also in task 0, collision)
```
Two writers, two correct paths, no row mixing within a file. Delta protocol satisfied.

**File rolling (within a tuple's writer):**
- Per-writer UUID + counter sequence: `{partition_path}/part-{counter:05}-{uuid}.parquet`
- Check `bytes_written()` at parquet row-group boundaries
- When >= `target_file_size` (from `delta.targetFileSize`): close current, spawn finalize in `tokio::JoinSet<Result<Vec<Add>>>` (background upload), open next with `sequence + 1`

**Add-action emission:**
- Each finalized writer returns `Vec<Add>` (one per rolled file)
- After stream end: drain `JoinSet` via `join_next().await`
- Engine concatenates Add actions; output stream of Add rows
- Add fields: `path`, `partition_values`, `size`, `num_rows`, stats

**Compilation flow (fits the logical-plan setup):**
1. Compile IR child to LogicalPlan (after EC7 makes logical compile errorless)
2. `state.optimize()` runs the full logical optimizer
3. `state.create_physical_plan()` lowers to physical
4. Wrap with `KernelPartitionedWriteExec` (which declares its hash-distribution requirement)
5. Physical optimizer's `EnforceDistribution` auto-inserts `RepartitionExec` upstream

Same sink-wrap pattern as Results/Relation/ConsumeByKdf. The write subtree is physical-only (DataFusion has no logical concept of partitioned write).

**Prerequisites:**
1. D5 (kernel file namer call site exists)
2. EC7 (logical compile is errorless)
3. Kernel file namer API extended to `next_data_path(partition_values, writer_uuid, sequence) -> Url`

**Industry validation:** delta-rs, iceberg-rust, Spark `FileFormatWriter` all use this exact pattern. None use DataFusion's `DataSinkExec` for partitioned writes — custom Exec is the convention.

**LOC:** net ~+150 over D5's ~80-LOC residual. Value: correctness (file rolling) + perf (parallelism via declared distribution) + memory bounding (rolling caps file size).

### B15 — Delete `compile/join.rs`; move validations to IR constructor
**Files:** `delta-kernel-datafusion-engine/src/compile/join.rs` (delete), `compile/mod.rs` (delete dispatch arm at lines 218-219), `kernel/src/plans/ir/declarative.rs` (extend `DeclarativePlanNode::join` validation).

Three concerns mixed in 95 LOC: validations (hint must be Hash, build_keys non-empty, key-length parity), JoinType translation, and physical lowering via direct `HashJoinExec::try_new`. All three belong elsewhere:

1. **Validations move to `DeclarativePlanNode::join`** — return `DeltaResult<Self>`, validate at IR construction (consistent with F6).
2. **Physical lowering moves to `LogicalPlanBuilder::join_*`** in `compile/logical.rs` — same `HashJoinExec` underneath, but optimizer chooses `PartitionMode` instead of hardcoding `CollectLeft`.
3. **The single join site in the codebase** (`fsr/full_state.rs:1353`) has sink `Results` → already routes through `compile/logical.rs`. The physical compile arm at `mod.rs:218` is dead code.

Also strips redundant validations in `compile/logical.rs:495,498` (now done at IR construction).
**LOC:** ~−85.

### B16 — Move sink-input validations to kernel IR construction
**Files:** `delta-kernel-datafusion-engine/src/compile/{load_sink,write_sink}.rs` (delete or shrink), `kernel/src/plans/ir/{nodes/sinks,declarative}.rs` (extend).

Same pattern as B15 applied to LoadSink + WriteSink. Today `compile/load_sink.rs` (148 LOC) is ~70% validation + schema arithmetic that belongs in kernel:

- `resolve_leaf_field` (27 LOC) — this IS D6's `resolve_field_path` helper
- `passthrough_output_field` + `expected_materialized_schema` (22 LOC) — move to `LoadSink::expected_output_schema(upstream)` method
- IR-consistency parts of `validate_load_sink` (~38 LOC) — move to `DeclarativePlanNode::into_load` (validates column refs + output schema match)
- Engine capability checks (JSON+dv_ref, JSON+RowIndex) stay engine-side, but live in executor dispatch (B13's home), not in a separate compile_load_terminal

**Combined with B13**, `compile/load_sink.rs` deletes entirely. `compile/write_sink.rs` shrinks similarly.

**LOC:** ~−170 (engine: −222; kernel: +50-80; net engine deletion dominates).

---

## Phase C — framework simplification

### C1 — Replace hand-rolled coroutine with `genawaiter2`
**Files:** `kernel/src/plans/state_machines/framework/coroutine/{generator,driver,phase,mod}.rs`, `state_machine.rs`. The external sync `get_operation`/`advance` API is load-bearing for FFI. The hand-rolled stackless-coroutine machinery isn't — `genawaiter2` provides exactly that primitive. Add `genawaiter2 = "0.100"`; delete the hand-rolled bodies; `CoroutineSM` becomes a thin wrapper. Also deletes the `StateMachine` trait (one implementor).
**LOC:** ~−725.

### `engine_error.rs` — Drop manual `Clone` impl
Acknowledged footgun that silently drops the source chain.
**LOC:** ~−15.

### Follow-on: parallel plan execution (unlocked by B6 + B8 + B13 + EC10 + EC26)
Once sinks dispatch in the executor and intermediates are session-registered tables, the executor sees plan-level dataflow and can schedule independent plans concurrently via `tokio::spawn + join_all`.

EC26 reshapes the parallelism opportunity: the composable phase already gets DataFusion-native parallelism within a single LogicalPlan, so cross-plan parallelism is now between *phases* (overlapping independent Loads, or starting Phase 2 compose while Phase 1 Load is still finalizing). The pre-EC26 framing of "parallelize all plans" no longer applies — re-evaluate value after EC26 lands.

---

## Phase D — push Delta details into kernel

### D1 — Delete partition handling from `compile/scan.rs`
`ScanNode` has no partition values/columns in the IR. Engine speculatively added partition-handling that nothing asked for. Delete the hive-parsing helpers + `table_partition_cols` plumbing. If partition columns are needed downstream, they come from upstream IR (F1/F2's `LoadSink` with `passthrough_columns`), not via `ScanNode`.
**LOC:** ~−120 engine-side, zero kernel-side.

### D3 — Kernel DV-descriptor parser + engine delete
Add `DeletionVectorDescriptor::from_struct_array_row(...)` to kernel. Engine deletes the ~80 LOC of hand-rolled struct-row parsing (`dv_descriptor_from_struct_row`, `column_pref`, `utf8_value_at`, etc.). Engine keeps DV orchestration (accepted contract).
**LOC:** net ~−50.

### D4 — Drop name fallback in fork's field-ID projection
Simplify the field-id-projection patch (now on the fork branch) from mixed-mode to ID-only. Delta column-mapping is table-level all-or-nothing per protocol spec.
**LOC:** ~−80 inside the fork.

### D5 — Use kernel file namer for partitioned writes
Engine retains row bucketing by partition value tuple; deletes `encode_hive_segment`, `push_pct`, `hive_dir_relative`, `destination_base_dir`, `OpenWriter`. Calls kernel writer with `(table_root, partition_values, batch)` per group.
**LOC:** ~−336 (416 → ~80). Kernel: zero additions.

### D6 — Kernel `arrow_ext::resolve_field_path` helper
Engine has two near-identical implementations of "walk a dotted path through nested struct columns" (`load_sink.rs:263-303`, `nullability_validation.rs:252-277`). Add one canonical kernel helper; engine sweeps both sites.
**LOC:** ~−35 net.

---

## Phase EC — engine architecture/code quality

### EC1 — Extract `expect_single_child` helper
Eight `ExecutionPlan` impls each have the same 6-line `if children.len() != 1` check in `with_new_children`. One helper. Sequence after B6/B8/B9/B10 land.
**LOC:** ~−40.

### EC2 — `wrap_delta_err` helper for `DataFusionError::External`
10+ sites repeat `.map_err(|e| DataFusionError::External(Box::new(e)))?`. One helper in `error.rs`. Stacks with B1.
**LOC:** ~−10.

### EC3 — Fix `unreachable!()` in `executor.rs:209`
CLAUDE.md: NEVER panic in production code. Return a typed `DeltaError`/`EngineError`.
**LOC:** +3.

### EC4 — Engine doc-comment sweep + `#![warn(missing_docs)]`
~31 undocumented public items in `compile/` and `exec/shape/`. Add one-sentence doc per item. Adopt `#![warn(missing_docs)]` so future violations fail at compile time.
**LOC:** +~50 doc lines.

### EC5+6 — Micro-cleanups (matches!, full paths)
1. `compile/scan.rs:376` — `matches!(node.file_type, FileType::Parquet)` → `node.file_type == FileType::Parquet`.
2. `exec/shape/{row_index, relation_sink, nullability_validation}.rs` — replace repeated `delta_kernel::arrow::datatypes::SchemaRef` with the imported alias.
**LOC:** ~−8.

### EC7 — Fix `compile_plan_logical` anti-pattern — **REGRESSED; HARD BLOCKER**
The `Result<Option<LogicalPlan>>` pattern is now propagated through ~10 sites in the new `compile/logical.rs` (lines 506-517, 555, 567, 573, 711, 716, 780, 783, 786, 789, 804) plus a final `_ => Ok(None)` fall-through at line 907. The "executor falls back to physical" rationale is moot because `executor.rs:301-303` errors out instead of falling back. **This task is now blocking every other compile/* sweep** — adding new helpers atop the `Ok(None)` surface makes them harder to remove.

**Target signature:**
```rust
pub fn compile_root_logical(
    root: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, EngineError>;
```
**Hard rules:** No `Option` in the return type (compilation either succeeds or surfaces a typed error). `EngineError` per EC8. Sink-type whitelist moves to the caller (`executor.rs:compile_plan_for_execution`).

```rust
// BEFORE (compile/mod.rs):
pub fn compile_plan_logical(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Option<LogicalPlan>, DeltaError> {
    if !matches!(plan.sink.sink_type, Results(_) | Relation(_) | ConsumeByKdf(_) | Load(_)) {
        return Ok(None);    // ← anti-pattern: soft signal for "unsupported"
    }
    compile_declarative_node_logical(&plan.root, ctx)
}

// At every internal call site (compile/logical.rs):
let Some(probe_plan) = compile_declarative_node_logical(probe, ctx)? else {
    return Ok(None);    // ← propagated Option<_> taints every caller
};

// In match arms throughout compile/logical.rs:
_ => return Ok(None),   // ← unsupported plan shapes silently bail

// AFTER:
pub fn compile_root_logical(
    root: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, EngineError> {
    // No sink check; the function trusts its inputs.
}

// Caller (executor.rs:compile_plan_for_execution):
let use_logical = matches!(
    plan.sink.sink_type,
    SinkType::Results(_) | SinkType::Relation(_) | SinkType::ConsumeByKdf(_) | SinkType::Load(_)
);
if use_logical {
    let logical = compile_root_logical(&plan.root, ctx)?;
    // ...
} else {
    compile_plan_physical(plan, ctx)?
}

// Internal call sites:
let probe_plan = compile_declarative_node_logical(probe, ctx)?;   // ← just `?`

// Match arms:
_ => return Err(EngineError::internal(format!("unsupported plan shape: {variant}"))),
```

Sweeps **all** related `Ok(None)` / `let-else return None` / `_ => return Ok(None)` patterns in `compile/`. They share one root cause; the cleanup is one sweep, not N point fixes.
**LOC:** ~−25 to −40. The architectural win is bigger: the function signature matches what it does.

### EC8 — Standardize engine internals on `EngineError`; sweep `.map_err`
Engine-internal functions return `Result<T, EngineError>`. Public API entry points translate at the boundary (one `.map_err` per public method, not one per internal call). Add focused helpers (`from_datafusion`, `into_delta`). Most internal-to-internal calls propagate via `?` with no `.map_err` at all.
**LOC:** ~−100 to −150 beyond B1. Plus architectural clarity: error type tells you which layer you're in.

### EC9 — Simplify window-rename projection with `LogicalPlanBuilder::with_column_renamed`
`compile/logical.rs:424-455` builds a 30-LOC `Vec<Expr>` projection to rename DataFusion's auto-generated window output names to user-chosen names. Replace with:
```rust
let mut builder = LogicalPlanBuilder::from(window_plan);
for (from, to) in window_expr_names {
    builder = builder.with_column_renamed(from, to)?;
}
```
Eliminates `input_col_count`, the schema-walk by index, and a `"missing window output"` error case that becomes unreachable. The resulting code reads as "rename each window output" instead of "rebuild the entire projection list and hope the indices match."
**LOC:** ~−25.

### EC10 — Replace `RelationBatchRegistry` with `SessionContext::register_batch` — **PARTIAL + NEW DUAL-WRITE TRAP**
Recent branch work *added* a second registration: `executor.rs:632-661` (`register_relation_batches`) now writes to both `relation_registry` HashMap AND `session_ctx.register_table` as `TableReference::Bare { table: handle.name }`. Only the registry is read (`build_relation_ref_logical` in `sources/relation_ref.rs:62-100`). Dual-write creates an invariant ("register both, never one") that will silently break under EC26's ViewTable lifecycle.

**EC10 now subsumes EC28** (originally proposed): pick one canonical sink (SessionContext catalog), delete the other write, and switch `build_relation_ref_logical` to `ctx.read_table(handle.name())`. The function itself + its MemTable allocation become dead and delete.

**Architectural simplification surfaced by DataFusion-expert audit.** Our custom `HashMap<u64, Vec<Vec<RecordBatch>>>` duplicates what DataFusion already provides.

- Sink drain (B6 dispatch) calls `ctx.register_batch(handle.name(), batch)?`
- `RelationRef` leaf compiles to `ctx.read_table(handle.name())?`
- Delete `exec/sources/relation_ref.rs` entirely and the registry threading

```rust
// BEFORE:
pub struct DataFusionExecutor {
    relation_registry: Arc<RelationBatchRegistry>,  // ← HashMap<u64, Vec<Vec<RecordBatch>>>
    // ...
}

// On RelationSink drain (B6):
self.relation_registry.register(handle.id, batches);

// On RelationRef leaf compile (exec/sources/relation_ref.rs):
let partitions = self.relation_registry.get_cloned(handle.id)?;
MemorySourceConfig::try_new_exec(&[partitions], schema, None)?

// AFTER:
pub struct DataFusionExecutor {
    session_ctx: SessionContext,
    // ↑ holds the table registry natively; no parallel HashMap
}

// On RelationSink drain (B6 + EC10):
let concatenated = concat_batches(&handle.schema_arrow()?, &batches)?;
self.session_ctx.register_batch(&handle.name(), concatenated)?;

// On RelationRef leaf compile:
let table_provider = self.session_ctx.table(&handle.name()).await?;
LogicalPlanBuilder::scan(&handle.name(), provider_as_source(table_provider), None)?.build()?
```

Wins beyond LOC: DataFusion optimizer sees plan-N → plan-N+1 dataflow as a normal `TableScan` and can push filters/projections across plan boundaries. Schema lives at the session level. `EXPLAIN` output becomes legible. Unlocks parallel plan execution more naturally.
**LOC:** ~−100. Coordinate with B6.

### EC11 — Document residual-filter policy for scan predicates
DataFusion-expert audit confirmed the engine's choice to apply `ScanNode` predicates as residual `FilterExec` (rather than parquet pushdown) is correct for kernel NULL semantics. Document why at the predicate-handling site and at `compile/logical.rs:130` so a future contributor doesn't "optimize" by enabling pushdown and silently change NULL behavior.
**LOC:** +10 doc lines.

### EC15 — Replace `logical.rs` manual projection helpers with `LogicalPlanBuilder` native methods — **REGRESSED**
Now four helpers in the new `compile/logical.rs` reinvent builder methods:
- `canonicalize_output_to_kernel_schema` (lines 264-286)
- `append_row_index_column` (414-448)
- `append_constant_i64_column` (450-468)
- `drop_named_column` (487-501)

Each enumerates every input column manually + modifies one. Replace with `with_column_renamed`, `with_column`, `drop_columns`. Same anti-pattern as EC9; same fix.

Also fold in: `compile/logical.rs:680-694` four-arm `matches!` on `(Expression, DataType)` that decides whether to skip an output cast — replace with an `Expression::produces_struct()` predicate (or kernel-side schema check) so new Struct-producing expression variants don't silently miscast.
**LOC:** ~−80.

### EC16 — Simplify `scan_to_listing_logical_plan` ordinal+union dance — **REGRESSED (now three places)**
`compile/logical.rs:346-411` reimplements the per-file ordinal+union dance using EC15's manual helpers. Now in three locations: `compile/logical.rs` (the new file), `compile/logical.rs::scan_to_listing_logical_plan`, and `compile/scan.rs:430-455`. With B9's `Union { ordered: true }` compile pattern + EC15's `LogicalPlanBuilder` adoption, this collapses dramatically.

Plus minor: `row_index_name` clone → as_deref; investigate JSON-only `relax_nested_nullability_for_scan` special case.
**LOC:** ~−100.

### EC17 — Rename `compile/` files and functions to make target type explicit
The engine has two parallel translation paths (kernel IR → DataFusion `LogicalPlan` vs kernel IR → `Arc<dyn ExecutionPlan>`) but both files in `compile/` and both function names start with `compile_*`. The output type is the only signal of which path is which.

Rename to make targets explicit:
- `compile/logical.rs` → `compile/to_logical.rs`
- physical compile logic in `compile/mod.rs` → `compile/to_physical.rs` (keep `mod.rs` as dispatch only)
- `compile_plan_logical` / `compile_declarative_node_logical` → `to_logical_plan` / `to_logical_plan_node`
- `compile_declarative_node` → `to_physical_plan_node`
- Update module doc comments to say what each file produces and which DataFusion stage runs next

**LOC:** 0 (rename only); cognitive-load reduction is the win.

### EC18 — Migrate `compile/window.rs` to `LogicalPlanBuilder::window`
`compile/window.rs` (292 LOC) builds `SortExec` + `CoalescePartitionsExec` + `WindowAggExec` + `ProjectionExec` chain directly with manual `PhysicalSortExpr` / `PhysicalExpr` translation. Migrate to `LogicalPlanBuilder::window` + dispatcher-via-logical-path. The helpers (`translate_partition_by`, `translate_order_by`, `sort_for_window`, `coalesce_for_window`, `build_window_exec`, `cast_window_outputs_to_long`, `kernel_to_arrow_schema`) all delete. Move validations to `WindowNode::new` per B15.
**LOC:** ~−190 (from 292 to ~80).

### EC19 — Clean up `compile/scan.rs` residual smells (after D1 + D2)
Six smaller smells beyond what D1/D2 cover:
1. Merge duplicate row-index Arrow-field injection (`parquet_scan_arrow_schema_and_virtual_columns` + `nullability_target_arrow_schema`)
2. `file_meta_to_partitioned` defensive timestamp fallback — move invariant to FileMeta
3. Empty-files check + same-object-store invariant → `ScanNode::new` validation
4. Build-raw-scan Parquet/JSON arm duplication → helper
5. Per-file ordinal+union dance → unify with EC16 + B9 pattern
6. `compile_residual_scan_filter_native`'s NULL-preserving filter → kernel helper (EC11 documents the policy)

**LOC:** ~−70 on top of D1/D2.

### EC20 — Split `compile/logical.rs` into `compile/to_logical/` subtree
After EC15/EC16/EC17 land, `compile/logical.rs` is still ~400 LOC of a single module covering scan, window, join, project, union, values, relation_ref. Split into one file per IR variant:
```
src/compile/to_logical/
  mod.rs           # entry + dispatch
  scan.rs / window.rs / join.rs / project.rs / union.rs / values.rs / relation_ref.rs
```
**LOC:** 0 (split); locality win.

### EC21 — Split `compile/expr_translator.rs` into `compile/expr/` subtree
After B11 + the in-flight ParseJson/MapToStruct work, `expr_translator.rs` (710 LOC) is still a single module. Split by expression family:
```
src/compile/expr/
  mod.rs           # entry points + dispatch
  scalar.rs / predicate.rs / transform.rs / variadic.rs / if_case.rs / json.rs
```
Coordinate with active ParseJson/MapToStruct work — `json.rs` is the natural home.
**LOC:** 0 (split).

### EC22 — Split `executor.rs` into `executor/` subtree
After B6/B8/B12/B13/B16 + EC26 land, `executor.rs` reshapes around **phase shape**, not sink type. The natural decomposition:
```
src/executor/
  mod.rs              # DataFusionExecutor struct + public entry points
  drive.rs            # consolidated drive_coroutine_sm + ResultsHandler enum (post-B12)
  phase.rs            # PhaseOperation entry — splits Plans into composable group + Load list (EC26)
  compose.rs          # composable-phase compilation: ViewTable registration + single LogicalPlan compile
  dispatch_load.rs    # Load sink dispatch (post-B13)
  dispatch_writes.rs  # Write + PartitionedWrite dispatch
```
Results and Relation sinks always flow through `compose.rs` (they live inside a composable phase by EC26's rule). ConsumeByKdf, Load, Write, PartitionedWrite each get their own dispatch path. Net: discovery win + each file has one architectural responsibility.

**Gated on EC26** — without it, the natural decomposition is per-sink-type, which is the wrong shape.

**LOC:** 0 (split).

### EC23 — Hoist Assert wrapping out of executor + extract CompileContext factory
**Files:** `delta-kernel-datafusion-engine/src/executor.rs`

Two related cleanups:

1. **Move Assert wrapping into the compiler** (lines 204-215). Executor currently inspects `plan.root` for `DeclarativePlanNode::Assert` and stitches in `KernelAssertExec` after logical compile — couples sink dispatch to a specific IR intermediate node type. Includes a doubled `if let Assert { ... } = &plan.root` because the borrow checker forces unbinding between extracting `node` and `child`, plus an `unreachable!()` that EC3 catches. Compiler should emit the Assert-wrapped plan; executor just dispatches by sink type.

2. **Extract CompileContext factory methods** for the 5 sites that construct it with `Arc::clone × 4`:
```rust
fn compile_context(&self) -> CompileContext;
fn compile_context_with_phase(&self, state: &PhaseState) -> CompileContext;
```
Each call site collapses to one line.

**LOC:** ~−40. Coordinate with EC3 (deletes the unreachable!()), EC7 (rewrites compile_plan_for_execution dispatch), EC22 (where these factories land in executor/mod.rs).

### EC24 — Consolidate stream/drain helpers in executor.rs
**Files:** `delta-kernel-datafusion-engine/src/executor.rs:585-637`

Five overlapping helpers form a 2×2 matrix of `(coalesce vs partition-0)` × `(collect vs drain vs stream)`:
- `collect_all_partitions` / `single_results_stream` / `root_partition0_stream` / `drain_plan_stream` / `drain_results_stream`

Replace with one unified helper + idiomatic futures combinators (`try_collect`, `try_for_each`):
```rust
fn execute_plan(&self, physical: Arc<dyn ExecutionPlan>, coalesce: bool)
    -> Result<SendableRecordBatchStream, DeltaError>;
```

Call sites become standard stream consumption.
**LOC:** ~−40.

### EC25 — Fix `unreachable!()` in `binary_pred_to_df`; keep translator validations as-is
**Files:** `delta-kernel-datafusion-engine/src/compile/expr_translator.rs:180`

The expression translator already returns `DeltaResult<Expr>`, so its defensive checks (empty column path, empty Junction preds, IN-requires-literal-array, empty COALESCE args) produce typed errors at compile time. **Leave those in place** — pushing them into kernel IR construction with structural-type changes (NonEmpty paths, first+rest tuples, separate `BinaryPredicate::in_list` variant) is a much bigger blast radius (cascading through builders, callers, tests) for marginal benefit. The translator is fallible; let it report.

Remaining scope is one CLAUDE.md violation:
```rust
let df_op = match op {
    BinaryPredicateOp::Equal => Operator::Eq,
    // ...
    BinaryPredicateOp::In => unreachable!("handled above"),  // ← NEVER panic in prod
};
```
`In` is hoisted above this match (line 169-171), so the arm is structurally dead. Fix: replace with a typed error (`unsupported("In op should be dispatched separately")`). Making it structurally impossible would require splitting `BinaryPredicateOp` and changing kernel IR — out of scope.

Also the `"expr_translator: "` prefix repeated on every error message in this file folds into EC8 (EngineError variants carry their own context; the prefix becomes redundant).

**LOC:** ~−2 plus minor cleanup via EC8. Same `unreachable!()` anti-pattern as EC3.

### EC26 — Composable-phase execution model
**Architectural shift: shrink the materialization boundary from "every plan" to "every Load."**

Today the executor materializes between every `Plan` in `Vec<Plan>` regardless of sink type. EC26 lets plans connected only by `Relation`/`Results` sinks compose into one DataFusion LogicalPlan that the optimizer sees end-to-end; only `Load` sinks force a phase boundary.

**The rule:**
> *Composable phase*: any plan whose sink is `Relation` or `Results` and whose internals are `Scan`/`RelationRef`/`Filter`/`Project`/`Window`/`Join`/`Union` collapses into one DataFusion LogicalPlan within its yield.
> *Load phase*: each `Load` sink runs as its own phase. Its input subtree may itself be composable but the Load terminates that subtree.

**FSR under EC26** (4 phases instead of 1 yield with 5 plans):
```
Phase 1 (Load):       commit_load          - Values -> LoadSink(json), registers FSR_COMMIT_RAW
Phase 2 (Composable): commit_dedup +
                      checkpoint_top +
                      sidecar_path_list*   - one LogicalPlan; registers commit_dedup, checkpoint_top
                                             (*if has_sidecars: also a path list)
Phase 3 (Load*):      sidecar_load         - LoadSink(parquet), registers FSR_SIDECAR_ACTIONS
Phase 4 (Composable): results              - one LogicalPlan: Union + LeftAntiJoin + retention + project
```

**Concrete wins:**

1. **Phase 4 results** becomes one LogicalPlan containing `Union(commit_dedup, LeftAntiJoin(checkpoint_full, commit_keys)) -> Filter(retention) -> Project(action_output_schema)`. DataFusion can push retention predicates through the Union into per-source scans, drop `FSR_JOIN_KEY_COL` after the antijoin, reorder the antijoin by cost, and fuse projections.

2. **Phase 2 checkpoint scan dedup**: `checkpoint_top` and `sidecar_path_list` both scan `checkpoint_files` with different projections. Composition turns this into one Scan with two consumers via `ViewTable`; the optimizer picks one-scan-two-projections or two-scans-narrow-columns by cost.

3. **Logical-level optimizer for relation refs**: combined with EC10, relation refs become `TableScan` over `MemTable`/`ViewTable`, manipulable by the LogicalPlan optimizer (projection/predicate pushdown into the materialized relation).

**Cross-phase relations** flow through `SessionContext` via EC10's `register_batch`. **Intra-phase sibling relations** register as `ViewTable` providers *before* any sibling in the yield is compiled — the compiler's `RelationRef(handle)` lowering calls `ctx.read_table(handle.name())` and the ViewTable inlines the producer's LogicalPlan.

**Implementation outline:**

*Kernel side* (`build_fsr_plans` in `fsr/full_state.rs`): yields multiple `phase.execute(PhaseOperation::Plans(...), step_name)` calls instead of one bundled vector. KC7's `FsrContext` becomes the natural home for per-phase generators (`load_commits_phase`, `compose_checkpoint_phase`, `load_sidecars_phase`, `compose_results_phase`).

*Engine side* (`executor.rs`): per-yield:
1. Inspect plans; group by sink type. Load plans run individually; Relation/Results batch into one composable group.
2. Composable group: register each `Relation`-sink plan's root LogicalPlan as a `ViewTable` in SessionContext under `handle.name()` *before* compiling. Compile each plan into a LogicalPlan; execute. The `Results` plan terminates the drain.
3. Each `Load` plan: compile input subtree (RelationRefs to already-registered handles -> `read_table`), execute, run the LoadSink dispatch (B13).
4. After each phase: materialized relations `register_batch`; ViewTable registrations torn down at end-of-yield to avoid catalog pollution.

*Compiler side* (`compile/logical.rs`): `RelationRef(handle)` already lowers to `ctx.read_table(handle.name())` after EC10. No semantic change — ViewTables (sibling) and MemTables (cross-yield) look uniform through `read_table`.

**Resolved questions:**
- *Re-scan vs. materialize-once for `checkpoint_top`*: Phase 2 materializes checkpoint_top before Phase 3 runs; Phase 3's sidecar_load input compiles to `read_table("checkpoint_top") -> Filter -> Project`.
- *`passthrough_columns` on LoadSink*: stays imperative inside the Load dispatch.
- *ViewTable lifetime*: scoped per-yield (register at compose start, deregister at compose end).

**Prerequisites:** EC10 (cross-phase relation primitive), B6 (RelationSink as executor dispatch), B13 (KernelLoadSinkExec as executor dispatch), EC12 (FileListingExec as TableProvider so `Scan` composes naturally).

**LOC:** ~+200 (compose logic, ViewTable registration) / ~−150 (per-plan Exec wrapping deleted with B6/B13). Net ~+50. Win is architectural — the optimizer sees the actual DAG within each composable phase. Scan SM benefits minimally (1-2 plans per yield, dominated by cross-yield deps); FSR is the primary consumer.

**Risk:** ViewTable scoped register/deregister semantics need verification in current DataFusion. Executor's "split yield into Load phases + composable phase" grouping is a new abstraction that needs careful testing — particularly that intra-yield relation lifetimes don't leak across yields.

---

### EC12 — Convert `FileListingExec` to a `TableProvider` (gated on EC10)
Consistency follow-on. Split the current ~283-LOC monolithic Exec into a `FileListingTableProvider` (schema lives once, ~80 LOC) + a slim scan-Exec returned by `provider.scan(projection, ...)` (~170 LOC). Compile `FileListingNode` to `ctx.register_table(name, provider)?` + `LogicalPlanBuilder::scan(name, ...)`. Stays lazy.

After EC10 + EC12, every row-producing leaf in the engine uses the same `ctx.register_* + ctx.read_table()` pattern.
**LOC:** ~−30. The win is architectural alignment.

### EC27 — File upstream DataFusion bugs + delete optimizer-rule + CSE workarounds
**Largest new debt surfaced by branch work.** ~220 LOC + 3 disabled rules across two files compensate for two distinct DataFusion miscompiles.

**`executor.rs:165-184`** disables three optimizer rules: `enable_leaf_expression_pushdown`, `optimize_projections`, `optimize_unions`. Inline TODO references "apache/datafusion#20432" (not filed).

**`compile/logical.rs:39-262`** contains 220 LOC of structural workaround:
- `collect_top_level_column_roots`
- `RewriteRootColumn`
- `ColumnPathCounter`
- `RewriteHoistedPath`
- `hoist_repeated_column_paths`

Plus `compile/logical.rs:595-651` adds a `colliding_inputs` rename layer to "insulate input names from output names." All neutralize the CSE duplicate-`__common_expr_N` bug + leaf-projection-pushdown bug.

**Plan:**
1. File upstream issues against `apache/datafusion` with minimal reproducers from FSR scan-replay.
2. Track upstream fixes (issue/PR numbers in this task).
3. Delete the workarounds + re-enable rules in lockstep with each upstream fix landing in a pinned DataFusion version.

Until upstream lands, these stay — but every PR that touches `compile/logical.rs` must understand they're load-bearing.
**LOC after upstream:** ~−220 + 3 re-enabled rules.

### EC28 — Move nullability validation into `NullabilityValidationExec`
`executor.rs:87-118` (`nested_non_null_validations_from_field` + `nullability_validations_for_target_schema`) introspects Arrow nested types in the executor. This belongs inside `NullabilityValidationExec::from_target_schema(...)` so the executor doesn't depend on Arrow's nested type system. After D6 (kernel `arrow_ext::resolve_field_path`) lands, consider whether kernel should own the path-walking instead.

Coordinate with EC29 (Arrow realignment), D6 (kernel helper), and the existing `NullabilityValidationExec` API.
**LOC:** ~−40.

### EC29 — Consolidate Arrow type-realignment recursion in `KernelLoadSinkExec`
`exec/shape/load_sink.rs:670-765` adds ~100 LOC of recursive `realign_array_to_field` / `realign_struct_array` / `realign_list_array` / `realign_large_list_array` / `realign_map_array` rebuilding Arrow arrays bottom-up to match a target field's nullability/metadata. This logic overlaps with what `NullabilityValidationExec` does and with D6's pending `arrow_ext::resolve_field_path`. Three copies of nested-Arrow walking is the wrong direction.

**Plan:** consolidate into either (a) a kernel `arrow_ext::realign_array_to_field` helper (extends D6) or (b) `NullabilityValidationExec` if scope is purely engine-side. Coordinate with B13's executor-dispatch move — once the LoadSink's "realign for nullability" happens at dispatch time, the realignment may collapse to a single call site.
**LOC:** ~−60 after consolidation.

### EC30 — Single ParseJson/MapToStruct lowering site
Same JSON-extraction lowering exists in three places after branch work: `compile/mod.rs:411-462` (`translate_projection_expr`), `compile/expr_translator.rs:73-85` (`Expression::ParseJson` arm), and `compile/json_parse.rs:111-122`. Fold to one entry point called by both `translate_projection_expr` and `kernel_expr_to_df`.

Coordinate with B11 (the broader `expr_translator` simplification) and EC21 (split into `compile/expr/` subtree — `json.rs` is the natural home).
**LOC:** ~−40.

---

## Phase KC — kernel `plans/` code quality

### KC1 — Hoist long test JSON to module constants
`state_machines/fsr/full_state.rs:6174-6327` has test JSON strings 105–229 chars repeated across 8+ tests, violating CLAUDE.md 100-char line width. Hoist to module-top `const TEST_LOG_<NAME>: &str = r#"..."#;`.
**LOC:** ~−20.

### KC2 — Fix `delta_codes!` macro line width
`errors/mod.rs:44` has a 129-char macro definition. Break to multi-line.
**LOC:** +5.

### KC3 — Inline single-use helpers
`dv_unique_id_expr()` (called once from line 550 in `fsr/full_state.rs`) and `OrderingSpec::asc/desc` (2-3 trivial call sites). Inline or document the intent.
**LOC:** ~−18.

### KC4 — Sweep `.map_err` to `.or_delta` in `fsr/full_state.rs`
`state_machines/fsr/full_state.rs:139-145` has three sites using `.map_err(|e| delta_error!(...))` where the existing `.or_delta()` trait suffices. Sweep similar patterns in other plans/ files.
**LOC:** ~−10.

### KC5 — Kernel `plans/` doc-comment sweep + `#![warn(missing_docs)]`
Four confirmed gaps on public items (`OrderingSpec::asc/desc`, `ScanNode::new`, `SchemaQueryNode::new`). Sweep for more; adopt `#![warn(missing_docs)]`.
**LOC:** +13 doc lines.

### KC6 — Split `fsr/full_state.rs` — extract Scan SMs and checkpoint-shape discovery
**Files:** `kernel/src/plans/state_machines/fsr/full_state.rs` (2203 LOC: 1500 src + 700 test)

The file mixes three orthogonal concerns into one module:

1. **FSR planning** (`build_fsr_plans` + 4 `build_*_plan` builders + FSR helpers + `retention_filter` + `fsr_dedup_key`) — belongs in `fsr/full_state.rs`.
2. **Scan metadata/data replay SMs** (lines 192-396: `impl Scan` with `scan_metadata_state_machine`, `replay_scan_data_plans`, `replay_scan_plans`, the 127-LOC `replay_scan_state_machine` coroutine + helpers `replay_partition_columns`, `scan_metadata_plans_with_shape`, `build_sidecar_discovery_plan`, and scan-specific schema/projection helpers like `scan_live_actions_*`, `scan_data_*`, `scan_actions_with_parsed_projection`, `action_schema_with_augmented_add`, `scan_partition_values_*`). These are *downstream consumers* of `build_fsr_plans`; they belong in `kernel/src/plans/state_machines/scan/{mod.rs,replay.rs}`.
3. **Checkpoint shape discovery** (`checkpoint_shape_from_*`, `first_checkpoint_url`, `snapshot_has_checkpoint_files`, `checkpoint_format_from_path`, `checkpoint_actions_schema_projection`) — used by both FSR and scan. Belongs in `state_machines/checkpoint_shape.rs` as a peer of `fsr/` and `scan/`.

`ScanBuilder::build_replay` (1-liner at lines 175-180) moves into the scan-SM module.

After split, `fsr/full_state.rs` is ~800 LOC of pure FSR planning. The reader sees `build_fsr_plans` near the top with the 4 plan builders below it — today the load-bearing structure is buried under 500 LOC of unrelated Scan SM code.

**LOC:** ~0 net (pure reorganization), major readability win. Prerequisite for KC7/KC8.

### KC7 — Introduce `FsrContext` to make the 4-stage FSR pipeline visible — **MAIN GOAL NOT DONE**
Branch work added a user-facing `FullState` / `FullStateBuilder` facade at `full_state.rs:138-184` (entry point for `Snapshot::full_state_builder`). That's different work — it doesn't touch the internals of `build_fsr_plans`. The 80-line interleaved setup KC7 targeted is still there at `:672-751` and in fact *grew* (new `checkpoint_top_handle`, new `build_checkpoint_top_load_plan`). Original KC7 scope unchanged; the facade work is complementary.

Also fold in:
- `FullStateBuilder::with_stats` (lines 160-184) is a documented no-op "API-level compatibility shim" — resolve by implementing, removing, or unifying with `ScanBuilder::with_stats`.
**Files:** `kernel/src/plans/state_machines/fsr/full_state.rs:593-673` (post-KC6)

`build_fsr_plans` is 80 LOC of interleaved setup (resource extraction, 3 schema computations, 3 RelationHandle allocations, 2 retention timestamps) before the actual 5-plan-push sequence. The 4-stage shape of FSR — *which is the most important structural fact* — is the least prominent thing in the function.

Hoist setup into an `FsrContext` struct; each stage becomes a method:
```rust
pub fn build_fsr_plans(snapshot: &Snapshot, shape: CheckpointShape) -> Result<Vec<Plan>, DeltaError> {
    let ctx = FsrContext::derive(snapshot, shape)?;
    let mut plans = vec![ctx.commit_load()?, ctx.commit_dedup()?];
    if ctx.has_checkpoint() { plans.push(ctx.checkpoint_top()?); }
    if ctx.has_sidecars()   { plans.push(ctx.sidecar_load()?); }
    plans.push(ctx.results()?);
    Ok(plans)
}
```
The top-level function now lists all stages declaratively. `FsrContext` holds the handles + schemas + retention values it derived once; the per-stage methods read fields from `self` instead of taking 4-6 arguments each. Eliminates handle-recomputation and the long argument lists on every `build_*_plan` free function.

**LOC:** ~−50. Depends on KC6.

### KC8 — Decompose `replay_scan_state_machine` via `Scan::resolve_checkpoint_shape` — **PARTIAL**
Branch work extracted `resolve_checkpoint_shape_for_scan` as a *free function* at `full_state.rs:380-471`; the SM at `:285-323` now collapses to ~30 LOC of orchestration. **Remaining work:** rename to a method on `Scan`, and fold the 7 `.map_err(|e| delta_error!(...))` wrappers still present (5 inside `resolve_checkpoint_shape_for_scan`, 2 in `replay_scan_state_machine`) per KC4.
**Files:** `kernel/src/plans/state_machines/fsr/full_state.rs:270-396` (moves to `state_machines/scan/replay.rs` via KC6)

The coroutine is 127 LOC with four conditional layers nested in one function (JSON vs Parquet checkpoint → has_sidecars or not → first-sidecar exists). Six `.map_err(|e| delta_error!(...))` wrappers repeat the same 6-line pattern.

Extract `Scan::resolve_checkpoint_shape(&self, phase: &mut Phase) -> Result<CheckpointShape, DeltaError>` that owns the SchemaQuery + sidecar-discovery + first-sidecar-schema dance. The coroutine collapses to 5 sequential lines:
```rust
let shape = scan.resolve_checkpoint_shape(&mut phase).await?;
let (metadata, live_actions) = scan_metadata_plans_with_shape(&scan, shape)?;
phase.execute(PhaseOperation::Plans(metadata), "scan.replay.metadata").await?;
let data = scan.replay_scan_data_plans(live_actions)?;
phase.execute(PhaseOperation::Plans(data), "scan.replay.data").await?;
```
Intrinsic complexity (shape discovery) lives where it belongs; orchestration becomes obviously sequential.

**LOC:** ~−30. Coordinate with KC4 (`.or_delta` sweep eliminates per-call wrappers).

### KC10 — Move scan-with-row-index schema derivation onto `ScanNode`
**File:** `delta-kernel-datafusion-engine/src/compile/mod.rs:216-232`. `node_output_schema` for `Scan` reconstructs the effective output schema (with row-index column when present) inline. This derivation lives in three places today (here, the old `compile/load_sink.rs`, and `executor.rs`'s target-schema construction). Move to a kernel method `ScanNode::effective_output_schema()` that is the single source of truth. Engine call sites become `scan_node.effective_output_schema()`.
**LOC:** ~−30 engine; ~+15 kernel. Net: clearer ownership, fewer places to drift.

### KC11 — Resolve `RelationId` vs `RelationHandle` alias
**File:** `kernel/src/plans/ir/nodes/sinks.rs:107-111`. The `RelationId = RelationHandle` alias was added as a migration shim "to smooth migration from id-only wiring to explicit `(name, schema)` contracts." No migration path is documented. Either complete the split (callers using `RelationId` should be using a real ID type) or remove the alias and standardize on `RelationHandle`.
**LOC:** ~−15.

### KC12 — Inline `DvKind` while it has one variant
**File:** `kernel/src/plans/ir/nodes/sinks.rs:159-211`. `DvKind` has a single variant `Descriptor`. YAGNI typing infrastructure with no second case. Inline or replace with a `#[non_exhaustive]` marker enum until a real second encoding shows up.
**LOC:** ~−20.

### KC13 — Replace `panic!` in `scan_data_projection` with typed error
**File:** `kernel/src/plans/state_machines/fsr/full_state.rs:1080`. `scan_data_projection` calls `panic!("scan_data_projection: missing physical field for logical field `{}`", field.name())` on a missing physical field. CLAUDE.md: NEVER panic in production code. Return a `DeltaError`. The function signature needs to return `Result<_>`, which propagates one level up to `replay_scan_data_plans`.
**LOC:** +5 (panic→typed error).

### KC14 — Move `ScanBuilder::build_replay` next to `ScanBuilder`
**File:** `kernel/src/plans/state_machines/fsr/full_state.rs:185-189`. `impl ScanBuilder { fn build_replay }` is a one-liner adapter declared in the FSR module. Reach-around — fold into `scan/mod.rs` (or its KC6 successor module). Sibling of KC6.
**LOC:** 0 (move).

### KC15 — Collapse `action_read_schema` vs `action_output_schema`
**File:** `kernel/src/plans/state_machines/fsr/full_state.rs:826-857`. The two schema constructors differ only in whether nested fields are made all-nullable. 30 LOC of parallel field declarations that drift independently. Collapse to one builder parameterized by `relaxed_nesting: bool`. Fold into F1 (LazyLock canonical schemas).
**LOC:** ~−25.

### KC9 — Compact `fsr_dedup_key` 5-arm CASE table — **PARTIAL**
Branch work landed semantic edits: meta/domain arms simplified to single-column keys, literal lower-cased to `"metadata"`, `domainMetadata.configuration` dropped from the key. **Remaining:** structural compaction (the 5 arms × `null_str.clone()` slot-padding shape is unchanged at `:1154-1242`).
**Files:** `kernel/src/plans/state_machines/fsr/full_state.rs:1075-1156`

80 LOC manually building `Expression::array(vec![kind_literal, id1, id2, id3])` for each of 5 arms (`file`/`protocol`/`metaData`/`domainMetadata`/`txn`), with most slots filled by `null_str.clone()`. Replace with a small data table + helper:
```rust
fn dedup_key_arm(kind: &str, ids: [Option<Expression>; 3]) -> Expression { ... }
// uniform arms driven by a [(predicate, kind, [id; 3])] table; file arm handled separately
```
**LOC:** ~−40. Coordinate with F2 (`col`/`lit` free fns tighten the table further).

---

## Phase F — fluent plan construction

Architecture-first refactor of kernel/IR plan-construction code (primarily `state_machines/fsr/full_state.rs`). Roughly LOC-neutral but big qualitative win: build-time validation, cached schemas, Transform-driven projections, fluent expression API. Future state machines (snapshot construction, etc.) get clean construction infrastructure for free.

### F1 — `LazyLock` canonical schemas + `const` field-name arrays
Move repeated schema constructors (`action_read_schema()`, `action_output_schema()`, `path_size_*_schema()`, `sidecar_only_schema()`, `join_key_only_schema()`) to LazyLock-cached statics in `kernel/src/plans/schemas.rs`. Constructed once at first use, shared via Arc.
**LOC:** ~−10 net, plus runtime savings (no per-call rebuilding).

### F2 — `ExpressionExt` + `PredicateExt` extension traits + free fns
New module `kernel/src/plans/ir/expr_ext.rs` (~90 LOC) — extension traits over existing `Expression`/`Predicate` types (no wrapper types). One-line delegations to kernel's existing constructors.

Free functions: `col`, `lit`, `null_of`, `any_of`, `all_of`.
Methods on `Expression`: `is_null`, `is_not_null`, `eq`/`ne`/`gt`/`ge`/`lt`/`le`, `distinct_from`, `in_list`, `plus`/`minus`/`times`/`div`, `or_lit`, `or_else`, `cast`, `to_json`.
Methods on `Predicate`: `and`, `or`, `not`.

**Sketch of the module:**
```rust
// kernel/src/plans/ir/expr_ext.rs
pub trait IntoColumnPath { fn into_column(self) -> ColumnName; }
impl IntoColumnPath for &str { /* single column */ }
impl IntoColumnPath for (&str, &str) { /* 2-level path */ }
impl<const N: usize> IntoColumnPath for [&str; N] { /* N-level path */ }

pub fn col(path: impl IntoColumnPath) -> Expression {
    Expression::Column(path.into_column())
}
pub fn lit(s: impl Into<Scalar>) -> Expression { Expression::Literal(s.into()) }
pub fn null_of(dt: DataType) -> Expression { Expression::literal(Scalar::Null(dt)) }
pub fn any_of(preds: impl IntoIterator<Item = Predicate>) -> Predicate { Predicate::or_from(preds) }
pub fn all_of(preds: impl IntoIterator<Item = Predicate>) -> Predicate { Predicate::and_from(preds) }

pub trait ExpressionExt: Sized {
    fn is_null(self) -> Predicate;
    fn is_not_null(self) -> Predicate;
    fn eq(self, rhs: impl Into<Expression>) -> Predicate;
    fn gt(self, rhs: impl Into<Expression>) -> Predicate;
    fn or_lit(self, default: impl Into<Scalar>) -> Expression;
    // ... etc
}
impl ExpressionExt for Expression {
    fn is_null(self) -> Predicate { Predicate::is_null(self) }
    fn eq(self, rhs: impl Into<Expression>) -> Predicate { Predicate::eq(self, rhs.into()) }
    fn or_lit(self, default: impl Into<Scalar>) -> Expression {
        Expression::coalesce([self, Expression::literal(default.into())])
    }
    // ... one-line delegations
}
```

**Call-site comparison:**
```rust
// BEFORE:
let add_path_present = Predicate::is_not_null(Expression::column(["add", "path"]));
let dv_ok = Predicate::or(
    Predicate::is_null(Expression::column(["remove"])),
    Predicate::gt(
        Expression::coalesce([Expression::column(["remove", "deletionTimestamp"]),
                              Expression::literal(Scalar::Long(0))]),
        Expression::literal(Scalar::Long(min_ts)),
    ),
);

// AFTER (with `use delta_kernel::plans::ir::expr_ext::*;`):
let add_path_present = col(("add", "path")).is_not_null();
let dv_ok = col("remove").is_null()
    .or(col(("remove", "deletionTimestamp")).or_lit(0i64).gt(lit(min_ts)));
```

**LOC:** ~+90 helper module, enables ~−200 LOC of call-site savings.

### F4 — `SchemaBuildExt` trait — fluent schema augmentation
Extension trait on `SchemaRef`: `with_field`, `with_nullable`, `with_not_null`, `extend_from`, `restrict_to`. Result-returning only when input is actually invalid.
**LOC:** ~+60 helper, ~−70 at call sites.

### F5 — Kernel `Transform` signature loosening + engine `expr_translator` Transform support
**Kernel:** loosen `Transform::with_replaced_field` / `with_inserted_field` to accept `impl Into<ExpressionRef>` (additive, backwards-compatible).

```rust
// kernel/src/expressions/mod.rs — change from:
pub fn with_replaced_field(mut self, name: impl Into<String>, expr: ExpressionRef) -> Self { ... }
// to:
pub fn with_replaced_field(mut self, name: impl Into<String>, expr: impl Into<ExpressionRef>) -> Self {
    let field_transform = self.field_transform(name);
    field_transform.exprs.push(expr.into());
    field_transform.is_replace = true;
    self
}
```

Combined with `From<Expression> for ExpressionRef` (which is `Arc::new`), call sites pass bare `Expression` / `col(...)` values directly:
```rust
Transform::new_nested(["add"])
    .with_inserted_field(Some("stats"), parse_json(col(("add", "stats")), schema))
    // ^^^ no Arc::new wrapping
```

**Engine:** compile `Expression::Transform` in `delta-kernel-datafusion-engine/src/compile/expr_translator.rs` — currently returns `Unsupported` at lines 66-68. Implement by walking `Transform`'s `field_transforms` map and `prepended_fields`, emitting a DataFusion projection that mirrors kernel's semantics in `kernel/src/engine/arrow_expression/mod.rs:352`.

Prerequisite for F9.
**LOC:** ~+5 kernel, ~+50 engine.

### F6 — `DeclarativePlanNode` builders return `DeltaResult<Self>` + validate column refs eagerly
Every builder method that accepts an expression returns `DeltaResult<Self>` and walks the expression tree validating column refs against the upstream schema at construction time. Failure happens *at the line where the bad reference is introduced*, not at runtime far away.

Validator walks `Expression` tree (Column, Binary, Struct, Coalesce, If, Variadic, Unary, Transform, MapToStruct, ParseJson). Catches typos, stale refs, wrong struct paths, schema drift.

```rust
// Signature change:
impl DeclarativePlanNode {
    // BEFORE:
    pub fn filter(self, p: Arc<Expression>) -> Self { ... }
    // AFTER:
    pub fn filter(self, p: impl Into<Arc<Expression>>) -> DeltaResult<Self> {
        let p = p.into();
        validate_column_refs(&p, self.output_schema())?;
        Ok(Self::filter_unchecked(self, p))
    }
}

// Validator behavior:
fn validate_column_refs(expr: &Expression, input: &SchemaRef) -> DeltaResult<()>;
// Walks Expression tree. For each Column(["add", "path"]):
//   1. "add" must exist as top-level field in input
//   2. "add"'s DataType::Struct must contain "path"
//   3. Errors include the failing path + available fields at that level
```

**Use-site example:**
```rust
// BEFORE: bad column ref fails at runtime far from this line
let plan = relation_ref(handle)
    .filter(Arc::new(Predicate::is_not_null(Expression::column(["typo_column"])).into()));

// AFTER: fails at construction with error pointing here
let plan = relation_ref(handle)
    .filter(col("typo_column").is_not_null())?;
//                                            ^ error caught here:
//   "column path 'typo_column' references non-existent field;
//    available: [add, remove, protocol, metaData, ...]"
```

Builder additions: `project_transform(Transform)` infers output schema from Transform structure. `project(exprs, schema)` kept for the rare general case. **No schema-inferring "select N columns" variants** — that's the optimizer's job.
**LOC:** ~+80 validator.

### F7 — Convenience combinators
`union_with`, `left_anti_join_on`, `inner_join_on`, `window_row_number` — all validating internally.
**LOC:** ~+50.

### F8 — `plan::` constructor module + LoadSink/RelationSink output-schema validation
`plan::scan`, `plan::values`, `plan::relation_ref` free functions (take `&RelationHandle`, not owned). Plus sink validation: LoadSink/RelationSink output schema must equal the root project's output schema (no extras, no missing) — codifies "materialized relations are exactly what downstream reads."
**LOC:** ~+80.

### F9 — Sweep `full_state.rs` — Transform-based projections + fluent predicates + cached schemas

**Example rewrite — `retention_filter` (28 LOC → 13):**
```rust
// BEFORE:
fn retention_filter(min_file_retention_timestamp: i64, txn_expiration_cutoff: Option<i64>) -> Predicate {
    let removal_ts = Expression::coalesce([
        Expression::column(["remove", "deletionTimestamp"]),
        Expression::literal(Scalar::Long(0)),
    ]);
    let remove_ok = Predicate::or(
        Predicate::is_null(Expression::column(["remove"])),
        Predicate::gt(removal_ts, Expression::literal(Scalar::Long(min_file_retention_timestamp))),
    );
    let txn_ok = match txn_expiration_cutoff {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            Predicate::is_null(Expression::column(["txn"])),
            Predicate::is_null(Expression::column(["txn", "lastUpdated"])),
            Predicate::gt(Expression::column(["txn", "lastUpdated"]),
                          Expression::literal(Scalar::Long(cutoff))),
        ]),
    };
    Predicate::and(remove_ok, txn_ok)
}

// AFTER:
fn retention_filter(min_file_ts: i64, txn_expiry: Option<i64>) -> Predicate {
    let remove_ok = col("remove").is_null()
        .or(col(("remove", "deletionTimestamp")).or_lit(0i64).gt(lit(min_file_ts)));
    let txn_ok = match txn_expiry {
        None => Predicate::literal(true),
        Some(cutoff) => any_of([
            col("txn").is_null(),
            col(("txn", "lastUpdated")).is_null(),
            col(("txn", "lastUpdated")).gt(lit(cutoff)),
        ]),
    };
    remove_ok.and(txn_ok)
}
```

**Example rewrite — `scan_actions_with_parsed_projection` (35 LOC → 11) using `Transform`:**
```rust
// AFTER (Transform-based; passthrough fields implicit):
fn scan_actions_with_parsed_projection(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Transform {
    let mut add = Transform::new_nested(["add"]);
    if let Some(schema) = stats_parsed_schema {
        add = add.with_inserted_field(Some("stats"),
            parse_json(col(("add", "stats")), schema.clone()));
    }
    if partition_values_parsed_schema.is_some() {
        add = add.with_inserted_field(Some("partitionValues"),
            map_to_struct(col(("add", "partitionValues"))));
    }
    Transform::new_top_level().with_replaced_field("add", add)
}
```
The 11 unchanged `add` fields and 5 untouched top-level fields are implicit — Transform doesn't enumerate them.

Per-function savings:

| Function | Before | After | Δ |
|---|---:|---:|---:|
| `retention_filter` | 28 | 13 | −15 |
| `fsr_dedup_key` | 82 | ~45 | −37 |
| `scan_actions_with_parsed_projection` | 35 | ~12 | −23 |
| `build_results_plan` | 85 | ~35 | −50 |
| `build_commit_dedup_plan` | 50 | ~20 | −30 |
| `build_commit_load_plan` | 35 | ~20 | −15 |
| `build_sidecar_load_plan` | 42 | ~22 | −20 |
| `scan_data_projection` | 30 | ~18 | −12 |
| `scan_live_actions_projection` | 17 | ~10 | −7 |
| `load_materialized_schema` + 5 schema constructors | ~90 | ~10 | −80 |
| Misc Arc::new / into_delta_default noise | varies | | ~−40 |

Net `full_state.rs`: ~880 LOC → ~540 LOC (~−340).

### F10 — Sweep `Scan::scan_metadata_state_machine` + `Scan::replay_scan_data_plans`
Apply F1–F8 to remaining plan-construction call sites.
**LOC:** ~−40.

---

## Phase T — tests

### T1 — Extract parquet I/O helpers to `test_utils`
`write_i64_parquet`, `read_parquet_batches`, `kernel_schema_one_i64`, `file_meta` duplicated across 2-4 engine test files. Unblocks T2-T5.
**LOC:** ~−35.

### T2 — rstest `window_row_number.rs` (5 → 1)
**LOC:** ~−80.

### T3 — rstest `parquet_field_id_parity.rs` (3 → 1)
Coordinate with D4 — the "mixed" case may need adjustment after name fallback drops.
**LOC:** ~−50.

### T4 — rstest `cross_engine_parity.rs` (9 → ~3)
Lowest assertion density in the suite (avg < 1 per test); strong duplication signal.
**LOC:** ~−60.

### T5 — rstest `load_sink_and_schema_query.rs` (5 → 2)
Uses T1's helpers.
**LOC:** ~−60.

### T6 — Audit `kernel/src/plans/*` inline tests
Sweep the 11 modules the agent didn't sample. Largest: `ir/declarative.rs` (462 LOC of tests; T15 covers), `state_machines/fsr/full_state.rs` (443 LOC of tests; T16 covers), `errors/mod.rs` (135 LOC; T17 covers).
**LOC:** variable.

### T7 — Delete plan-shape assertion tests in `scan_correctness.rs`
Two tests walk the compiled physical plan for specific Exec names (`OrderedUnionExec`, `FilterExec`) — implementation-detail assertions that break on every refactor. Sibling parity tests cover the behavior.
**LOC:** ~−95.

**General rule (adopted):** delete tests that grep the physical plan tree for Exec names.

### T8 — Trim coroutine driver inline tests (after C1)
After `genawaiter2` swap, `coroutine/driver.rs` becomes a thin wrapper with little internal mechanics to unit-test. Keep 1-2 contract tests; delete the rest.
**LOC:** ~−100.

### T9 — Audit `engine_error.rs` inline tests (144 LOC)
For a typed error, 144 LOC of tests is heavy. Trim trivial Display / `thiserror` / FFI round-trip tests.
**LOC:** ~−50 to −80.

### T10 — Delete redundant engine test files
- `df_insert_sm.rs` (39 LOC) — duplicates other write-sink coverage; tests A4-deleted adapter
- `phase_operation_sm.rs` (166 LOC) — duplicates `SumRowsConsumer` with `relation_and_kdf_sinks.rs`; framework internals exposed to C1
- `join_literals.rs` (142 LOC) — covered by `cross_engine_parity.rs:126-174`
- `acceptance_workload_reader_smoke.rs` (42 LOC) — pure JSON deserialization

**LOC:** ~−389.

### T11 — Merge `snapshot_full_state_fsr.rs` into `fsr_real.rs`
Duplicates fixture + path-extraction; only adds a "non-empty batches" smoke that `fsr_real.rs` covers more rigorously.
**LOC:** ~−102.

### T12 — Shrink `write_sink.rs` + `partitioned_write_sink.rs`
Keep 1 parquet + 1 JSON round-trip per file. Coordinate with D5 — shrink `partitioned_write_sink.rs` after D5 lands.
**LOC:** ~−264.

### T13 — Shrink `sm_kernel_reference_parity.rs` vs `cross_engine_parity`
Significant overlap with cross_engine_parity's kernel-vs-DF pattern. Merge non-overlapping cases into `cross_engine_parity`; delete the rest.
**LOC:** ~−120.

### T14 — Extract test helpers (`SumRowsConsumer`, schema builders) to `test_utils`
~80 LOC of duplicated helpers across 2-5 engine test files. Coordinates with T1.
**LOC:** ~−80.

### T15 — rstest `ir/declarative.rs` builder tests (29 → 8)
Heavy clustering of builder-API tests. Consolidate via rstest with `#[case]` per variant. Keep FSR/KDF tests that aren't parameterizable.
**LOC:** ~−180.

### T16 — Delete plan-walker tests in `fsr/full_state.rs`
~200 LOC of plan-shape assertions with hand-rolled walkers (`plan_sink_kind`, `plan_reads_relation_named`, `results_plan_carries_anti_join`, `subtree_references`). Same anti-pattern as T7. Merge to one test asserting the sink terminal type sequence; delete the walker helpers.
**LOC:** ~−200.

### T17 — rstest `errors/mod.rs` template tests
Three `render_template` tests share identical flow. One `#[rstest]`. Keep macro tests, `or_delta` chain test, upstream catalog parity test.
**LOC:** ~−40.

### T18 — Gitignore plan-dump debug artifacts
`fsr_phase_plan_dump.txt`, `plan_dump.txt` — test-generated debug dumps. Add to `.gitignore`.
**LOC:** 0 in repo; prevents accidental commits.

### T19 — Consolidate `full_state.rs` inline tests (rstest + shared helpers)
Beyond what T16 deletes (~200 LOC of plan-walker tests + helpers), the remaining test block has more bloat:

1. **Extract `setup_full_scan(table)` helper** — 4 tests share the same `load_test_table → ScanBuilder → scan_metadata → replay_scan_data` setup. Saves ~15 LOC.
2. **Rstest the scan tests** (4 → 1 with `#[case]` per assertion class). Saves ~50 LOC.
3. **Extract `eval_on_lines(lines, schema, expr, dt)` helper** — replaces 7-line "parse JSON → ArrowEngineData → batch → evaluate" boilerplate. Used by 4 tests. Saves ~35 LOC.
4. **Rstest `fsr_dedup_key_eval_various_action_types`** — internal closure already half-way to rstest. Promote to real `#[rstest]`. Saves ~10 LOC.
5. **Rstest `scan_data_projection_*` (3 → 1)** with `#[case]` per scenario. Saves ~60 LOC.
6. **Add `assert_column_at(projection, idx, path)` helper** — replaces verbose `matches!(... Expression::Column(c) if c == ...)` pattern. Saves ~15 LOC.
7. **Delete `fsr_dedup_key_debug_string_is_non_trivial`** — 3-LOC test of `Debug` trait, not behavior.

After T16 + T19, `full_state.rs` test block: ~650 LOC → ~275 LOC (~−58%).
**LOC:** ~−175 beyond T16.

**General principle**: rstest is the default consolidation pattern. Any cluster of 2+ tests sharing setup/flow → one `#[rstest]` with `#[case]` per variant.

### T20 — rstest `expr_translator.rs` binary-predicate + primitive-literal tests
Modest consolidation. Best clusters:
- Merge `translates_predicate_eq_lt_gt` + `translates_distinct_to_is_distinct_from` (4 binary-op cases → 1 rstest)
- rstest `translates_primitive_literals` (4 inline asserts → 1)
- Possibly merge `coalesce_single_arg_unwraps` + `translates_coalesce_to_nested_case_chain` (arity cases)

About 11 of the 22 test functions are deliberately NOT consolidated (structurally distinct `Expr` outputs that don't parameterize cleanly).
**LOC:** ~−60 to −80.

### T21 — Move `tests/` files to inline unit tests where they exercise internals
Seven files in `delta-kernel-datafusion-engine/tests/` (~1,575 LOC) test internal `compile/`/`exec/` helpers via the executor. Move each to inline `#[cfg(test)] mod tests` in the source it exercises:

| tests/ file | LOC | Move to |
|---|---:|---|
| `scan_correctness.rs` | 303 | `src/compile/scan.rs` |
| `window_row_number.rs` | 233 | `src/compile/window.rs` |
| `parquet_field_id_parity.rs` | 220 | `src/compile/scan.rs` |
| `write_sink.rs` | 123 | `src/compile/write_sink.rs` or `src/executor.rs` (after B16) |
| `partitioned_write_sink.rs` | 241 | `src/exec/shape/partitioned_write.rs` |
| `load_sink_and_schema_query.rs` | 362 | `src/executor.rs` (after B13) |
| `relation_and_kdf_sinks.rs` | 93 | `src/executor.rs` (after B6/B8) |

Stays in `tests/` (true integration): `fsr_real.rs`, `sm_kernel_reference_parity.rs`, `cross_engine_parity.rs`, `checkpoint_v2_classic_df.rs`.

After T10/T11/T21: `tests/` goes from 16 files / 3,766 LOC → 4 files / ~1,700 LOC.
**LOC:** 0 (move). Compile-time + locality wins.

### T22 — Arrow-poking test helpers (`assert_batches_eq`, typed accessors, tmp dest)
Engine tests are bloated with manual `.column(i).as_primitive::<T>().value(0)` chains, parquet reader-builder boilerplate, repeated tempdir+URL setup. Add to `test-utils`:
- `tmp_parquet_dest()` / `tmp_json_dest()` — TempDir + URL + path
- `read_parquet(path)` / `read_json(path)` — collect batches
- `assert_batches_eq(actual, expected_lines)` — pretty-format comparison (DataFusion-internal style)
- Typed `assert_int64_at(batch, row, col, expected)` and column-equality helpers for sanity checks

Sweep all engine integration tests to use them.
**LOC:** ~−250 to −300 across engine tests.

### T23 — Delete speculative `ReadMetadataDatafusionRunner`
**File:** `benchmarks/src/runners.rs` (~+272 LOC delta). New `ReadMetadataDatafusionRunner` was added in recent branch work; its `execute()` calls `collect_fsr_file_metas` and `black_box`es the returned files without measuring anything meaningful (no data read, just FSR file enumeration). Speculative benchmark scaffolding orthogonal to cleanup goals — delete until a real workload needs to measure FSR file enumeration latency in isolation.
**LOC:** ~−150 of the +272 delta.

### T24 — Replace string-matching error suppression in `collect_fsr_file_metas`
**File:** `benchmarks/src/runners.rs` (within `collect_fsr_file_metas`). Two sites match `error.detail.contains("coroutine completed during start without yielding any work")` and return `Ok(Vec::new())`. Fragile workaround covering a real bug in the empty-fileset coroutine path. Fix the underlying coroutine behavior (probably belongs in `framework/coroutine/driver.rs` — empty-yield should be a structured `Ok` outcome, not an error).
**LOC:** ~−15 in benchmarks; +small kernel fix.

### T25 — Restore acceptance-test failure-bucket diagnostics
**File:** `acceptance/tests/acceptance_workloads_datafusion.rs`. Recent branch work deleted the `classify_expected_df_failure` allow-list (~60 LOC) and replaced it with an implicit "skip if kernel also fails" gate (`if ... .is_err() { return Ok(()); }`). Coverage of *DataFusion-only* failures is preserved, but enumerated divergence buckets are lost — when DataFusion silently starts failing on something kernel handles fine, the test now passes vacuously. Restore an explicit allow-list (or per-test annotation) so failing-bucket coverage stays diagnosable.
**LOC:** +50 (restoring the allow-list).

### T26 — Delete `fsr_plans_compile_on_logical_path` plan-shape assertion
**File:** `delta-kernel-datafusion-engine/tests/fsr_real.rs`. New test asserts which `SinkType` variants must take the logical path — same anti-pattern that T7/T16 calls out (testing implementation shape, not behavior). Coverage already exists in compile-direction parity tests. Delete.
**LOC:** ~−30.

---

## Suggested PR ordering

Independent phases. A few orderings minimize merge conflicts.

1. **Wave 1 — A1 + A4 + A5 bundled** (~3,674 LOC pure deletion, no logic review).
2. **Wave 2 — A2 (fork + delete vendor dir)** (~−15,600 LOC; org coordination for fork repo).
3. **Wave 3 — EC8 + B1** (engine internals on `EngineError` + error-helper sweep). EC8 lands first or together; cleans every subsequent engine PR's diff.
4. **Wave 4 — T1 + T14** (test helpers to `test_utils`). Unblocks T2-T5.
5. **Wave 5 — Phase EC structural** — EC3, EC5+6, EC7, EC9, EC25 (`unreachable!()` fixes + style cleanups touching `compile/`).
6. **Wave 6 — B12** (executor drive + `execute_phase_operation*` consolidation). Lands before sinks-cluster Wave so the consolidation surface is single-copy when B6/B8/B13 modify dispatch.
7. **Wave 7 — B10 + B5 + B6 + B8 + B13 + EC10 + EC12** (sinks/sources cluster: row-producing leaves migrate to `SessionContext` registry + table providers; all three custom sink Execs move to executor dispatch).
8. **Wave 8 — EC22 + EC23 + EC24** (executor.rs split + Assert hoist + stream/drain consolidation, post-sinks-cluster).
9. **Wave 9 — EC26** (composable-phase execution model). Gated on EC10/B6/B13/EC12; reshapes the executor's per-phase logic.
10. **Wave 10 — C1 (`genawaiter2` swap)** + `Clone` strip + EC1 + EC2 (framework + Exec boilerplate helpers, after sinks are gone).
11. **Wave 11 — D3 + D6** (small kernel additions).
12. **Wave 12 — D1 + D5** (read/write partition pair, ideally same PR).
13. **Wave 13 — D4 (fork commit)** + bump rev pin.
14. **Wave 14 — KC6** (split `fsr/full_state.rs`). Prerequisite for KC7/KC8/KC9 + F9/F10.
15. **Wave 15 — KC7 + KC8 + KC9 + F-series + remaining T-items** (kernel `plans/` cleanup pass: FsrContext, scan-SM decomposition, fluent IR construction, FSR builder sweep).
16. **Wave 16+ — Remaining cleanup** (B2, B3c, B9, B11, KC1-KC5, EC4, EC11).

---

## Per-module LOC projection

| Module | Before | After (core + fork) | Δ |
|---|---:|---:|---:|
| `vendor/datafusion-datasource-parquet/` | 15,606 | 0 | −15,606 |
| Workspace `Cargo.toml` patch entry | ~3 | ~6 | +3 |
| `kernel/src/plans/errors/` | ~3,830 | ~165 | −3,665 |
| `kernel/src/plans/state_machines/framework/` | ~1,800 | ~640 | −1,160 |
| `kernel/src/plans/state_machines/df/` | ~108 | 0 | −108 |
| `kernel/src/plans/state_machines/operation_type` | ~30 | ~10 | −20 |
| `kernel/src/plans/ir/` (incl. tests) | ~2,000 | ~1,830 | ~−170 |
| `kernel/src/plans/state_machines/fsr/` (incl. tests) | ~2,400 | ~2,200 | ~−200 |
| `kernel/src/plans/kdf/` | ~1,200 | ~1,200 | 0 |
| `delta-kernel-datafusion-engine/src/compile/` | 3,000 | ~2,790 (or ~2,340 with A3) | −210 to −660 |
| `delta-kernel-datafusion-engine/src/exec/` | 2,877 | ~1,320 | −1,557 |
| `delta-kernel-datafusion-engine/src/executor.rs` | 601 | ~535 | ~−66 |
| `delta-kernel-datafusion-engine/src/error.rs` | 93 | ~130 | +37 |
| `delta-kernel-datafusion-engine/tests/` | 3,766 | ~2,070 | −1,696 |
| Kernel inline tests in `plans/*` | ~1,200 | ~700 | −500 |
| Kernel additions (D3, D6) | — | +55 | +55 |
| **Total branch diff vs main** | **+38,876** | **~+14,500** | **~−24,400** |

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| C1 breaks an FSR edge case that depended on coroutine ordering | Run full `acceptance/tests/acceptance_workloads_datafusion.rs` + targeted SM tests. The 1:1 protocol maps to `genawaiter2::sync::Gen::resume_with` exactly. |
| Fork repo ownership long-term | Use org-owned fork (delta-io/datafusion). Personal forks are fragile. |
| Fork rebase against upstream churn | Pin to commit SHA; rebase explicitly on DataFusion version bumps. |
| EC8 (`EngineError` everywhere) breaks compilation across many files | Type-checker-guided sweep. Land EC8 + B1 together; the compile errors guide the migration. |
| EC10 schema mismatch on registered tables | `RelationHandle.schema()` is the authoritative schema at registration time; verify it matches what `RelationRef` lookups expect. |
| B6/B8 break sink dispatch in executor | Per-call-site migration; `cross_engine_parity.rs` + FSR acceptance tests as regression net. |
| D3/D5/D6 kernel API additions get stuck in review | Each is a separate PR with a focused proposal; gate behind `internal-api` feature if needed. |
| B11's `ScalarValue::try_from` produces different output than the hand-written primitive match | Verify per `PrimitiveType` — `Timestamp` tz "UTC" preservation and `Decimal128` precision/scale are the highest-risk cases. |
| Plan-walker tests (T7, T16) delete legitimate coverage | Paired deletions: sibling parity tests already cover the behavior. Confirm before each delete. |
| EC26 ViewTable register/deregister semantics differ across DataFusion versions | Verify scoped register/deregister works in pinned version before depending on it; fall back to per-yield `SessionContext` fresh-clone if needed. |
| EC26 intra-yield relation lifetimes leak across yields | Strict per-yield "scratch catalog" pattern: register at compose start, deregister at compose end. Test by running back-to-back yields and asserting catalog is clean between them. |
| KC6 split breaks downstream import paths | Pure reorganization — `pub use` re-exports at `fsr/mod.rs` + `state_machines/mod.rs` preserve external paths during transition; remove re-exports once callers update. |

---

## Deferred / closed / parked items

| Item | Status | Reason |
|---|---|---|
| **A3** — delete `compile/logical.rs` | Closed (audited, not viable) | logical.rs and mod.rs are intentionally separate paths covering different operators (logical: Hash joins, row_number windows, 4 sink types; physical: the rest). The 12 `Ok(None)` sites are real feature gaps, not cleanup targets. EC7 converts them to typed errors with contextual reasons but logical.rs stays. |
| **A2a, A2b** | Superseded | Folded into A2 (fork) |
| **A6** — delete forward-declared sinks | Closed | Sinks are live code |
| **B3a, B3b** | Folded | B3a absorbed by B10; B3b reframed into D5 |
| **B3d** — `flatten().cloned()` | Rolls into B5 | Same file |
| **B4** — shrink `compile/join.rs` | Closed | No meat after B1's error sweep lands |
| **B7** — `FileListingExec` → `MemorySourceConfig` (eager) | Closed | Replaced by EC12 (lazy `TableProvider`) |
| **C2** — move FSR plan-builders out of `state_machines/` | Closed | FSR genuinely is a state machine |
| **C3** — fold `EngineError` into `DeltaErrorCode` | Closed | Engine/kernel error layering is intentional architecture (the EC8 fix is the *other* direction: standardize engine on `EngineError`) |
| **D2** — kernel scan/validation schema pair | Parked | Real semantic gap between Delta NOT NULL and Arrow nullability; defense-in-depth value of `NullabilityValidationExec` unclear |
| **E1** — vendor fully gone | Subsumed | A2 (fork) delivers this directly |
| **Phase F applied to `kernel/src/plans/kdf/*`** | Closed (audited) | kdf/* (~1,000 LOC) audited against FSR patterns; already uses F1 LazyLock caching, never builds raw `Expression::column` chains (uses `RowVisitor`/`GetData`), no manual schema construction. KDF is the model the FSR cleanup aspires to. |
| **EC28-original** — Pick one materialization sink for `Relation` handles | Folded into EC10 | EC10 description updated to subsume the dual-write cleanup (delete registry write, switch RelationRef to `ctx.read_table`). EC28 slot reused for the nullability-validation hoist. |
| **`kernel/src/schema/diff.rs` +390-line delta** | Not a smell | Pure rustfmt reflow of nested test-data builders. Run `cargo +nightly fmt --workspace` and commit separately; don't create a cleanup task. |
| **Smaller smells folded into existing tasks** | Folded | `Snapshot::scan_replay_builder` asymmetry → KC6; `ensure_commit_raw_schema_for_dedup` runtime check → F6 (validation-at-construction); duplicated cast-skipping 4-arm `matches!` → EC15; Arrow nested-realignment overlap → D6 + EC29. |

---

## Principles adopted

These crystallized through the planning conversations and reviews. Apply to new code in PR review.

### Architecture

1. **Engine adapts, kernel owns Delta.** Anything in `delta-kernel-datafusion-engine/src/` that reasons about Delta semantics (partition layout, deletion vectors parsing, hive paths, field-ID semantics, NOT NULL enforcement) needs to move to or behind a kernel API. Engine speaks DataFusion + kernel public types only.

2. **Reaching into Arrow's nested type system is a smell.** Manual recursive walkers over Arrow `DataType` (e.g., `relax_nested_nullability_for_scan`, `cast_array_to_field`, hypothetical `widen_union_type`) signal a leaky kernel↔engine schema contract. Flag any new such walker at PR review.

3. **Sinks live above the plan tree.** Plans compile to "produce a stream." Executor handles materialization, KDF state harvest, etc. based on `SinkType`. Sinks are not `ExecutionPlan`s.

4. **Engine internals use `EngineError`; translation to `DeltaError` happens at the public boundary, exactly once per entry point.** The error type tells you which layer you're in.

5. **Row-producing leaves register as DataFusion tables.** `RelationRef` → `ctx.read_table(handle.name())`. `FileListing` → `ctx.register_table(name, FileListingTableProvider)`. Plan-to-plan dataflow becomes visible to the optimizer.

6. **No `Ok(None)` "not supported, fall back" patterns.** Functions either compile their input or fail with a typed error. Dispatch on supported shapes happens at the caller, not via soft return-value signals.

7. **Per-tuple file routing lives inside the writer, not upstream.** Hash partitioning at the plan level (`RepartitionExec(Hash(partition_cols, N))` — declared via `ExecutionPlan::required_input_distribution`, not manually wrapped) ensures rows with the same partition-tuple land in the same task. Multiple distinct tuples can collide into the same task; that's correctness-safe because the writer's internal `HashMap<PartitionTuple, PartitionWriter>` (fanout pattern, delta-rs/iceberg-rust convention) routes each tuple to its own file under its own directory. Hash collisions cost parallelism, not correctness.

8. **Declare distribution requirements; don't manually wrap with `RepartitionExec`.** Custom Execs that need partitioned input override `required_input_distribution`. DataFusion's `EnforceDistribution` optimizer rule auto-inserts the redistribution upstream only when needed (free elimination when input already satisfies the requirement). Same pattern `HashJoinExec` uses. Manual wrapping fights the optimizer.

9. **Validation lives at the data structure's construction site — not in compilers that read it.** If `DeclarativePlanNode::join(...)` accepts invalid join configurations, every compiler downstream has to re-validate them. Put the check in the constructor (return `DeltaResult<Self>`), let compilers trust the IR. Compiler code is for *translation*, not validation. Same applies to schema invariants, expression shape constraints, sink configuration.

10. **Compilers translate; they don't lower-level.** If a kernel IR node has a DataFusion-blessed lowering path (`LogicalPlanBuilder::join_inner`, `with_column_renamed`, etc.), use it — let the optimizer + physical planner choose the physical operator. Reaching directly for `HashJoinExec::try_new` / `SortExec::new` skips optimization and hardcodes choices (like `PartitionMode::CollectLeft`) that should be data-driven.

11. **Tests assert behavior, not Arrow internals.** `assert_batches_eq` over `pretty_format_batches` output reads as the actual table the test expects — it doubles as documentation. Manual `.column(i).as_primitive::<T>().value(0)` chains hide intent in array-descent ceremony. Reserve typed single-value accessors (`assert_int64_at`, etc.) for sanity checks; everything else uses the table-comparison form. DataFusion's own tests use this pattern.

12. **Tests live next to the code they exercise — `tests/` is reserved for things that test the public API.** If a test exercises `pub(crate)` helpers via the public API just to reach them, it's a unit test wearing an integration-test disguise. Move it inline. `tests/` becomes a short, principled list of "user-facing behavior" cases.

### Library use over reinvention

7. **Use DataFusion's `LogicalPlanBuilder` helpers before writing manual `Vec<Expr>` projections.** `with_column_renamed`, `with_column`, `aggregate`, `filter`, `sort`, `join_*` — check what's available before reaching for `.project(vec![...])`.

8. **Take the dep when it replaces a hand-rolled equivalent.** `genawaiter2` for coroutines. `datafusion_functions::coalesce` for COALESCE. `percent-encoding` for URL escaping. Tight-dep policy is for transitive supply-chain risk, not for refusing maintained one-purpose crates.

9. **Vendor crates die fast.** When patches don't go upstream quickly, host on a git fork via `[patch.crates-io] { git, rev }`. Do not maintain large in-repo vendor copies.

### Test hygiene

10. **Use `rstest` for parameterized tests.** N near-identical tests that differ only in inputs/expected outputs collapse to one `#[rstest]` with N `#[case]`s.

11. **Delete tests that grep the physical plan tree for Exec names.** They encode implementation details as test requirements and break on every refactor. Semantic coverage usually exists in sibling parity tests.

12. **Reuse helpers from `test_utils`.** Per-file scaffolding duplicated across 3+ test files is a sign of missing utility — promote it.

### Style

13. **CLAUDE.md is the authoritative style reference.** 100-char line width, `==` over `matches!` for single-variant, `not_null`/`nullable` over `StructField::new(_, _, bool)`, no emoji/unicode in comments, never panic in production code, doc comments on all public items.

14. **`#![warn(missing_docs)]` on both crates** to catch new violations at compile time.

15. **No full-path overuse** — `delta_kernel::plans::errors::DeltaError` written out in every function signature is noise. Import once.
