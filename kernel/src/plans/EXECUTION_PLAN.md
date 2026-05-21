# Plans Architecture — Shipping Plan

End-to-end delivery of the new architecture described in `RENAME_PROPOSAL.md`:
SSA IR + `Context`-as-builder + slim state-machine framework + DataFusion
executor driving the new `Step` variants. The shipping unit is **a migrated
pipeline running end-to-end on the new infrastructure**.

Renames are not milestones in this plan. They happen incidentally as code is
touched — new files use new names; deleted files take their old names with them.

## Strategy

1. **Foundations first** (Milestones 0–1): build new types + Context/Cursor
   alongside the existing code. Nothing ships to users; nothing existing breaks.
2. **Migrate pipelines** (Milestones 2–3): each migrated SM is a shipping
   milestone. FSR first (smaller, cleaner test surface); Scan second.
3. **Engine adaptation** rides each pipeline migration — the DataFusion executor
   learns to drive the new `Step::Consume` variant as the first SM that needs it
   lands.
4. **Cleanup last** (Milestone 4): once both pipelines run on the new code
   path, delete the old IR + RelationRegistry + sinks + the residual
   `Step::Plans` variant.

Each milestone exits with:
- `cargo build --workspace --all-features` clean
- `cargo nextest run --workspace --all-features` green
- `cargo clippy --workspace --benches --tests --all-features -- -D warnings` clean
- `cargo +nightly fmt` clean
- Documented exit criteria (below) satisfied

---

## Milestone 0: Foundations

**Lands:** core types + canonical type inference, alongside the existing IR.
Nothing existing changes.

### Work

- `Expression::infer_type(&Schema) -> Result<DataType>` and
  `Expression::is_nullable(&Schema) -> bool`. Single canonical walk covering
  every `Expression` variant.
- New IR types in `kernel/src/plans/ir/` (next to existing `declarative.rs`):
  - `Plan { stmts: Vec<Stmt> }`
  - `Stmt { node, inputs: Vec<Ref>, output: Ref }`
  - `Ref(u32)`
  - `Node` enum (sources + transforms; **no** `MaterializeRelation`, **no**
    `RelationRef`; `Window` excluded — `MaxByVersion` subsumes the only use)
  - `JoinKind { Inner, LeftAnti }`
  - `ResultPlan { plan: Plan, result: Ref }`
- Add `Step::Consume { stmts: Vec<Stmt>, drain: (Ref, Box<dyn KernelConsumer>) }`
  variant to the existing `PhaseOperation` enum. Existing `Plans(Vec<Plan>)`
  variant stays for now.

### Exit criteria

- Unit tests for every `Node` variant constructor + every Expression type
  inference case (column, literal, binary ops, casts, function calls, struct
  construction, transforms).
- Round-trip Debug tests for `Plan` / `Stmt` / `Node`.
- All existing tests still pass (additive change only).

---

## Milestone 1: Context + Cursor

**Lands:** the new builder surface. Still no pipeline migrated; old code
unaffected.

### Work

- `Context` with `Rc<RefCell<ContextState>>`, session_id, dispatch handle to
  `PhaseCo`.
- `Cursor` (Clone) holding shared `Rc` + ref_id + session_id.
- Source methods on Context: `scan_parquet`, `scan_json`, `values`, `list_files`.
- Cursor monadic transforms: `filter`, `project`, `project_with_schema`,
  `select`, `select_columns`, `load`, `max_by_version`, `inner_join`,
  `left_anti_join`, `union_all`, `union_ordered`.
- Schema-edit helpers: `append_col`, `prepend_col`, `insert_col_after`,
  `insert_col_before`, `replace_col`, `rename_col`, `drop_col`, `drop_cols`.
- Dispatch methods: `consume(cursor, consumer)`, `schema_query(path)`,
  `into_result_plan(cursor)`.
- Dead-statement elimination (backward reachability) inside `consume` and
  `into_result_plan`.

### Exit criteria

- Unit tests per cursor method, including schema validation failures (column
  not found, type mismatch, predicate non-Boolean, join key absent).
- Tests for session_id bump invalidating stale cursors.
- Tests for DCE: build a plan with unreachable Stmts, verify only the reachable
  set survives the emitted `Step::Consume` / `ResultPlan`.
- Borrow tests: cursor methods don't hold `RefCell` guards across await;
  dispatch methods correctly release the guard before yielding to `co`.
- Microbenchmark: building a representative reconciliation-shaped SSA on the
  new Context vs the old PlanBuilder. Performance must be within 2x of the
  existing builder; document any regression with rationale.

---

## Milestone 2: FullState (FSR) ships on the new architecture

**Lands:** FSR runs end-to-end on the new IR + Context + engine path. This is
the first user-facing milestone.

### Work

Kernel side:
- Rewrite `resolve_checkpoint_shape` to use `ctx.schema_query(...)` and
  `ctx.consume(cursor, SidecarCollector::new(...))` (no more `consume_phase`
  closure).
- Rewrite `build_reconciliation` to take `&Context` and return `Cursor`. Drop
  the `Pair` abstraction; `action_pair.rs` shrinks to schema-only helpers
  (`SCAN_BASE_SCHEMA`, `FSR_BASE_SCHEMA`, `with_stats`, `with_partition_values`,
  `with_sidecar_field`).
- Rewrite `FullState::operation` (currently `state_machine`) to use ctx
  directly and end in `ctx.into_result_plan(terminal)`.
- The intra-reconciliation pipeline collapses to one SSA plan: no
  `into_relation` calls, no named intermediate relations, no plan accumulator.

DataFusion engine:
- Handle `Step::Consume { stmts, drain }`: compile the SSA, execute the
  subgraph feeding `drain.0`, feed batches to the consumer, return its typed
  output through `StepResult`.
- Handle the new lazy `ResultPlan { plan, result }` return: compile the SSA,
  register the relation at `result` as a lazy stream.
- Keep `Step::Plans(Vec<Plan>)` handling for Scan (still on old path).
- Existing `Step::SchemaQuery` handler unchanged.

### Exit criteria

- All FSR tests pass on the new code path. (Include the integration suite under
  `kernel/tests/` and acceptance tests under `acceptance/`.)
- Parity tests: compare output of new FSR vs old FSR for a representative set
  of tables (with/without checkpoints, with/without V2 multipart, with/without
  stats, with/without partition values). Outputs must be semantically identical
  (same actions, same retention behavior, same DV state).
- Performance: new FSR runs within 10% of old FSR on benchmark tables. Document
  any regression with cause + plan to address (or accept).
- `delta-kernel-datafusion-engine` integration tests pass against new FSR.

### Notes

- After this milestone, FSR has zero references to `DeclarativePlanNode`,
  `RelationRegistry`, `RelationHandle`, `SinkType`, `LoadSink`, `Pair`, or
  `consume_phase`. Scan still uses them.

---

## Milestone 3: Scan (with data phase) ships on the new architecture

**Lands:** Scan runs end-to-end on the new infrastructure.

### Work

Kernel side:
- Rewrite `Scan::operation` (currently `scan_state_machine`) to use the
  new flow.
- Reuse `resolve_checkpoint_shape` and `build_reconciliation` already migrated
  in Milestone 2.
- Scan data phase: filter to add-only rows, `load(LoadSpec { ... })` with
  nested `file_meta.path = col(["add", "path"])` etc. (no `LIVE_ACTIONS`
  intermediate).
- Final physical→logical projection via `scan_data_projection(&state_info)?` →
  `(Vec<ExpressionRef>, SchemaRef)` → `raw_data.project_with_schema(...)`.

DataFusion engine:
- Already handles new `Step::Consume` from Milestone 2. May need minor work
  on nested-path `file_meta` in Load's TableProvider if not already supported.

### Exit criteria

- All scan tests pass (including data-phase tests, column-mapping tests, DV
  tests, partition tests).
- Parity tests against old Scan output (same rows, same order where Scan
  promises one, same column mapping).
- Performance: within 10% of old Scan. Document regressions.

---

## Milestone 4: Cleanup

**Lands:** old infrastructure deleted; final mechanical renames.

### Work

Kernel side, delete:
- `plans/ir/declarative.rs` (`DeclarativePlanNode`, old `PlanBuilder`)
- `plans/ir/nodes/sinks.rs` (`SinkType`, `LoadSink`, `ConsumeSink`)
- `plans/ir/relation_registry.rs`
- `RelationHandle` (old shape with UUID id)
- `Step::Plans(Vec<Plan>)` variant
- `Coroutine::consume_phase` (replaced by `ctx.consume`)
- `Pair` type alias + helpers (already mostly gone after M2; sweep what remains)

Engine side, delete:
- Name registry (`RelationProviders` in `DataFusionExecutor`)
- `Step::Plans` dispatch path
- `Relation`/`Consume` sink dispatch helpers (replaced by `Step::Consume`)

Renames (mechanical sweep, done at end so they don't churn migration PRs):
- `plans/kdf/` → `plans/kernel_consumers/`
- `ConsumerKdf` → `KernelConsumer`
- `KdfOutput` → `KernelConsumerOutput`
- `KdfStateToken` → `KernelConsumerToken`
- `ConsumerKdfId` → `KernelConsumerKind`
- `PhaseOperation` enum name → `Step`
- `PhaseState` → `StepResult`
- `AdvanceResult` → `NextStep`
- `phase_name` → `step_name`
- `get_operation` → `get_step`
- `advance` → `submit`
- `CoroutineSM` → `Coroutine`
- `record_schemas.rs` → `record_types.rs`

### Exit criteria

- No code references to deleted types.
- Clippy clean on all features (default, `default-engine-rustls`,
  `default-engine-native-tls`, `internal-api`, `arrow-XX`).
- Workspace `cargo doc` clean.
- All tests pass.

---

## Risk areas

1. **`Expression::infer_type` coverage** (M0): missing variants surface as
   runtime errors deep in builder calls. Exhaustive tests + parity with
   existing evaluator type-resolution behavior.
2. **`RefCell` guard across `.await`** (M1): structural bug class. Code review
   + dedicated test invoking dispatch methods under tokio with concurrent
   cursor activity.
3. **Parity regressions** (M2, M3): SSA migration changes the shape of work
   the engine sees; the engine may execute it slightly differently. Parity
   tests on representative tables catch this.
4. **Performance regressions** (M2, M3): more bookkeeping per cursor call
   (RefCell, session_id, DCE). Microbenchmarks at M1; FSR/Scan benchmarks at
   M2/M3.
5. **Engine-side TableProvider for nested `file_meta` paths** (M3): the data
   phase Load now uses `col(["add", "path"])` instead of a flat `path` column.
   Verify the DataFusion executor's file scanner resolves nested-path file
   metadata correctly.

## Out of scope

- **CDF (Change Data Feed)**: pinned for a follow-up. The architecture
  supports it (validated in design), but no implementation work in this plan.
- **Write paths**: `Transaction`-driven writes are untouched. The new IR
  could host them but that's not in this migration.
- **DataFusion-side optimization beyond Step adaptation**: e.g., new join
  algorithms, push-down improvements. Only what's needed to drive the new
  `Step::Consume` + lazy `ResultPlan`.

## Sequencing notes

- M0 + M1 can ship as 2–4 PRs each. They're additive.
- M2 is a single coherent PR (FSR + engine support together). Splitting it
  would require keeping engine-side dispatch on two code paths, increasing
  surface area.
- M3 likewise a single PR (Scan + any remaining engine work).
- M4 may split into 2–3 PRs by domain (kernel deletes, engine deletes,
  renames). Renames happen last so they don't churn migration PRs.

A reasonable wall-clock estimate: M0–M1 ~ 1 week, M2 ~ 1–2 weeks (parity is
the long pole), M3 ~ 1 week (most infra already in place from M2), M4 ~ 2–3
days. Total ~3–5 weeks for a single engineer.
