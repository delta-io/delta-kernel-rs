# Plans Subsystem Rename & Restructure Proposal

Synthesis of the in-tree `plans/` subsystem and scovich's `declarative-plan-baby-steps`
exploration. Goal: tighten the language, collapse the sink/plan hierarchy into a single
SSA algebra, and replace "state machine" terminology with something that matches what the
code actually is.

## Motivation

- ~~**"State machine"** is wrong: there are no states/transitions. The trait describes a
  multi-step procedure driven by message-passing.~~
  Considered renaming `StateMachine` → `Operation`/`Procedure`/`Routine`/etc. Decision:
  **keep `StateMachine`**. Familiar to current contributors; the rename's accuracy gain
  doesn't outweigh the migration cost. The method renames (`get_step`, `submit`) and
  associated vocabulary (`Step`, `StepResult`, `NextStep`) still apply — they're tied
  to the step-centric framing, independent of the trait name.
- **"Sink"** is overloaded: `Relation` is an output, `Load` is a transform, `Consume` is
  a true drain. Three radically different semantics under one umbrella.
- **"KDF" / "Kernel-Defined Function"** is misleading: these are stateful batch observers,
  not functions.
- **`Plan` as a recursive tree** fights itself when cross-plan sharing via named relations
  is heavy (current scan/FSR reconciliation). SSA shape matches the actual dataflow better.

## Mental model

Two layers:

- **IR (`Plan`)** — SSA dataflow DAG that describes pure compute.
- **State machine** — kernel-authored multi-step procedure that issues `Step`s to the
  engine and consumes results, eventually returning a typed `Output`. Implemented via
  the `StateMachine` trait.

Engine boundary is **message-passing**: `get_step → run → submit → continue or done`.

Side effects (materialize relations, drain into consumers) are declared on the **Step
envelope**, not in the SSA. The SSA itself is pure compute.

## IR

```rust
pub struct Plan {
    pub stmts: Vec<Stmt>,
}

pub struct Stmt {
    pub node: Node,
    pub inputs: Vec<Ref>,
    pub output: Ref,              // always Ref — every stmt produces a value
}

pub struct Ref(u32);               // SSA reference; plan-scoped; sequentially minted

pub enum Node {
    // Sources
    ListFiles   { start_from: Url },
    ReadJson    { files: Vec<FileMeta>, schema: SchemaRef },
    ReadParquet { files: Vec<FileMeta>, schema: SchemaRef },
    Values      { schema: SchemaRef, rows: Vec<Vec<Scalar>> },

    // Transforms (pure compute, schema inferred from inputs+params)
    Project { named_exprs: Vec<(String, ExpressionRef)> },
    Filter  { predicate: PredicateRef },
    Union   { ordered: bool },
    Load    { file_schema: SchemaRef,
              file_type: FileType,
              base_url: Option<Url>,
              passthrough_columns: Vec<ColumnName>,
              file_meta: ScanFileColumns,
              dv_ref: Option<DvRef> },
    MaxByVersion { group_by: Vec<ExpressionRef>,
                   version_column: ExpressionRef,
                   value_columns: Vec<String> },
    EquiJoin { kind: JoinKind,
               key_pairs: Vec<(ExpressionRef, ExpressionRef)> },
}

pub enum JoinKind { Inner, LeftAnti }
```

### Schema storage on Node variants

- **Stores schema** (caller declares because Node alone can't determine it):
  `ReadJson`, `ReadParquet`, `Values`, `Load` (`file_schema`).
- **Schema inferred by builder** from inputs + params:
  `Project` (from `named_exprs` + input types), `Filter` (input pass-through),
  `Union` (from inputs), `MaxByVersion` (group_by exprs + value column types from input),
  `EquiJoin` (kind + input schemas), `ListFiles` (canonical constant).

### Sinks dissolved; no cross-Step relations

`SinkType` is gone. Of the three former sink semantics:

- **`Load` sink** → `Load { ... }` Node (a regular unary transform from upstream metadata
  rows to file row stream).
- **`Consume` sink** → its own Step variant: `Step::Consume { stmts, drain: (Ref, KernelConsumer) }`.
  A Consume Step's sole output is the drained consumer's typed result, returned via
  `StepResult`.
- **`Relation` sink** → gone. No materialize concept exists. Cross-Step data flows via
  consumer outputs the SM body holds as locals (small data) or by re-reading the source
  in subsequent Steps (commit logs etc. are cheap to re-read).

SSA is **truly pure compute**. Every Stmt has a Ref output; no Node side-effects the
engine's state.

### State machine outputs

There are two ways a state machine surfaces work to the engine:

- **`Step`s yielded mid-execution**: eager work — validation, schema discovery,
  state accumulation. Each is a `Step::Consume` (drain into a `KernelConsumer`) or a
  `Step::SchemaQuery`. The consumer's output flows back through `StepResult` and the
  SM body uses it for the next decision.
- **`ResultPlan` returned at the end**: lazy work — the SSA program for the user's
  result, plus the Ref in that program that is the result. The driver streams it after
  the state machine completes.

## Context owns plan-construction state

There is no separate `PlanBuilder` type. The Context owns everything:

- The PhaseCo (yield channel)
- The stmt accumulator + per-Ref schema table (the in-flight SSA being built)
- A session counter that invalidates cursors after each dispatch

Cursors hold a shared `Rc` to the same state — not a borrow. That decouples cursor
lifetimes from `&mut Context` dispatch, so `ctx.consume(cursor, ...)` works without
borrow conflicts.

```rust
pub struct Context<'a> {
    co: &'a mut PhaseCo,               // outside the Rc; only dispatch touches it
    state: Rc<RefCell<ContextState>>,  // shared with cursors
}

struct ContextState {
    stmts: Vec<Stmt>,
    ref_schemas: Vec<SchemaRef>,        // indexed by Ref.0
    session_id: u32,                    // bumps on each dispatch; invalidates old cursors
}

#[derive(Clone)]
pub struct Cursor {
    state: Rc<RefCell<ContextState>>,
    ref_id: Ref,
    session_id: u32,                    // captured at mint time
}
```

`Cursor` is `Clone` (not `Copy`) because it owns an `Rc`. Branching needs explicit
`.clone()`. Stale cursors (used after a dispatch invalidated their session) are caught
at runtime by the session-id check.

### Source methods on Context

```rust
impl Context<'_> {
    pub fn scan_parquet(&self, files: Vec<FileMeta>, schema: SchemaRef) -> Result<Cursor>;
    pub fn scan_json   (&self, files: Vec<FileMeta>, schema: SchemaRef) -> Result<Cursor>;
    pub fn values      (&self, schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> Result<Cursor>;
    pub fn list_files  (&self, start_from: Url) -> Result<Cursor>;
}
```

Each source mints a Ref into `ContextState`, captures the current `session_id`, and
returns a `Cursor`.

### Dispatch and terminal methods on Context

```rust
impl Context<'_> {
    /// Drain the accumulator, eliminate dead Stmts (backward reachability from `cursor`),
    /// build a Step::Consume, yield, return the consumer's typed output. Bumps `session_id`.
    pub async fn consume<T>(
        &mut self,
        cursor: Cursor,
        consumer: impl Consumer<Output = T> + 'static,
    ) -> Result<T, DeltaError>;

    /// Yield a Step::SchemaQuery; return the schema.
    pub async fn schema_query(&mut self, path: String) -> Result<SchemaRef, DeltaError>;

    /// Consume self. Drain the accumulator, eliminate dead Stmts (reachability from
    /// `cursor`), return ResultPlan { plan, result: cursor.ref_id() }.
    pub fn into_result_plan(self, cursor: Cursor) -> Result<ResultPlan, DeltaError>;
}
```

Both `consume` and `into_result_plan` run a **backward-reachability pass** from the
terminal Ref before building the Step / ResultPlan. Any Stmts not reachable from the
terminal are dropped — experimental branches, abandoned chains, intermediates whose
output goes unused get pruned automatically. Surviving Stmts retain their original Ref
values (no renumbering); the engine treats Refs as opaque ids and tolerates gaps.

### Transform methods on Cursor

```rust
impl Cursor {
    // ── Projection primitives ──────────────────────────────────────────────────────
    /// Identity-by-name projection from a target schema. Each field in `schema` becomes
    /// `col(field_name)` against the input; builder validates names + types exist.
    pub fn select(self, schema: SchemaRef) -> Result<Self>;

    /// Narrow to a subset of input fields by name. Output schema is derived from input
    /// (those fields' types).
    pub fn select_columns(self, names: impl IntoIterator<Item = impl Into<String>>) -> Result<Self>;

    /// Explicit named projection. Builder infers each expression's output type against
    /// the input schema; output schema is `(name, inferred_type)` per pair.
    pub fn project(
        self,
        named_exprs: impl IntoIterator<Item = (impl Into<String>, ExpressionRef)>,
    ) -> Result<Self>;

    /// Project with caller-supplied output schema. Positional: exprs[i] becomes
    /// schema.fields()[i]. No type inference — the schema declares names + types
    /// directly. Use when the helper builds both lists together (e.g., physical→logical
    /// column mapping driven by StateInfo).
    pub fn project_with_schema(
        self,
        exprs: Vec<ExpressionRef>,
        schema: SchemaRef,
    ) -> Result<Self>;

    // ── Schema-edit helpers (sugar over project) ───────────────────────────────────
    pub fn append_col       (self, name: impl Into<String>, expr: ExpressionRef) -> Result<Self>;
    pub fn prepend_col      (self, name: impl Into<String>, expr: ExpressionRef) -> Result<Self>;
    pub fn insert_col_after (self, after:  impl AsRef<str>, name: impl Into<String>, expr: ExpressionRef) -> Result<Self>;
    pub fn insert_col_before(self, before: impl AsRef<str>, name: impl Into<String>, expr: ExpressionRef) -> Result<Self>;
    pub fn replace_col      (self, name: impl AsRef<str>,   expr: ExpressionRef) -> Result<Self>;
    pub fn rename_col       (self, old:  impl AsRef<str>,   new:  impl Into<String>) -> Result<Self>;
    pub fn drop_col         (self, name: impl AsRef<str>) -> Result<Self>;
    pub fn drop_cols        (self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self>;

    // ── Filter / file expansion / aggregate ─────────────────────────────────────────
    pub fn filter(self, predicate: PredicateRef) -> Result<Self>;
    pub fn load(self, spec: LoadSpec) -> Result<Self>;
    pub fn max_by_version(
        self,
        group_by: Vec<ExpressionRef>,
        version: ExpressionRef,
        value_columns: Vec<String>,
    ) -> Result<Self>;

    // ── Joins (named by kind; only two exist) ───────────────────────────────────────
    pub fn inner_join    (self, other: Cursor, key_pairs: Vec<(ExpressionRef, ExpressionRef)>) -> Result<Self>;
    pub fn left_anti_join(self, other: Cursor, key_pairs: Vec<(ExpressionRef, ExpressionRef)>) -> Result<Self>;

    // ── N-ary ───────────────────────────────────────────────────────────────────────
    pub fn union_all    (self, others: &[Cursor]) -> Result<Self>;
    pub fn union_ordered(self, others: &[Cursor]) -> Result<Self>;

    // ── Accessors ───────────────────────────────────────────────────────────────────
    pub fn schema(&self) -> SchemaRef;
    pub fn ref_id(&self) -> Ref;
}
```

### When to use which projection method

| Method | Use when |
|---|---|
| `select(schema)` | You have a target schema; projection is identity-by-name. Validates types match. |
| `select_columns([name])` | Narrow to a known subset of input fields by name. |
| `project([(name, expr)])` | Explicit per-field projection; builder infers types from each expression. |
| `project_with_schema(exprs, schema)` | A kernel-internal helper produced both the exprs and the target schema together (e.g., physical→logical column mapping). |
| `append_col` / `prepend_col` / `insert_col_after` / `insert_col_before` | Add one column relative to existing input fields. |
| `replace_col` / `rename_col` | Edit a single input field in place. |
| `drop_col` / `drop_cols` | Remove fields from the input schema. |

Schema-edit helpers compile to a single `Project` Node internally — same as `project([(name, expr)])`, just with the right `(name, expr)` list constructed against the input schema.

Each method:
1. `borrow_mut` the state, check `session_id` matches.
2. Read input Ref's schema; validate parameters against it (expression type inference,
   column existence, schema compatibility for joins/unions).
3. Compute output schema; mint a fresh Ref; push Stmt + schema.
4. Drop the borrow; return a new Cursor.

No `.await` inside cursor methods — borrows are always released synchronously.

### Schema management

Lives entirely in `ContextState.ref_schemas` (indexed by `Ref.0`). Populated and
validated automatically by every cursor method. The user never threads schemas through
manually.

Where schemas enter the system (caller-supplied):
- Sources: `scan_parquet/scan_json/values` take `schema: SchemaRef`. `list_files` is canonical.
- Projections that declare a target shape: `select`, `select_via`, `LoadSpec.file_schema`.

Where schemas are inferred (no caller input):
- `filter` — pass-through.
- `project` — per-expr `Expression::infer_type(input_schema)` → `(name, inferred_type)`.
- `select_columns` — pluck named fields from input.
- `add_column` — input ++ `(name, infer_type(expr, input))`.
- `max_by_version` — `group_by` exprs (inferred) ++ `value_columns` lifted from input.
- `inner_join` / `left_anti_join` — convention (left ++ right inner; left for left-anti).
- `union_all` / `union_ordered` — from inputs; builder asserts all match by name + type.

`Expression::infer_type(&Schema) -> DataType` is the single canonical walk all of this
hinges on. Kernel doesn't have it centralized today — surfaces as an open
implementation question.

### What the user can't do

- Construct a Stmt directly (no PlanBuilder, no `append` method exposed).
- Smuggle a Ref in from outside ctx (Refs are minted internally only).
- Use a cursor from a prior session (session_id check rejects).
- Read or mutate `ref_schemas` directly — only via `cursor.schema()`.

### Cross-Step data flow

No materialize. No cross-Step named relations. Data flows across Steps via:
- Consumer outputs returned from `ctx.consume(...).await` — held as locals in the SM body.
- Schemas returned from `ctx.schema_query(...).await` — same.
- Re-reading the source (commit log, checkpoint manifest, etc.) in subsequent phases.

## State machine framework

```rust
pub trait StateMachine {
    type Output;

    fn get_step(&mut self) -> Result<Step, DeltaError>;
    fn submit(
        &mut self,
        step_result: Result<StepResult, EngineError>,
    ) -> Result<NextStep<Self::Output>, DeltaError>;

    fn step_name(&self) -> &'static str;
    fn live_relations(&self) -> Vec<String> { Vec::new() }
}

pub enum NextStep<O> {
    Continue,
    Done(O),
}

pub enum Step {
    /// Execute the SSA program; drain the named Ref into the consumer. Eager — engine
    /// runs the subgraph feeding `drain.0` and feeds batches to the consumer. The
    /// consumer's typed output flows back through `StepResult`.
    Consume {
        stmts: Vec<Stmt>,
        drain: (Ref, Box<dyn KernelConsumer>),
    },

    /// Read a single file's schema. No SSA program, no consumer — just a metadata
    /// fetch. The schema returns through `StepResult`.
    SchemaQuery(SchemaQueryNode),
}

pub struct StepResult { /* consumer output by token + schema slot */ }

/// The state machine's terminal value. Driver runs `plan` lazily and streams the
/// relation produced at `result`.
pub struct ResultPlan {
    pub plan: Plan,
    pub result: Ref,
}
```

Engine-side driver loop:

```rust
loop {
    let step = op.get_step()?;
    let step_result = engine.run_step(step).await;
    match op.submit(step_result)? {
        NextStep::Continue => continue,
        NextStep::Done(output) => return Ok(output),
    }
}
```

## Consumers (was KDF)

```rust
pub trait KernelConsumer { /* observer */ }
pub trait KernelConsumerOutput: KernelConsumer { type Output; ... }
pub struct KernelConsumerToken { ... }
pub enum KernelConsumerKind { CheckpointHint, MetadataProtocol, SidecarCollector }
```

## Coroutine helper

Concrete `impl StateMachine` that wraps an `async fn` body. Same role as today's `CoroutineSM`,
renamed `Coroutine`.

## Surviving projections (the whole pipeline)

After all the simplifications, **three** projection sites remain in the FSR + scan pipeline:

| # | Site | What it does | API |
|---|---|---|---|
| 1 | Sidecar derivation | Extract nested `(sidecar.path, sidecar.sizeInBytes)` | `.project([(name, expr)])` |
| 2 | Checkpoint align (V2-multipart) | Drop `sidecar` field to match union schema | `.select(action_schema)` |
| 3 | Data phase column mapping | Physical→logical with column mapping + metadata column synthesis | `scan_data_projection(&state_info)` → `(exprs, schema)` → `.project_with_schema(exprs, schema)` |

All other projections in today's code dissolve because:

- `MaxByVersion` and `EquiJoin` take expressions for group/key columns — no `add_column` ceremony, no `select` to drop join keys.
- Engines push down projection automatically — no need to pre-narrow the build side of joins.
- The scan terminal reshape (action_pair → flat scan_file_row) dissolves into Load with nested file_meta paths.
- The `Pair` abstraction is gone — identity projection uses `.select(schema)`.

## Module layout

```
kernel/src/plans/
├── ir/
│   └── plan.rs              // Plan, Stmt, Node, Ref, ResultPlan
├── operations/              // was state_machines/
│   └── framework/
│       ├── context.rs       // Context, Cursor, ContextState (replaces PlanBuilder)
│   ├── framework/
│   │   ├── state_machine.rs // StateMachine trait, NextStep, ResultPlan
│   │   ├── step.rs          // Step, SchemaQueryNode
│   │   ├── step_result.rs   // StepResult
│   │   ├── engine_error.rs
│   │   └── coroutine/       // Coroutine driver
│   └── scan/
│       ├── full_state.rs
│       └── file_scan.rs
├── kernel_consumers/        // was kdf/
├── record_types.rs          // was record_schemas.rs
├── errors/                  // unchanged
└── mod.rs
```

## Rename table

| Today | New |
|---|---|
| `plans/state_machines/` | `plans/operations/` |
| `plans/kdf/` | `plans/kernel_consumers/` |
| `record_schemas.rs` | `record_types.rs` |
| `StateMachine` | `StateMachine` (kept) |
| `CoroutineSM` | `Coroutine` |
| `get_operation` | `get_step` |
| `advance` | `submit` |
| `PhaseOperation` | `Step` |
| `PhaseState` | `StepResult` |
| `AdvanceResult` | `NextStep` |
| `phase_name` | `step_name` |
| `ConsumerKdf` | `KernelConsumer` |
| `KdfOutput` | `KernelConsumerOutput` |
| `KdfStateToken` | `KernelConsumerToken` |
| `ConsumerKdfId` | `KernelConsumerKind` |
| `DeclarativePlanNode` | `Node` (in `Stmt`) |
| `Plan { root, sink }` | `Plan { stmts }` |
| `PlanBuilder` (separate type) | absorbed into `Context`; cursor methods live on `Cursor` |
| `SinkType`, `LoadSink` | dissolved (Load is a Node; drain on Step::Consume envelope) |
| `RelationHandle`, `RelationRef` Node | deleted (no cross-Step relations; no materialize) |
| `RelationRegistry` (separate type) | absorbed into `ContextState` (stmts + ref_schemas + session_id) |
| `EquiJoin { kind, ... }` + `JoinKind` enum on cursor API | named cursor methods: `inner_join`, `left_anti_join` (Node still uses kind) |
| `Window` Node | deleted (MaxByVersion subsumes the only use) |
| `Pair = (SchemaRef, Vec<Arc<Expression>>)` | deleted |
| `project_pair(pair)` | `.select(schema)` / `.project([(name, expr)])` / `.project_with_schema(exprs, schema)` / schema-edit helpers |
| `SCAN_BASE`, `FSR_BASE` (as Pair) | `SCAN_BASE_SCHEMA`, `FSR_BASE_SCHEMA` (SchemaRef) |
| `with_stats(pair, ...)`, `with_partition_values(pair, ...)` | schema-only helpers |
| `scan_file_row_pair(parts)` | deleted (#7 dissolved into Load with nested file_meta) |
| `Window` Node | deleted (MaxByVersion subsumes the only use) |
| `ResultPlan { plans, result_relation }` | `ResultPlan { plan: Plan, result: Ref }` |
| `FullState`, `DeltaError`, `EngineError` | unchanged |

## End-to-end: shared reconciliation

```rust
/// Build the entire reconciliation as one SSA plan. Returns the cursor at the reconciled output.
fn build_reconciliation(
    ctx: &Context<'_>,
    snapshot: &Snapshot,
    shape: &CheckpointShape,
    base_schema: &SchemaRef,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: ExpressionRef,
) -> Result<Cursor, DeltaError> {
    let action_schema = with_partition_values(
        &with_stats(base_schema, stats, /* native= */ false),
        parts,
    );
    let identity_not_null: ExpressionRef = dedup_key.is_not_null().into();

    let commits = commit_cover_rows(snapshot.log_segment())?;
    let log_root = snapshot.log_segment().log_root.clone();
    let checkpoint_files = collect_checkpoint_files(snapshot);
    let has_checkpoint = !checkpoint_files.is_empty();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;

    // Stage 1: commit_load
    let commit_raw = ctx
        .values(path_size_version_schema(), commit_rows(&commits))?
        .load(LoadSpec {
            file_schema: action_schema.clone(),
            file_type: FileType::Json,
            base_url: Some(log_root.clone()),
            passthrough_columns: vec![ColumnName::new(["version"])],
            file_meta: default_file_meta(),
            dv_ref: None,
        })?;

    // Stage 2: commit_dedup — filter + max_by_version (expression-keyed)
    let commit_dedup = commit_raw
        .filter(identity_not_null.clone())?
        .max_by_version(
            vec![dedup_key.clone()],
            col(["version"]).into(),
            action_value_columns(base_schema),
        )?;

    if !has_checkpoint {
        return commit_dedup.filter(retention_filter(min_file_ts, txn_expiry).into());
    }

    // Stage 3: checkpoint_top — always re-scan; no cross-Step manifest handoff
    let checkpoint_schema = if shape.has_sidecars {
        with_sidecar_field(&action_schema)
    } else {
        action_schema.clone()
    };
    let checkpoint = match shape.file_format {
        FileFormat::Parquet => ctx.scan_parquet(checkpoint_files.clone(), checkpoint_schema.clone())?,
        FileFormat::Json    => ctx.scan_json   (checkpoint_files.clone(), checkpoint_schema.clone())?,
    };

    // Stage 4: sidecar_load (V2-multipart only). Surviving projection #1 (nested extract).
    let aligned = if shape.has_sidecars {
        let sidecars = checkpoint
            .filter(col([SIDECAR_NAME]).is_not_null().into())?
            .project([
                ("path",        col([SIDECAR_NAME, "path"]).into()),
                ("sizeInBytes", col([SIDECAR_NAME, "sizeInBytes"]).into()),
            ])?
            .load(LoadSpec {
                file_schema: action_schema.clone(),
                file_type: FileType::Parquet,
                base_url: Some(log_root.join("_sidecars/")?),
                passthrough_columns: vec![],
                file_meta: default_file_meta(),
                dv_ref: None,
            })?;

        // Surviving projection #2 (narrow checkpoint to drop sidecar field).
        let top = checkpoint.select(action_schema.clone())?;
        top.union_with(&[sidecars], /* ordered= */ false)?
    } else {
        checkpoint
    };

    // Stage 5: anti-join + union + retention (no add_column ceremony, no trailing select)
    let keyed = aligned.filter(identity_not_null)?;
    let survivors = keyed.left_anti_join(
        commit_dedup.clone(),
        vec![(dedup_key.clone(), dedup_key.clone())],
    )?;

    commit_dedup
        .union_all(&[survivors])?
        .filter(retention_filter(min_file_ts, txn_expiry).into())
}
```

### Schema flow through `build_reconciliation`

Each cursor's schema, traced from sources to the returned `reconciled`. `AS` =
"action subset" (the value columns from `base_schema` — six fields for FSR, two for
Scan). `action_schema` augments `AS`'s `add` with parsed stats / parsed partition
values when requested.

| Step | Cursor | Schema |
|---|---|---|
| `ctx.values(...)` | upstream | `{ path, size, version }` |
| `.load(...)` | `commit_raw` | `action_schema ++ [version]` |
| `.filter(identity_not_null)` | — | unchanged |
| `.max_by_version(...)` | `commit_dedup` | `action_schema` (group key + version are aggregation-internal, dropped from output) |
| `ctx.scan_parquet(checkpoint_files, checkpoint_schema)` | `checkpoint` | `action_schema` (or `action_schema ++ [sidecar]` for V2-multipart) |
| `.filter(SIDECAR_NAME IS NOT NULL).project(path,sizeInBytes).load(...)` | `sidecars` | `action_schema` (Load `passthrough_columns: []`) |
| `.drop_col(SIDECAR_NAME)` | `top` | `action_schema` |
| `.union_all([sidecars])` | `aligned` | `action_schema` |
| `.filter(identity_not_null)` | `keyed` | `action_schema` |
| `.left_anti_join(commit_dedup, keys)` | `survivors` | `action_schema` (left-anti passes left side through unchanged) |
| `.union_all([survivors]).filter(retention)` | `reconciled` (returned) | `action_schema` |

Three points worth noting:

1. **`MaxByVersion`'s output drops the group key and version**. The group key was an
   *expression*, not a materialized column, so there's nothing to drop. Version is
   internal to the aggregation. Output = the listed `value_columns` (looked up from
   input).
2. **All Stage 4 / 5 outputs converge on `action_schema`**. Both sides of the union
   (`commit_dedup` and `survivors`) have the same schema by construction — no
   alignment projection needed.
3. **`reconciled.schema() == action_schema`**. FSR returns it directly (caller streams
   action-shaped rows). Scan threads it through one more Load + projection (data
   phase) before terminal.

## End-to-end: FullState (FSR)

```rust
impl FullState {
    pub fn operation(&self) -> Coroutine<ResultPlan> {
        let snapshot = self.snapshot.clone();
        let state_info = self.state_info.clone();

        Coroutine::new("fsr", move |mut ctx| async move {
            // May yield Step::SchemaQuery / Step::Consume internally.
            let shape = resolve_checkpoint_shape(&mut ctx, snapshot.as_ref(), state_info.as_ref()).await?;
            let stats = state_info.as_ref().and_then(|si| si.physical_stats_schema.clone());

            // Build the terminal SSA directly against ctx; ctx.into_result_plan
            // drains, prunes dead Stmts via backward reachability, and returns.
            let terminal = build_reconciliation(
                &ctx, snapshot.as_ref(), &shape,
                &FSR_BASE_SCHEMA, stats, /* parts= */ None,
                Arc::new(fsr_dedup_key()),
            )?;
            ctx.into_result_plan(terminal)
        })
    }
}
```

## End-to-end: Scan with data phase

```rust
impl Scan {
    pub fn operation(&self) -> Coroutine<ResultPlan> {
        let scan = self.clone();

        Coroutine::new("scan", move |mut ctx| async move {
            let shape = resolve_checkpoint_shape(&mut ctx, scan.snapshot().as_ref(), Some(scan.state_info())).await?;
            let stats = scan.physical_stats_schema();
            let parts = scan.data_stage_partition_schema();
            let logical_schema = scan.logical_schema().clone();
            let physical_schema = scan.physical_schema().clone();
            let table_root = scan.snapshot().table_root().clone();
            let state_info = scan.state_info().clone();

            let reconciled = build_reconciliation(
                &ctx, scan.snapshot().as_ref(), &shape,
                &SCAN_BASE_SCHEMA, stats, parts.clone(),
                Arc::new(scan_file_dedup_key()),
            )?;

            // #7 dissolved: no intermediate flat scan-file-row reshape.
            // Filter to add-only rows, then Load with nested file_meta paths.
            let raw_data = reconciled
                .filter(col(["add"]).is_not_null().into())?
                .load(LoadSpec {
                    file_schema: physical_schema,
                    file_type: FileType::Parquet,
                    base_url: Some(table_root),
                    passthrough_columns: vec![ColumnName::new(["add"])],
                    file_meta: ScanFileColumns {
                        path: ColumnName::new(["add", "path"]),
                        size: Some(ColumnName::new(["add", "size"])),
                        record_count: None,
                    },
                    dv_ref: Some(DvRef::skip(ColumnName::new(["add", "deletionVector"]))),
                })?;

            // Surviving projection #3: physical → logical column mapping.
            // Helper builds both exprs and the target schema together.
            let (exprs, target_schema) = scan_data_projection(&state_info)?;
            let terminal = raw_data.project_with_schema(exprs, target_schema)?;
            ctx.into_result_plan(terminal)
        })
    }
}
```

## What the unified design achieves

- **The reconciliation itself collapses to one SSA program** — no intra-reconciliation
  Step boundaries, no named intermediate relations, returned as `ResultPlan { plan, result }`
  and streamed lazily by the driver.
- **Three actual projections** across the whole FSR+scan pipeline — each doing real
  schema work (extract, narrow, map). Everything else dissolves.
- **`Cursor` is `Clone`** (holds an `Rc` to ctx state, not a borrow) — branching/sharing
  needs explicit `.clone()`. Decouples cursor lifetimes from `&mut Context` dispatch so
  `ctx.consume(cursor, ...)` works without borrow conflicts.
- **Dead-statement elimination at every dispatch** — `ctx.consume` and
  `ctx.into_result_plan` run backward reachability from the terminal Ref and prune
  unreached Stmts before yielding. Experimental branches and unused intermediates never
  reach the engine.
- **Pure SSA**: every Stmt produces a Ref, no `Option<Ref>` anywhere, no side-effecting
  Node variants. No materialize concept anywhere.
- **One Step variant per output kind**: `Consume` returns consumer state (eager),
  `SchemaQuery` returns a schema (eager). State machines with mid-flight needs (e.g.,
  CDF: validate-then-read; FSR/Scan: shape resolution) yield one or more
  `Consume`/`SchemaQuery` Steps before returning `ResultPlan`.
- **No cross-Step relations**. Cross-Step data flows via consumer outputs (small data
  via SM body locals) or by re-reading the source in subsequent SSA (cheap for commit
  logs, checkpoint manifests, schemas, etc.). The driver/engine doesn't need a name
  registry.

### Pre-Steps for FSR/Scan: shape resolution

FSR and Scan both yield up to three pre-Steps to resolve the checkpoint shape before
building the reconciliation plan:

1. **`Step::SchemaQuery`** on the top-level checkpoint parquet, when the `_last_checkpoint`
   hint lacks a schema. Detects sidecars + `stats_parsed` compatibility.
2. **`Step::Consume`** (V2 multipart only): scan the manifest files; drain a
   `SidecarCollector` consumer that returns `Vec<SidecarUrl>`. The SM body holds the
   URLs locally — no materialized relation.
3. **`Step::SchemaQuery`** on the first sidecar parquet (V2 multipart with stats requested):
   re-probes `stats_parsed`.

The terminal reconciliation built against `ctx` then **re-scans the same checkpoint files**.
Manifest files are scanned twice (once eagerly for sidecar URL extraction, once lazily
during the user's stream) — acceptable cost because manifest files are typically MB-scale
and the scans run in parallel with everything else lazily.

In sync:

```rust
async fn resolve_checkpoint_shape(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    physical_stats_schema: Option<&SchemaRef>,
) -> Result<CheckpointShape, DeltaError> {
    let mut shape = checkpoint_shape_from_last_checkpoint(snapshot)?;
    shape.requested_stats_schema = physical_stats_schema.cloned();

    // Step::SchemaQuery #1: top-level checkpoint
    if snapshot_has_checkpoint_files(snapshot) && snapshot.log_segment().checkpoint_schema().is_none() {
        if shape.file_format == FileFormat::Json {
            shape.has_sidecars = true;
        } else {
            let cp_schema = ctx.schema_query(first_checkpoint_url(snapshot)?).await?;
            shape.has_sidecars = cp_schema.contains(SIDECAR_NAME);
            shape.actions_schema_subset = checkpoint_actions_schema_projection(&cp_schema)?;
            shape.has_stats_parsed = physical_stats_schema.is_some_and(|reqd|
                LogSegment::schema_has_compatible_stats_parsed(&cp_schema, reqd));
        }
    }

    // Step::Consume + Step::SchemaQuery #2 (V2 multipart only)
    if shape.has_sidecars {
        let checkpoint_files = collect_checkpoint_files(snapshot);
        let manifest_schema = checkpoint_manifest_scan_schema(/* include_sidecar= */ true);
        let log_root = snapshot.log_segment().log_root.clone();

        let manifest = match shape.file_format {
            FileFormat::Parquet => ctx.scan_parquet(checkpoint_files.clone(), manifest_schema.clone())?,
            FileFormat::Json    => ctx.scan_json   (checkpoint_files.clone(), manifest_schema.clone())?,
        };
        let sidecar_rows = manifest.filter(col([SIDECAR_NAME]).is_not_null().into())?;
        let sidecar_files = ctx
            .consume(sidecar_rows, SidecarCollector::new(log_root))
            .await?;

        if let (Some(reqd), Some(first)) = (physical_stats_schema, sidecar_files.first()) {
            let sidecar_schema = ctx.schema_query(first.location.as_str().to_string()).await?;
            shape.has_stats_parsed = LogSegment::schema_has_compatible_stats_parsed(&sidecar_schema, reqd);
        }
    }

    Ok(shape)
}
```

`CheckpointShape` no longer carries a `manifest_relation: Option<RelationHandle>` field —
the relation is gone. `build_reconciliation` always re-scans the checkpoint files itself.

## Open implementation questions

These were not resolved during synthesis; surface during the migration:

- **`Expression::infer_type(&Schema) -> DataType`**: needs a single canonical walk that
  the builder can use. Kernel already has type knowledge scattered across evaluators;
  centralize it.
- **Kernel-internal projection helpers**: ad-hoc functions per state machine that return
  `(Vec<ExpressionRef>, SchemaRef)` for feeding to `cursor.project_with_schema`. The
  scan data phase has `scan_data_projection(&state_info)`; CDF will likely add its own.
  These are not part of the cursor API surface — just regular kernel-side functions.
- **`RefCell` footgun**: cursor methods must release the borrow before returning, and
  dispatch methods (`consume`, `schema_query`) must release the borrow before any
  `.await`. The shape: `borrow_mut` → mutate state → drop guard (via inner scope) →
  await on `self.co`. Holding a RefCell guard across an await is forbidden by
  construction. Documented; structurally safe for this API shape.
- **Cross-Step data scaling**: if a consumer's output is large enough that holding it
  in the SM body memory is a problem (rare — consumer outputs are typically small status
  values, not relation contents), an engine-side cache or a "stash this Ref under a
  driver-private handle" extension would be needed. Not required for FSR/Scan/CDF.

## Status

Vocabulary settled. Not yet implemented. Migration order:
(1) consumers/file renames (operations module kept under existing `state_machines/`
name or renamed independently), (2) `StateMachine` trait + framework (slim:
`Step::Consume` + `Step::SchemaQuery`), (3) IR SSA rewrite (`Plan`, `Stmt`, `Node`,
`Ref`), (4) `Context` + `Cursor` (Rc/RefCell shared state, session_id, dead-code
elimination on dispatch), (5) pipeline rewrite (FSR, Scan) using `ctx`-as-builder
returning `ResultPlan` directly, (6) datafusion executor adaption (no name registry;
just consumer drains + final streaming of `ResultPlan.plan`).
