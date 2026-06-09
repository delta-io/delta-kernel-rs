//! Plan-construction context and builder.
//!
//! [`Context`] owns the in-flight plan and a per-Ref schema table. [`PlanBuilder`] is a
//! shared handle to a specific [`Ref`] in that program; builder methods append new
//! `PlanNode`s and return new [`PlanBuilder`]s threading the freshly minted Refs. Cursors are
//! [`Clone`] (cheap `Rc` clone) so branching is explicit.
//!
//! State is shared via `Rc<RefCell<ContextState>>` so transform methods can mutate without
//! requiring `&mut Context`. SMs are CPU-only sequencers and never need to cross thread
//! boundaries, so `Rc` (vs. `Arc`) is the right shape. See the
//! `CoroutineSM` module docs for the `!Send` rationale.
//!
//! # Stale-builder protection
//!
//! Two checks gate every dispatch site:
//!
//! 1. *Cross-context identity* -- [`Context::reduce`] and [`Context::into_result_plan`] call
//!    `ensure_same_context(&builder)` (uses [`Rc::ptr_eq`]) before any state access. Two fresh
//!    `Context`s would both start at `session_id = 0`, so a builder from a different context would
//!    silently pass the session check and dispatch against the wrong accumulator.
//! 2. *Cross-yield freshness* -- each builder captures the context's `session_id` at mint time.
//!    Dispatch methods ([`Context::reduce`], [`Context::schema_query`]) bump the id after the yield
//!    to invalidate cursors held across the boundary; [`Context::into_result_plan`] consumes
//!    `self`, mooting the concern for terminal dispatch.
//!
//! # `RefCell` discipline
//!
//! PlanBuilder methods borrow_mut, mutate state, and drop the guard before returning. Dispatch
//! methods drop the borrow before any `engine.yield_(...).await`. Holding a `Ref` / `RefMut`
//! across an await would panic at runtime; this is structurally avoided by every method
//! in this module.
//!
//! [`CoroutineSM`]: crate::plans::state_machines::framework::coroutine::CoroutineSM
//! [`PlanNode`]: crate::plans::ir::plan::PlanNode
//! [`Ref`]: crate::plans::ir::plan::Ref

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;

use url::Url;

use crate::expressions::{ColumnName, ExpressionRef, IntoColumnName, PredicateRef, Scalar};
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{
    EquiJoinNode, FilterNode, ListFilesNode, LoadNode, MaxByVersionNode, ProjectNode, ReduceSink,
    ScanJsonNode, ScanParquetNode, UnionNode, ValuesNode,
};
use crate::plans::ir::plan::{JoinKind, NodeKind, Plan, PlanNode, Ref, ResultPlan};
use crate::plans::kernel_reducers::{Extractor, KernelReducer, KernelReducerOutput};
use crate::plans::schema_expr::check::{
    check_projection, check_select, infer_projection_schema, validate_exprs,
};
use crate::plans::schema_expr::field_op::{
    compile_field_op, load_output_schema, narrow_schema_to, FieldOp,
};
use crate::plans::state_machines::framework::coroutine::{Engine, StepResume, StepYield};
use crate::plans::state_machines::framework::state_machine::{
    EngineRequest, EngineResponse, SchemaQuery,
};
use crate::schema::{SchemaRef, StructField};
use crate::FileMeta;

// ============================================================================
// Local error shorthand
// ============================================================================

/// Build a Delta-agnostic [`BoxedSource`][crate::plans::errors::BoxedSource] from a
/// `format!`-style message. The plan builder is a generic plan-construction tool that knows
/// nothing about Delta error codes -- every internal sanity check produces a boxed error here
/// and relies on `impl From<BoxedSource> for DeltaError` to lift it at the `?` boundary with
/// the catch-all [`DeltaCommandInvariantViolation`][crate::plans::errors::DeltaErrorCode] code.
///
/// Use [`DeltaResultExt::or_delta`][crate::plans::errors::DeltaResultExt::or_delta] when a more
/// specific code is appropriate.
macro_rules! pb_err {
    ($($arg:tt)*) => {
        <$crate::plans::errors::BoxedSource as ::std::convert::From<::std::string::String>>::from(
            ::std::format!($($arg)*),
        )
    };
}

// ============================================================================
// Shared state
// ============================================================================

/// In-flight plan being built, the per-Ref schema table, and a session counter.
///
/// Ref ids are derived directly from `plan.stmts.len()` at push time: each node's
/// output Ref equals its index in `stmts`, so the counter and the vector length are
/// always in lockstep. No separate `next_ref` field is needed.
#[derive(Debug, Default)]
struct ContextState {
    plan: Plan,
    /// Per-Ref output schemas. `ref_schemas[i]` is the output schema of `plan.stmts[i]`.
    ref_schemas: Vec<SchemaRef>,
    /// Bumped by dispatch methods to invalidate cursors held across yields.
    session_id: u32,
}

impl ContextState {
    /// Append a node to the in-flight plan, mint its output Ref, and record
    /// `schema` for the new Ref.
    fn push_node(&mut self, kind: NodeKind, inputs: Vec<Ref>, schema: SchemaRef) -> Ref {
        let output = Ref(self.plan.stmts.len() as u32);
        self.plan.stmts.push(PlanNode {
            kind,
            inputs,
            output,
        });
        self.ref_schemas.push(schema);
        debug_assert_eq!(self.ref_schemas.len(), self.plan.stmts.len());
        output
    }

    /// Validates that `builder` was minted in the current session (not stale across dispatch).
    fn ensure_fresh(&self, builder: &PlanBuilder) -> Result<(), DeltaError> {
        if self.session_id != builder.session_id {
            return Err(pb_err!(
                "stale builder: builder session={cs} expected {ss}",
                cs = builder.session_id,
                ss = self.session_id,
            )
            .into());
        }
        Ok(())
    }

    /// Bump the session counter, invalidating any `PlanBuilder` minted before this
    /// call. Cursors held across the next [`Self::ensure_fresh`] will error. Called
    /// at every dispatch boundary.
    fn invalidate_cursors(&mut self) {
        self.session_id = self.session_id.wrapping_add(1);
    }

    /// Drain the in-flight plan for shipment to the engine. Empties the per-Ref
    /// schema table and invalidates cursors (so the Refs in the returned plan, which
    /// restart from 0 on the next push, can't collide with any stale handle). The
    /// caller pipes the returned plan through DCE before yielding it.
    fn take_plan_for_dispatch(&mut self) -> Plan {
        self.ref_schemas.clear();
        self.invalidate_cursors();
        mem::take(&mut self.plan)
    }
}

// ============================================================================
// Context
// ============================================================================

/// Plan-construction context.
///
/// Source methods mint root [`PlanBuilder`]s, dispatch methods
/// ([`Self::reduce`] / [`Self::schema_query`]) yield steps through a coroutine, and
/// [`Self::into_result_plan`] is the terminal sync drain (backward-reachability DCE).
#[derive(Default)]
pub struct Context {
    state: Rc<RefCell<ContextState>>,
}

impl Context {
    /// Construct a fresh context with an empty plan. Equivalent to
    /// [`Context::default()`]; provided for idiomatic call-site phrasing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a source node with no inputs and return a handle to its output Ref.
    fn push_source(&self, kind: NodeKind, schema: SchemaRef) -> Result<PlanBuilder, DeltaError> {
        let state = &self.state;
        let mut s = state.borrow_mut();
        let r = s.push_node(kind, vec![], schema);
        Ok(PlanBuilder::new(state, r, s.session_id))
    }

    /// Append a `NodeKind::ListFiles` source. Output schema is the engine-canonical listing
    /// shape supplied by the caller (the kernel does not pin it; consumers wire to the
    /// engine's listing schema).
    pub fn list_files(
        &self,
        start_from: Url,
        listing_schema: impl Into<SchemaRef>,
    ) -> Result<PlanBuilder, DeltaError> {
        self.push_source(
            NodeKind::ListFiles(ListFilesNode { start_from }),
            listing_schema.into(),
        )
    }

    /// Append a `NodeKind::ScanParquet` source over Parquet `files`. Output schema
    /// is the caller-declared `schema`.
    pub fn scan_parquet(
        &self,
        files: Vec<FileMeta>,
        schema: impl Into<SchemaRef>,
    ) -> Result<PlanBuilder, DeltaError> {
        let schema = schema.into();
        let node = ScanParquetNode {
            files,
            schema: Arc::clone(&schema),
        };
        self.push_source(NodeKind::ScanParquet(node), schema)
    }

    /// Append a `NodeKind::ScanJson` source over newline-delimited JSON `files`. Output
    /// schema is the caller-declared `schema`.
    pub fn scan_json(
        &self,
        files: Vec<FileMeta>,
        schema: impl Into<SchemaRef>,
    ) -> Result<PlanBuilder, DeltaError> {
        let schema = schema.into();
        let node = ScanJsonNode {
            files,
            schema: Arc::clone(&schema),
        };
        self.push_source(NodeKind::ScanJson(node), schema)
    }

    /// Append a `NodeKind::Values` source. Rows accept any nested iterable whose leaf
    /// values are convertible into [`Scalar`]: e.g. `[[1i64, 2], [3, 4]]` or a
    /// `Vec<Vec<Scalar>>`. Combined with the per-leaf [`Into<Scalar>`] conversions
    /// for primitive types, callers seldom need a `vec!` or explicit `Scalar::Long`.
    /// For empty rows, pass `Vec::<Vec<Scalar>>::new()` (or a typed helper) so the
    /// item type can be inferred.
    pub fn values<R, S>(
        &self,
        schema: impl Into<SchemaRef>,
        rows: impl IntoIterator<Item = R>,
    ) -> Result<PlanBuilder, DeltaError>
    where
        R: IntoIterator<Item = S>,
        S: Into<Scalar>,
    {
        let schema = schema.into();
        let rows: Vec<Vec<Scalar>> = rows
            .into_iter()
            .map(|r| r.into_iter().map(Into::into).collect())
            .collect();
        let node = ValuesNode {
            schema: Arc::clone(&schema),
            rows,
        };
        self.push_source(NodeKind::Values(node), schema)
    }

    /// Verifies that `builder` was minted by `self` (not by some other `Context`). The
    /// session_id check inside `ensure_fresh` is necessary but not sufficient: two
    /// contexts will both start at session 0 and a freshly-built builder from another
    /// context would silently pass the session check while reading/draining the wrong
    /// state. The Rc identity check rules that out.
    fn ensure_same_context(&self, builder: &PlanBuilder) -> Result<(), DeltaError> {
        if !Rc::ptr_eq(&self.state, &builder.state) {
            return Err(pb_err!(
                "builder was minted by a different Context; only the originating Context \
                 may dispatch or drain it",
            )
            .into());
        }
        Ok(())
    }

    /// Drain the accumulator into a [`EngineRequest::Reduce`] yielded through `engine` and recover
    /// the reducer's typed output via [`Extractor`].
    ///
    /// Backward-reachability DCE narrows the plan to stmts transitively reachable
    /// from `builder`. After the yield resumes, the accumulator is reset (empty plan +
    /// empty schema table) and `session_id` is bumped so any cursors held across the
    /// boundary fail at runtime.
    ///
    /// Borrows the shared state only briefly (mutation + drain) and drops the borrow
    /// before the awaited yield, so the await never holds a `RefMut`.
    #[allow(dead_code)]
    pub(crate) async fn reduce<S>(
        &self,
        engine: &mut Engine,
        builder: PlanBuilder,
        reducer: S,
        step_name: &'static str,
    ) -> Result<S::Output, DeltaError>
    where
        S: KernelReducer + KernelReducerOutput + 'static,
    {
        self.ensure_same_context(&builder)?;
        let sink = ReduceSink::new_reducer(reducer);
        let extractor = Extractor::for_reducer::<S>(sink.token.clone());
        let terminal = builder.ref_id;
        let stmts: Vec<PlanNode> = {
            let mut s = self.state.borrow_mut();
            s.ensure_fresh(&builder)?;
            s.take_plan_for_dispatch().reachable_from(terminal).stmts
        };
        // Drop the builder handle so the only remaining Rc on `state` is `self`'s.
        drop(builder);

        let operation = EngineRequest::Reduce {
            stmts,
            terminal,
            sink,
        };
        let StepResume(result) = engine
            .yield_(StepYield {
                operation,
                step_name,
            })
            .await;
        let payload = result.map_err(DeltaError::invariant)?;
        let EngineResponse::Reducer(handle) = payload else {
            return Err(pb_err!(
                "reduce: executor returned non-Reducer payload `{payload:?}` for \
                 EngineRequest::Reduce",
            )
            .into());
        };
        extractor.extract(handle).map_err(DeltaError::invariant)
    }

    /// Yield a [`EngineRequest::SchemaQuery`] and return the schema delivered by the engine.
    ///
    /// Bumps `session_id` so any cursors held across the boundary fail at runtime.
    /// The accumulator is preserved -- callers typically use the returned schema to
    /// build fresh sources, and old refs become unreachable from new cursors (the next
    /// `reduce`'s DCE prunes them).
    #[allow(dead_code)]
    pub(crate) async fn schema_query(
        &self,
        engine: &mut Engine,
        path: impl Into<String>,
        step_name: &'static str,
    ) -> Result<SchemaRef, DeltaError> {
        self.state.borrow_mut().invalidate_cursors();
        let operation = EngineRequest::SchemaQuery(SchemaQuery::new(path.into()));
        let StepResume(result) = engine
            .yield_(StepYield {
                operation,
                step_name,
            })
            .await;
        let payload = result.map_err(DeltaError::invariant)?;
        let EngineResponse::Schema(schema) = payload else {
            return Err(pb_err!(
                "schema_query: executor returned non-Schema payload `{payload:?}` for \
                 EngineRequest::SchemaQuery",
            )
            .into());
        };
        Ok(schema)
    }

    /// Drain the accumulator into a [`ResultPlan`] keyed at `builder`. Performs
    /// backward-reachability DCE; only stmts transitively reachable from
    /// `builder.ref_id()` survive.
    ///
    /// Returns an error if `builder` is from a different context (or stale across a
    /// dispatch). All cursors must be dropped before this call -- the context state is
    /// solely owned at terminal time.
    pub fn into_result_plan(self, builder: PlanBuilder) -> Result<ResultPlan, DeltaError> {
        self.ensure_same_context(&builder)?;
        // Validate session before tearing down. Drop the borrow before `Rc::try_unwrap`.
        {
            let s = self.state.borrow();
            s.ensure_fresh(&builder)?;
        }
        // Drop the builder's Rc handle so try_unwrap can succeed.
        drop(builder.state);
        let result = builder.ref_id;
        let inner = Rc::try_unwrap(self.state)
            .map(|cell| cell.into_inner())
            .map_err(|_| {
                pb_err!(
                    "into_result_plan: outstanding PlanBuilder handles -- drop other Cursors before \
                     calling this method",
                )
            })?;
        let plan = inner.plan.reachable_from(result);
        Ok(ResultPlan { plan, result })
    }
}

// ============================================================================
// PlanBuilder
// ============================================================================

/// Handle to a Ref in the in-flight plan. PlanBuilder methods append further
/// stmts and return new Cursors threading the minted Refs.
#[derive(Clone, Debug)]
pub struct PlanBuilder {
    state: Rc<RefCell<ContextState>>,
    ref_id: Ref,
    session_id: u32,
}

impl PlanBuilder {
    /// Mint a fresh handle for `ref_id` against `state`. Captures `session_id` so the
    /// handle can be invalidated by dispatch methods (see module-level docs).
    fn new(state: &Rc<RefCell<ContextState>>, ref_id: Ref, session_id: u32) -> Self {
        Self {
            state: Rc::clone(state),
            ref_id,
            session_id,
        }
    }

    /// The Ref this builder points at.
    pub fn ref_id(&self) -> Ref {
        self.ref_id
    }

    /// The schema of the value at this Ref.
    ///
    /// Returns an error if the builder is stale.
    pub fn schema(&self) -> Result<SchemaRef, DeltaError> {
        let state = self.state.borrow();
        state.ensure_fresh(self)?;
        Ok(state
            .ref_schemas
            .get(self.ref_id.0 as usize)
            .cloned()
            .ok_or_else(|| {
                pb_err!(
                    "schema: ref {ref_id} has no recorded schema",
                    ref_id = self.ref_id.0
                )
            })?)
    }

    /// Validates that `other` shares the same underlying context state.
    fn ensure_same_context(&self, other: &PlanBuilder) -> Result<(), DeltaError> {
        if !Rc::ptr_eq(&self.state, &other.state) {
            return Err(pb_err!("builder from a different context cannot be combined").into());
        }
        Ok(())
    }

    /// Append a unary node (`self` as sole input) and return a handle to its output Ref.
    fn push_unary(self, kind: NodeKind, schema: SchemaRef) -> Result<Self, DeltaError> {
        self.push_nary(kind, &[], schema)
    }

    /// Append an n-ary node over `self` and `others`, returning a handle to its output Ref.
    fn push_nary(
        self,
        kind: NodeKind,
        others: &[&PlanBuilder],
        schema: SchemaRef,
    ) -> Result<Self, DeltaError> {
        let state = &self.state;
        for other in others {
            self.ensure_same_context(other)?;
        }
        let mut s = state.borrow_mut();
        s.ensure_fresh(&self)?;
        for other in others {
            s.ensure_fresh(other)?;
        }
        let mut input_refs = vec![self.ref_id];
        input_refs.extend(others.iter().map(|b| b.ref_id));
        let r = s.push_node(kind, input_refs, schema);
        Ok(PlanBuilder::new(state, r, s.session_id))
    }

    /// Single plumbing primitive for every projection-emitting method. Each caller
    /// hands us the validated `(output_schema, named_exprs)` -- this just packages
    /// them into `NodeKind::Project` and pushes the node.
    fn push_project(
        self,
        output_schema: SchemaRef,
        named_exprs: Vec<(String, ExpressionRef)>,
    ) -> Result<Self, DeltaError> {
        let node = ProjectNode {
            named_exprs,
            output_schema: Arc::clone(&output_schema),
        };
        self.push_unary(NodeKind::Project(node), output_schema)
    }

    /// Apply a nested field edit and append the resulting projection node.
    fn apply_field_op(self, op: FieldOp) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let (output_schema, pairs) = compile_field_op(&input_schema, &op)?;
        self.push_project(output_schema, pairs)
    }

    // === Filter / Project ====================================================

    /// Append a `NodeKind::Filter`. Output schema is unchanged.
    pub fn filter(self, predicate: impl Into<PredicateRef>) -> Result<Self, DeltaError> {
        let predicate = predicate.into();
        let schema = self.schema()?;
        self.push_unary(NodeKind::Filter(FilterNode { predicate }), schema)
    }

    /// Append a `NodeKind::Project` with inferred output schema.
    ///
    /// Each pair becomes one output field `(name, infer_type(expr))`. Inference is
    /// narrow -- unsupported expressions error out and direct callers to
    /// [`Self::project_with_schema`]. Schema derivation lives in
    /// `schema_expr::check::infer_projection_schema`.
    pub fn project(
        self,
        named_exprs: impl IntoIterator<Item = (impl Into<String>, impl Into<ExpressionRef>)>,
    ) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let (output_schema, pairs) = infer_projection_schema(&input_schema, named_exprs)?;
        self.push_project(output_schema, pairs)
    }

    /// Append a `NodeKind::Project` with caller-supplied output schema. Positional:
    /// the i-th expression becomes `schema.fields()[i]`. Validation lives in
    /// `schema_expr::check::check_projection`: arity + per-expression bidirectional
    /// type check against the target field's type, with **structural** type
    /// equivalence (struct field names of nested types are ignored) so column-mapping
    /// physical -> logical renames are accepted.
    pub fn project_with_schema(
        self,
        exprs: impl IntoIterator<Item = impl Into<ExpressionRef>>,
        schema: SchemaRef,
    ) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let pairs = check_projection(&input_schema, exprs, &schema)?;
        self.push_project(schema, pairs)
    }

    // === Schema-edit sugar (over project) ====================================

    /// Identity-by-name projection: every field in `schema` becomes `col(name)` against
    /// the input, strict-type-checked field by field. Validation + pair construction
    /// live in `schema_expr::check::check_select`.
    pub fn select(self, schema: SchemaRef) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let pairs = check_select(&input_schema, &schema)?;
        self.push_project(schema, pairs)
    }

    /// Append a caller-typed `(field, expr)` after the existing fields. The supplied
    /// [`StructField`] declares both the new column's name and its full nullability +
    /// data type. The expression is bidirectionally checked against the input schema
    /// with `field.data_type()` as the expected output -- see
    /// `schema_expr::check::check_expression` for the supported rules (including
    /// `MapToStruct` and `Struct`, which require an expected type from context).
    pub fn append_col_typed(
        self,
        field: StructField,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        self.apply_field_op(FieldOp::append(ColumnName::default(), field, expr.into()))
    }

    /// Insert `leaf` immediately after `sibling`'s position in its parent struct.
    ///
    /// `sibling`'s prefix (all components except the last) names the parent struct;
    /// `leaf.name()` is the new column's name. Errors if `sibling` doesn't resolve, if
    /// the parent isn't a struct, or if `leaf.name()` collides with an existing sibling.
    /// `sibling` accepts any [`IntoColumnName`] value -- a `&str` for top-level fields,
    /// a tuple/array or `column_name!` macro for nested paths.
    pub fn insert_col_after(
        self,
        sibling: impl IntoColumnName,
        leaf: StructField,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        self.apply_field_op(FieldOp::insert_after_sibling(
            sibling.into_column_name(),
            leaf,
            expr.into(),
        )?)
    }

    /// Replace the field at `path` with `new_field` (allowing rename + retype) and
    /// substitute `new_expr` in its place.
    ///
    /// The path's leaf and `new_field.name()` may differ (rename) or match (in-place
    /// retype). Errors if `path` doesn't resolve, if its parent isn't a struct, or if a
    /// rename would collide with an existing sibling. `path` accepts any
    /// [`IntoColumnName`] value.
    pub fn replace_col(
        self,
        path: impl IntoColumnName,
        new_field: StructField,
        new_expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        self.apply_field_op(FieldOp::replace(
            path.into_column_name(),
            new_field,
            new_expr.into(),
        )?)
    }

    /// Remove the field at `path` from its parent struct.
    ///
    /// Errors if `path` doesn't resolve or if its parent isn't a struct. `path` accepts
    /// any [`IntoColumnName`] value -- a `&str` for top-level fields, a tuple/array or
    /// `column_name!` macro for nested paths.
    pub fn drop_col(self, path: impl IntoColumnName) -> Result<Self, DeltaError> {
        self.apply_field_op(FieldOp::drop_(path.into_column_name())?)
    }

    // === Load / aggregate / join / union =====================================

    /// File-reader transform. Output schema is `node.file_schema` plus a field per
    /// `passthrough_columns` entry (each field's type is taken from the input schema).
    /// Computation is shared with the datafusion lowering path via
    /// `schema_expr::field_op::load_output_schema`.
    pub fn load(self, node: LoadNode) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let output_schema =
            load_output_schema(&node.file_schema, &node.passthrough_columns, &input_schema)?;
        self.push_unary(NodeKind::Load(node), output_schema)
    }

    /// Top-1-per-group aggregate ordered by `version` descending. The group-by expressions
    /// and the version column are aggregation-internal: the output projects only the
    /// listed `value_columns`, lifted from the input schema in declared order.
    ///
    /// The validator ensures every name in `value_columns` resolves in the input.
    /// Group-by expressions and `version` are type-checked against the input schema
    /// (well-formedness + column-ref resolution) via
    /// [`schema_expr::check::validate_exprs`]; their inferred types are discarded
    /// since they do not contribute to the output.
    ///
    /// [`schema_expr::check::validate_exprs`]: crate::plans::schema_expr::check::validate_exprs
    pub fn max_by_version(
        self,
        group_by: impl IntoIterator<Item = impl Into<ExpressionRef>>,
        version: impl Into<ExpressionRef>,
        value_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DeltaError> {
        let group_by: Vec<ExpressionRef> = group_by.into_iter().map(Into::into).collect();
        let version_column: ExpressionRef = version.into();
        let value_columns: Vec<String> = value_columns.into_iter().map(Into::into).collect();
        let input_schema = self.schema()?;
        validate_exprs(
            &input_schema,
            group_by.iter().cloned().chain([version_column.clone()]),
        )?;
        let output_schema = narrow_schema_to(&input_schema, &value_columns, "max_by_version")?;
        let node = MaxByVersionNode {
            group_by,
            version_column,
            value_columns,
        };
        self.push_unary(NodeKind::MaxByVersion(node), output_schema)
    }

    /// Left-anti join: rows of `self` whose key matches no row of `other`. Output schema
    /// mirrors `self`.
    pub fn left_anti_join(
        self,
        other: PlanBuilder,
        key_pairs: impl IntoIterator<Item = (impl Into<ExpressionRef>, impl Into<ExpressionRef>)>,
    ) -> Result<Self, DeltaError> {
        let key_pairs: Vec<(ExpressionRef, ExpressionRef)> = key_pairs
            .into_iter()
            .map(|(l, r)| (l.into(), r.into()))
            .collect();
        // Validate each key pair against its source schema up front. This is a hot path
        // for reconciliation (one anti-join per FSR plan) so catching bad column refs at
        // plan-build time avoids a confusing runtime error from the executor.
        let left_schema = self.schema()?;
        let right_schema = other.schema()?;
        validate_exprs(&left_schema, key_pairs.iter().map(|(l, _)| l.clone()))?;
        validate_exprs(&right_schema, key_pairs.iter().map(|(_, r)| r.clone()))?;
        let output_schema = left_schema;
        let node = EquiJoinNode {
            kind: JoinKind::LeftAnti,
            key_pairs,
        };
        self.push_nary(NodeKind::EquiJoin(node), &[&other], output_schema)
    }

    /// Unordered union with one or more other cursors. All inputs must share the same
    /// schema (strict equality).
    pub fn union_all(self, others: &[PlanBuilder]) -> Result<Self, DeltaError> {
        self.union(others, false)
    }

    /// Order-preserving union with one or more other cursors. All inputs must share the
    /// same schema.
    pub fn union_ordered(self, others: &[PlanBuilder]) -> Result<Self, DeltaError> {
        self.union(others, true)
    }

    /// Shared body: validate per-input schema equality, then push `NodeKind::Union`.
    fn union(self, others: &[PlanBuilder], ordered: bool) -> Result<Self, DeltaError> {
        let first_schema = self.schema()?;
        for o in others {
            if o.schema()? != first_schema {
                return Err(pb_err!("union: input schemas disagree").into());
            }
        }
        let others_refs: Vec<&PlanBuilder> = others.iter().collect();
        self.push_nary(
            NodeKind::Union(UnionNode { ordered }),
            &others_refs,
            first_schema,
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{col, lit, Expression, Predicate};
    use crate::plans::ir::plan::JoinKind;
    use crate::plans::kernel_reducers::{
        FinishedHandle, KdfControl, KernelReducerKind, KernelReducerOutput,
    };
    use crate::plans::state_machines::framework::coroutine::CoroutineSM;
    use crate::plans::state_machines::framework::state_machine::{
        EngineResponse, NextStep, StateMachine,
    };
    use crate::schema::{DataType, StructType};
    use crate::{DeltaResult, EngineData};

    #[derive(Debug, Clone, Copy)]
    enum FieldOpRejectCase {
        InsertAfterMissingSibling,
        InsertAfterNameCollision,
        ReplaceColMissingPath,
        DropColMissingPath,
        SelectMissingField,
        MaxByVersionUnknownValueColumn,
    }

    /// Type-annotated empty rows literal for `Context::values` in tests that exercise
    /// builder shape only (and don't care about row data). Avoids the
    /// generic-inference failure of bare `vec![]` against `values`'s `impl IntoIterator`
    /// signature.
    fn no_rows() -> Vec<Vec<Scalar>> {
        vec![]
    }

    fn id_ts_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::STRING),
                StructField::nullable("ts", DataType::LONG),
            ])
            .unwrap(),
        )
    }

    /// Sources mint sequential Refs with the declared output schema.
    #[test]
    fn sources_mint_refs_and_record_schemas() {
        let ctx = Context::new();
        let v = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let j = ctx.scan_json(vec![], id_ts_schema()).unwrap();
        let p = ctx.scan_parquet(vec![], id_ts_schema()).unwrap();
        assert_eq!(v.ref_id(), Ref(0));
        assert_eq!(j.ref_id(), Ref(1));
        assert_eq!(p.ref_id(), Ref(2));
        assert_eq!(*v.schema().unwrap(), *id_ts_schema());
    }

    /// `into_result_plan` runs DCE and returns ResultPlan with the surviving stmts.
    #[test]
    fn into_result_plan_runs_dce() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let dead = ctx.values(id_ts_schema(), no_rows()).unwrap(); // unreachable
        let kept = src.filter(col("id").is_not_null()).unwrap();
        // Drop the dead branch handle so try_unwrap works and DCE removes it.
        drop(dead);
        let result_plan = ctx.into_result_plan(kept).unwrap();
        let outputs: Vec<u32> = result_plan.plan.stmts.iter().map(|s| s.output.0).collect();
        assert_eq!(outputs, vec![0, 2]); // src=0 + filter=2; dead values=1 dropped
        assert_eq!(result_plan.result, Ref(2));
    }

    /// Filter passes through schema unchanged.
    #[test]
    fn filter_preserves_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let f = src.filter(Arc::new(Predicate::column(["id"]))).unwrap();
        assert_eq!(*f.schema().unwrap(), *id_ts_schema());
    }

    /// `project` derives output schema from inferred expression types.
    #[test]
    fn project_infers_output_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let projected = src
            .project([("id_out", col("id")), ("ts_out", col("ts"))])
            .unwrap();
        let schema = projected.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id_out", "ts_out"]);
        assert_eq!(
            schema.field("id_out").unwrap().data_type(),
            &DataType::STRING
        );
        assert_eq!(schema.field("ts_out").unwrap().data_type(), &DataType::LONG);
    }

    /// `project_with_schema` honors the caller-supplied output schema and reports a
    /// length mismatch when exprs vs fields disagree.
    #[test]
    fn project_with_schema_validates_length_and_uses_caller_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();

        let target = Arc::new(
            StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap(),
        );
        let p = src
            .clone()
            .project_with_schema([col("id")], Arc::clone(&target))
            .unwrap();
        assert_eq!(*p.schema().unwrap(), *target);

        let err = src
            .project_with_schema([col("id"), col("ts")], target)
            .unwrap_err();
        assert!(
            err.to_string().contains("projection arity mismatch"),
            "got: {err}"
        );
    }

    // === Point-edit primitives over nested paths ============================

    /// Schema with an `add` struct, used to exercise nested point-edits.
    fn nested_add_schema() -> SchemaRef {
        let add = StructType::try_new(vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
        ])
        .unwrap();
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("commitInfo", DataType::STRING),
                StructField::nullable("add", add),
                StructField::nullable("remove", DataType::STRING),
            ])
            .unwrap(),
        )
    }

    /// Top-level `insert_col_after` keeps existing fields and appends the new leaf
    /// immediately after the sibling.
    #[test]
    fn insert_col_after_top_level_inserts_after_sibling() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let out = src
            .insert_col_after(
                "id",
                StructField::nullable("mid", DataType::LONG),
                col("ts"),
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "mid", "ts"]);
        assert_eq!(schema.field("mid").unwrap().data_type(), &DataType::LONG);
    }

    /// `insert_col_after` walks into a nested struct and inserts `add.stats_parsed`
    /// after `add.stats`. The expression is a `Struct` whose output type the builder
    /// derives bidirectionally from `parsed_field.data_type()`.
    #[test]
    fn insert_col_after_nested_inserts_in_parent_struct() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), no_rows()).unwrap();
        let stats_struct =
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap();
        let parsed_field = StructField::nullable("stats_parsed", stats_struct.clone());
        let parsed_expr = Expression::struct_from([lit(0i64)]);
        let out = src
            .insert_col_after(["add", "stats"], parsed_field, parsed_expr)
            .unwrap();
        let schema = out.schema().unwrap();
        let DataType::Struct(add) = schema.field("add").unwrap().data_type() else {
            panic!("expected struct");
        };
        let names: Vec<&str> = add.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["path", "stats", "stats_parsed"]);
        assert_eq!(
            add.field("stats_parsed").unwrap().data_type(),
            &DataType::from(stats_struct),
        );
    }

    /// `insert_col_after` errors when the sibling does not exist at the requested path,
    /// when the new leaf collides with an existing sibling, when `replace_col` / `drop_col`
    /// paths do not resolve, when `select` targets a missing field, or when
    /// `max_by_version` lists an unknown value column.
    #[rstest]
    #[case::insert_after_missing_sibling(FieldOpRejectCase::InsertAfterMissingSibling, "missing")]
    #[case::insert_after_name_collision(
        FieldOpRejectCase::InsertAfterNameCollision,
        "already exists"
    )]
    #[case::replace_col_missing_path(FieldOpRejectCase::ReplaceColMissingPath, "missing")]
    #[case::drop_col_missing_path(FieldOpRejectCase::DropColMissingPath, "missing")]
    #[case::select_missing_field(FieldOpRejectCase::SelectMissingField, "missing")]
    #[case::max_by_version_unknown_value_column(
        FieldOpRejectCase::MaxByVersionUnknownValueColumn,
        "missing"
    )]
    fn field_op_rejects_invalid(#[case] case: FieldOpRejectCase, #[case] needle: &str) {
        let ctx = Context::new();
        let err = match case {
            FieldOpRejectCase::InsertAfterMissingSibling => {
                let src = ctx.values(nested_add_schema(), no_rows()).unwrap();
                src.insert_col_after(
                    ["add", "missing"],
                    StructField::nullable("x", DataType::LONG),
                    lit(0i64),
                )
                .unwrap_err()
            }
            FieldOpRejectCase::InsertAfterNameCollision => {
                let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
                src.insert_col_after("id", StructField::nullable("ts", DataType::LONG), lit(0i64))
                    .unwrap_err()
            }
            FieldOpRejectCase::ReplaceColMissingPath => {
                let src = ctx.values(nested_add_schema(), no_rows()).unwrap();
                src.replace_col(
                    ["add", "missing"],
                    StructField::nullable("missing", DataType::STRING),
                    col(["add", "path"]),
                )
                .unwrap_err()
            }
            FieldOpRejectCase::DropColMissingPath => {
                let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
                src.drop_col("missing").unwrap_err()
            }
            FieldOpRejectCase::SelectMissingField => {
                let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
                let target = Arc::new(
                    StructType::try_new(vec![StructField::nullable("missing", DataType::STRING)])
                        .unwrap(),
                );
                src.select(target).unwrap_err()
            }
            FieldOpRejectCase::MaxByVersionUnknownValueColumn => {
                let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
                src.max_by_version([col("id")], col("ts"), ["missing"])
                    .unwrap_err()
            }
        };
        assert!(err.to_string().contains(needle), "got: {err}");
    }

    /// `replace_col` retypes a field in place when the new field's name matches.
    #[test]
    fn replace_col_in_place_retypes_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let out = src
            .replace_col(
                "ts",
                StructField::nullable("ts", DataType::STRING),
                col("id"),
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "ts"]);
        assert_eq!(schema.field("ts").unwrap().data_type(), &DataType::STRING);
    }

    /// `replace_col` with a nested path renames + retypes a child field, walking the
    /// parent struct with `col(...)` references for unchanged siblings.
    #[test]
    fn replace_col_nested_renames_and_retypes() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), no_rows()).unwrap();
        let stats_struct =
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap();
        let new_field = StructField::nullable("stats_parsed", stats_struct.clone());
        let new_expr = Expression::struct_from([lit(0i64)]);
        let out = src
            .replace_col(["add", "stats"], new_field, new_expr)
            .unwrap();
        let schema = out.schema().unwrap();
        let DataType::Struct(add) = schema.field("add").unwrap().data_type() else {
            panic!("expected struct");
        };
        let names: Vec<&str> = add.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["path", "stats_parsed"]);
        assert_eq!(
            add.field("stats_parsed").unwrap().data_type(),
            &DataType::from(stats_struct),
        );
    }

    /// `drop_col` removes a top-level field.
    #[test]
    fn drop_col_top_level_removes_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let out = src.drop_col("ts").unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id"]);
    }

    /// `left_anti_join` output schema mirrors the left input.
    #[test]
    fn left_anti_join_output_mirrors_left() {
        let ctx = Context::new();
        let l = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let r_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let r = ctx.values(r_schema, no_rows()).unwrap();

        let anti = l.left_anti_join(r, [(col("id"), col("rid"))]).unwrap();
        assert_eq!(*anti.schema().unwrap(), *id_ts_schema());
    }

    /// `union_all` validates all input schemas match.
    #[test]
    fn union_all_requires_matching_schemas() {
        let ctx = Context::new();
        let a = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let b = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let unioned = a.clone().union_all(&[b]).unwrap();
        assert_eq!(*unioned.schema().unwrap(), *id_ts_schema());

        let other_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("x", DataType::STRING)]).unwrap(),
        );
        let bad = ctx.values(other_schema, no_rows()).unwrap();
        let err = a.union_all(&[bad]).unwrap_err();
        assert!(err.to_string().contains("disagree"), "got: {err}");
    }

    /// Cursors from different contexts cannot be combined.
    #[test]
    fn cross_context_cursors_rejected() {
        let ctx_a = Context::new();
        let ctx_b = Context::new();
        let a = ctx_a.values(id_ts_schema(), no_rows()).unwrap();
        let b = ctx_b.values(id_ts_schema(), no_rows()).unwrap();
        let err = a.left_anti_join(b, [(col("id"), col("id"))]).unwrap_err();
        assert!(err.to_string().contains("different context"), "got: {err}");
    }

    /// `into_result_plan` fails when other cursors still hold the state Rc.
    #[test]
    fn into_result_plan_fails_with_outstanding_cursor() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let _other = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let err = ctx.into_result_plan(src).unwrap_err();
        assert!(
            err.to_string().contains("outstanding PlanBuilder"),
            "got: {err}",
        );
    }

    /// `max_by_version` output schema is exactly `value_columns` lifted from the input
    /// (group_by exprs and version are aggregation-internal and not surfaced).
    #[test]
    fn max_by_version_output_schema_is_value_columns_only() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let out = src.max_by_version([col("id")], col("ts"), ["ts"]).unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["ts"]);
        assert_eq!(schema.field("ts").unwrap().data_type(), &DataType::LONG);
    }

    // === CoroutineSM dispatch tests ============================================

    /// Test KernelReducer that echoes its constructor input as the output value.
    #[derive(Debug, Clone)]
    struct EchoReducer {
        value: i64,
    }

    impl KernelReducer for EchoReducer {
        fn kind(&self) -> KernelReducerKind {
            KernelReducerKind::CheckpointHint
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
        fn apply(&mut self, _batch: &dyn EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }

    impl KernelReducerOutput for EchoReducer {
        type Output = i64;
        fn into_output(self) -> Result<i64, DeltaError> {
            Ok(self.value)
        }
    }

    /// `reduce` yields a `EngineRequest::Reduce` with the DCE'd stmts and the builder's terminal
    /// Ref, the engine resumes with the reducer's finished handle, and the SM body
    /// recovers the typed output.
    #[test]
    fn reduce_yields_step_reduce_and_recovers_typed_output() {
        let mut sm = CoroutineSM::<i64>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let _dead = ctx.values(id_ts_schema(), no_rows()).unwrap();
            let builder = ctx
                .values(id_ts_schema(), no_rows())
                .unwrap()
                .filter(Arc::new(col("id").is_not_null()))
                .unwrap();
            ctx.reduce(&mut engine, builder, EchoReducer { value: 7 }, "drain")
                .await
        })
        .unwrap();

        // First yielded step is the Reduce.
        assert_eq!(sm.step_name(), "drain");
        let token = match sm.get_step().unwrap() {
            EngineRequest::Reduce {
                stmts,
                terminal,
                sink,
            } => {
                // DCE: only `values(reachable)` and `filter` remain; `_dead` is dropped.
                let outputs: Vec<u32> = stmts.iter().map(|s| s.output.0).collect();
                assert_eq!(outputs, vec![1, 2]);
                assert_eq!(terminal, Ref(2));
                sink.token.clone()
            }
            other => panic!("expected EngineRequest::Reduce, got {other:?}"),
        };

        // Engine drains the sink and resumes with a finished handle.
        let payload = EngineResponse::Reducer(FinishedHandle {
            token,
            erased: Box::new(EchoReducer { value: 7 }),
        });
        let next = sm.submit(Ok(payload)).unwrap();
        match next {
            NextStep::Done(v) => assert_eq!(v, 7),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// `Context::reduce` resets the Ref counter so the next source minted after
    /// the reduce boundary starts at `Ref(0)` again. The full plan ships to the
    /// engine and the session-id bump invalidates any held cursors, so reusing
    /// Ref ids across the boundary is safe and keeps post-reduce Refs compact.
    #[test]
    fn reduce_resets_ref_counter() {
        let mut sm = CoroutineSM::<u32>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let v0 = ctx.values(id_ts_schema(), no_rows()).unwrap();
            let v1 = v0.filter(Arc::new(col("id").is_not_null())).unwrap();
            assert_eq!(v1.ref_id(), Ref(1));
            let _: i64 = ctx
                .reduce(&mut engine, v1, EchoReducer { value: 0 }, "drain")
                .await?;
            let post = ctx.values(id_ts_schema(), no_rows()).unwrap();
            Ok(post.ref_id().0)
        })
        .unwrap();

        let token = match sm.get_step().unwrap() {
            EngineRequest::Reduce { sink, .. } => sink.token.clone(),
            other => panic!("expected EngineRequest::Reduce, got {other:?}"),
        };
        let payload = EngineResponse::Reducer(FinishedHandle {
            token,
            erased: Box::new(EchoReducer { value: 0 }),
        });
        match sm.submit(Ok(payload)).unwrap() {
            NextStep::Done(v) => assert_eq!(v, 0, "post-reduce Ref must be 0"),
            other => panic!("expected Done, got {other:?}"),
        }
    }

    /// Resuming a `EngineRequest::Reduce` with a non-Reducer payload surfaces an internal
    /// error -- the executor produced a payload that doesn't match the yielded step.
    #[test]
    fn reduce_resume_with_wrong_payload_variant_errors() {
        let mut sm = CoroutineSM::<i64>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let builder = ctx.values(id_ts_schema(), no_rows()).unwrap();
            ctx.reduce(&mut engine, builder, EchoReducer { value: 1 }, "drain")
                .await
        })
        .unwrap();
        let _ = sm.get_step().unwrap();
        let err = sm
            .submit(Ok(EngineResponse::Schema(id_ts_schema())))
            .unwrap_err();
        assert!(
            err.to_string().contains("non-Reducer payload"),
            "got: {err}",
        );
    }

    /// `schema_query` yields a `EngineRequest::SchemaQuery`, the engine resumes with a schema,
    /// and the SM body receives it.
    #[test]
    fn schema_query_yields_step_schemaquery_and_returns_schema() {
        let target_schema = id_ts_schema();
        let target_clone = Arc::clone(&target_schema);
        let mut sm = CoroutineSM::<bool>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let s = ctx
                .schema_query(&mut engine, "/cp.parquet", "footer")
                .await?;
            Ok(s == target_clone)
        })
        .unwrap();

        assert_eq!(sm.step_name(), "footer");
        match sm.get_step().unwrap() {
            EngineRequest::SchemaQuery(node) => assert_eq!(node.file_path, "/cp.parquet"),
            other => panic!("expected SchemaQuery, got {other:?}"),
        }

        match sm
            .submit(Ok(EngineResponse::Schema(target_schema)))
            .unwrap()
        {
            NextStep::Done(v) => assert!(v),
            _ => panic!("expected Done(true)"),
        }
    }

    /// Resuming a `EngineRequest::SchemaQuery` with a non-Schema payload surfaces an internal
    /// error -- the executor produced a payload that doesn't match the yielded step.
    #[test]
    fn schema_query_resume_with_wrong_payload_variant_errors() {
        let mut sm = CoroutineSM::<()>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let _ = ctx
                .schema_query(&mut engine, "/x.parquet", "footer")
                .await?;
            Ok(())
        })
        .unwrap();
        let _ = sm.get_step().unwrap();
        let err = sm.submit(Ok(EngineResponse::Empty)).unwrap_err();
        assert!(err.to_string().contains("non-Schema payload"), "got: {err}",);
    }

    /// After `schema_query` bumps the session, cursors built before the dispatch are
    /// stale and operations on them fail.
    #[test]
    fn schema_query_invalidates_pre_dispatch_cursors() {
        let target_schema = id_ts_schema();
        let mut sm = CoroutineSM::<()>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let pre = ctx.values(id_ts_schema(), no_rows()).unwrap();
            let _ = ctx
                .schema_query(&mut engine, "/x.parquet", "footer")
                .await?;
            // Using `pre` after dispatch must fail with a stale-builder error.
            let err = pre.filter(Arc::new(Predicate::column(["id"]))).unwrap_err();
            assert!(err.to_string().contains("stale builder"), "got: {err}");
            Ok(())
        })
        .unwrap();

        // Drive through the schema_query yield.
        let _ = sm.get_step().unwrap();
        match sm
            .submit(Ok(EngineResponse::Schema(target_schema)))
            .unwrap()
        {
            NextStep::Done(()) => {}
            _ => panic!("expected Done"),
        }
    }

    /// Plan stmt for `left_anti_join` records `[left, right]` as inputs in that order.
    #[test]
    fn left_anti_join_records_left_right_inputs() {
        let ctx = Context::new();
        let left = ctx.values(id_ts_schema(), no_rows()).unwrap();
        let right_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let right = ctx.values(right_schema, no_rows()).unwrap();
        let left_ref = left.ref_id();
        let right_ref = right.ref_id();
        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("rid")))];
        let joined = left.left_anti_join(right, key).unwrap();
        let joined_ref = joined.ref_id();
        let plan = ctx.into_result_plan(joined).unwrap();
        let stmt = plan.plan.node(joined_ref).unwrap();
        assert_eq!(stmt.inputs, vec![left_ref, right_ref]);
        assert!(matches!(
            stmt.kind,
            NodeKind::EquiJoin(EquiJoinNode {
                kind: JoinKind::LeftAnti,
                ..
            })
        ));
    }
}
