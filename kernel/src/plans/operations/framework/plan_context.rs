//! Plan-construction context and cursor (SSA builder).
//!
//! [`Context`] owns the in-flight SSA program and a per-Ref schema table. [`Cursor`] is a
//! shared handle to a specific [`Ref`] in that program; cursor methods append new
//! [`Stmt`]s and return new [`Cursor`]s threading the freshly minted Refs. Cursors are
//! [`Clone`] (cheap `Rc` clone) so branching is explicit.
//!
//! State is shared via `Rc<RefCell<ContextState>>` so transform methods can mutate without
//! requiring `&mut Context`. SMs are CPU-only sequencers and never need to cross thread
//! boundaries, so `Rc` (vs. `Arc`) is the right shape. See the
//! [`Coroutine`](crate::plans::operations::framework::coroutine::driver::Coroutine) module
//! docs for the `!Send` rationale.
//!
//! # Stale-cursor protection
//!
//! Each cursor captures the context's `session_id` at mint time. Dispatch methods
//! ([`Context::consume`], [`Context::schema_query`]) bump the id to invalidate cursors
//! held across yields; [`Context::into_result_plan`] consumes `self`, which moots the
//! concern for terminal dispatch.
//!
//! # `RefCell` discipline
//!
//! Cursor methods borrow_mut, mutate state, and drop the guard before returning. Dispatch
//! methods drop the borrow before any `co.yield_(...).await`. Holding a `Ref` / `RefMut`
//! across an await would panic at runtime; this is structurally avoided by every method
//! in this module.
//!
//! [`Stmt`]: crate::plans::ir::ssa::Stmt
//! [`Ref`]: crate::plans::ir::ssa::Ref

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use url::Url;

use crate::expressions::{ColumnName, Expression, ExpressionRef, PredicateRef, Scalar};
use crate::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
use crate::plans::ir::nodes::{ConsumeSink, DvRef, FileType, ScanFileColumns};
use crate::plans::ir::schema_inference::infer_expression_type;
use crate::plans::ir::ssa::{JoinKind, Node, Plan, Ref, ResultPlan};
use crate::plans::kernel_consumers::{Extractor, KernelConsumer, KernelConsumerOutput};
use crate::plans::operations::framework::coroutine::context::{StepCo, StepResume, StepYield};
use crate::plans::operations::framework::step::{SchemaQueryNode, Step};
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{delta_error, FileMeta};

// ============================================================================
// Shared state
// ============================================================================

/// In-flight SSA being built, the per-Ref schema table, and a session counter.
#[derive(Debug)]
struct ContextState {
    plan: Plan,
    /// Per-Ref output schemas. Indexed by `Ref.0` during construction (Refs are
    /// minted sequentially by [`Plan::push`]).
    ref_schemas: Vec<SchemaRef>,
    /// Bumped by dispatch methods to invalidate cursors held across yields.
    session_id: u32,
}

// ============================================================================
// Context
// ============================================================================

/// Plan-construction context.
///
/// Source methods mint root [`Cursor`]s, dispatch methods
/// ([`Self::consume`] / [`Self::schema_query`]) yield steps through a coroutine, and
/// [`Self::into_result_plan`] is the terminal sync drain (backward-reachability DCE).
pub struct Context {
    state: Rc<RefCell<ContextState>>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// Construct a fresh context with an empty plan.
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(ContextState {
                plan: Plan::new(),
                ref_schemas: Vec::new(),
                session_id: 0,
            })),
        }
    }

    /// Append a [`Node::ListFiles`] source. Output schema is the engine-canonical listing
    /// shape supplied by the caller (the kernel does not pin it; consumers wire to the
    /// engine's listing schema).
    pub fn list_files(
        &self,
        start_from: Url,
        listing_schema: SchemaRef,
    ) -> Result<Cursor, DeltaError> {
        push_source(&self.state, Node::ListFiles { start_from }, listing_schema)
    }

    /// Append a [`Node::ReadJson`] source. Output schema is the caller-declared `schema`.
    pub fn scan_json(&self, files: Vec<FileMeta>, schema: SchemaRef) -> Result<Cursor, DeltaError> {
        let s = Arc::clone(&schema);
        push_source(&self.state, Node::ReadJson { files, schema }, s)
    }

    /// Append a [`Node::ReadParquet`] source. Output schema is the caller-declared `schema`.
    pub fn scan_parquet(
        &self,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> Result<Cursor, DeltaError> {
        let s = Arc::clone(&schema);
        push_source(&self.state, Node::ReadParquet { files, schema }, s)
    }

    /// Append a [`Node::Values`] source.
    pub fn values(&self, schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> Result<Cursor, DeltaError> {
        let s = Arc::clone(&schema);
        push_source(&self.state, Node::Values { schema, rows }, s)
    }

    /// Drain the accumulator into a [`Step::Consume`] yielded through `co` and recover
    /// the consumer's typed output via [`Extractor`].
    ///
    /// Backward-reachability DCE narrows the SSA program to stmts transitively reachable
    /// from `cursor`. After the yield resumes, the accumulator is reset (empty plan +
    /// empty schema table) and `session_id` is bumped so any cursors held across the
    /// boundary fail at runtime.
    ///
    /// Borrows the shared state only briefly (mutation + drain) and drops the borrow
    /// before the awaited yield, so the await never holds a [`RefMut`](std::cell::RefMut).
    // Currently exercised only by tests; PR6 wires it into FSR.
    #[allow(dead_code)]
    pub(crate) async fn consume<S>(
        &self,
        co: &mut StepCo,
        cursor: Cursor,
        consumer: S,
        step_name: &'static str,
    ) -> Result<S::Output, DeltaError>
    where
        S: KernelConsumer + KernelConsumerOutput + 'static,
    {
        let sink = ConsumeSink::new_consumer(consumer);
        let extractor = Extractor::for_consumer::<S>(sink.token.clone());
        let terminal = cursor.ref_id;
        let stmts: Vec<crate::plans::ir::ssa::Stmt> = {
            let mut s = borrow_state_mut(&self.state);
            ensure_fresh(&s, &cursor)?;
            let plan = std::mem::take(&mut s.plan);
            s.ref_schemas.clear();
            s.session_id = s.session_id.wrapping_add(1);
            plan.reachable_from(terminal).stmts
        };
        // Drop the cursor handle so the only remaining Rc on `state` is `self`'s.
        drop(cursor);

        let StepResume(result) = co
            .yield_(StepYield {
                operation: Step::Consume {
                    stmts,
                    terminal,
                    sink,
                },
                step_name,
                live_relations: Vec::new(),
            })
            .await;
        let step_result = result.map_err(|e| e.into_delta_typed())?;
        extractor
            .extract(&step_result)
            .map_err(|e| e.into_delta_typed())
    }

    /// Yield a [`Step::SchemaQuery`] and return the schema delivered by the engine.
    ///
    /// Bumps `session_id` so any cursors held across the boundary fail at runtime.
    /// The accumulator is preserved -- callers typically use the returned schema to
    /// build fresh sources, and old refs become unreachable from new cursors (the next
    /// `consume`'s DCE prunes them).
    // Currently exercised only by tests; PR6 wires it into FSR.
    #[allow(dead_code)]
    pub(crate) async fn schema_query(
        &self,
        co: &mut StepCo,
        path: impl Into<String>,
        step_name: &'static str,
    ) -> Result<SchemaRef, DeltaError> {
        {
            let mut s = borrow_state_mut(&self.state);
            s.session_id = s.session_id.wrapping_add(1);
        }
        let StepResume(result) = co
            .yield_(StepYield {
                operation: Step::SchemaQuery(SchemaQueryNode::new(path.into())),
                step_name,
                live_relations: Vec::new(),
            })
            .await;
        let step_result = result.map_err(|e| e.into_delta_typed())?;
        step_result.take_schema()
    }

    /// Drain the accumulator into a [`ResultPlan`] keyed at `cursor`. Performs
    /// backward-reachability DCE; only stmts transitively reachable from
    /// `cursor.ref_id()` survive.
    ///
    /// Returns an error if `cursor` is from a different context (or stale across a
    /// dispatch). All cursors must be dropped before this call -- the context state is
    /// solely owned at terminal time.
    pub fn into_result_plan(self, cursor: Cursor) -> Result<ResultPlan, DeltaError> {
        // Validate session before tearing down. Drop the borrow before `Rc::try_unwrap`.
        {
            let s = borrow_state(&self.state);
            ensure_fresh(&s, &cursor)?;
        }
        // Drop the cursor's Rc handle so try_unwrap can succeed.
        drop(cursor.state);
        let result = cursor.ref_id;
        let inner = Rc::try_unwrap(self.state)
            .map(|cell| cell.into_inner())
            .map_err(|_| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "into_result_plan: outstanding Cursor handles -- drop other Cursors before \
                     calling this method",
                )
            })?;
        let plan = inner.plan.reachable_from(result);
        Ok(ResultPlan { plan, result })
    }
}

// ============================================================================
// Cursor
// ============================================================================

/// Handle to a Ref in the in-flight SSA program. Cursor methods append further
/// stmts and return new Cursors threading the minted Refs.
#[derive(Clone, Debug)]
pub struct Cursor {
    state: Rc<RefCell<ContextState>>,
    ref_id: Ref,
    session_id: u32,
}

impl Cursor {
    /// The SSA Ref this cursor points at.
    pub fn ref_id(&self) -> Ref {
        self.ref_id
    }

    /// The schema of the value at this Ref.
    ///
    /// Returns an error if the cursor is stale.
    pub fn schema(&self) -> Result<SchemaRef, DeltaError> {
        let state = borrow_state(&self.state);
        ensure_fresh(&state, self)?;
        state
            .ref_schemas
            .get(self.ref_id.0 as usize)
            .cloned()
            .ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "schema: ref {ref_id} has no recorded schema",
                    ref_id = self.ref_id.0,
                )
            })
    }

    // === Filter / Project ====================================================

    /// Append a [`Node::Filter`]. Output schema is unchanged.
    pub fn filter(self, predicate: PredicateRef) -> Result<Self, DeltaError> {
        let schema = self.schema()?;
        push_unary(&self.state, &self, Node::Filter { predicate }, schema)
    }

    /// Append a [`Node::Project`] with inferred output schema.
    ///
    /// Each pair becomes one output field `(name, infer_type(expr))`. Inference is
    /// narrow -- unsupported expressions error out and direct callers to
    /// [`Self::project_with_schema`].
    pub fn project<I, K, V>(self, named_exprs: I) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<ExpressionRef>,
    {
        let input_schema = self.schema()?;
        let pairs: Vec<(String, Arc<Expression>)> = named_exprs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let mut fields = Vec::with_capacity(pairs.len());
        for (name, expr) in &pairs {
            let ty = infer_expression_type(expr.as_ref(), &input_schema)?;
            fields.push(StructField::nullable(name.clone(), ty));
        }
        let output_schema: SchemaRef = arc_struct_or_invariant(fields)?;
        push_unary(
            &self.state,
            &self,
            Node::Project { named_exprs: pairs },
            output_schema,
        )
    }

    /// Append a [`Node::Project`] with caller-supplied output schema. Positional:
    /// `exprs[i]` becomes `schema.fields()[i]`. No type inference -- the schema declares
    /// names and types directly. Use when both lists are produced together (e.g. physical
    /// to logical column mapping).
    pub fn project_with_schema(
        self,
        exprs: Vec<ExpressionRef>,
        schema: SchemaRef,
    ) -> Result<Self, DeltaError> {
        let field_count = schema.fields().count();
        if exprs.len() != field_count {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "project_with_schema: expected {field_count} expressions, got {got}",
                got = exprs.len(),
            ));
        }
        let pairs: Vec<(String, Arc<Expression>)> = schema
            .fields()
            .map(|f| f.name().clone())
            .zip(exprs)
            .collect();
        push_unary(
            &self.state,
            &self,
            Node::Project { named_exprs: pairs },
            schema,
        )
    }

    // === Schema-edit sugar (over project) ====================================

    /// Identity-by-name projection: every field in `schema` becomes `col(name)` against
    /// the input. Validates that each name exists in the input and that types match.
    pub fn select(self, schema: SchemaRef) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let pairs: Vec<(String, Arc<Expression>)> = schema
            .fields()
            .map(|f| {
                let name = f.name().clone();
                let input_field = input_schema.field(&name).ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "select: field {name:?} not present in input schema",
                    )
                })?;
                if input_field.data_type() != f.data_type() {
                    return Err(delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "select: field {name:?} type mismatch -- input {input:?} vs target {target:?}",
                        input = input_field.data_type(),
                        target = f.data_type(),
                    ));
                }
                Ok((name.clone(), Arc::new(Expression::column([&name]))))
            })
            .collect::<Result<_, DeltaError>>()?;
        push_unary(
            &self.state,
            &self,
            Node::Project { named_exprs: pairs },
            schema,
        )
    }

    /// Narrow to a subset of input fields by name. Output schema mirrors input field
    /// types in the order listed.
    pub fn select_columns<I, S>(self, names: I) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let input_schema = self.schema()?;
        let mut pairs: Vec<(String, Arc<Expression>)> = Vec::new();
        let mut fields: Vec<StructField> = Vec::new();
        for name in names {
            let name = name.as_ref().to_string();
            let f = input_schema.field(&name).ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "select_columns: field {name:?} not in input schema",
                )
            })?;
            fields.push(f.clone());
            pairs.push((name.clone(), Arc::new(Expression::column([name]))));
        }
        let output_schema = arc_struct_or_invariant(fields)?;
        push_unary(
            &self.state,
            &self,
            Node::Project { named_exprs: pairs },
            output_schema,
        )
    }

    /// Append `(name, expr)` after the existing fields.
    pub fn append_col(
        self,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let name = name.into();
        let expr = expr.into();
        let schema = self.schema()?;
        let mut pairs = identity_named_exprs(&schema);
        pairs.push((name, expr));
        self.project(pairs)
    }

    /// Insert `(name, expr)` before all existing fields.
    pub fn prepend_col(
        self,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let name = name.into();
        let expr = expr.into();
        let schema = self.schema()?;
        let mut pairs = vec![(name, expr)];
        pairs.extend(identity_named_exprs(&schema));
        self.project(pairs)
    }

    /// Insert `(name, expr)` immediately after the field named `after`.
    pub fn insert_col_after(
        self,
        after: impl AsRef<str>,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let after = after.as_ref();
        let schema = self.schema()?;
        let pairs = insert_named(&schema, after, name.into(), expr.into(), Side::After)?;
        self.project(pairs)
    }

    /// Insert `(name, expr)` immediately before the field named `before`.
    pub fn insert_col_before(
        self,
        before: impl AsRef<str>,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let before = before.as_ref();
        let schema = self.schema()?;
        let pairs = insert_named(&schema, before, name.into(), expr.into(), Side::Before)?;
        self.project(pairs)
    }

    /// Replace the existing field named `name`'s expression. Output type follows
    /// inference of the new expression.
    pub fn replace_col(
        self,
        name: impl AsRef<str>,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let name = name.as_ref();
        let expr = expr.into();
        let schema = self.schema()?;
        if schema.field(name).is_none() {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "replace_col: field {name:?} not in input schema",
            ));
        }
        let pairs: Vec<(String, ExpressionRef)> = schema
            .fields()
            .map(|f| {
                let fname = f.name().clone();
                if fname == name {
                    (fname, Arc::clone(&expr))
                } else {
                    let e = Expression::column([&fname]);
                    (fname, Arc::new(e))
                }
            })
            .collect();
        self.project(pairs)
    }

    /// Rename one input field. Output type is unchanged.
    pub fn rename_col(
        self,
        old: impl AsRef<str>,
        new: impl Into<String>,
    ) -> Result<Self, DeltaError> {
        let old = old.as_ref();
        let new = new.into();
        let schema = self.schema()?;
        if schema.field(old).is_none() {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "rename_col: field {old:?} not in input schema",
            ));
        }
        let pairs: Vec<(String, ExpressionRef)> = schema
            .fields()
            .map(|f| {
                let fname = f.name().clone();
                let out_name = if fname == old {
                    new.clone()
                } else {
                    fname.clone()
                };
                (out_name, Arc::new(Expression::column([&fname])))
            })
            .collect();
        self.project(pairs)
    }

    /// Drop one input field by name.
    pub fn drop_col(self, name: impl AsRef<str>) -> Result<Self, DeltaError> {
        let name = name.as_ref();
        let schema = self.schema()?;
        if schema.field(name).is_none() {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "drop_col: field {name:?} not in input schema",
            ));
        }
        let pairs: Vec<(String, ExpressionRef)> = schema
            .fields()
            .filter(|f| f.name() != name)
            .map(|f| {
                let fname = f.name().clone();
                (fname.clone(), Arc::new(Expression::column([fname])))
            })
            .collect();
        self.project(pairs)
    }

    /// Drop several input fields by name.
    pub fn drop_cols<I, S>(self, names: I) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let drops: std::collections::HashSet<String> =
            names.into_iter().map(|n| n.as_ref().to_string()).collect();
        let schema = self.schema()?;
        for n in &drops {
            if schema.field(n).is_none() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "drop_cols: field {n:?} not in input schema",
                ));
            }
        }
        let pairs: Vec<(String, ExpressionRef)> = schema
            .fields()
            .filter(|f| !drops.contains(f.name()))
            .map(|f| {
                let fname = f.name().clone();
                (fname.clone(), Arc::new(Expression::column([fname])))
            })
            .collect();
        self.project(pairs)
    }

    // === Load / aggregate / join / union =====================================

    /// File-reader transform. Output schema is `spec.file_schema` plus a field per
    /// `passthrough_columns` entry (each field's type is taken from the input schema).
    pub fn load(self, spec: LoadSpec) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let mut fields: Vec<StructField> = spec.file_schema.fields().cloned().collect();
        for col in &spec.passthrough_columns {
            let path = col.path();
            let leaf_name = path
                .last()
                .ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "load: passthrough column path is empty",
                    )
                })?
                .clone();
            let walk = input_schema
                .walk_column_fields(col)
                .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
            let leaf = walk.last().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "load: passthrough column resolved to empty path",
                )
            })?;
            fields.push(StructField::nullable(leaf_name, leaf.data_type().clone()));
        }
        let output_schema = arc_struct_or_invariant(fields)?;
        let LoadSpec {
            file_schema,
            file_type,
            base_url,
            passthrough_columns,
            file_meta,
            dv_ref,
        } = spec;
        push_unary(
            &self.state,
            &self,
            Node::Load {
                file_schema,
                file_type,
                base_url,
                passthrough_columns,
                file_meta,
                dv_ref,
            },
            output_schema,
        )
    }

    /// Top-1-per-group aggregate ordered by `version` descending. The group-by expressions
    /// and the version column are aggregation-internal: the output projects only the
    /// listed `value_columns`, lifted from the input schema in declared order.
    ///
    /// The validator ensures every name in `value_columns` resolves in the input. Group-by
    /// expression types are inferred (and must therefore be supported by
    /// [`infer_expression_type`]) but do not contribute fields to the output -- they are
    /// validated for the lowering's benefit only.
    pub fn max_by_version(
        self,
        group_by: Vec<ExpressionRef>,
        version: ExpressionRef,
        value_columns: Vec<String>,
    ) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        for expr in &group_by {
            // Validate group expressions resolve against the input schema. Inferred type
            // is discarded -- the output schema is derived solely from `value_columns`.
            infer_expression_type(expr.as_ref(), &input_schema)?;
        }
        let mut fields: Vec<StructField> = Vec::with_capacity(value_columns.len());
        for name in &value_columns {
            let f = input_schema.field(name).ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "max_by_version: value column {name:?} not in input schema",
                )
            })?;
            fields.push(f.clone());
        }
        let output_schema = arc_struct_or_invariant(fields)?;
        push_unary(
            &self.state,
            &self,
            Node::MaxByVersion {
                group_by,
                version_column: version,
                value_columns,
            },
            output_schema,
        )
    }

    /// Inner join. Output schema is the left fields followed by the right fields.
    /// Caller is responsible for renaming any name collisions ahead of the join.
    pub fn inner_join(
        self,
        other: Cursor,
        key_pairs: Vec<(ExpressionRef, ExpressionRef)>,
    ) -> Result<Self, DeltaError> {
        ensure_same_context(&self, &other)?;
        let left_schema = self.schema()?;
        let right_schema = other.schema()?;
        let mut fields: Vec<StructField> = left_schema.fields().cloned().collect();
        fields.extend(right_schema.fields().cloned());
        let output_schema = arc_struct_or_invariant(fields)?;
        push_binary(
            &self.state,
            &self,
            &other,
            Node::EquiJoin {
                kind: JoinKind::Inner,
                key_pairs,
            },
            output_schema,
        )
    }

    /// Left-anti join: rows of `self` whose key matches no row of `other`. Output schema
    /// mirrors `self`.
    pub fn left_anti_join(
        self,
        other: Cursor,
        key_pairs: Vec<(ExpressionRef, ExpressionRef)>,
    ) -> Result<Self, DeltaError> {
        ensure_same_context(&self, &other)?;
        let output_schema = self.schema()?;
        push_binary(
            &self.state,
            &self,
            &other,
            Node::EquiJoin {
                kind: JoinKind::LeftAnti,
                key_pairs,
            },
            output_schema,
        )
    }

    /// Unordered union with one or more other cursors. All inputs must share the same
    /// schema (strict equality).
    pub fn union_all(self, others: &[Cursor]) -> Result<Self, DeltaError> {
        push_union(&self.state, &self, others, /* ordered= */ false)
    }

    /// Order-preserving union with one or more other cursors. All inputs must share the
    /// same schema.
    pub fn union_ordered(self, others: &[Cursor]) -> Result<Self, DeltaError> {
        push_union(&self.state, &self, others, /* ordered= */ true)
    }
}

// ============================================================================
// LoadSpec
// ============================================================================

/// Configuration for [`Cursor::load`]. Mirrors the fields of [`Node::Load`].
#[derive(Debug, Clone)]
pub struct LoadSpec {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub passthrough_columns: Vec<ColumnName>,
    pub file_meta: ScanFileColumns,
    pub dv_ref: Option<DvRef>,
}

// ============================================================================
// Helpers
// ============================================================================

fn borrow_state(state: &Rc<RefCell<ContextState>>) -> std::cell::Ref<'_, ContextState> {
    state.borrow()
}

fn borrow_state_mut(state: &Rc<RefCell<ContextState>>) -> std::cell::RefMut<'_, ContextState> {
    state.borrow_mut()
}

fn ensure_fresh(state: &ContextState, cursor: &Cursor) -> Result<(), DeltaError> {
    if state.session_id != cursor.session_id {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "stale cursor: cursor session={cs} expected {ss}",
            cs = cursor.session_id,
            ss = state.session_id,
        ));
    }
    Ok(())
}

fn push_source(
    state: &Rc<RefCell<ContextState>>,
    node: Node,
    schema: SchemaRef,
) -> Result<Cursor, DeltaError> {
    let mut s = borrow_state_mut(state);
    let r = s.plan.push(node, vec![]);
    s.ref_schemas.push(schema);
    debug_assert_eq!(s.ref_schemas.len(), s.plan.stmts.len());
    let session_id = s.session_id;
    Ok(Cursor {
        state: Rc::clone(state),
        ref_id: r,
        session_id,
    })
}

fn push_unary(
    state: &Rc<RefCell<ContextState>>,
    cursor: &Cursor,
    node: Node,
    schema: SchemaRef,
) -> Result<Cursor, DeltaError> {
    let mut s = borrow_state_mut(state);
    ensure_fresh(&s, cursor)?;
    let r = s.plan.push(node, vec![cursor.ref_id]);
    s.ref_schemas.push(schema);
    let session_id = s.session_id;
    Ok(Cursor {
        state: Rc::clone(state),
        ref_id: r,
        session_id,
    })
}

fn ensure_same_context(left: &Cursor, right: &Cursor) -> Result<(), DeltaError> {
    if !Rc::ptr_eq(&left.state, &right.state) {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "cursor from a different context cannot be combined",
        ));
    }
    Ok(())
}

fn push_binary(
    state: &Rc<RefCell<ContextState>>,
    left: &Cursor,
    right: &Cursor,
    node: Node,
    schema: SchemaRef,
) -> Result<Cursor, DeltaError> {
    if !Rc::ptr_eq(state, &right.state) {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "cursor from a different context cannot be combined",
        ));
    }
    let mut s = borrow_state_mut(state);
    ensure_fresh(&s, left)?;
    ensure_fresh(&s, right)?;
    let r = s.plan.push(node, vec![left.ref_id, right.ref_id]);
    s.ref_schemas.push(schema);
    let session_id = s.session_id;
    Ok(Cursor {
        state: Rc::clone(state),
        ref_id: r,
        session_id,
    })
}

fn push_union(
    state: &Rc<RefCell<ContextState>>,
    first: &Cursor,
    others: &[Cursor],
    ordered: bool,
) -> Result<Cursor, DeltaError> {
    let first_schema = first.schema()?;
    let mut inputs = Vec::with_capacity(1 + others.len());
    inputs.push(first.ref_id);
    for o in others {
        if !Rc::ptr_eq(state, &o.state) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "union: cursor from a different context cannot be combined",
            ));
        }
        let other_schema = o.schema()?;
        if other_schema != first_schema {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "union: input schemas disagree",
            ));
        }
        inputs.push(o.ref_id);
    }
    let mut s = borrow_state_mut(state);
    ensure_fresh(&s, first)?;
    for o in others {
        ensure_fresh(&s, o)?;
    }
    let r = s.plan.push(Node::Union { ordered }, inputs);
    s.ref_schemas.push(first_schema);
    let session_id = s.session_id;
    Ok(Cursor {
        state: Rc::clone(state),
        ref_id: r,
        session_id,
    })
}

fn arc_struct_or_invariant(fields: Vec<StructField>) -> Result<SchemaRef, DeltaError> {
    StructType::try_new(fields)
        .map(Arc::new)
        .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)
}

fn identity_named_exprs(schema: &StructType) -> Vec<(String, ExpressionRef)> {
    schema
        .fields()
        .map(|f| {
            let fname = f.name().clone();
            (fname.clone(), Arc::new(Expression::column([fname])))
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Side {
    Before,
    After,
}

fn insert_named(
    schema: &StructType,
    anchor: &str,
    new_name: String,
    new_expr: ExpressionRef,
    side: Side,
) -> Result<Vec<(String, ExpressionRef)>, DeltaError> {
    if schema.field(anchor).is_none() {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "insert: anchor field {anchor:?} not in input schema",
        ));
    }
    let mut pairs: Vec<(String, ExpressionRef)> = Vec::with_capacity(schema.fields().count() + 1);
    for f in schema.fields() {
        let fname = f.name().clone();
        if matches!(side, Side::Before) && fname == anchor {
            pairs.push((new_name.clone(), Arc::clone(&new_expr)));
        }
        pairs.push((fname.clone(), Arc::new(Expression::column([fname.clone()]))));
        if matches!(side, Side::After) && fname == anchor {
            pairs.push((new_name.clone(), Arc::clone(&new_expr)));
        }
    }
    Ok(pairs)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use uuid::Uuid;

    use super::*;
    use crate::expressions::{col, Predicate};
    use crate::plans::ir::ssa::JoinKind;
    use crate::plans::kernel_consumers::{
        FinishedHandle, KdfControl, KernelConsumerKind, KernelConsumerOutput,
    };
    use crate::plans::operations::framework::coroutine::driver::Coroutine;
    use crate::plans::operations::framework::state_machine::{NextStep, StateMachine};
    use crate::plans::operations::framework::step_result::StepResult;
    use crate::schema::DataType;
    use crate::DeltaResult;

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
        let v = ctx.values(id_ts_schema(), vec![]).unwrap();
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
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let dead = ctx.values(id_ts_schema(), vec![]).unwrap(); // unreachable
        let kept = src.filter(Arc::new(col("id").is_not_null())).unwrap();
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
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let f = src
            .filter(Arc::new(Predicate::BooleanExpression(col("id"))))
            .unwrap();
        assert_eq!(*f.schema().unwrap(), *id_ts_schema());
    }

    /// `project` derives output schema from inferred expression types.
    #[test]
    fn project_infers_output_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let projected = src
            .project([
                ("id_out", Arc::new(col("id")) as ExpressionRef),
                ("ts_out", Arc::new(col("ts")) as ExpressionRef),
            ])
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
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();

        let target = Arc::new(
            StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap(),
        );
        let exprs: Vec<ExpressionRef> = vec![Arc::new(col("id"))];
        let p = src
            .clone()
            .project_with_schema(exprs, Arc::clone(&target))
            .unwrap();
        assert_eq!(*p.schema().unwrap(), *target);

        let bad: Vec<ExpressionRef> = vec![Arc::new(col("id")), Arc::new(col("ts"))];
        let err = src.project_with_schema(bad, target).unwrap_err();
        assert!(
            err.to_string().contains("expected 1 expressions"),
            "got: {err}"
        );
    }

    /// Schema-edit sugar (drop/rename/append/replace) compiles and produces the right
    /// output schema by name and type.
    #[test]
    fn schema_edit_sugar_changes_field_layout() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();

        // append_col + drop_col + rename_col chained
        let out = src
            .append_col("ts2", Arc::new(col("ts")) as ExpressionRef)
            .unwrap()
            .drop_col("ts")
            .unwrap()
            .rename_col("ts2", "version")
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "version"]);
        assert_eq!(
            schema.field("version").unwrap().data_type(),
            &DataType::LONG
        );
    }

    /// `insert_col_after` and `insert_col_before` produce the right field order.
    #[test]
    fn insert_col_positions_field_correctly() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let after = src
            .clone()
            .insert_col_after("id", "mid", Arc::new(col("id")) as ExpressionRef)
            .unwrap();
        let before = src
            .insert_col_before("ts", "early", Arc::new(col("id")) as ExpressionRef)
            .unwrap();

        let after_schema = after.schema().unwrap();
        let after_names: Vec<&str> = after_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(after_names, ["id", "mid", "ts"]);

        let before_schema = before.schema().unwrap();
        let before_names: Vec<&str> = before_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(before_names, ["id", "early", "ts"]);
    }

    /// `select` rejects target fields not in the input schema.
    #[test]
    fn select_rejects_missing_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let target = Arc::new(
            StructType::try_new(vec![StructField::nullable("missing", DataType::STRING)]).unwrap(),
        );
        let err = src.select(target).unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    /// `inner_join` concatenates left and right schemas; `left_anti_join` mirrors left.
    #[test]
    fn join_variants_pick_correct_output_schema() {
        let ctx = Context::new();
        let l = ctx.values(id_ts_schema(), vec![]).unwrap();
        let r_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let r = ctx.values(Arc::clone(&r_schema), vec![]).unwrap();

        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("rid")))];

        let inner = l.clone().inner_join(r.clone(), key.clone()).unwrap();
        let inner_schema = inner.schema().unwrap();
        let inner_names: Vec<&str> = inner_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(inner_names, ["id", "ts", "rid", "rts"]);

        let anti = l.left_anti_join(r, key).unwrap();
        assert_eq!(*anti.schema().unwrap(), *id_ts_schema());
    }

    /// `union_all` validates all input schemas match.
    #[test]
    fn union_all_requires_matching_schemas() {
        let ctx = Context::new();
        let a = ctx.values(id_ts_schema(), vec![]).unwrap();
        let b = ctx.values(id_ts_schema(), vec![]).unwrap();
        let unioned = a.clone().union_all(&[b]).unwrap();
        assert_eq!(*unioned.schema().unwrap(), *id_ts_schema());

        let other_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("x", DataType::STRING)]).unwrap(),
        );
        let bad = ctx.values(other_schema, vec![]).unwrap();
        let err = a.union_all(&[bad]).unwrap_err();
        assert!(err.to_string().contains("disagree"), "got: {err}");
    }

    /// Cursors from different contexts cannot be combined.
    #[test]
    fn cross_context_cursors_rejected() {
        let ctx_a = Context::new();
        let ctx_b = Context::new();
        let a = ctx_a.values(id_ts_schema(), vec![]).unwrap();
        let b = ctx_b.values(id_ts_schema(), vec![]).unwrap();
        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("id")))];
        let err = a.inner_join(b, key).unwrap_err();
        assert!(err.to_string().contains("different context"), "got: {err}");
    }

    /// `into_result_plan` fails when other cursors still hold the state Rc.
    #[test]
    fn into_result_plan_fails_with_outstanding_cursor() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let _other = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = ctx.into_result_plan(src).unwrap_err();
        assert!(err.to_string().contains("outstanding Cursor"), "got: {err}",);
    }

    /// `max_by_version` output schema is exactly `value_columns` lifted from the input
    /// (group_by exprs and version are aggregation-internal and not surfaced).
    #[test]
    fn max_by_version_output_schema_is_value_columns_only() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let out = src
            .max_by_version(
                vec![Arc::new(col("id"))],
                Arc::new(col("ts")),
                vec!["ts".to_string()],
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["ts"]);
        assert_eq!(schema.field("ts").unwrap().data_type(), &DataType::LONG);
    }

    /// `max_by_version` rejects value columns that are not present in the input schema.
    #[test]
    fn max_by_version_rejects_unknown_value_column() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = src
            .max_by_version(
                vec![Arc::new(col("id"))],
                Arc::new(col("ts")),
                vec!["missing".to_string()],
            )
            .unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    // === Coroutine dispatch tests ============================================

    /// Test KernelConsumer that echoes its constructor input as the output value.
    #[derive(Debug, Clone)]
    struct EchoConsumer {
        value: i64,
    }

    impl KernelConsumer for EchoConsumer {
        fn kind(&self) -> KernelConsumerKind {
            KernelConsumerKind::CheckpointHint
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
        fn apply(&mut self, _batch: &dyn crate::EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }

    impl KernelConsumerOutput for EchoConsumer {
        type Output = i64;
        fn into_output(self) -> Result<i64, DeltaError> {
            Ok(self.value)
        }
    }

    /// `consume` yields a `Step::Consume` with the DCE'd stmts and the cursor's terminal
    /// Ref, the engine resumes with the consumer's finished handle, and the SM body
    /// recovers the typed output.
    #[test]
    fn consume_yields_step_consume_and_recovers_typed_output() {
        let mut sm = Coroutine::<i64>::new("test", |mut co, _sm_id| async move {
            let ctx = Context::new();
            let _dead = ctx.values(id_ts_schema(), vec![]).unwrap();
            let cursor = ctx
                .values(id_ts_schema(), vec![])
                .unwrap()
                .filter(Arc::new(col("id").is_not_null()))
                .unwrap();
            ctx.consume(&mut co, cursor, EchoConsumer { value: 7 }, "drain")
                .await
        })
        .unwrap();

        // First yielded step is the Consume.
        assert_eq!(sm.step_name(), "drain");
        let token = match sm.get_step().unwrap() {
            Step::Consume {
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
            other => panic!("expected Step::Consume, got {other:?}"),
        };

        // Engine drains the sink and resumes with a finished handle.
        let result = StepResult::empty();
        result.submit_consumer_handle(FinishedHandle {
            token,
            sm_id: Uuid::new_v4(),
            sm_kind: "test",
            step_name: "drain",
            erased: Box::new(EchoConsumer { value: 7 }),
        });
        let next = sm.submit(Ok(result)).unwrap();
        match next {
            NextStep::Done(v) => assert_eq!(v, 7),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// `schema_query` yields a `Step::SchemaQuery`, the engine resumes with a schema,
    /// and the SM body receives it.
    #[test]
    fn schema_query_yields_step_schemaquery_and_returns_schema() {
        let target_schema = id_ts_schema();
        let target_clone = Arc::clone(&target_schema);
        let mut sm = Coroutine::<bool>::new("test", move |mut co, _sm_id| async move {
            let ctx = Context::new();
            let s = ctx.schema_query(&mut co, "/cp.parquet", "footer").await?;
            Ok(s == target_clone)
        })
        .unwrap();

        assert_eq!(sm.step_name(), "footer");
        match sm.get_step().unwrap() {
            Step::SchemaQuery(node) => assert_eq!(node.file_path, "/cp.parquet"),
            other => panic!("expected SchemaQuery, got {other:?}"),
        }

        let result = StepResult::empty();
        result.submit_schema(target_schema);
        match sm.submit(Ok(result)).unwrap() {
            NextStep::Done(v) => assert!(v),
            _ => panic!("expected Done(true)"),
        }
    }

    /// After `schema_query` bumps the session, cursors built before the dispatch are
    /// stale and operations on them fail.
    #[test]
    fn schema_query_invalidates_pre_dispatch_cursors() {
        let target_schema = id_ts_schema();
        let mut sm = Coroutine::<()>::new("test", move |mut co, _sm_id| async move {
            let ctx = Context::new();
            let pre = ctx.values(id_ts_schema(), vec![]).unwrap();
            let _ = ctx.schema_query(&mut co, "/x.parquet", "footer").await?;
            // Using `pre` after dispatch must fail with a stale-cursor error.
            let err = pre
                .filter(Arc::new(Predicate::BooleanExpression(col("id"))))
                .unwrap_err();
            assert!(err.to_string().contains("stale cursor"), "got: {err}");
            Ok(())
        })
        .unwrap();

        // Drive through the schema_query yield.
        let _ = sm.get_step().unwrap();
        let result = StepResult::empty();
        result.submit_schema(target_schema);
        match sm.submit(Ok(result)).unwrap() {
            NextStep::Done(()) => {}
            _ => panic!("expected Done"),
        }
    }

    /// SSA stmt for `inner_join` records `[left, right]` as inputs in that order.
    #[test]
    fn inner_join_records_left_right_inputs() {
        let ctx = Context::new();
        let left = ctx.values(id_ts_schema(), vec![]).unwrap();
        let right_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let right = ctx.values(right_schema, vec![]).unwrap();
        let left_ref = left.ref_id();
        let right_ref = right.ref_id();
        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("rid")))];
        let joined = left.inner_join(right, key).unwrap();
        let joined_ref = joined.ref_id();
        let plan = ctx.into_result_plan(joined).unwrap();
        let stmt = plan.plan.stmt(joined_ref).unwrap();
        assert_eq!(stmt.inputs, vec![left_ref, right_ref]);
        assert!(matches!(
            stmt.node,
            Node::EquiJoin {
                kind: JoinKind::Inner,
                ..
            }
        ));
    }
}
