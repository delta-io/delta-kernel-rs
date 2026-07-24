//! Fluent builder for declarative [`Plan`]s: a source, chained transforms, then
//! [`PlanBuilder::build`] / [`PlanBuilder::build_opt`]. Each transform consumes `self` and returns
//! a new builder; clone a builder (cheap) to feed it into more than one transform. Validating
//! methods return [`DeltaResult`] and surface errors at the call site.
//!
//! A source with no rows (e.g. a scan over no files) is the *absent* relation. The builder treats
//! it as dead code: transforms validate as usual but propagate absence, eliminating the parts of
//! the plan that cannot contribute rows (a union drops absent arms, an anti-join with an absent
//! build forwards its probe, etc.). [`build`](PlanBuilder::build) collapses the plan to its
//! minimal correct shape, emitting a single empty [`Values`] node when the whole relation is
//! absent, so it always yields a runnable plan; [`build_opt`](PlanBuilder::build_opt) yields
//! `None` for it.
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::PlanBuilder;
//! use delta_kernel::expressions::col;
//! use delta_kernel::plans::ir::nodes::Operator;
//! use delta_kernel::schema::{DataType, StructField, StructType};
//!
//! let schema = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
//!
//! // A source with no rows is the *absent* relation: it produces no rows, so the builder
//! // eliminates it as dead code, collapsing the plan to its minimal correct shape. Downstream
//! // transforms still validate and chain exactly as they would over a populated source.
//! let scan = PlanBuilder::values(schema, vec![])?
//!     .filter(col!("id").is_not_null())?;
//!
//! // `build_opt` reports the eliminated plan as `None` ...
//! assert!(scan.clone().build_opt()?.is_none());
//! // ... while `build` keeps the plan runnable, replacing the absent relation with a single
//! // empty `Values` node.
//! let plan = scan.build()?;
//! let [node] = plan.nodes.as_slice() else { panic!("expected one node") };
//! assert!(matches!(&node.op, Operator::Values(values) if values.rows.is_empty()));
//! # Ok::<(), delta_kernel::Error>(())
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use super::ir::nodes::{
    Aggregate, AggregateBuilder, FileType, Filter, Load, Operator, Project, ScanFile, ScanJson,
    ScanParquet, SemiJoin, UnionAll, Values,
};
use super::ir::plan::{Plan, PlanNode};
use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar};
use crate::schema::SchemaRef;
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::utils::CollectInto;
use crate::{DeltaResult, Error};

/// One node of a plan DAG: an operator, its inputs, and its output schema. Node identity is the
/// `Arc`'s address ([`PlanBuilder::build`] dedups shared subgraphs by pointer). Inputs are always
/// present nodes -- combinators drop or forward an absent input.
#[derive(Debug)]
struct BuilderNode {
    op: Operator,
    inputs: Vec<BuilderNodeRef>,
    schema: SchemaRef,
}

type BuilderNodeRef = Arc<BuilderNode>;

/// A present node, or the *absent* (uninhabited) relation, which still carries its output schema.
#[derive(Clone, Debug)]
enum PlanBuilderRoot {
    Present(BuilderNodeRef),
    Absent(SchemaRef),
}

/// A [`Plan`] under construction. See the module docs.
#[derive(Clone, Debug)]
#[must_use]
pub struct PlanBuilder(PlanBuilderRoot);

impl PlanBuilder {
    /// A present relation applying `op` (output `schema`) over `inputs`.
    fn present(schema: SchemaRef, op: impl Into<Operator>, inputs: Vec<BuilderNodeRef>) -> Self {
        PlanBuilder(PlanBuilderRoot::Present(Arc::new(BuilderNode {
            op: op.into(),
            inputs,
            schema,
        })))
    }

    /// The uninhabited relation over `schema`.
    fn absent(schema: SchemaRef) -> Self {
        PlanBuilder(PlanBuilderRoot::Absent(schema))
    }

    /// The output schema of this relation -- carried whether present or absent.
    fn schema(&self) -> &SchemaRef {
        match &self.0 {
            PlanBuilderRoot::Present(node) => &node.schema,
            PlanBuilderRoot::Absent(schema) => schema,
        }
    }

    /// Apply a single-input transform: wrap `op` (output `schema`) over a present input, or stay
    /// absent. Callers validate against [`Self::schema`] first.
    fn unary_op_or_absent(self, schema: SchemaRef, op: impl Into<Operator>) -> Self {
        match self.0 {
            PlanBuilderRoot::Present(node) => Self::present(schema, op, vec![node]),
            PlanBuilderRoot::Absent(_) => Self::absent(schema),
        }
    }

    /// A Parquet scan source over `files` producing rows matching `schema`.
    ///
    /// `file_constant_columns` names the per-file-constant output columns (see [`ScanParquet`]);
    /// pass `&[]` when there are none. An empty `files` yields the absent relation.
    ///
    /// Produces an error when any file's constant count differs from `file_constant_columns`'s
    /// length, or a `file_constant_columns` entry is absent from `schema`.
    pub fn scan_parquet(
        files: impl IntoIterator<Item = impl Into<ScanFile>>,
        file_constant_columns: &[&str],
        schema: impl Into<SchemaRef>,
    ) -> DeltaResult<Self> {
        Self::scan_source(FileType::Parquet, files, file_constant_columns, schema)
    }

    /// A newline-delimited JSON scan source over `files` producing rows matching `schema`. See
    /// [`ScanJson`]. Exhibits same behaviour as [`Self::scan_parquet`].
    pub fn scan_json(
        files: impl IntoIterator<Item = impl Into<ScanFile>>,
        file_constant_columns: &[&str],
        schema: impl Into<SchemaRef>,
    ) -> DeltaResult<Self> {
        Self::scan_source(FileType::Json, files, file_constant_columns, schema)
    }

    /// Shared body of [`Self::scan_parquet`] / [`Self::scan_json`]. Normalizes and validates the
    /// inputs, then yields the absent relation for an empty file set, else the scan source node for
    /// `file_type`.
    fn scan_source(
        file_type: FileType,
        files: impl IntoIterator<Item = impl Into<ScanFile>>,
        file_constant_columns: &[&str],
        schema: impl Into<SchemaRef>,
    ) -> DeltaResult<Self> {
        let schema = schema.into();
        let files = Vec::from_iter(files.into_iter().map(Into::into));
        let cols = Vec::from_iter(file_constant_columns.iter().map(|&c| c.to_string()));
        for (i, file) in files.iter().enumerate() {
            if file.file_constants.len() != cols.len() {
                return Err(Error::generic(format!(
                    "scan: file {i} has {} constant value(s) {:?} for {} file-constant column(s) {:?}",
                    file.file_constants.len(),
                    file.file_constants,
                    cols.len(),
                    cols,
                )));
            }
        }
        check_file_constant_columns(&schema, &cols, "scan file_constant_columns")?;
        if files.is_empty() {
            return Ok(Self::absent(schema));
        }
        let file_constant_columns = cols;
        let op = match file_type {
            FileType::Parquet => Operator::from(ScanParquet {
                files,
                file_constant_columns,
                schema: Arc::clone(&schema),
            }),
            FileType::Json => Operator::from(ScanJson {
                files,
                file_constant_columns,
                schema: Arc::clone(&schema),
            }),
        };
        Ok(Self::present(schema, op, vec![]))
    }

    /// Inline literal rows. See [`Values`] for the row encoding. Empty `rows` yields the absent
    /// relation.
    ///
    /// Produces an error when any row's width differs from `schema`'s top-level field count.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
    /// let plan = PlanBuilder::values(schema, vec![vec![1.into()], vec![2.into()]])?.build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn values(schema: impl Into<SchemaRef>, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        let schema = schema.into();
        let width = schema.fields().count();
        for (i, row) in rows.iter().enumerate() {
            if row.len() != width {
                return Err(Error::generic(format!(
                    "values: row {i} has {} value(s) {row:?} but schema has {width} field(s) {:?}",
                    row.len(),
                    Vec::from_iter(schema.fields().map(|f| f.name())),
                )));
            }
        }
        if rows.is_empty() {
            return Ok(Self::absent(schema));
        }
        let op = Values::new(schema, rows);
        Ok(Self::present(op.schema.clone(), op, vec![]))
    }

    /// Keep rows where `predicate` holds. Output schema is unchanged. See [`Filter`].
    ///
    /// Produces an error when `predicate` references a column absent from the input schema.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::expressions::col;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
    /// let plan = PlanBuilder::values(schema, vec![vec![1.into()], vec![2.into()]])?
    ///     .filter(col!("id").is_not_null())?
    ///     .build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn filter(self, predicate: impl Into<PredicateRef>) -> DeltaResult<Self> {
        let predicate = predicate.into();
        check_columns_resolve(self.schema(), predicate.references(), "filter")?;
        let schema = Arc::clone(self.schema());
        Ok(self.unary_op_or_absent(schema, Filter { predicate }))
    }

    /// Project `self` through `expr` into rows of the caller-declared `schema`. `expr` must be a
    /// struct constructor / patch matching `schema`. See [`Project`].
    ///
    /// Produces an error when `expr` references a column absent from the input schema. References
    /// inside a `StructPatch` are validated by [`ProjectionStructPatchBuilder`] when the patch is
    ///
    /// [`ProjectionStructPatchBuilder`]: crate::struct_patch::ProjectionStructPatchBuilder
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::expressions::{col, Expression};
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let input = Arc::new(StructType::try_new([
    ///     StructField::not_null("id", DataType::INTEGER),
    ///     StructField::nullable("name", DataType::STRING),
    /// ])?);
    /// let out = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
    /// // Keep only `id`.
    /// let plan = PlanBuilder::values(input, vec![vec![1.into(), "a".into()]])?
    ///     .project(Expression::struct_from([col!("id")]), out)?
    ///     .build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn project(
        self,
        expr: impl Into<ExpressionRef>,
        schema: impl Into<SchemaRef>,
    ) -> DeltaResult<Self> {
        let schema = schema.into();
        let expr = expr.into();
        check_columns_resolve(self.schema(), expr.references(), "project")?;
        Ok(self.unary_op_or_absent(Arc::clone(&schema), Project { expr, schema }))
    }

    /// Project `self` by editing its columns. `edit` receives a [`ProjectionStructPatchBuilder`]
    /// rooted at `self`'s schema and records field edits (replace/drop/append); the lowered patch
    /// and the resulting output schema feed the [`Project`] node together. See [`Project`].
    ///
    /// Produces an error when the patch fails to build -- e.g. a replaced or dropped field is
    /// absent from the input schema, or a nested path does not resolve.
    pub fn project_patch(
        self,
        patch: impl FnOnce(ProjectionStructPatchBuilder<'_>) -> ProjectionStructPatchBuilder<'_>,
    ) -> DeltaResult<Self> {
        let (out, expr) = patch(ProjectionStructPatchBuilder::new(self.schema())).build()?;
        self.project(expr, out)
    }

    /// Read data files named by `self`'s rows. Output schema is `load.schema`. See [`Load`].
    ///
    /// Produces an error when a `file_meta`/deletion-vector column is absent from the input schema,
    /// or a `file_constant_columns` entry is absent from the input (its broadcast source) or from
    /// `load.schema` (the output).
    pub fn load(self, load: Load) -> DeltaResult<Self> {
        let meta = [
            &load.file_meta.path_column,
            &load.file_meta.file_size_column,
            &load.file_meta.num_records_column,
            &load.dv_column,
        ];
        check_columns_resolve(self.schema(), meta, "load")?;
        // File-constant columns are sourced upstream and emitted in the output, so check both.
        check_file_constant_columns(
            self.schema(),
            &load.file_constant_columns,
            "load file_constant source",
        )?;
        check_file_constant_columns(
            &load.schema,
            &load.file_constant_columns,
            "load file_constant",
        )?;
        Ok(self.unary_op_or_absent(Arc::clone(&load.schema), load))
    }

    /// Aggregate `self` into `aggregate` (build one with [`Aggregate::group_by`]). The output
    /// schema is the group keys followed by the aggregate columns. See [`Aggregate`].
    ///
    /// Over an absent input the result is group-arity dependent: a **grouped** aggregate yields
    /// zero groups and so is absent; a **global** aggregate (no group keys) still yields one row,
    /// so it aggregates an empty [`Values`] input and lets the engine produce that row.
    ///
    /// Produces an error when building `aggregate` fails: a group key or an aggregate's operand
    /// column is absent from its input schema, or two output columns would share a name.
    pub fn aggregate(self, aggregate: impl TryInto<Aggregate, Error = Error>) -> DeltaResult<Self> {
        let aggregate = aggregate.try_into()?;
        let schema = Arc::clone(&aggregate.schema);
        match self.0 {
            PlanBuilderRoot::Present(node) => Ok(Self::present(schema, aggregate, vec![node])),
            // A grouped aggregate over the empty relation has zero groups, hence absent.
            PlanBuilderRoot::Absent(_) if !aggregate.group_by.is_empty() => {
                Ok(Self::absent(schema))
            }
            // A global aggregate always emits one row, even over no input: aggregate an empty
            // Values relation and let the engine produce that row.
            PlanBuilderRoot::Absent(input_schema) => {
                let input = Self::present(
                    Arc::clone(&input_schema),
                    Values::new(input_schema, vec![]),
                    vec![],
                );
                Ok(input.unary_op_or_absent(schema, aggregate))
            }
        }
    }

    /// Aggregate `self`, grouped by `keys`. `aggs` receives an [`AggregateBuilder`] rooted at
    /// `self`'s schema and adds the aggregate columns (see e.g. [`AggregateBuilder::max`]).
    ///
    /// Mirrors [`Self::aggregate`] but spares the caller from naming the input schema, just as
    /// [`Self::project_patch`] does for [`Self::project`]. See [`Aggregate`].
    ///
    /// Produces an error under the same conditions as [`Self::aggregate`].
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::expressions::column_name;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([
    ///     StructField::not_null("id", DataType::INTEGER),
    ///     StructField::nullable("version", DataType::LONG),
    /// ])?);
    /// // Latest `version` per `id`.
    /// let plan = PlanBuilder::values(schema, vec![vec![1.into(), 7i64.into()]])?
    ///     .aggregate_by([column_name!("id")], |a| a.max(column_name!("version")))?
    ///     .build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn aggregate_by(
        self,
        keys: impl CollectInto<Vec<ColumnName>>,
        aggs: impl FnOnce(AggregateBuilder) -> AggregateBuilder,
    ) -> DeltaResult<Self> {
        let builder = Aggregate::group_by(Arc::clone(self.schema()), keys);
        self.aggregate(aggs(builder))
    }

    /// Aggregate `self` without grouping. `aggs` receives an [`AggregateBuilder`] rooted at
    /// `self`'s schema and adds aggregate columns (see e.g. [`AggregateBuilder::max`]).
    ///
    /// Mirrors [`Self::aggregate`] but infers the input schema and uses
    /// [`Aggregate::ungrouped`] as the builder root.
    ///
    /// Produces an error under the same conditions as [`Self::aggregate`].
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::expressions::column_name;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([
    ///     StructField::not_null("id", DataType::INTEGER),
    ///     StructField::nullable("version", DataType::LONG),
    /// ])?);
    /// // Latest version across all rows.
    /// let plan = PlanBuilder::values(schema, vec![vec![1.into(), 7i64.into()]])?
    ///     .aggregate_ungrouped(|a| a.max(column_name!("version")))?
    ///     .build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn aggregate_ungrouped(
        self,
        aggs: impl FnOnce(AggregateBuilder) -> AggregateBuilder,
    ) -> DeltaResult<Self> {
        let builder = Aggregate::ungrouped(Arc::clone(self.schema()));
        self.aggregate(aggs(builder))
    }

    /// Semi join: emit the `self` (probe) rows that have a match in `build` on the join keys.
    /// Output schema mirrors `self`. Inputs are recorded as `[probe, build]`. See [`SemiJoin`].
    ///
    /// Keys are columns (e.g. [`column_name!`](crate::expressions::column_name)), matched pairwise.
    ///
    /// Produces an error when the probe and build key counts differ, or a probe/build key is absent
    /// from its input schema.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::expressions::column_name;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
    /// let probe = PlanBuilder::values(Arc::clone(&schema), vec![vec![1.into()], vec![2.into()]])?;
    /// let allow = PlanBuilder::values(schema, vec![vec![2.into()]])?;
    /// // Probe rows whose `id` appears in `allow`.
    /// let plan = probe.semi_join(allow, [column_name!("id")], [column_name!("id")])?.build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn semi_join(
        self,
        build: PlanBuilder,
        probe_keys: impl IntoIterator<Item = ColumnName>,
        build_keys: impl IntoIterator<Item = ColumnName>,
    ) -> DeltaResult<Self> {
        self.semi_join_impl(build, false, probe_keys, build_keys)
    }

    /// Anti join: emit the `self` (probe) rows that have *no* match in `build` on the join keys.
    /// An inverted [`SemiJoin`]; otherwise as [`Self::semi_join`].
    pub fn anti_join(
        self,
        build: PlanBuilder,
        probe_keys: impl IntoIterator<Item = ColumnName>,
        build_keys: impl IntoIterator<Item = ColumnName>,
    ) -> DeltaResult<Self> {
        self.semi_join_impl(build, true, probe_keys, build_keys)
    }

    fn semi_join_impl(
        self,
        build: PlanBuilder,
        inverted: bool,
        probe_keys: impl IntoIterator<Item = ColumnName>,
        build_keys: impl IntoIterator<Item = ColumnName>,
    ) -> DeltaResult<Self> {
        let probe_keys = Vec::from_iter(probe_keys);
        let build_keys = Vec::from_iter(build_keys);
        if probe_keys.len() != build_keys.len() {
            return Err(Error::generic(format!(
                "join: {} probe key(s) {probe_keys:?} but {} build key(s) {build_keys:?}; \
                 they must match in length.",
                probe_keys.len(),
                build_keys.len(),
            )));
        }
        check_columns_resolve(self.schema(), &probe_keys, "join probe")?;
        check_columns_resolve(build.schema(), &build_keys, "join build")?;
        // An uninhabited probe always produces an uninhabited result; forward it unchanged.
        let PlanBuilderRoot::Present(probe) = self.0 else {
            return Ok(self);
        };
        let build = match build.0 {
            PlanBuilderRoot::Present(build) => build,
            // An absent build side matches nothing, so an anti-join forwards an unfiltered
            // probe side while a semi-join is uninhabited.
            PlanBuilderRoot::Absent(_) if inverted => {
                return Ok(PlanBuilder(PlanBuilderRoot::Present(probe)))
            }
            PlanBuilderRoot::Absent(_) => return Ok(Self::absent(Arc::clone(&probe.schema))),
        };
        let node = SemiJoin {
            inverted,
            probe_keys,
            build_keys,
        };
        let schema = Arc::clone(&probe.schema);
        Ok(Self::present(schema, node, vec![probe, build]))
    }

    /// Unordered bag union of `inputs` into a [`UnionAll`]. All inputs must share the same schema;
    /// absent inputs are dropped, and a lone present input is forwarded unchanged.
    ///
    /// Produces an error when `inputs` is empty, or two inputs have differing schemas.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use delta_kernel::PlanBuilder;
    /// # use delta_kernel::schema::{DataType, StructField, StructType};
    /// let schema = Arc::new(StructType::try_new([StructField::not_null("id", DataType::INTEGER)])?);
    /// let a = PlanBuilder::values(Arc::clone(&schema), vec![vec![1.into()]])?;
    /// let b = PlanBuilder::values(schema, vec![vec![2.into()]])?;
    /// let plan = PlanBuilder::union_all([a, b])?.build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn union_all(inputs: impl IntoIterator<Item = PlanBuilder>) -> DeltaResult<Self> {
        let inputs = Vec::from_iter(inputs);
        let Some(schema) = inputs.first().map(|b| Arc::clone(b.schema())) else {
            return Err(Error::generic("union_all: requires at least one input"));
        };
        if let Some(i) = inputs.iter().position(|b| b.schema() != &schema) {
            return Err(Error::generic(format!(
                "union_all: input {i} has a schema differing from input 0: {:?} vs {:?}",
                inputs[i].schema(),
                schema,
            )));
        }
        let mut present = Vec::from_iter(inputs.into_iter().filter_map(|b| match b.0 {
            PlanBuilderRoot::Present(node) => Some(node),
            PlanBuilderRoot::Absent(_) => None,
        }));
        Ok(match present.len() {
            0 => Self::absent(schema),
            1 => PlanBuilder(PlanBuilderRoot::Present(present.swap_remove(0))),
            _ => Self::present(schema, UnionAll, present),
        })
    }

    /// Linearize the DAG reachable from `self` into a [`Plan`]. An absent relation builds to a
    /// single empty [`Values`] node carrying its schema, so the result is always a runnable plan.
    pub fn build(&self) -> DeltaResult<Plan> {
        Ok(match &self.0 {
            PlanBuilderRoot::Present(root) => Self::build_plan(root),
            PlanBuilderRoot::Absent(schema) => Plan {
                nodes: vec![PlanNode::new(
                    Values::new(Arc::clone(schema), vec![]),
                    vec![],
                )],
            },
        })
    }

    /// Like [`Self::build`], but yields `None` for an absent relation instead of an empty plan.
    pub fn build_opt(&self) -> DeltaResult<Option<Plan>> {
        Ok(match &self.0 {
            PlanBuilderRoot::Present(root) => Some(Self::build_plan(root)),
            PlanBuilderRoot::Absent(_) => None,
        })
    }

    /// Linearize the DAG rooted at `root` into a topologically sorted [`Plan`] with `root` as the
    /// last (terminal) node. A node's inputs always precede it, referenced by their index in
    /// [`Plan::nodes`]. Shared subgraphs are emitted once; multiple nodes can reference them.
    fn build_plan(root: &BuilderNodeRef) -> Plan {
        // Maps a node's Arc pointer to its index, deduping shared subgraphs. Pointers are stable
        // because `root` pins all reachable nodes for the lifetime of `build_plan`.
        fn emit(
            node: &BuilderNodeRef,
            nodes: &mut Vec<PlanNode>,
            emitted: &mut HashMap<*const BuilderNode, usize>,
        ) -> usize {
            let key = Arc::as_ptr(node);
            if let Some(&index) = emitted.get(&key) {
                return index;
            }
            // Recurse before `entry` so the closure borrows only `nodes`, not `emitted`.
            let inputs = Vec::from_iter(node.inputs.iter().map(|i| emit(i, nodes, emitted)));
            *emitted.entry(key).or_insert_with(|| {
                nodes.push(PlanNode::new(node.op.clone(), inputs));
                nodes.len() - 1
            })
        }
        let mut nodes = Vec::new();
        emit(root, &mut nodes, &mut HashMap::new());
        Plan { nodes }
    }
}

/// Error if any of `cols` fails to resolve against `schema` (nested paths supported).
fn check_columns_resolve<'a>(
    schema: &SchemaRef,
    cols: impl IntoIterator<Item = &'a ColumnName>,
    ctx: &str,
) -> DeltaResult<()> {
    for col in cols {
        schema.field_at(col).map_err(|_| {
            Error::generic(format!(
                "{ctx}: column `{col}` not found; schema has {:?}",
                Vec::from_iter(schema.fields().map(|f| f.name())),
            ))
        })?;
    }
    Ok(())
}

/// Error unless each of `names` is a top-level, non-metadata field of `schema`. A file-constant is
/// one scalar broadcast into a top-level field, so nested paths and engine-generated metadata
/// columns are rejected.
fn check_file_constant_columns<'a>(
    schema: &SchemaRef,
    names: impl IntoIterator<Item = &'a String>,
    ctx: &str,
) -> DeltaResult<()> {
    for name in names {
        let Some(field) = schema.field(name.as_str()) else {
            return Err(Error::generic(format!(
                "{ctx}: column `{name}` not found; schema has {:?}",
                Vec::from_iter(schema.fields().map(|f| f.name())),
            )));
        };
        if field.is_metadata_column() {
            return Err(Error::generic(format!(
                "{ctx}: column `{name}` is a metadata column"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, column_name, lit, Expression};
    use crate::plans::ir::nodes::{FileType, LoadColumnFileMeta};
    use crate::schema::{DataType, StructField, StructType};
    use crate::FileMeta;

    /// A single-file scan (present), no file-constant columns -- the trivial scan fixture.
    fn scan(schema: SchemaRef) -> PlanBuilder {
        PlanBuilder::scan_parquet([test_file("file:///a.parquet")], &[], schema).unwrap()
    }

    fn test_file(path: &str) -> FileMeta {
        FileMeta {
            location: url::Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    fn id_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "id",
            DataType::STRING,
        )]))
    }

    fn x_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "x",
            DataType::LONG,
        )]))
    }

    /// A one-row (all-null) `Values` source over `schema` -- the common present-source fixture.
    /// (Empty rows would collapse to absent.)
    fn vals(schema: SchemaRef) -> PlanBuilder {
        let row = Vec::from_iter(schema.fields().map(|f| Scalar::null(f.data_type().clone())));
        PlanBuilder::values(schema, vec![row]).unwrap()
    }

    /// The absent (uninhabited) relation over `schema`.
    fn absent_over(schema: SchemaRef) -> PlanBuilder {
        PlanBuilder::values(schema, vec![]).unwrap()
    }

    /// The absent (uninhabited) relation over `id_schema`.
    fn absent_src() -> PlanBuilder {
        absent_over(id_schema())
    }

    /// A discriminant tag for asserting a node's operator without matching its payload.
    fn op_tag(op: &Operator) -> &'static str {
        match op {
            Operator::ScanParquet(_) => "scan_parquet",
            Operator::ScanJson(_) => "scan_json",
            Operator::Values(_) => "values",
            Operator::Filter(_) => "filter",
            Operator::Project(_) => "project",
            Operator::Load(_) => "load",
            Operator::Aggregate(_) => "aggregate",
            Operator::SemiJoin(_) => "semi_join",
            Operator::UnionAll(_) => "union_all",
        }
    }

    /// Build `builder` and assert its nodes match `expected` one-for-one in order, each described
    /// by its `(inputs, op)`: the indices of the nodes it references and its operator
    /// [tag](op_tag). A node is identified by its index in `nodes`. Returns the built [`Plan`]
    /// for further inspection.
    fn assert_plan(builder: PlanBuilder, expected: &[(&[usize], &str)]) -> Plan {
        let plan = builder.build().unwrap();
        assert_eq!(plan.nodes.len(), expected.len(), "node count");
        for (i, (node, &(inputs, op))) in plan.nodes.iter().zip(expected).enumerate() {
            assert_eq!(node.inputs.as_slice(), inputs, "node {i} inputs");
            assert_eq!(op_tag(&node.op), op, "node {i} op");
        }
        plan
    }

    /// A source records its declared schema and builds to a single node.
    #[test]
    fn source_builds_single_node() {
        let src = scan(id_schema());
        assert_eq!(src.schema(), &id_schema());
        assert_plan(src, &[(&[], "scan_parquet")]);
    }

    /// `{ id, part }`, with `part` used as a file-constant column.
    fn part_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("part", DataType::STRING),
        ]))
    }

    /// A single-file scan with one file constant `"p1"` for the `part` column.
    fn one_constant_file(path: &str) -> ScanFile {
        ScanFile {
            meta: test_file(path),
            file_constants: vec!["p1".into()],
        }
    }

    /// Both scan sources carry file-constant columns and record them on the node.
    #[test]
    fn scans_record_file_constant_columns() -> DeltaResult<()> {
        let part = vec!["part".to_string()];

        let parquet = PlanBuilder::scan_parquet(
            vec![one_constant_file("file:///a.parquet")],
            &["part"],
            part_schema(),
        )?;
        assert_eq!(parquet.schema(), &part_schema());
        let plan = assert_plan(parquet, &[(&[], "scan_parquet")]);
        let Operator::ScanParquet(node) = &plan.nodes[0].op else {
            panic!("expected ScanParquet");
        };
        assert_eq!(node.file_constant_columns, part);

        let json = PlanBuilder::scan_json(
            vec![one_constant_file("file:///a.json")],
            &["part"],
            part_schema(),
        )?;
        let plan = assert_plan(json, &[(&[], "scan_json")]);
        let Operator::ScanJson(node) = &plan.nodes[0].op else {
            panic!("expected ScanJson");
        };
        assert_eq!(node.file_constant_columns, part);
        Ok(())
    }

    /// Filter passes the input schema through unchanged.
    #[test]
    fn filter_preserves_schema() -> DeltaResult<()> {
        let filtered = vals(id_schema()).filter(col!("id").is_not_null())?;
        assert_eq!(filtered.schema(), &id_schema());
        Ok(())
    }

    /// A method chain linearizes in topological order with the terminal last.
    #[test]
    fn monadic_chain_is_topological() -> DeltaResult<()> {
        assert_plan(
            scan(id_schema())
                .filter(col!("id").is_not_null())?
                .filter(col!("id").is_null())?,
            &[(&[], "scan_parquet"), (&[0], "filter"), (&[1], "filter")],
        );
        Ok(())
    }

    /// A value reused by two consumers (SSA style) is emitted exactly once: the shared source
    /// node appears a single time and both consumers reference its index.
    #[test]
    fn shared_source_emitted_once() -> DeltaResult<()> {
        let a = vals(id_schema());
        let b = a.clone().filter(col!("id").is_not_null())?;
        let c = a.filter(col!("id").is_null())?;

        // `a` is emitted once as node 0; both filters reference it.
        assert_plan(
            PlanBuilder::union_all([b, c])?,
            &[
                (&[], "values"),
                (&[0], "filter"),
                (&[0], "filter"),
                (&[1, 2], "union_all"),
            ],
        );
        Ok(())
    }

    /// Nodes not reachable from the terminal are dropped (dead-code elimination).
    #[test]
    fn unreachable_nodes_are_dropped() -> DeltaResult<()> {
        let src = vals(id_schema());
        let _dead = src.clone().filter(col!("id").is_null())?; // never reaches the terminal
        let kept = src.filter(col!("id").is_not_null())?;

        // Only src(0) and the kept filter(1) survive.
        assert_plan(kept, &[(&[], "values"), (&[0], "filter")]);
        Ok(())
    }

    /// A shared *intermediate* (a node with its own input) feeding two consumers is emitted once,
    /// and both consumers reference its single index.
    #[test]
    fn shared_intermediate_emitted_once_diamond() -> DeltaResult<()> {
        let mid = vals(id_schema()).filter(col!("id").is_not_null())?;
        let left = mid.clone().filter(col!("id").is_not_null())?;
        let right = mid.filter(col!("id").is_null())?;

        // src(0) and mid(1) each emitted once; left(2) and right(3) both reference mid.
        assert_plan(
            PlanBuilder::union_all([left, right])?,
            &[
                (&[], "values"),
                (&[0], "filter"),
                (&[1], "filter"),
                (&[1], "filter"),
                (&[2, 3], "union_all"),
            ],
        );
        Ok(())
    }

    /// `union_all` over two or more present inputs keeps `self`'s schema and records all inputs.
    #[test]
    fn union_all_records_present_inputs() -> DeltaResult<()> {
        let inputs = Vec::from_iter((0..3).map(|_| vals(id_schema())));
        assert_plan(
            PlanBuilder::union_all(inputs)?,
            &[
                (&[], "values"),
                (&[], "values"),
                (&[], "values"),
                (&[0, 1, 2], "union_all"),
            ],
        );
        Ok(())
    }

    /// `union_all` with a single present input forwards it unchanged -- no `UnionAll` node.
    #[test]
    fn union_all_of_one_forwards() -> DeltaResult<()> {
        assert_plan(
            PlanBuilder::union_all([vals(id_schema())])?,
            &[(&[], "values")],
        );
        Ok(())
    }

    // === Absent (uninhabited) relation =======================================

    /// Empty sources and transforms over an absent input all collapse to the absent relation, which
    /// `build_opt` reports as `None`.
    #[rstest::rstest]
    #[case::empty_parquet(PlanBuilder::scan_parquet(Vec::<FileMeta>::new(), &[], id_schema()))]
    #[case::empty_json(PlanBuilder::scan_json(Vec::<FileMeta>::new(), &[], id_schema()))]
    #[case::empty_values(PlanBuilder::values(id_schema(), vec![]))]
    #[case::filter(absent_src().filter(col!("id").is_not_null()))]
    #[case::project(absent_src().project(Expression::struct_from([col!("id")]), id_schema()))]
    #[case::project_patch(absent_src().project_patch(|p| p.drop("id")))]
    #[case::load(absent_over(load_input_schema()).load(load_node(load_output_schema())))]
    #[case::grouped_aggregate(absent_src().aggregate(
        Aggregate::group_by(part_schema(), [column_name!("id")]).max(column_name!("part"))))]
    #[case::semi_join_absent_build(
        vals(id_schema()).semi_join(absent_src(), [column_name!("id")], [column_name!("id")]))]
    #[case::semi_join_absent_probe(
        absent_src().semi_join(vals(x_schema()), [column_name!("id")], [column_name!("x")]))]
    #[case::anti_join_absent_probe(
        absent_src().anti_join(vals(x_schema()), [column_name!("id")], [column_name!("x")]))]
    #[case::union_all_of_absent(PlanBuilder::union_all([absent_src(), absent_src()]))]
    fn collapses_to_absent(#[case] builder: DeltaResult<PlanBuilder>) -> DeltaResult<()> {
        assert!(builder?.build_opt()?.is_none()); // absent has no plan to run
        Ok(())
    }

    /// An absent relation still carries its schema, and `build` materializes it as an empty
    /// `Values` node.
    #[test]
    fn absent_carries_schema_and_builds_empty_values() -> DeltaResult<()> {
        let absent = absent_src();
        assert_eq!(absent.schema(), &id_schema());
        let plan = assert_plan(absent, &[(&[], "values")]);
        let Operator::Values(values) = &plan.nodes[0].op else {
            panic!("expected Values");
        };
        assert!(values.rows.is_empty());
        assert_eq!(values.schema, id_schema());
        Ok(())
    }

    /// `union_all` drops absent arms, so a lone present arm survives and is forwarded unchanged --
    /// no `UnionAll` node.
    #[test]
    fn union_all_forwards_lone_present_arm() -> DeltaResult<()> {
        assert_plan(
            PlanBuilder::union_all([absent_src(), vals(id_schema())])?,
            &[(&[], "values")],
        );
        Ok(())
    }

    /// An anti join with an absent build forwards its probe unchanged: it subtracts nothing.
    #[test]
    fn anti_join_over_absent_build_forwards_probe() -> DeltaResult<()> {
        let anti = vals(id_schema()).anti_join(
            absent_src(),
            [column_name!("id")],
            [column_name!("id")],
        )?;
        assert_plan(anti, &[(&[], "values")]);
        Ok(())
    }

    /// `aggregate` outputs the group keys followed by the aggregate columns.
    #[test]
    fn aggregate_outputs_keys_then_aggs() -> DeltaResult<()> {
        // Group by `id`, take the latest `part` by version -- output is `{id, part}`.
        let agg = vals(part_schema()).aggregate(
            Aggregate::group_by(part_schema(), [column_name!("id")])
                .max_non_null_by(column_name!("part"), column_name!("id")),
        )?;
        assert_eq!(agg.schema(), &part_schema());
        assert_plan(agg, &[(&[], "values"), (&[0], "aggregate")]);
        Ok(())
    }

    /// `aggregate_by` roots the `AggregateBuilder` at the input schema, yielding the same plan as
    /// `aggregate` with an explicitly schema'd builder.
    #[test]
    fn aggregate_by_infers_input_schema() -> DeltaResult<()> {
        let agg = vals(part_schema()).aggregate_by([column_name!("id")], |a| {
            a.max_non_null_by(column_name!("part"), column_name!("id"))
        })?;
        assert_eq!(agg.schema(), &part_schema());
        assert_plan(agg, &[(&[], "values"), (&[0], "aggregate")]);
        Ok(())
    }

    /// `aggregate_ungrouped` roots the `AggregateBuilder` at the input schema without grouping.
    #[test]
    fn aggregate_ungrouped_infers_input_schema() -> DeltaResult<()> {
        let agg = vals(part_schema()).aggregate_ungrouped(|a| a.max(column_name!("part")))?;
        let schema = agg.schema();
        assert_eq!(schema.fields().count(), 1);
        assert!(schema.field("part").is_some());
        assert_plan(agg, &[(&[], "values"), (&[0], "aggregate")]);
        Ok(())
    }

    /// A global (no group keys) aggregate over an absent input aggregates an empty `Values`
    /// relation, so the engine produces the single output row.
    #[test]
    fn aggregate_global_over_absent_aggregates_empty_values() -> DeltaResult<()> {
        let agg = absent_src().aggregate(
            Aggregate::group_by(id_schema(), Vec::<ColumnName>::new()).max(column_name!("id")),
        )?;
        let plan = assert_plan(agg, &[(&[], "values"), (&[0], "aggregate")]);
        let Operator::Values(values) = &plan.nodes[0].op else {
            panic!("expected Values input");
        };
        assert!(values.rows.is_empty());
        Ok(())
    }

    /// A global (no group keys) aggregate over a present input outputs just the aggregate columns.
    #[test]
    fn aggregate_global_over_present_outputs_aggs_only() -> DeltaResult<()> {
        let agg = vals(part_schema()).aggregate(
            Aggregate::group_by(part_schema(), Vec::<ColumnName>::new()).max(column_name!("part")),
        )?;
        let schema = agg.schema();
        assert_eq!(schema.fields().count(), 1);
        assert!(schema.field("part").is_some());
        let plan = assert_plan(agg, &[(&[], "values"), (&[0], "aggregate")]);
        let Operator::Aggregate(node) = &plan.nodes[1].op else {
            panic!("expected Aggregate");
        };
        assert!(node.group_by.is_empty());
        Ok(())
    }

    /// `{ outer: { a, b }, c }` -- a nested schema for exercising `project_patch`.
    fn nested_ab_c() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable(
                "outer",
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::LONG),
                    StructField::nullable("b", DataType::STRING),
                ]),
            ),
            StructField::nullable("c", DataType::LONG),
        ]))
    }

    /// `project_patch` lowers field edits and the output schema together: a nested replace, a
    /// top-level append, and a drop are all reflected in the resulting schema.
    #[test]
    fn project_patch_edits_track_schema() -> DeltaResult<()> {
        let patched = vals(nested_ab_c()).project_patch(|p| {
            p.replace_at(
                ["outer"],
                "b",
                StructField::nullable("b2", DataType::LONG),
                lit(0i64),
            )
            .append(StructField::nullable("d", DataType::LONG), lit(1i64))
            .drop("c")
        })?;

        let expected: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::nullable(
                "outer",
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::LONG),
                    StructField::nullable("b2", DataType::LONG),
                ]),
            ),
            StructField::nullable("d", DataType::LONG),
        ]));
        assert_eq!(patched.schema(), &expected);
        assert_plan(patched, &[(&[], "values"), (&[0], "project")]);
        Ok(())
    }

    /// A shared edit `fn` can be reused across multiple `project_patch` sites: it is higher-ranked
    /// over the patch-builder lifetime, which a `let` closure cannot express.
    #[test]
    fn project_patch_accepts_shared_fn() -> DeltaResult<()> {
        fn drop_c(p: ProjectionStructPatchBuilder<'_>) -> ProjectionStructPatchBuilder<'_> {
            p.drop("c")
        }
        let a = vals(nested_ab_c()).project_patch(drop_c)?;
        let b = vals(nested_ab_c()).project_patch(drop_c)?;
        assert_eq!(a.schema(), b.schema());
        Ok(())
    }

    /// `project` records the caller's declared output schema verbatim.
    #[test]
    fn project_records_declared_schema() -> DeltaResult<()> {
        let out = x_schema();
        let p = vals(id_schema()).project(Expression::struct_from([col!("id")]), out.clone())?;
        assert_eq!(p.schema(), &out);
        assert_plan(p, &[(&[], "values"), (&[0], "project")]);
        Ok(())
    }

    /// `semi_join`/`anti_join` mirror the probe schema, record `[probe, build]` inputs in order,
    /// and set `inverted` accordingly.
    #[rstest::rstest]
    fn join_mirrors_probe_and_orders_inputs(
        #[values(false, true)] inverted: bool,
    ) -> DeltaResult<()> {
        let probe = vals(id_schema());
        let build = vals(x_schema());
        let joined = if inverted {
            probe.anti_join(build, [column_name!("id")], [column_name!("x")])?
        } else {
            probe.semi_join(build, [column_name!("id")], [column_name!("x")])?
        };
        assert_eq!(joined.schema(), &id_schema());

        let plan = assert_plan(
            joined,
            &[(&[], "values"), (&[], "values"), (&[0, 1], "semi_join")],
        );
        let Operator::SemiJoin(node) = &plan.nodes[2].op else {
            panic!("expected SemiJoin");
        };
        assert_eq!(node.inverted, inverted);
        Ok(())
    }

    /// Build a [`Load`] over the canonical column names used by [`load_input_schema`].
    fn load_node(out_schema: SchemaRef) -> Load {
        let file_meta = LoadColumnFileMeta::new(
            column_name!("path"),
            column_name!("size"),
            column_name!("num_records"),
        );
        Load::new(out_schema, FileType::Parquet, file_meta, column_name!("dv"))
            .with_base_url(url::Url::parse("memory:///").unwrap())
            .with_file_constant_columns(["version"])
    }

    /// An upstream (input) schema carrying every column `load_node` reads, including the
    /// file-constant source column `version`.
    fn load_input_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("num_records", DataType::LONG),
            StructField::nullable("dv", DataType::STRING),
            StructField::nullable("version", DataType::LONG),
        ]))
    }

    /// `load_node`'s output schema: a file-read column plus the file-constant column `version`.
    fn load_output_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("version", DataType::LONG),
        ]))
    }

    /// `load`'s output schema is the [`Load`] payload's schema (not input passthrough), and it
    /// records the upstream as its single input.
    #[test]
    fn load_sets_output_schema_and_records_input() -> DeltaResult<()> {
        let out = load_output_schema();
        let loaded = vals(load_input_schema()).load(load_node(out.clone()))?;
        assert_eq!(loaded.schema(), &out);
        assert_plan(loaded, &[(&[], "values"), (&[0], "load")]);
        Ok(())
    }

    /// [`load_input_schema`] with `omit` removed -- exercises each upstream column `load` resolves.
    fn load_input_missing(omit: &str) -> SchemaRef {
        let fields = ["path", "size", "num_records", "dv", "version"]
            .into_iter()
            .filter(|c| *c != omit)
            .map(|c| StructField::nullable(c, DataType::STRING));
        Arc::new(StructType::new_unchecked(fields))
    }

    /// Every validating method surfaces its malformed input as an error at the call site, with a
    /// message that names the offending column or mismatch.
    #[rstest::rstest]
    // sources
    #[case::scan_file_constant_arity("constant value",
        || PlanBuilder::scan_parquet(vec![one_constant_file("file:///a.parquet")], &[], part_schema()))]
    #[case::scan_unknown_file_constant("`nope` not found",
        || PlanBuilder::scan_parquet(vec![one_constant_file("file:///a.parquet")], &["nope"], part_schema()))]
    #[case::values_row_width("schema has 1 field",
        || PlanBuilder::values(id_schema(), vec![vec!["a".into(), "b".into()]]))]
    // transforms referencing an absent column
    #[case::filter_unknown_column("`nope` not found",
        || vals(id_schema()).filter(col!("nope").is_not_null()))]
    #[case::project_unknown_column("`nope` not found",
        || vals(id_schema()).project(Expression::struct_from([col!("nope")]), id_schema()))]
    #[case::project_patch_missing_field("does not exist",
        || vals(id_schema()).project_patch(|p| p.drop("nope")))]
    #[case::aggregate_unknown_value("not found in schema",
        || vals(id_schema()).aggregate(Aggregate::group_by(id_schema(), Vec::<ColumnName>::new()).max(column_name!("nope"))))]
    #[case::aggregate_unknown_group_by("not found in schema",
        || vals(id_schema()).aggregate(Aggregate::group_by(id_schema(), [column_name!("nope")]).max(column_name!("id"))))]
    // union
    #[case::union_schema_disagrees("differing",
        || PlanBuilder::union_all([vals(id_schema()), vals(x_schema())]))]
    #[case::union_empty("at least one input", || PlanBuilder::union_all([]))]
    // semi-join keys
    #[case::join_unknown_probe_key("join probe: column `nope`",
        || vals(id_schema()).semi_join(vals(x_schema()), [column_name!("nope")], [column_name!("x")]))]
    #[case::join_unknown_build_key("join build: column `nope`",
        || vals(id_schema()).anti_join(vals(x_schema()), [column_name!("id")], [column_name!("nope")]))]
    #[case::join_key_arity("must match",
        || vals(id_schema()).semi_join(vals(x_schema()), [column_name!("id")], [column_name!("x"), column_name!("x")]))]
    // load: each upstream column it reads, plus the output-side file-constant
    #[case::load_missing_path("`path`", || vals(load_input_missing("path")).load(load_node(load_output_schema())))]
    #[case::load_missing_size("`size`", || vals(load_input_missing("size")).load(load_node(load_output_schema())))]
    #[case::load_missing_num_records("`num_records`", || vals(load_input_missing("num_records")).load(load_node(load_output_schema())))]
    #[case::load_missing_dv("`dv`", || vals(load_input_missing("dv")).load(load_node(load_output_schema())))]
    #[case::load_missing_file_constant_source("`version`", || vals(load_input_missing("version")).load(load_node(load_output_schema())))]
    #[case::load_file_constant_absent_from_output("`version`", || vals(load_input_schema()).load(load_node(id_schema())))]
    fn rejects(#[case] needle: &str, #[case] make: impl Fn() -> DeltaResult<PlanBuilder>) {
        let err = make().unwrap_err();
        assert!(err.to_string().contains(needle), "got: {err}");
    }
}
