use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use tracing::{debug, error};

use crate::actions::visitors::SelectionVectorVisitor;
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS};
use crate::error::DeltaResult;
use crate::expressions::{
    column_expr, column_name, BinaryPredicateOp, ColumnName, Expression as Expr, ExpressionRef,
    JunctionPredicateOp, OpaquePredicateOpRef, Predicate as Pred, PredicateRef, Scalar,
};
use crate::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator, KernelPredicateEvaluatorDefaults,
};
use crate::scan::data_skipping::stats_schema::is_skipping_eligible_datatype;
use crate::scan::log_replay::PARTITION_VALUES_PARSED_NAME;
use crate::scan::metrics::ScanMetrics;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{Engine, EngineData, Error, ExpressionEvaluator, PredicateEvaluator, RowVisitor as _};

pub(crate) mod stats_schema;
#[cfg(test)]
mod tests;

use delta_kernel_derive::internal_api;

/// Rewrites a predicate to a predicate that can be used to skip files based on their stats.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// We normalize each binary operation to a comparison between a column and a literal value and
/// rewrite that in terms of the min/max values of the column.
/// For example, `1 < a` is rewritten as `minValues.a > 1`.
///
/// For Unary `Not`, we push the Not down using De Morgan's Laws to invert everything below the Not.
///
/// Unary `IsNull` checks if the null counts indicate that the column could contain a null.
///
/// The junction operations are rewritten as follows:
/// - `AND` is rewritten as a conjunction of the rewritten operands where we just skip operands that
///   are not eligible for data skipping.
/// - `OR` is rewritten only if all operands are eligible for data skipping. Otherwise, the whole OR
///   predicate is dropped.
#[cfg(test)]
pub(crate) fn as_data_skipping_predicate(pred: &Pred) -> Option<Pred> {
    let stats_columns = all_referenced_columns(pred);
    DataSkippingPredicateCreator::new(&Default::default(), &stats_columns).eval(pred)
}

/// Permissive stats-columns set for tests that exercise rewrite mechanics, not the gate.
#[cfg(test)]
pub(crate) fn all_referenced_columns(pred: &Pred) -> HashSet<ColumnName> {
    pred.references().into_iter().cloned().collect()
}

#[cfg(test)]
pub(crate) fn as_data_skipping_predicate_with_partitions(
    pred: &Pred,
    partition_columns: &HashSet<ColumnName>,
) -> Option<Pred> {
    let stats_columns = all_referenced_columns(pred);
    DataSkippingPredicateCreator::new(partition_columns, &stats_columns).eval(pred)
}

/// Like [`as_data_skipping_predicate_with_partitions`] but invokes
/// [`KernelPredicateEvaluator::eval_sql_where`] instead of [`KernelPredicateEvaluator::eval`].
#[cfg(test)]
fn as_sql_data_skipping_predicate(
    pred: &Pred,
    partition_columns: &HashSet<ColumnName>,
) -> Option<Pred> {
    let stats_columns = all_referenced_columns(pred);
    as_sql_data_skipping_predicate_with_stats_columns(pred, partition_columns, &stats_columns)
}

/// Like [`as_sql_data_skipping_predicate`] but only rewrites references to columns in
/// `stats_columns`; other columns return `None` from the `get_*_stat` methods and
/// junction-fold into NULL literals.
pub(crate) fn as_sql_data_skipping_predicate_with_stats_columns(
    pred: &Pred,
    partition_columns: &HashSet<ColumnName>,
    stats_columns: &HashSet<ColumnName>,
) -> Option<Pred> {
    DataSkippingPredicateCreator::new(partition_columns, stats_columns).eval_sql_where(pred)
}

#[internal_api]
pub(crate) struct DataSkippingFilter {
    /// Evaluator that extracts file-level statistics from the input batch. The caller provides
    /// the expression at construction time, which determines where stats come from:
    /// - Scan path: `column_expr!("stats_parsed")` reads the already-parsed struct from a
    ///   transformed batch (where `add.*` fields are flattened to top-level columns).
    /// - Table changes path: `Expression::parse_json(column_expr!("add.stats"), schema)` parses
    ///   JSON from a raw action batch (where stats are nested under `add.stats`).
    stats_evaluator: Arc<dyn ExpressionEvaluator>,
    skipping_evaluator: Arc<dyn PredicateEvaluator>,
    filter_evaluator: Arc<dyn PredicateEvaluator>,
    /// Metrics to record data skipping statistics.
    metrics: Option<Arc<ScanMetrics>>,
}

impl DataSkippingFilter {
    /// Creates a new data skipping filter. Returns `None` if there is no predicate, or the
    /// predicate is ineligible for data skipping.
    ///
    /// NOTE: `None` is equivalent to a trivial filter that always returns TRUE (= keeps all files),
    /// but using an `Option` lets the engine easily avoid the overhead of applying trivial filters.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators
    /// - `predicate`: Optional predicate for data skipping
    /// - `stats_schema`: The data stats schema (numRecords, nullCount, minValues, maxValues). Pass
    ///   `None` if no data stats are available.
    /// - `stats_expr`: Expression to extract data stats from the batch, producing output matching
    ///   `stats_schema`. For example, `column_expr!("stats_parsed")` for pre-parsed stats, or
    ///   `Expression::parse_json(column_expr!("add.stats"), stats_schema)` for JSON parsing.
    /// - `partition_schema`: Schema of typed partition columns referenced by the predicate
    ///   (physical names). Pass `None` if no partition columns are referenced.
    /// - `partition_expr`: Expression to extract partition values from the batch, producing output
    ///   matching `partition_schema`. Typically a `MapToStruct` expression that converts the
    ///   `partitionValues` string map into a typed struct. Only used when `partition_schema` is
    ///   `Some`.
    /// - `is_add_expr`: Boolean expression that is true for Add rows and false for Remove and other
    ///   non-file rows (e.g. `path IS NOT NULL` against a transformed batch, or `add.path IS NOT
    ///   NULL` against a raw action batch). Used to guard partition predicates and opaque-predicate
    ///   rewrites so non-Add rows are never filtered out.
    /// - `input_schema`: Schema of the batch that will be passed to [`apply()`](Self::apply)
    /// - `stats_columns`: Physical leaf paths whose stats are in `stats_schema`. References to
    ///   other data columns fold to NULL (keeping the file). Must line up with `stats_schema` --
    ///   otherwise the rewritten predicate references columns the unified schema doesn't have and
    ///   evaluator construction fails.
    /// - `metrics`: Optional metrics to record data skipping statistics.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        engine: &dyn Engine,
        predicate: Option<PredicateRef>,
        stats_schema: Option<&SchemaRef>,
        stats_expr: ExpressionRef,
        partition_schema: Option<&SchemaRef>,
        partition_expr: ExpressionRef,
        is_add_expr: ExpressionRef,
        input_schema: SchemaRef,
        stats_columns: &HashSet<ColumnName>,
        metrics: Option<Arc<ScanMetrics>>,
    ) -> Option<Self> {
        static FILTER_PRED: LazyLock<PredicateRef> =
            LazyLock::new(|| Arc::new(column_expr!("output").distinct(Expr::literal(false))));
        static FILTER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            Arc::new(StructType::new_unchecked([StructField::nullable(
                "output",
                DataType::BOOLEAN,
            )]))
        });

        let predicate = predicate?;
        debug!("Creating a data skipping filter for {:#?}", predicate);

        // Build the unified evaluation schema and extraction expression. Data stats and partition
        // values are conceptually separate, but the evaluator needs a single schema/expression.
        let (unified_schema, unified_expr, partition_columns) =
            Self::build_unified_schema_and_expr(
                stats_schema,
                stats_expr,
                partition_schema,
                partition_expr,
                is_add_expr,
            )?;

        let stats_evaluator = engine
            .evaluation_handler()
            .new_expression_evaluator(
                input_schema,
                unified_expr,
                unified_schema.as_ref().clone().into(),
            )
            .inspect_err(|e| error!("Failed to create stats evaluator: {e}"))
            .ok()?;

        // Skipping happens in several steps:
        //
        // 1. The stats evaluator extracts file-level statistics from the batch (the expression
        //    provided by the caller determines how: reading a pre-parsed column, parsing JSON,
        //    etc.)
        //
        // 2. The predicate (skipping evaluator) produces false for any file whose stats prove we
        //    can safely skip it. A value of true means the stats say we must keep the file, and
        //    null means we could not determine whether the file is safe to skip, because its stats
        //    were missing/null.
        //
        // 3. The selection evaluator does DISTINCT(col(predicate), 'false') to produce true (=
        //    keep) when the predicate is true/null and false (= skip) when the predicate is false.
        let skipping_evaluator = engine
            .evaluation_handler()
            .new_predicate_evaluator(
                unified_schema.clone(),
                Arc::new(as_sql_data_skipping_predicate_with_stats_columns(
                    &predicate,
                    &partition_columns,
                    stats_columns,
                )?),
            )
            .inspect_err(|e| error!("Failed to create skipping evaluator: {e}"))
            .ok()?;

        // The filter evaluator operates on the skipping evaluator's output, which is a single
        // boolean column named "output" (not the unified stats schema).
        let filter_evaluator = engine
            .evaluation_handler()
            .new_predicate_evaluator(FILTER_SCHEMA.clone(), FILTER_PRED.clone())
            .inspect_err(|e| error!("Failed to create filter evaluator: {e}"))
            .ok()?;

        Some(Self {
            stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
            metrics,
        })
    }

    /// Builds a filter over raw `{ add, remove, ... }` action batches, parsing file stats from
    /// the JSON `add.stats` string. This is the shape used by log-replay paths that skip
    /// *before* transforming actions (the change-data-feed path and the incremental scan),
    /// unlike the scan path which reads pre-parsed `stats_parsed` from transformed batches.
    ///
    /// The stats schema is derived from the predicate's column references via
    /// [`TableConfiguration::build_expected_stats_schemas`], matching the write side exactly;
    /// references outside the table's stats columns fold to NULL (keeping the file). Partition
    /// values are parsed from the raw `add.partitionValues` string map with
    /// [`Expression::map_to_struct`], so predicates over partition columns prune too.
    ///
    /// Returns `None` (equivalent to keep-all) when the predicate is ineligible for data skipping
    /// (references no stats columns, or fails evaluator construction).
    ///
    /// Pruning applies only to Add rows: Remove and cdc rows (null `add.path`) are never pruned,
    /// guarded by the `add.path IS NOT NULL` `is_add` expression wired in below. Callers on the
    /// change-data-feed path rely on this so tombstones survive a non-matching predicate.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators.
    /// - `physical_predicate`: The predicate, already lowered to physical column names.
    /// - `table_configuration`: Source of the stats schema, partition schema, and stats-column
    ///   gate.
    /// - `input_schema`: Schema of the raw action batches passed to [`apply()`](Self::apply).
    pub(crate) fn for_raw_action_batch(
        engine: &dyn Engine,
        physical_predicate: PredicateRef,
        table_configuration: &TableConfiguration,
        input_schema: SchemaRef,
    ) -> Option<Self> {
        // Predicate refs become the `requested_physical_columns` filter; refs outside
        // `physical_stats_columns` fold to NULL via the gate below. Clustering columns are
        // deliberately not required here (see the scan path and #2588): loading clustering
        // domain metadata would require a separate scan of the log, which this filter avoids.
        let predicate_refs: Vec<ColumnName> = physical_predicate
            .references()
            .into_iter()
            .cloned()
            .collect();
        let physical_stats_columns = table_configuration.physical_stats_columns_set(None);
        let physical_stats_schema = table_configuration
            .build_expected_stats_schemas(None, Some(&predicate_refs))
            .ok()?
            .physical;
        let partition_schema = table_configuration.predicate_partition_schema(&predicate_refs);

        // Parse JSON stats from the raw action batch's `add.stats` column, parse partition values
        // from the raw `add.partitionValues` string map, and identify Add rows by
        // `add.path IS NOT NULL` (raw batches keep the nested layout).
        let stats_expr = Arc::new(Expr::parse_json(
            column_expr!("add.stats"),
            physical_stats_schema.clone(),
        ));
        let partition_expr = Arc::new(Expr::map_to_struct(column_expr!("add.partitionValues")));
        let is_add_expr = Arc::new(Pred::is_not_null(column_expr!("add.path")).into());
        Self::new(
            engine,
            Some(physical_predicate),
            Some(&physical_stats_schema),
            stats_expr,
            partition_schema.as_ref(),
            partition_expr,
            is_add_expr,
            input_schema,
            &physical_stats_columns,
            None,
        )
    }

    /// Builds the unified schema and extraction expression from separate data stats and partition
    /// value inputs. Returns `None` if neither stats nor partition values are available.
    ///
    /// The caller provides a flat stats schema (e.g. `{ numRecords, minValues: { x } }`) and an
    /// expression to extract it from the input batch. This function wraps both under a
    /// `stats_parsed` struct field, producing a nested schema like
    /// `{ stats_parsed: { numRecords, minValues: { x } } }` and a corresponding
    /// `struct_from([stats_expr])` extraction expression. This ensures the unified schema aligns
    /// with the `stats_parsed.*` prefixed column references emitted by
    /// `DataSkippingPredicateCreator`. Partition values are similarly wrapped under
    /// `partitionValues_parsed` when present.
    fn build_unified_schema_and_expr(
        physical_stats_schema: Option<&SchemaRef>,
        stats_expr: ExpressionRef,
        physical_partition_schema: Option<&SchemaRef>,
        partition_expr: ExpressionRef,
        is_add_expr: ExpressionRef,
    ) -> Option<(SchemaRef, ExpressionRef, HashSet<ColumnName>)> {
        let partition_columns: HashSet<ColumnName> = physical_partition_schema
            .map(|s| s.fields().map(|f| ColumnName::new([f.name()])).collect())
            .unwrap_or_default();

        let stats_field =
            |stats: &SchemaRef| StructField::nullable("stats_parsed", stats.as_ref().clone());
        let partition_field =
            |ps: &SchemaRef| StructField::nullable("partitionValues_parsed", ps.as_ref().clone());
        let is_add_field = StructField::not_null("is_add", DataType::BOOLEAN);

        // Always include an `is_add` boolean (extracted by the caller-provided `is_add_expr`,
        // true for Add rows and false for Remove/non-file rows) so that predicates can guard
        // against filtering Remove rows: partition predicates and opaque-predicate rewrites are
        // wrapped with `OR(NOT is_add, ...)` (see `guard_for_removes`).
        let unified_schema = match (physical_stats_schema, physical_partition_schema) {
            (Some(stats), Some(ps)) => Arc::new(StructType::new_unchecked([
                stats_field(stats),
                partition_field(ps),
                is_add_field,
            ])),
            (Some(stats), None) => Arc::new(StructType::new_unchecked([
                stats_field(stats),
                is_add_field,
            ])),
            (None, Some(ps)) => Arc::new(StructType::new_unchecked([
                partition_field(ps),
                is_add_field,
            ])),
            (None, None) => return None,
        };

        let unified_expr = match (
            physical_stats_schema.is_some(),
            physical_partition_schema.is_some(),
        ) {
            (true, true) => Arc::new(Expr::struct_from([stats_expr, partition_expr, is_add_expr])),
            (true, false) => Arc::new(Expr::struct_from([stats_expr, is_add_expr])),
            (false, true) => Arc::new(Expr::struct_from([partition_expr, is_add_expr])),
            (false, false) => return None,
        };

        Some((unified_schema, unified_expr, partition_columns))
    }

    /// Apply the DataSkippingFilter to an EngineData batch. Returns a selection vector
    /// which can be applied to the batch to find rows that passed data skipping.
    pub(crate) fn apply(&self, batch: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        let start_time = Instant::now();
        let batch_len = batch.len();

        let file_stats = self.stats_evaluator.evaluate(batch)?;
        require!(
            file_stats.len() == batch_len,
            Error::internal_error(format!(
                "stats evaluator output length {} != batch length {}",
                file_stats.len(),
                batch_len
            ))
        );

        let skipping_predicate = self.skipping_evaluator.evaluate(&*file_stats)?;
        require!(
            skipping_predicate.len() == batch_len,
            Error::internal_error(format!(
                "skipping evaluator output length {} != batch length {}",
                skipping_predicate.len(),
                batch_len
            ))
        );

        let selection_vector = self
            .filter_evaluator
            .evaluate(skipping_predicate.as_ref())?;
        debug_assert_eq!(selection_vector.len(), batch_len);
        require!(
            selection_vector.len() == batch_len,
            Error::internal_error(format!(
                "filter evaluator output length {} != batch length {}",
                selection_vector.len(),
                batch_len
            ))
        );

        let mut visitor = SelectionVectorVisitor::default();
        visitor.visit_rows_of(selection_vector.as_ref())?;

        if visitor.num_filtered > 0 {
            debug!(
                "data skipping filtered {}/{batch_len} rows from batch",
                visitor.num_filtered
            );
        }

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.add_predicate_filtered(visitor.num_filtered);
            metrics.add_predicate_eval_time_ns(start_time.elapsed().as_nanos() as u64)
        }

        Ok(visitor.selection_vector)
    }
}

/// Rewrites a predicate for parquet row group skipping in checkpoint/sidecar files.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// Adds IS NULL guards on each stat column reference so the parquet RowGroupFilter
/// conservatively keeps row groups containing files with missing stats (null stat values
/// are invisible to footer min/max). The rewrite is `stats_parsed.*`-rooted, so `col_a > 100`
/// becomes:
/// ```text
/// OR(stats_parsed.maxValues.col_a IS NULL, stats_parsed.maxValues.col_a > 100)
/// ```
///
/// Partition columns are excluded since their values live in `add.partitionValues_parsed`,
/// not `add.stats_parsed`. `physical_partition_columns` is the table's full partition list;
/// pass an empty set for unpartitioned tables.
///
/// `physical_stats_columns` restricts rewrites to columns which are expected to have stats
/// collected; other references fold to NULL (keeps the file). Must match the column set
/// used to build `physical_stats_schema`, otherwise the row-group filter sees a missing
/// field.
pub(crate) fn as_checkpoint_skipping_predicate(
    pred: &Pred,
    physical_partition_columns: &HashSet<ColumnName>,
    physical_stats_columns: &HashSet<ColumnName>,
) -> Option<Pred> {
    CheckpointDataSkippingPredicateCreator {
        data_skipping_columns: DataSkippingColumns {
            physical_partition_columns,
            physical_stats_columns,
        },
    }
    .eval(pred)
}

/// Maps an ordering and inversion flag to the corresponding comparison predicate.
fn comparison_predicate(ord: Ordering, col: Expr, val: &Scalar, inverted: bool) -> Pred {
    let pred_fn = match (ord, inverted) {
        (Ordering::Less, false) => Pred::lt,
        (Ordering::Less, true) => Pred::ge,
        (Ordering::Equal, false) => Pred::eq,
        (Ordering::Equal, true) => Pred::ne,
        (Ordering::Greater, false) => Pred::gt,
        (Ordering::Greater, true) => Pred::le,
    };
    pred_fn(col, val.clone())
}

/// Collects sub-predicates into a junction (AND/OR), replacing unsupported sub-predicates (None)
/// with a single NULL literal to preserve correct three-valued logic. One NULL is enough to
/// produce the correct behavior during predicate evaluation; additional NULLs are redundant.
fn collect_junction_preds(
    mut op: JunctionPredicateOp,
    preds: &mut dyn Iterator<Item = Option<Pred>>,
    inverted: bool,
) -> Pred {
    if inverted {
        op = op.invert();
    }
    let mut keep_null = true;
    let preds: Vec<_> = preds
        .flat_map(|p| match p {
            Some(pred) => Some(pred),
            None => keep_null.then(|| {
                keep_null = false;
                Pred::null_literal()
            }),
        })
        .collect();
    Pred::junction(op, preds)
}

/// Adjusts a comparison value before comparing against a max stat, to account for the Delta
/// protocol allowing timestamp stats to be truncated to millisecond precision (see Per-file
/// Statistics: "Timestamp columns are truncated down to milliseconds"). Truncation floors to
/// the nearest millisecond, so: `stored_max <= actual_max <= stored_max + 999us`. We subtract
/// 999us from the comparison value to avoid incorrectly pruning files whose actual max may be
/// higher than the stored (truncated) max.
///
/// For example, if a file's actual max is `4_000_500us` (4.000500s), Spark truncates the
/// stored max stat to `4_000_000us` (4.000s). A predicate `ts > 4_000_400` would incorrectly
/// prune this file by comparing against the truncated max. By adjusting the comparison value
/// to `4_000_400 - 999 = 3_999_401`, we ensure the file is kept.
///
/// Non-timestamp values pass through unchanged.
fn adjust_scalar_for_max_stat_truncation(val: &Scalar) -> Scalar {
    match val {
        Scalar::Timestamp(micros) => Scalar::Timestamp(micros.saturating_sub(999)),
        Scalar::TimestampNtz(micros) => Scalar::TimestampNtz(micros.saturating_sub(999)),
        other => other.clone(),
    }
}

/// The `partitionValues_parsed.<col>` reference holding a partition column's exact value, which
/// serves as both its min and max stat. `col` is a top-level physical partition name.
fn partition_value_expr(col: &ColumnName) -> Expr {
    Expr::from(ColumnName::new([PARTITION_VALUES_PARSED_NAME]).join(col))
}

/// A column carries min/max stats iff it's a primitive whose type supports min/max skipping.
/// Boolean / Binary, Array, Map, and Variant leaves carry nullCount only. Struct columns
/// have no per-struct stats; only their primitive leaves do, recursively.
/// Must match `MinMaxStatsTransform`'s acceptance rule. Otherwise the predicate creator
/// emits refs to min/max fields the stats schema doesn't contain.
fn has_min_max_stats(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Primitive(ptype) if is_skipping_eligible_datatype(ptype))
}

/// Column metadata shared by the data-skipping predicate creators.
struct DataSkippingColumns<'a> {
    /// Physical names of partition columns (always single-segment: Delta partition columns are
    /// top-level). Stats for these come from `partitionValues_parsed.<col>` (exact values) instead
    /// of min/max ranges; each creator applies its own partition policy.
    physical_partition_columns: &'a HashSet<ColumnName>,
    /// Physical leaf paths whose stats are present in `stats_parsed` (honors
    /// `delta.dataSkippingNumIndexedCols`, `delta.dataSkippingStatsColumns`, and required
    /// columns). Must match the column set used to build `physical_stats_schema`; otherwise the
    /// rewritten predicate references columns absent from the unified schema.
    physical_stats_columns: &'a HashSet<ColumnName>,
}

impl DataSkippingColumns<'_> {
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        self.physical_partition_columns.contains(col)
    }

    fn is_stats_column(&self, col: &ColumnName) -> bool {
        self.physical_stats_columns.contains(col)
    }

    /// Data column -> `stats_parsed.minValues.<col>`, or `None` when unindexed or not
    /// min/max-eligible.
    fn data_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        (self.is_stats_column(col) && has_min_max_stats(data_type))
            .then(|| Expr::from(column_name!("stats_parsed", MIN_VALUES).join(col)))
    }

    /// Data column -> `stats_parsed.maxValues.<col>`, or `None` when unindexed or not
    /// min/max-eligible.
    fn data_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        (self.is_stats_column(col) && has_min_max_stats(data_type))
            .then(|| Expr::from(column_name!("stats_parsed", MAX_VALUES).join(col)))
    }

    /// Data column -> `stats_parsed.nullCount.<col>`, or `None` when unindexed.
    fn data_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        self.is_stats_column(col)
            .then(|| Expr::from(column_name!("stats_parsed", NULL_COUNT).join(col)))
    }

    fn rowcount_stat(&self) -> Expr {
        column_expr!("stats_parsed", NUM_RECORDS)
    }
}

/// Rewrites user predicates into stats-based predicates for data skipping.
///
/// For data columns, rewrites to `stats_parsed.minValues.*`/`stats_parsed.maxValues.*`/
/// `stats_parsed.nullCount.*`.
/// For partition columns, rewrites to `partitionValues_parsed.*` since the partition value is
/// the exact value for every row in the file (serving as both min and max).
struct DataSkippingPredicateCreator<'a> {
    data_skipping_columns: DataSkippingColumns<'a>,
}

impl<'a> DataSkippingPredicateCreator<'a> {
    fn new(
        physical_partition_columns: &'a HashSet<ColumnName>,
        physical_stats_columns: &'a HashSet<ColumnName>,
    ) -> Self {
        Self {
            data_skipping_columns: DataSkippingColumns {
                physical_partition_columns,
                physical_stats_columns,
            },
        }
    }

    fn is_partition_column(&self, col: &ColumnName) -> bool {
        self.data_skipping_columns.is_partition_column(col)
    }

    /// Wraps a predicate with `OR(NOT is_add, pred)` to protect Remove rows from being
    /// filtered. Used for partition predicates (Remove rows have null add-side partition
    /// values, which would incorrectly evaluate to false) and for opaque-predicate rewrites
    /// (op-computed verdicts bypass kernel's null-stats folding). The `is_add` column ensures
    /// non-Add rows always pass the filter.
    fn guard_for_removes(&self, pred: Pred) -> Pred {
        Pred::or(Pred::not(Pred::from(column_name!("is_add"))), pred)
    }
}

impl DataSkippingPredicateEvaluator for DataSkippingPredicateCreator<'_> {
    type Output = Pred;
    type ColumnStat = Expr;

    /// Retrieves the minimum value of a column. For partition columns, returns the exact
    /// partition value (which serves as both min and max). Returns `None` for data columns
    /// outside the stat-columns set or whose type is not min/max-eligible.
    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            Some(partition_value_expr(col))
        } else {
            self.data_skipping_columns.data_min_stat(col, data_type)
        }
    }

    /// Retrieves the maximum value of a column. For partition columns, returns the exact
    /// partition value. Returns `None` for data columns outside the stat-columns set or whose
    /// type is not min/max-eligible.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            Some(partition_value_expr(col))
        } else {
            self.data_skipping_columns.data_max_stat(col, data_type)
        }
    }

    /// Compares a column's max stat against a literal value, adjusting for timestamp
    /// truncation on non-partition columns. Partition values are exact and not subject to
    /// JSON stats truncation, so no adjustment is needed. For data columns, the comparison
    /// value is adjusted by [`adjust_scalar_for_max_stat_truncation`].
    fn partial_cmp_max_stat(
        &self,
        col: &ColumnName,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Pred> {
        let max = self.get_max_stat(col, &val.data_type())?;
        if self.is_partition_column(col) {
            return self.eval_partial_cmp(ord, max, val, inverted);
        }
        let adjusted = adjust_scalar_for_max_stat_truncation(val);
        self.eval_partial_cmp(ord, max, &adjusted, inverted)
    }

    /// Retrieves the null count of a column. Partition columns don't have nullCount stats,
    /// nor do data columns outside the stat-columns set.
    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        if self.is_partition_column(col) {
            None
        } else {
            self.data_skipping_columns.data_nullcount_stat(col)
        }
    }

    /// Retrieves the row count statistic.
    fn get_rowcount_stat(&self) -> Option<Expr> {
        Some(self.data_skipping_columns.rowcount_stat())
    }

    /// For partition columns, wraps the comparison with `OR(NOT is_add, comparison)` so that
    /// Remove rows (which have null add-side partition values) are never filtered.
    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Expr,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        // Detect partition columns by the prefix set in get_min_stat/get_max_stat.
        let is_partition = matches!(&col, Expr::Column(name)
            if name.path().first().is_some_and(|f| f == PARTITION_VALUES_PARSED_NAME));
        let cmp = comparison_predicate(ord, col, val, inverted);
        Some(if is_partition {
            self.guard_for_removes(cmp)
        } else {
            cmp
        })
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    /// For partition columns, checks `partitionValues_parsed.<col> IS [NOT] NULL` directly,
    /// wrapped with `OR(NOT is_add, ...)` to protect Remove rows from being filtered.
    /// For data columns, uses nullCount stats.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        if self.is_partition_column(col) {
            let pv_expr = partition_value_expr(col);
            let pred = if inverted {
                Pred::is_not_null(pv_expr)
            } else {
                Pred::is_null(pv_expr)
            };
            Some(self.guard_for_removes(pred))
        } else {
            let safe_to_skip = match inverted {
                true => self.get_rowcount_stat()?, // all-null
                false => Expr::literal(0i64),      // no-null
            };
            Some(Pred::ne(self.get_nullcount_stat(col)?, safe_to_skip))
        }
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
            .map(Pred::literal)
    }

    /// Rewrites an opaque predicate via its `as_data_skipping_predicate`, wrapped with
    /// `OR(NOT is_add, ...)`. Opaque rewrites may embed op-computed verdicts (e.g. an engine
    /// callback behind FFI) that bypass kernel's null-stats folding, so kernel itself must
    /// guarantee that non-Add rows (Removes and other actions, which carry null stats) are never
    /// filtered -- dropping a Remove from log replay would resurrect a deleted file.
    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Pred> {
        let pred = op.as_data_skipping_predicate(self, exprs, inverted)?;
        Some(self.guard_for_removes(pred))
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Pred>>,
        inverted: bool,
    ) -> Option<Pred> {
        Some(collect_junction_preds(op, preds, inverted))
    }
}

/// Like [`DataSkippingPredicateCreator`] but adds IS NULL guards on stat column references for safe
/// parquet row group filtering in checkpoint/sidecar files. Partition columns are excluded since
/// their values live in `add.partitionValues_parsed`, not `add.stats_parsed`.
struct CheckpointDataSkippingPredicateCreator<'a> {
    data_skipping_columns: DataSkippingColumns<'a>,
}

impl CheckpointDataSkippingPredicateCreator<'_> {
    /// Returns true if the column is a partition column (no stats in `stats_parsed`).
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        self.data_skipping_columns.is_partition_column(col)
    }
}

impl DataSkippingPredicateEvaluator for CheckpointDataSkippingPredicateCreator<'_> {
    type Output = Pred;
    type ColumnStat = Expr;

    // The stat methods produce `stats_parsed.*`-rooted references; the checkpoint skipping path
    // then scopes them under `add`. Partition columns return None since their values live in
    // `add.partitionValues_parsed`.

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        self.data_skipping_columns.data_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        self.data_skipping_columns.data_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        self.data_skipping_columns.data_nullcount_stat(col)
    }

    fn get_rowcount_stat(&self) -> Option<Expr> {
        Some(self.data_skipping_columns.rowcount_stat())
    }

    /// Compares a column's max stat against a literal value, adjusting for timestamp
    /// truncation. See [`adjust_scalar_for_max_stat_truncation`].
    ///
    /// No partition column guard needed: `get_max_stat` returns `None` for partition columns,
    /// so their exact values never reach the adjustment.
    fn partial_cmp_max_stat(
        &self,
        col: &ColumnName,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Pred> {
        let max = self.get_max_stat(col, &val.data_type())?;
        let adjusted = adjust_scalar_for_max_stat_truncation(val);
        self.eval_partial_cmp(ord, max, &adjusted, inverted)
    }

    /// Wraps a stat column comparison with an IS NULL guard.
    ///
    /// `col > 100` → `OR(stats_parsed.maxValues.col IS NULL, stats_parsed.maxValues.col > 100)`
    ///
    /// `col = 100` (calls this twice, once per stat):
    /// ```text
    /// AND(
    ///   OR(stats_parsed.minValues.col IS NULL, stats_parsed.minValues.col <= 100),
    ///   OR(stats_parsed.maxValues.col IS NULL, stats_parsed.maxValues.col >= 100)
    /// )
    /// ```
    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Expr,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        let comparison = comparison_predicate(ord, col.clone(), val, inverted);
        Some(Pred::or(Pred::is_null(col), comparison))
    }

    /// No guard needed — no stat column reference. `TRUE` → `Some(true)`.
    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    /// No guard needed — no stat column reference. `NULL IS NULL` → `Some(true)`.
    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    /// IS NULL guard on nullCount stat.
    ///
    /// `IS NULL` → `OR(nullCount.col IS NULL, nullCount.col != 0)`:
    /// column vs literal — RowGroupFilter can evaluate via footer stats.
    ///
    /// `IS NOT NULL` → returns `None`. The unguarded version produces
    /// `nullCount.col != numRecords`, which is column vs column. The RowGroupFilter can
    /// only resolve one column at a time, so it can never prune with this predicate.
    // TODO(#1873): IS NOT NULL pruning requires cross-column range comparison in RowGroupFilter.
    // Skippable when the nullCount and numRecords ranges don't overlap (e.g. nullCount in
    // [0, 0] vs numRecords in [500, 2000] proves all files have non-null values).
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        if inverted {
            return None; // IS NOT NULL: column vs column, can't prune (#1873)
        }
        let nullcount = self.get_nullcount_stat(col)?;
        let comparison = Pred::ne(nullcount.clone(), Expr::literal(0i64));
        Some(Pred::or(Pred::is_null(nullcount), comparison))
    }

    /// No guard needed — no stat column reference. `5 < 10` → `Some(true)`.
    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
            .map(Pred::literal)
    }

    /// Unsupported. Opaque predicates can construct stat column references directly,
    /// bypassing IS NULL guards and risking false pruning. Returns `None` to conservatively
    /// drop these from the skipping predicate.
    fn eval_pred_opaque(
        &self,
        _op: &OpaquePredicateOpRef,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Pred> {
        None
    }

    /// Combines sub-predicates with AND/OR. `col_a > 100 AND col_b < 50` →
    /// ```text
    /// AND(
    ///   OR(stats_parsed.maxValues.col_a IS NULL, stats_parsed.maxValues.col_a > 100),
    ///   OR(stats_parsed.minValues.col_b IS NULL, stats_parsed.minValues.col_b < 50)
    /// )
    /// ```
    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Pred>>,
        inverted: bool,
    ) -> Option<Pred> {
        Some(collect_junction_preds(op, preds, inverted))
    }
}
