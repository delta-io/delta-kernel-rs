//! Discovery and enforcement of CHECK constraints (in development, gated by the
//! `check-constraints-in-dev` cargo feature).
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`; the Delta protocol requires every row added to the table to satisfy
//! every constraint (a row passes only when a constraint evaluates to `true` -- both `false` and
//! `NULL` are violations). Kernel never sees row data on the write path, so enforcement is a
//! cooperative contract between kernel and the connector:
//!
//! - A connector acknowledges the contract by calling `Transaction::check_constraints` -- the act
//!   of reading the table's constraints is the acknowledgment. Without it, kernel fails data-adding
//!   commits to constrained tables so that connectors unaware of the feature cannot silently commit
//!   violating data.
//! - `Transaction::check_constraints` (discovery + acknowledgment) and
//!   `Snapshot::check_constraints` (discovery only) expose each constraint's raw SQL plus, when
//!   kernel can evaluate the expression, a kernel predicate.
//! - Kernel parses each constraint it can ([`CheckConstraint::predicate`] is then `Some`) and
//!   enforces it when the connector runs a [`CheckConstraintValidator`] (built via
//!   [`CheckConstraints::validator`]) over its data, erroring with
//!   [`Error::CheckConstraintViolation`] on the first violating row. Each constraint is a plain
//!   predicate the engine's [`EvaluationHandler`] evaluates against a batch. The connector
//!   validates the full logical batch *before partitioning*, so partition columns are present as
//!   ordinary per-row data and need no special handling -- the value validated is the value that
//!   determines the partition, hence what readers reconstruct from `add.partitionValues`.
//!   Enforcement is not wired into `write_parquet` or any write-context step; the connector runs
//!   the validator itself, before it partitions or writes.
//! - A constraint kernel cannot parse ([`CheckConstraint::predicate`] is `None`) -- currently
//!   anything beyond a single column-vs-literal comparison like `col1 < 10` -- exposes only its
//!   [`raw_sql`](CheckConstraint::raw_sql), which the connector's own SQL engine must enforce;
//!   kernel-driven validation fails closed on it.
//!
//! Kernel deliberately does not enumerate *which* constraints a connector must enforce -- that is
//! the connector's decision. The connector iterates the full set and, per constraint, uses the
//! parsed [`predicate`](CheckConstraint::predicate) when present or the
//! [`raw_sql`](CheckConstraint::raw_sql) otherwise. The one set-level signal kernel provides is
//! [`CheckConstraints::is_kernel_parsable`] (was every constraint parsed?).

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::UnaryExpressionOp::ToJson;
use crate::expressions::{parse_sql_simple_predicate, Expression, Predicate};
use crate::schema::{
    column_name, ColumnName, ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType,
};
use crate::utils::require;
use crate::{
    DeltaResult, EngineData, Error, EvaluationHandler, ExpressionEvaluator, PredicateEvaluator,
    PredicateRef,
};

/// Parses every CHECK constraint declared on the table, resolving each against `schema`.
/// `check_constraints` is the table's [`TableProperties::check_constraints`] map (name -> raw SQL);
/// the `delta.constraints.` prefix is matched case-insensitively and stripped during table-property
/// parsing (constraint names are preserved verbatim), so this function does no prefix stripping.
/// Constraints kernel cannot evaluate are still returned (with [`CheckConstraint::predicate`]
/// returning `None`) so that connectors with their own SQL engine can enforce them from the raw
/// SQL.
///
/// The map is a [`BTreeMap`], so the returned constraints are ordered by name -- discovery and any
/// downstream error ordering are deterministic without an explicit sort.
///
/// [`TableProperties::check_constraints`]: crate::table_properties::TableProperties::check_constraints
pub(crate) fn constraints_from_properties(
    check_constraints: &BTreeMap<String, String>,
    schema: SchemaRef,
) -> CheckConstraints {
    let constraints: Vec<_> = check_constraints
        .iter()
        .map(|(name, sql)| CheckConstraint::new(name, sql, schema.clone()))
        .collect();
    CheckConstraints(constraints.into())
}

/// All CHECK constraints on a table. Dereferences to a slice for per-constraint access.
///
/// Kernel does not partition the set into "kernel's" and "the connector's" constraints -- which
/// constraints a connector evaluates is its own decision. Instead it reports, per constraint, both
/// the parsed [`predicate`](CheckConstraint::predicate) (when kernel could parse the expression)
/// and the [`raw_sql`](CheckConstraint::raw_sql) (always), and answers one set-level question:
/// [`is_kernel_parsable`](Self::is_kernel_parsable) -- did kernel parse *every* constraint? The
/// connector iterates the set (via `Deref`) and, per constraint, uses the predicate when present or
/// the raw SQL otherwise.
// `Arc<[_]>` (not `Vec`) so cloning the set is an O(1) refcount bump; the discovery caches (added
// with the `Transaction`/`Snapshot` entry points) hand out cheap clones of a single parse.
#[derive(Debug, Clone, Default)]
pub struct CheckConstraints(Arc<[CheckConstraint]>);

impl CheckConstraints {
    /// True if kernel parsed every constraint into a [`predicate`](CheckConstraint::predicate) it
    /// can evaluate. The connector can then build a [`validator`](Self::validator) and let kernel
    /// evaluate all of them. When false, at least one constraint exposes only its
    /// [`raw_sql`](CheckConstraint::raw_sql), which the connector must evaluate with its own SQL
    /// engine (or fail the write).
    pub fn is_kernel_parsable(&self) -> bool {
        self.0.iter().all(|c| c.predicate().is_some())
    }

    /// Binds every parsable constraint to `evaluation_handler` for batch validation (equivalent to
    /// [`CheckConstraintValidator::try_new`]). Fails closed if any constraint is unparsable; check
    /// [`is_kernel_parsable`](Self::is_kernel_parsable) first and self-enforce the raw SQL of any
    /// constraint whose [`predicate`](CheckConstraint::predicate) is `None` to avoid that error.
    pub fn validator(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
    ) -> DeltaResult<CheckConstraintValidator> {
        CheckConstraintValidator::try_new(&self.0, evaluation_handler)
    }
}

impl std::ops::Deref for CheckConstraints {
    type Target = [CheckConstraint];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Whether kernel can evaluate a constraint's expression.
#[derive(Debug, Clone)]
enum ConstraintSupport {
    /// Kernel parsed the expression into a predicate it evaluates against each data batch. The
    /// batch carries every referenced column -- including partition columns, which the connector
    /// validates before partitioning, while they are still ordinary per-row data.
    Parsable(PredicateRef),
    /// The expression is outside the subset kernel's constraint parser supports; the payload is
    /// the parser's reason (e.g. an unresolved column vs. unsupported grammar), preserved so the
    /// validator's fail-closed error can explain which applies.
    Unsupported(String),
}

/// One CHECK constraint: its name, the raw SQL stored under `delta.constraints.<name>`, and the
/// parsed kernel predicate when kernel can evaluate the expression.
#[derive(Debug, Clone)]
pub struct CheckConstraint {
    name: String,
    raw_sql: String,
    support: ConstraintSupport,
    // The logical schema the predicate was resolved against; batches validated against this
    // constraint must conform to it.
    schema: SchemaRef,
}

impl CheckConstraint {
    pub(crate) fn new(
        name: impl Into<String>,
        raw_sql: impl Into<String>,
        schema: SchemaRef,
    ) -> Self {
        let raw_sql = raw_sql.into();
        let support = match parse_sql_simple_predicate(&raw_sql, &schema) {
            Ok(predicate) => ConstraintSupport::Parsable(Arc::new(predicate)),
            Err(reason) => ConstraintSupport::Unsupported(reason.to_string()),
        };
        Self {
            name: name.into(),
            raw_sql,
            support,
            schema,
        }
    }

    /// The constraint's name (the suffix of its `delta.constraints.<name>` configuration key).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The constraint's boolean SQL expression, exactly as stored in the table configuration.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }

    /// The parsed kernel predicate, or `None` when kernel could not parse the expression. When
    /// `None`, only [`Self::raw_sql`] is available and the connector must enforce it with its own
    /// SQL engine.
    pub fn predicate(&self) -> Option<&Predicate> {
        match &self.support {
            ConstraintSupport::Parsable(predicate) => Some(predicate),
            ConstraintSupport::Unsupported(_) => None,
        }
    }

    /// Validates that every row of `batch` satisfies this constraint. Equivalent to building a
    /// single-constraint [`CheckConstraintValidator`]; prefer the validator to amortize evaluator
    /// construction when validating multiple batches or constraints.
    ///
    /// # Errors
    ///
    /// - The constraint is connector-enforced (kernel could not parse it, so [`Self::predicate`] is
    ///   `None`): validation fails closed.
    /// - [`Error::CheckConstraintViolation`] if any row evaluates to `false` or `NULL` (the
    ///   protocol counts both as violations).
    /// - The engine fails to evaluate the predicate.
    pub fn validate(
        &self,
        batch: &dyn EngineData,
        evaluation_handler: &dyn EvaluationHandler,
    ) -> DeltaResult<()> {
        CheckConstraintValidator::try_new(std::slice::from_ref(self), evaluation_handler)?
            .validate(batch)
    }

    /// The fail-closed error for a constraint kernel could not parse; the connector must enforce
    /// its raw SQL with its own engine. `reason` is the parser's explanation, preserved from
    /// [`ConstraintSupport::Unsupported`].
    fn connector_enforced_error(&self, reason: &str) -> Error {
        Error::unsupported(format!(
            "CHECK constraint '{}' ({}) cannot be evaluated by kernel ({}); the connector must \
             enforce it with its own SQL engine before writing",
            self.name, self.raw_sql, reason
        ))
    }
}

// Output column names of the two value-rendering stages; see [`ViolationValuesRenderer`].
const REFERENCED_COLUMN: &str = "referenced";
const VALUES_COLUMN: &str = "values";

/// Renders the columns a constraint references as one JSON object per row, used to include the
/// violating row's values in [`Error::CheckConstraintViolation`] (mirroring Delta-Spark's
/// violation messages). Rendering is best-effort: it runs only after a violation is found, and
/// any rendering failure simply omits the values from the error.
///
/// Two evaluator stages are needed because the JSON encoder requires a *named* struct column as
/// input: stage one materializes the referenced columns as a struct column (names come from the
/// output schema), stage two JSON-encodes that column to a single STRING column.
struct ViolationValuesRenderer {
    referenced: Arc<dyn ExpressionEvaluator>,
    to_json: Arc<dyn ExpressionEvaluator>,
}

impl ViolationValuesRenderer {
    fn try_new(
        schema: &SchemaRef,
        predicate: &Predicate,
        evaluation_handler: &dyn EvaluationHandler,
    ) -> DeltaResult<Self> {
        // Sort the referenced columns for deterministic output (Delta-Spark does the same).
        let mut columns: Vec<&ColumnName> = predicate.references().into_iter().collect();
        columns.sort_by(|a, b| a.path().cmp(b.path()));

        let mut fields = Vec::with_capacity(columns.len());
        let mut field_exprs: Vec<Expression> = Vec::with_capacity(columns.len());
        for column in columns {
            // The predicate's column references are already canonical (the parser resolves them
            // against `schema`), so a direct path lookup resolves the leaf type.
            let leaf = schema.field_at(column)?.data_type().clone();
            // Flatten nested references to their dotted display name; the name only feeds the
            // rendered JSON keys.
            fields.push(StructField::nullable(column.path().join("."), leaf));
            field_exprs.push(column.clone().into());
        }
        let referenced_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            REFERENCED_COLUMN,
            DataType::Struct(Box::new(StructType::new_unchecked(fields))),
        )]));
        let referenced = evaluation_handler.new_expression_evaluator(
            schema.clone(),
            Arc::new(Expression::struct_from([Expression::struct_from(
                field_exprs,
            )])),
            DataType::Struct(Box::new(referenced_schema.as_ref().clone())),
        )?;

        let json_schema =
            StructType::new_unchecked([StructField::nullable(VALUES_COLUMN, DataType::STRING)]);
        let to_json = evaluation_handler.new_expression_evaluator(
            referenced_schema,
            Arc::new(Expression::struct_from([Expression::unary(
                ToJson,
                Expression::column([REFERENCED_COLUMN]),
            )])),
            DataType::Struct(Box::new(json_schema)),
        )?;

        Ok(Self {
            referenced,
            to_json,
        })
    }

    /// Renders the referenced-column values of `row` (an index into `batch`), or `None` if any
    /// step fails.
    fn render(&self, batch: &dyn EngineData, row: usize) -> Option<String> {
        let referenced = self.referenced.evaluate(batch).ok()?;
        let json = self.to_json.evaluate(referenced.as_ref()).ok()?;
        let mut visitor = StringAtRowVisitor {
            target_row: row,
            rows_visited: 0,
            value: None,
        };
        visitor.visit_rows_of(json.as_ref()).ok()?;
        visitor.value
    }
}

/// A constraint's predicate evaluator, bound once via [`CheckConstraintValidator::try_new`].
struct BoundConstraint {
    name: String,
    raw_sql: String,
    evaluator: Arc<dyn PredicateEvaluator>,
    values_renderer: Option<ViolationValuesRenderer>,
}

/// The table's kernel-parsable CHECK constraints bound to an engine's [`EvaluationHandler`], ready
/// to validate batches. Build it once and reuse it for every batch: construction fails closed on
/// any unparsable constraint immediately (before any data is written) and amortizes
/// predicate-evaluator creation across batches.
///
/// Every bound constraint is a plain predicate over a data batch. Because the connector validates
/// before partitioning, partition columns are present in the batch as ordinary per-row data, so
/// they need no special handling here.
///
/// Obtain one from [`CheckConstraints::validator`] (the set returned by
/// `Transaction::check_constraints` or `Snapshot::check_constraints`).
pub struct CheckConstraintValidator {
    bound: Vec<BoundConstraint>,
}

impl CheckConstraintValidator {
    /// Binds each parsable constraint's predicate to `evaluation_handler`, erroring (fail closed)
    /// on the first unparsable constraint (one whose [`CheckConstraint::predicate`] is `None`). A
    /// connector that self-enforces those with its own SQL engine should filter them out first and
    /// bind only the constraints that have a predicate.
    pub fn try_new(
        constraints: &[CheckConstraint],
        evaluation_handler: &dyn EvaluationHandler,
    ) -> DeltaResult<Self> {
        let mut bound = Vec::with_capacity(constraints.len());
        for constraint in constraints {
            let ConstraintSupport::Parsable(predicate) = &constraint.support else {
                let ConstraintSupport::Unsupported(reason) = &constraint.support else {
                    unreachable!("non-Parsable support is Unsupported");
                };
                return Err(constraint.connector_enforced_error(reason));
            };
            bound.push(BoundConstraint {
                name: constraint.name.clone(),
                raw_sql: constraint.raw_sql.clone(),
                evaluator: evaluation_handler
                    .new_predicate_evaluator(constraint.schema.clone(), predicate.clone())?,
                // Rendering violating values is a best-effort nicety; never fail binding over it.
                values_renderer: ViolationValuesRenderer::try_new(
                    &constraint.schema,
                    predicate,
                    evaluation_handler,
                )
                .ok(),
            });
        }
        Ok(Self { bound })
    }

    /// Validates that every row of `batch` satisfies every bound constraint. `batch` must use the
    /// table's logical schema, before any logical-to-physical transform, since constraints
    /// reference logical column names -- and it must still carry partition columns (the connector
    /// validates before partitioning drops them).
    ///
    /// # Errors
    ///
    /// [`Error::CheckConstraintViolation`] on the first row whose predicate does not evaluate to
    /// exactly `true` (`false` and `NULL` are both violations), or any engine evaluation error.
    pub fn validate(&self, batch: &dyn EngineData) -> DeltaResult<()> {
        for constraint in &self.bound {
            let result = constraint.evaluator.evaluate(batch)?;
            let mut visitor = CheckResultVisitor::default();
            visitor.visit_rows_of(result.as_ref())?;
            let Some(violation) = visitor.violation else {
                continue;
            };
            let values = constraint
                .values_renderer
                .as_ref()
                .and_then(|renderer| renderer.render(batch, violation.row))
                .map(|json| format!("; values: {json}"))
                .unwrap_or_default();
            return Err(Error::CheckConstraintViolation {
                name: constraint.name.clone(),
                expression: constraint.raw_sql.clone(),
                details: format!(
                    "row {} of the batch evaluated to {}{}",
                    violation.row,
                    if violation.result_was_null {
                        "NULL"
                    } else {
                        "false"
                    },
                    values,
                ),
            });
        }
        Ok(())
    }
}

/// The first violating row found while scanning a constraint's boolean "output" column.
struct FirstViolation {
    row: usize,
    result_was_null: bool,
}

/// Visits the boolean "output" column produced by evaluating a constraint predicate, recording
/// the first row that is not exactly `true` (`NULL` counts as a violation, matching the
/// protocol's "must return true" rule and NOT NULL invariants).
#[derive(Default)]
struct CheckResultVisitor {
    rows_visited: usize,
    violation: Option<FirstViolation>,
}

impl RowVisitor for CheckResultVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("output")], vec![DataType::BOOLEAN]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of CheckResultVisitor getters: {}",
                getters.len()
            ))
        );
        if self.violation.is_none() {
            for i in 0..row_count {
                let passed: Option<bool> = getters[0].get_opt(i, "check_constraint.output")?;
                if passed != Some(true) {
                    self.violation = Some(FirstViolation {
                        row: self.rows_visited + i,
                        result_was_null: passed.is_none(),
                    });
                    break;
                }
            }
        }
        self.rows_visited += row_count;
        Ok(())
    }
}

/// Extracts the STRING "values" column at one target row.
struct StringAtRowVisitor {
    target_row: usize,
    rows_visited: usize,
    value: Option<String>,
}

impl RowVisitor for StringAtRowVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("values")], vec![DataType::STRING]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of StringAtRowVisitor getters: {}",
                getters.len()
            ))
        );
        if let Some(i) = self.target_row.checked_sub(self.rows_visited) {
            if i < row_count {
                self.value = getters[0].get_opt(i, "check_constraint.values")?;
            }
        }
        self.rows_visited += row_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    fn schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("amount", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]))
    }

    // Constraints keyed by name -> raw SQL, mirroring `TableProperties::check_constraints` (the
    // `delta.constraints.` prefix is already stripped by table-property parsing).
    fn constraints(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn set_level_parsable_reflects_per_constraint_predicates() {
        // All parsable: a string-column and a numeric-column comparison. Every constraint exposes a
        // predicate, so the set-level answer is true.
        let all_parsable = constraints_from_properties(
            &constraints(&[("positive", "amount > 0"), ("name_check", "name = 'a'")]),
            schema(),
        );
        assert!(all_parsable.is_kernel_parsable());
        assert!(all_parsable.iter().all(|c| c.predicate().is_some()));

        // One constraint outside the single-comparison grammar flips the set-level answer. Kernel
        // does not say which constraints are the connector's; the connector iterates the set and
        // branches on predicate() -- the unparsable one exposes only its raw SQL.
        let mixed = constraints_from_properties(
            &constraints(&[
                ("positive", "amount > 0"),
                ("range", "amount > 0 AND amount < 100"),
            ]),
            schema(),
        );
        assert!(!mixed.is_kernel_parsable());
        let raw_only: Vec<_> = mixed
            .iter()
            .filter(|c| c.predicate().is_none())
            .map(|c| (c.name(), c.raw_sql()))
            .collect();
        assert_eq!(raw_only, [("range", "amount > 0 AND amount < 100")]);

        // No constraints: trivially parsable.
        assert!(constraints_from_properties(&constraints(&[]), schema()).is_kernel_parsable());
    }

    #[test]
    fn discovery_extracts_constraints_sorted_by_name() {
        let constraints = constraints_from_properties(
            &constraints(&[("b_check", "amount < 100"), ("a_check", "amount > 0")]),
            schema(),
        );
        let names: Vec<_> = constraints.iter().map(|c| c.name()).collect();
        assert_eq!(names, ["a_check", "b_check"]);
        assert!(constraints
            .iter()
            .all(|c| matches!(c.support, ConstraintSupport::Parsable(_))));
        assert_eq!(constraints[0].raw_sql(), "amount > 0");
    }

    #[test]
    fn token_spaced_comparisons_parse() {
        // Delta-Spark stores parser-round-tripped, token-spaced expression text; a simple
        // comparison must parse regardless of surrounding or internal whitespace.
        for sql in ["amount>0", "amount > 0", "amount   >   0", "  amount > 0  "] {
            let constraint = CheckConstraint::new("p", sql, schema());
            assert!(
                matches!(constraint.support, ConstraintSupport::Parsable(_)),
                "expected {sql:?} to parse"
            );
            assert!(constraint.predicate().is_some());
        }
    }

    #[test]
    fn functions_and_null_checks_expose_only_raw_sql() {
        // Expressions kernel cannot parse have no predicate; the connector falls back to raw SQL.
        for sql in ["length(name) > 0", "amount IS NOT NULL"] {
            let constraint = CheckConstraint::new("c", sql, schema());
            assert!(
                matches!(constraint.support, ConstraintSupport::Unsupported(_)),
                "expected '{sql}' to be unparsable"
            );
            assert!(constraint.predicate().is_none());
            assert_eq!(constraint.raw_sql(), sql, "raw sql must round-trip");
        }
    }

    #[test]
    fn unparsable_constraints_preserve_the_parser_reason() {
        // Unsupported grammar and unresolvable columns are different failure modes (Delta-Spark
        // raises distinct error classes); the preserved reason must say which one applies so a
        // later fail-closed error can surface it.
        let reason_for = |c: &CheckConstraint| {
            let ConstraintSupport::Unsupported(reason) = &c.support else {
                panic!("expected Unsupported");
            };
            reason.clone()
        };

        let junction = CheckConstraint::new("range", "amount > 0 AND amount < 100", schema());
        let reason = reason_for(&junction);
        assert!(
            reason.contains("only a single comparison"),
            "junction reason surfaces: {reason}"
        );

        let unknown_column = CheckConstraint::new("ghost", "nope > 0", schema());
        let reason = reason_for(&unknown_column);
        assert!(
            reason.contains("not found in schema"),
            "unresolved-column reason surfaces: {reason}"
        );
    }

    #[test]
    fn connector_enforced_error_names_the_constraint_and_preserves_the_reason() {
        // The fail-closed error a validator raises on an unparsable constraint must identify the
        // constraint (name + raw SQL) and carry the parser's reason, so a connector knows what to
        // self-enforce and why kernel could not.
        let junction = CheckConstraint::new("range", "amount > 0 AND amount < 100", schema());
        let ConstraintSupport::Unsupported(reason) = &junction.support else {
            panic!("expected Unsupported");
        };
        let msg = junction.connector_enforced_error(reason).to_string();
        assert!(msg.contains("range"), "names the constraint: {msg}");
        assert!(
            msg.contains("amount > 0 AND amount < 100"),
            "includes the raw SQL: {msg}"
        );
        assert!(
            msg.contains("only a single comparison"),
            "preserves the parser reason: {msg}"
        );
        assert!(
            msg.contains("connector must"),
            "directs the connector to self-enforce: {msg}"
        );
    }
}
