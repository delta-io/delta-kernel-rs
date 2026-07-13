//! Discovery of CHECK constraints (in development, gated by the `check-constraints-in-dev` cargo
//! feature).
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`; the Delta protocol requires every row added to the table to satisfy
//! every constraint (a row passes only when a constraint evaluates to `true` -- both `false` and
//! `NULL` are violations). Kernel never sees row data on the write path, so enforcement is a
//! cooperative contract between kernel and the connector; this module owns the first half of that
//! contract -- *discovery* -- while enforcement (a validator that evaluates each predicate against
//! a batch and reports [`crate::Error::CheckConstraintViolation`]) lands in later work.
//!
//! `constraints_from_properties` turns the table's constraints (already keyed by name in
//! `TableProperties::check_constraints`) into a [`CheckConstraints`] set. Each constraint is
//! classified during construction:
//!
//! - kernel parses the SQL into a predicate it can evaluate -- [`CheckConstraint::predicate`] is
//!   then `Some`. Currently only a single column-vs-literal (or column-vs-column) comparison like
//!   `col1 < 10` parses.
//! - kernel cannot parse it (anything richer -- functions, arithmetic, junctions, `IS NULL`) --
//!   [`CheckConstraint::predicate`] is `None` and only [`CheckConstraint::raw_sql`] is available,
//!   which a connector with its own SQL engine can enforce itself.
//!
//! Kernel deliberately does not enumerate *which* constraints a connector must enforce -- that is
//! the connector's decision. The connector iterates the full set and, per constraint, uses the
//! parsed [`predicate`](CheckConstraint::predicate) when present or the
//! [`raw_sql`](CheckConstraint::raw_sql) otherwise. The one set-level signal kernel provides is
//! [`CheckConstraints::is_kernel_parsable`] (was every constraint parsed?).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::expressions::{parse_sql_simple_predicate, Predicate};
use crate::schema::SchemaRef;
use crate::PredicateRef;

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
    /// can evaluate. When false, at least one constraint exposes only its
    /// [`raw_sql`](CheckConstraint::raw_sql), which the connector must evaluate with its own SQL
    /// engine (or fail the write).
    pub fn is_kernel_parsable(&self) -> bool {
        self.0.iter().all(|c| c.predicate().is_some())
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
    /// Kernel parsed the expression into a predicate it can evaluate against each data batch.
    Parsable(PredicateRef),
    /// The expression is outside the subset kernel's constraint parser supports; the payload is
    /// the parser's reason (e.g. an unresolved column vs. unsupported grammar), preserved so a
    /// fail-closed error can explain which applies.
    // TODO(#2896): the reason is surfaced by the validator's fail-closed error (later work).
    Unsupported(#[allow(dead_code)] String),
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
    // TODO(#2896): consumed by the validator (later work); dead until then.
    #[allow(dead_code)]
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
}
