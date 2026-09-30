//! Discovery of CHECK constraints.
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`. The Delta protocol requires every added row to satisfy every one.
//! Kernel never sees row data on the write path, so it does not evaluate constraints itself. It
//! exposes each constraint's raw SQL verbatim (via [`TableWriteExpressions`]) for a connector to
//! parse and enforce, and gates any commit that adds data or introduces a constraint on the
//! connector acknowledging that responsibility (see [`Transaction::ack_check_constraints`]).
//!
//! [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::schema::SchemaRef;
use crate::Snapshot;

/// Read-only discovery of a table's CHECK constraints, implemented by both [`Snapshot`] and
/// [`Transaction`] so a connector can discover from either.
///
/// A [`Snapshot`] returns the constraints at its version. A [`Transaction`] returns the
/// constraints the table has once the transaction commits, which for a create-table transaction
/// are the declared ones.
///
/// Discovery has no side effect: it does not acknowledge anything. A data-adding commit to a
/// constrained table still requires [`Transaction::ack_check_constraints`].
///
/// [`Transaction`]: crate::transaction::Transaction
/// [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints
pub trait TableWriteExpressions {
    /// The table's CHECK constraints. Empty when the table declares none.
    fn check_constraints(&self) -> CheckConstraints;
}

impl TableWriteExpressions for Snapshot {
    fn check_constraints(&self) -> CheckConstraints {
        self.table_configuration().check_constraints()
    }
}

/// One CHECK constraint: its name, the raw SQL stored under `delta.constraints.<name>`, and the
/// logical schema of the table that declares it.
///
/// Two constraints are equal when their name and raw SQL match. The schema is not part of the
/// constraint's identity.
#[derive(Debug, Clone)]
pub struct CheckConstraint {
    name: String,
    raw_sql: String,
    // The logical schema of the table that declares this constraint.
    #[allow(dead_code)]
    schema: SchemaRef,
}

impl PartialEq for CheckConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.raw_sql == other.raw_sql
    }
}

impl Eq for CheckConstraint {}

impl CheckConstraint {
    /// The constraint's name: the `<name>` suffix of its `delta.constraints.<name>` config key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The constraint's boolean SQL expression, verbatim from the table configuration. This is the
    /// authoritative form, and kernel does not parse or interpret it.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }
}

/// All CHECK constraints on a table. Dereferences to a slice for iteration.
#[derive(Debug, Clone, Default)]
pub struct CheckConstraints(Arc<[CheckConstraint]>);

impl CheckConstraints {
    /// Builds [`CheckConstraints`] from a table's already-parsed constraints: a map from each
    /// constraint's name to its raw SQL (the
    /// [`check_constraints`](crate::table_properties::TableProperties::check_constraints) field of
    /// [`TableProperties`](crate::table_properties::TableProperties)). Each entry becomes a
    /// [`CheckConstraint`] verbatim, carrying `schema`, the table's logical schema. The
    /// `delta.constraints.` prefix is already stripped from the names and non-constraint keys are
    /// already filtered out during property parsing, so this does no filtering of its own.
    pub(crate) fn from_parsed(constraints: &HashMap<String, String>, schema: &SchemaRef) -> Self {
        Self(
            constraints
                .iter()
                .map(|(name, raw_sql)| CheckConstraint {
                    name: name.clone(),
                    raw_sql: raw_sql.clone(),
                    schema: schema.clone(),
                })
                .collect(),
        )
    }
}

impl Deref for CheckConstraints {
    type Target = [CheckConstraint];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::schema_ref;

    fn parsed(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn test_schema() -> SchemaRef {
        schema_ref! {
            nullable "amount": LONG,
            nullable "name": STRING,
        }
    }

    #[test]
    fn maps_parsed_entries_to_constraints() {
        let schema = test_schema();
        let constraints = CheckConstraints::from_parsed(
            &parsed(&[("positive", "amount > 0"), ("named", "name = 'a'")]),
            &schema,
        );
        let mut discovered: Vec<_> = constraints
            .iter()
            .map(|c| (c.name(), c.raw_sql()))
            .collect();
        discovered.sort();
        let expected = [("named", "name = 'a'"), ("positive", "amount > 0")];
        assert_eq!(discovered, expected);

        let carries_schema = constraints.iter().all(|c| c.schema == schema);
        assert!(carries_schema);
    }

    #[test]
    fn empty_parsed_map_yields_no_constraints() {
        let constraints = CheckConstraints::from_parsed(&parsed(&[]), &test_schema());
        assert!(constraints.is_empty());
    }

    #[test]
    fn equality_ignores_schema() {
        let entries = parsed(&[("positive", "amount > 0")]);
        let with_table_schema = CheckConstraints::from_parsed(&entries, &test_schema());
        let with_other_schema =
            CheckConstraints::from_parsed(&entries, &schema_ref! { nullable "amount": LONG });
        assert_eq!(with_table_schema[0], with_other_schema[0]);
    }
}
