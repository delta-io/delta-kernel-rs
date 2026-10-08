//! Discovery of write expressions: expressions stored in the table that a connector must enforce
//! on every write, such as CHECK constraints.
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`. The Delta protocol requires every row in the table to satisfy every
//! one. Kernel never sees row data on the write path, so it does not evaluate constraints itself.
//! It exposes each constraint's raw SQL verbatim (via [`TableWriteExpressions`]) for a connector to
//! parse and enforce, and gates `write_state` and every commit on a table with constraints on the
//! connector acknowledging that responsibility (see [`Transaction::ack_check_constraints`]).
//!
//! [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints

use crate::table_properties::CheckConstraint;

/// Read-only discovery of a table's CHECK constraints, implemented by both [`Snapshot`] and
/// [`Transaction`] so a connector can discover from either.
///
/// A [`Snapshot`] returns the constraints at its version. A [`Transaction`] returns the
/// constraints the table has once the transaction commits. For a create-table transaction these
/// are the declared ones, with names lowercased.
///
/// Discovery has no side effect: it does not acknowledge anything. See
/// [`Transaction::ack_check_constraints`] for which operations require the acknowledgement.
///
/// [`Snapshot`]: crate::Snapshot
/// [`Transaction`]: crate::transaction::Transaction
/// [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints
pub trait TableWriteExpressions {
    /// The table's CHECK constraints, sorted by name and then SQL. Empty when the table declares
    /// none.
    fn check_constraints(&self) -> &[CheckConstraint];
}
