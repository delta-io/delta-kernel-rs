//! Tombstone and txn-expiration retention predicate used by FSR plan composition.
//!
//! Centralizes the column-path constants referenced by the retention predicate
//! ([`REMOVE_DELETION_TIMESTAMP`], [`TXN_LAST_UPDATED`]) and the
//! [`retention_filter`] builder.

use crate::expressions::{Expression, IntoColumnName, Predicate};

pub(super) const REMOVE_DELETION_TIMESTAMP: &[&str] = &["remove", "deletionTimestamp"];
pub(super) const TXN_LAST_UPDATED: &[&str] = &["txn", "lastUpdated"];

/// Tombstone / txn expiration predicate aligned with
/// [`crate::action_reconciliation::log_replay::ActionReconciliationVisitor::is_expired_tombstone`]
/// and txn retention checks in the same visitor (`kernel/src/action_reconciliation/log_replay.rs`).
///
/// `txn_expiration_cutoff` is `None` when `delta.setTransactionRetentionDuration` is unset — txn
/// rows are not filtered by age.
pub(super) fn retention_filter(min_file_ts: i64, txn_expiry: Option<i64>) -> Predicate {
    let remove_ok = Predicate::or(
        Expression::column(["remove"]).is_null(),
        Expression::Column(REMOVE_DELETION_TIMESTAMP.into_column_name())
            .or_lit(0i64)
            .gt(Expression::literal(min_file_ts)),
    );
    let txn_ok = match txn_expiry {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            Expression::column(["txn"]).is_null(),
            Expression::Column(TXN_LAST_UPDATED.into_column_name()).is_null(),
            Expression::Column(TXN_LAST_UPDATED.into_column_name()).gt(Expression::literal(cutoff)),
        ]),
    };
    Predicate::and(remove_ok, txn_ok)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::actions::{Remove, SetTransaction, REMOVE_NAME};
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField, StructType, ToSchema};
    use crate::utils::test_utils::string_array_to_engine_data;
    use crate::Engine;

    #[test]
    fn retention_filter_keeps_and_drops_tombstones() {
        let p = retention_filter(100, None);
        let engine = SyncEngine::new();
        let schema = Arc::new(
            StructType::try_new([
                StructField::nullable(REMOVE_NAME, Remove::to_schema()),
                StructField::nullable("txn", SetTransaction::to_schema()),
            ])
            .unwrap(),
        );
        let rows = StringArray::from(vec![
            r#"{"remove":{"path":"old","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
            r#"{"remove":{"path":"gone","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
        ]);
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), schema)
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let arr = evaluate_expression(&p.into(), batch, Some(&DataType::BOOLEAN)).unwrap();
        let b = arr.as_boolean();
        assert!(b.value(0));
        assert!(!b.value(1));
    }
}
