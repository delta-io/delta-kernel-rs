//! Row-identity column paths and dedup predicates used by FSR plan composition.
//!
//! Centralizes the column-path constants (`ADD_PATH`, `REMOVE_PATH`, `PROTOCOL_MIN_READER`,
//! `METADATA_ID`, `DOMAIN_METADATA_DOMAIN`, `TXN_APP_ID`) and the row-identity / dedup-key
//! predicates ([`fsr_dedup_key`], [`fsr_row_has_identity_predicate`]) consumed by the FSR
//! plan-builder. Carrying the identity-column constants here keeps the dedup-key contract
//! cohesive: when an action's identity changes the only file that needs to follow is this one.

use crate::actions::{DOMAIN_METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME};
use crate::expressions::{ColumnName, Expression, IntoColumnName, Predicate, Scalar};
use crate::schema::{ArrayType, DataType};

/// Synthetic column carrying the materialized dedup key array so hash joins only need top-level
/// column keys.
pub(super) const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";

// === Action column paths ===
//
// Stable nested column references used by `fsr_dedup_key`,
// `fsr_row_has_identity_predicate`, and the various `scan_*_projection` helpers.
// Centralizing them means renaming an action field is a one-line change here, not
// a sweep of string literals across multiple files (and silently introducing
// divergence between the helpers).
pub(super) const ADD_PATH: &[&str] = &["add", "path"];
pub(super) const REMOVE_PATH: &[&str] = &["remove", "path"];
pub(super) const PROTOCOL_MIN_READER: &[&str] = &["protocol", "minReaderVersion"];
pub(super) const METADATA_ID: &[&str] = &["metaData", "id"];
pub(super) const DOMAIN_METADATA_DOMAIN: &[&str] = &["domainMetadata", "domain"];
pub(super) const TXN_APP_ID: &[&str] = &["txn", "appId"];

pub(super) fn fsr_dedup_key() -> Expression {
    let null_str = || Expression::literal(Scalar::Null(DataType::STRING));
    let arm = |kind: &str, id1: Expression, id2: Expression, id3: Expression| {
        Expression::array(vec![Expression::literal(kind), id1, id2, id3])
    };
    // For file rows, the identity components come from either add or remove (one is non-null per
    // row); coalesce picks whichever side carries the value.
    let file_field = |suffix: &[&str]| {
        let add_path = ["add"].into_iter().chain(suffix.iter().copied());
        let remove_path = ["remove"].into_iter().chain(suffix.iter().copied());
        Expression::Column(ColumnName::new(add_path))
            .or_else(Expression::Column(ColumnName::new(remove_path)))
    };

    let file_arm = arm(
        "file",
        file_field(&["path"]),
        file_field(&["deletionVector", "storageType"]),
        file_field(&["deletionVector", "pathOrInlineDv"]),
    );
    let proto_arm = arm(PROTOCOL_NAME, null_str(), null_str(), null_str());
    // Metadata is a singleton table state action: latest row wins regardless of prior id.
    let meta_arm = arm("metadata", null_str(), null_str(), null_str());
    // Domain metadata is keyed by domain; newer rows replace older configs for that domain.
    let domain_arm = arm(
        DOMAIN_METADATA_NAME,
        Expression::column(["domainMetadata", "domain"]),
        null_str(),
        null_str(),
    );
    let txn_arm = arm(
        SET_TRANSACTION_NAME,
        Expression::column(["txn", "appId"]),
        null_str(),
        null_str(),
    );

    let null_list = Expression::literal(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        DataType::STRING,
        true,
    )))));

    Expression::case_when(
        vec![
            (
                Predicate::or(
                    Expression::Column(ADD_PATH.into_column_name()).is_not_null(),
                    Expression::Column(REMOVE_PATH.into_column_name()).is_not_null(),
                ),
                file_arm,
            ),
            (Expression::column(["protocol"]).is_not_null(), proto_arm),
            (
                Expression::Column(METADATA_ID.into_column_name()).is_not_null(),
                meta_arm,
            ),
            (
                Expression::column(["domainMetadata"]).is_not_null(),
                domain_arm,
            ),
            (Expression::column(["txn"]).is_not_null(), txn_arm),
        ],
        null_list,
    )
}

pub(super) fn fsr_row_has_identity_predicate() -> Predicate {
    Predicate::or_from([
        Expression::Column(ADD_PATH.into_column_name()).is_not_null(),
        Expression::Column(REMOVE_PATH.into_column_name()).is_not_null(),
        Expression::Column(PROTOCOL_MIN_READER.into_column_name()).is_not_null(),
        Expression::Column(METADATA_ID.into_column_name()).is_not_null(),
        Expression::Column(DOMAIN_METADATA_DOMAIN.into_column_name()).is_not_null(),
        Expression::Column(TXN_APP_ID.into_column_name()).is_not_null(),
    ])
}

#[cfg(test)]
mod tests {
    use super::super::schemas::action_schema;
    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::expressions::UnaryExpressionOp;
    use crate::utils::test_utils::parse_json_to_record_batch;

    #[test]
    fn fsr_dedup_key_eval_add_row_carries_path_and_dv_components() {
        // dv_unique_id is now `ToJson(Array(storageType, pathOrInlineDv))` rather than a
        // Plus-concatenated `unique_id_from_parts` string. Assert the encoded JSON carries the
        // path and DV identity components verbatim (any consumer that reduces over equality of
        // full JSON strings still dedups identical DVs).
        let line = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"dvpath","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let batch = parse_json_to_record_batch(StringArray::from(vec![line]), action_schema());

        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            &batch,
            Some(&DataType::STRING),
        )
        .unwrap();
        let s = out.as_string::<i32>().value(0);
        // Outer Array(["file", path_coalesce, dv_coalesce]) JSON-encodes to a JSON array; the dv
        // arm is itself a JSON-encoded array string `["u","dvpath"]` embedded as a JSON string.
        assert!(
            s.contains("p1.parquet"),
            "expected `p1.parquet` in json={s}"
        );
        assert!(s.contains("dvpath"), "expected `dvpath` in json={s}");
        // The Plus-as-concat byte form must not appear (regression guard).
        let stringy_concat = DeletionVectorDescriptor::unique_id_from_parts("u", "dvpath", Some(7));
        assert!(
            !s.contains(&stringy_concat),
            "json contains the legacy concat form `{stringy_concat}` -- dv_unique_id must use \
             ToJson(Array(...)), not Plus-as-string-concat: json={s}"
        );
    }

    #[test]
    fn fsr_dedup_key_debug_string_is_non_trivial() {
        let dbg = format!("{:?}", fsr_dedup_key());
        assert!(dbg.len() > 40, "{dbg}");
    }

    #[test]
    fn fsr_dedup_key_eval_various_action_types() {
        let schema = action_schema();
        let check = |line: &str, subs: &[&str]| {
            let batch = parse_json_to_record_batch(StringArray::from(vec![line]), schema.clone());
            let out = evaluate_expression(
                &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
                &batch,
                Some(&DataType::STRING),
            )
            .unwrap();
            let s = out.as_string::<i32>().value(0);
            for sub in subs {
                assert!(s.contains(sub), "line={line} json={s} missing {sub}");
            }
        };

        check(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            &["protocol"],
        );
        check(
            r#"{"metaData":{"id":"mid-9","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            &["metadata"],
        );
        check(
            r#"{"domainMetadata":{"domain":"dom-z","configuration":"conf-z","removed":false}}"#,
            &["dom-z"],
        );
        check(
            r#"{"txn":{"appId":"app-z","version":1,"lastUpdated":100}}"#,
            &["app-z"],
        );
        check(
            r#"{"remove":{"path":"r1.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            &["r1.parquet"],
        );
    }

    #[test]
    fn fsr_row_has_identity_accepts_protocol_metadata_domain_and_file_actions() {
        let rows = StringArray::from(vec![
            // file actions
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            // protocol / metadata / domain metadata / txn
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            // no action payload
            r#"{}"#,
        ]);
        let batch = parse_json_to_record_batch(rows, action_schema());

        let out = evaluate_expression(
            &fsr_row_has_identity_predicate().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should pass identity predicate");
        assert!(b.value(1), "remove row should pass identity predicate");
        assert!(b.value(2), "protocol row should pass identity predicate");
        assert!(b.value(3), "metaData row should pass identity predicate");
        assert!(
            b.value(4),
            "domainMetadata row should pass identity predicate"
        );
        assert!(b.value(5), "txn row should pass identity predicate");
        assert!(!b.value(6), "empty row should fail identity predicate");
    }
}
