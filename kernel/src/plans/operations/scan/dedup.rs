//! Dedup-key expressions for the reconciliation pipeline.
//!
//! [`fsr_dedup_key`] keys the full six-slot action set; [`scan_file_dedup_key`] keys
//! `{add, remove}` only. Both evaluate to NULL on rows that match no known slot, so the
//! pipeline's identity filter is simply `dedup_key IS NOT NULL`.

use crate::actions::{DOMAIN_METADATA_NAME, SET_TRANSACTION_NAME};
use crate::expressions::{col, Expression, Predicate, Scalar};
use crate::schema::{ArrayType, DataType};

/// Synthetic column carrying the materialized dedup key array so hash joins only need top-level
/// column keys.
pub(super) const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";

const ADD_PATH: &[&str] = &["add", "path"];
const REMOVE_PATH: &[&str] = &["remove", "path"];
const METADATA_ID: &[&str] = &["metaData", "id"];

/// For file rows, the identity components come from either `add.<suffix>` or
/// `remove.<suffix>` (one is non-null per row); coalesce picks whichever side carries the
/// value. Shared by both dedup-key builders.
fn file_field(suffix: &[&str]) -> Expression {
    let add_path = ["add"].into_iter().chain(suffix.iter().copied());
    let remove_path = ["remove"].into_iter().chain(suffix.iter().copied());
    Expression::column(add_path).or_else(Expression::column(remove_path))
}

/// `Array<String?>?` NULL — the fallback returned by both dedup-key `CASE` expressions when
/// no arm matches.
fn null_string_array() -> Expression {
    Expression::literal(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        DataType::STRING,
        true,
    )))))
}

/// Predicate that selects rows whose `add.path` OR `remove.path` is non-null.
fn is_file_row() -> Predicate {
    Predicate::or(col(ADD_PATH).is_not_null(), col(REMOVE_PATH).is_not_null())
}

/// `["file", path_coalesce, dv_storage_coalesce, dv_inline_coalesce]` — the file-row arm of
/// both dedup keys.
fn file_arm() -> Expression {
    Expression::array(vec![
        Expression::literal("file"),
        file_field(&["path"]),
        file_field(&["deletionVector", "storageType"]),
        file_field(&["deletionVector", "pathOrInlineDv"]),
    ])
}

pub(super) fn fsr_dedup_key() -> Expression {
    let null_str = || Expression::literal(Scalar::Null(DataType::STRING));
    let arm = |kind: &str, id1: Expression, id2: Expression, id3: Expression| {
        Expression::array(vec![Expression::literal(kind), id1, id2, id3])
    };
    Expression::case_when(
        vec![
            (is_file_row(), file_arm()),
            (
                col(["protocol"]).is_not_null(),
                arm(
                    crate::actions::PROTOCOL_NAME,
                    null_str(),
                    null_str(),
                    null_str(),
                ),
            ),
            // Metadata is a singleton table state action: latest row wins regardless of prior id.
            (
                col(METADATA_ID).is_not_null(),
                arm("metadata", null_str(), null_str(), null_str()),
            ),
            // Domain metadata is keyed by domain; newer rows replace older configs for that
            // domain.
            (
                col(["domainMetadata"]).is_not_null(),
                arm(
                    DOMAIN_METADATA_NAME,
                    col(["domainMetadata", "domain"]),
                    null_str(),
                    null_str(),
                ),
            ),
            (
                col(["txn"]).is_not_null(),
                arm(
                    SET_TRANSACTION_NAME,
                    col(["txn", "appId"]),
                    null_str(),
                    null_str(),
                ),
            ),
        ],
        null_string_array(),
    )
}

/// Scan-pipeline dedup key: file path identity over `{add, remove}` only.
///
/// Returns an `ARRAY<STRING?>?` expression of the form
/// `[kind, file_path, dv_storage, dv_inline_dv]` when the row carries an `add` or `remove`
/// action; otherwise evaluates to NULL. `dedup_key IS NOT NULL` serves as both the dedup
/// partition key and the "is this a valid scan-side action row" identity filter.
pub(super) fn scan_file_dedup_key() -> Expression {
    Expression::case_when(vec![(is_file_row(), file_arm())], null_string_array())
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
    fn fsr_dedup_key_is_null_for_unknown_rows() {
        // The pipeline's identity filter is `dedup_key IS NOT NULL`, so the dedup-key MUST
        // evaluate to NULL on rows that match no known slot. This is the load-bearing
        // contract.
        let rows = StringArray::from(vec![
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            r#"{}"#,
        ]);
        let batch = parse_json_to_record_batch(rows, action_schema());
        let out = evaluate_expression(
            &fsr_dedup_key().is_not_null().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should produce non-NULL dedup key");
        assert!(b.value(1), "remove row should produce non-NULL dedup key");
        assert!(b.value(2), "protocol row should produce non-NULL dedup key");
        assert!(b.value(3), "metaData row should produce non-NULL dedup key");
        assert!(
            b.value(4),
            "domainMetadata row should produce non-NULL dedup key"
        );
        assert!(b.value(5), "txn row should produce non-NULL dedup key");
        assert!(!b.value(6), "empty row should produce NULL dedup key");
    }

    #[test]
    fn scan_file_dedup_key_is_null_for_non_file_rows() {
        // The scan pipeline only cares about add/remove rows; everything else should be
        // filtered out via `dedup_key IS NOT NULL`.
        let rows = StringArray::from(vec![
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            r#"{}"#,
        ]);
        let batch = parse_json_to_record_batch(rows, action_schema());
        let out = evaluate_expression(
            &scan_file_dedup_key().is_not_null().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should produce non-NULL scan dedup key");
        assert!(
            b.value(1),
            "remove row should produce non-NULL scan dedup key"
        );
        assert!(
            !b.value(2),
            "protocol row should produce NULL scan dedup key"
        );
        assert!(
            !b.value(3),
            "metaData row should produce NULL scan dedup key"
        );
        assert!(
            !b.value(4),
            "domainMetadata row should produce NULL scan dedup key"
        );
        assert!(!b.value(5), "txn row should produce NULL scan dedup key");
        assert!(!b.value(6), "empty row should produce NULL scan dedup key");
    }
}
