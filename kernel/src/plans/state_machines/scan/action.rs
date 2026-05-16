//! Action-schema constructors, column-path constants, and the dedup / retention
//! predicates shared by the FSR plan-builders and the replay-scan plans.
//!
//! Centralizes the column-path string constants (`ADD_PATH`, `REMOVE_PATH`, ...), the action
//! schema builders (`action_schema`, `action_read_schema`, `action_output_schema` with
//! `LazyLock` caches), and the row-identity / retention predicates that the FSR pipeline
//! composes.

use std::sync::LazyLock;

use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::expressions::{ColumnName, Expression, IntoColumnName, Predicate, Scalar};
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::schema::{
    arc_schema, ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema,
};

/// Synthetic column carrying the materialized dedup key array so hash joins only need top-level
/// column keys (`delta-kernel-datafusion-engine/src/compile/join.rs`).
pub(super) const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";

// === Action column paths ===
//
// Stable nested column references used by `fsr_dedup_key`, `fsr_row_has_identity_predicate`,
// `retention_filter`, and the various `scan_*_projection` helpers. Centralizing them means
// renaming an action field is a one-line change here, not a sweep of string literals across
// the file (and silently introducing divergence between the helpers).
pub(super) const ADD_PATH: &[&str] = &["add", "path"];
pub(super) const REMOVE_PATH: &[&str] = &["remove", "path"];
pub(super) const REMOVE_DELETION_TIMESTAMP: &[&str] = &["remove", "deletionTimestamp"];
pub(super) const PROTOCOL_MIN_READER: &[&str] = &["protocol", "minReaderVersion"];
pub(super) const METADATA_ID: &[&str] = &["metaData", "id"];
pub(super) const DOMAIN_METADATA_DOMAIN: &[&str] = &["domainMetadata", "domain"];
pub(super) const TXN_APP_ID: &[&str] = &["txn", "appId"];
pub(super) const TXN_LAST_UPDATED: &[&str] = &["txn", "lastUpdated"];

/// Top-level action fields in canonical order, used as the base of [`action_schema`] and
/// [`augmented_action_schema`]. Each variant points at the typed `ToSchema` of its action.
fn all_action_fields() -> Vec<StructField> {
    vec![
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ]
}

/// Action schema for the action-replay stream.
///
/// When `relaxed_nesting` is true (transport schema used while replaying / unioning action rows
/// from heterogeneous sources), nested fields are made all-nullable to avoid planner-time cast
/// failures during engine materialization. When false, the strict per-action `ToSchema` is used
/// (kernel-visible output schema).
fn action_schema(relaxed_nesting: bool) -> SchemaRef {
    let relax_field = |f: StructField| -> StructField {
        if !relaxed_nesting {
            return f;
        }
        let DataType::Struct(inner) = f.data_type() else {
            return f;
        };
        let relaxed = schema_with_all_fields_nullable(inner.as_ref())
            .unwrap_or_else(|_| inner.as_ref().clone());
        StructField::nullable(f.name().as_str(), relaxed)
    };
    arc_schema(all_action_fields().into_iter().map(relax_field))
}

pub(super) fn action_read_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| action_schema(true));
    SCHEMA.clone()
}

pub(super) fn action_output_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| action_schema(false));
    SCHEMA.clone()
}

/// Schema = `{path: STRING, size: LONG, version: LONG?}`. With `with_version=true`, the version
/// column carries the per-commit version surfaced by `build_commit_dedup_plan`'s window.
pub(super) fn path_size_schema(with_version: bool) -> SchemaRef {
    let mut fields = vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
    ];
    if with_version {
        fields.push(StructField::not_null("version", DataType::LONG));
    }
    arc_schema(fields)
}

/// Action schema augmented with the [`Add`] slot replaced by an augmented struct that
/// carries optionally-parsed `stats_parsed` / `partitionValues_parsed` sub-fields. Used by
/// [`super::scan_replay`] when a predicate forces data-skipping projection.
pub(super) fn action_schema_with_augmented_add(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let mut add_fields: Vec<StructField> = Add::to_schema().fields().cloned().collect();
    if let Some(schema) = stats_parsed_schema {
        add_fields.push(StructField::nullable(
            "stats_parsed",
            schema.as_ref().clone(),
        ));
    }
    if let Some(schema) = partition_values_parsed_schema {
        add_fields.push(StructField::nullable(
            "partitionValues_parsed",
            schema.as_ref().clone(),
        ));
    }
    let mut fields = all_action_fields();
    fields[0] = StructField::nullable(ADD_NAME, StructType::new_unchecked(add_fields));
    arc_schema(fields)
}

/// Schema = [`action_read_schema`] plus the dedup-key column [`FSR_JOIN_KEY_COL`]. Used by
/// the `FSR_COMMIT_DEDUP` relation handle and by the projection that materializes
/// the dedup key on the checkpoint side of the LeftAnti.
/// Action schema augmented with the FSR join key column (`__fsr_join_k: ARRAY<STRING>?`), and
/// optionally a per-commit `version: LONG` column. The version column is required only by
/// `build_commit_dedup_plan`'s row_number window (`ORDER BY version DESC`) and is dropped
/// before the relation is persisted, so its presence in the schema is plan-internal.
pub(super) fn augmented_action_schema(with_version: bool) -> Result<SchemaRef, DeltaError> {
    let join_key_type = DataType::Array(Box::new(ArrayType::new(DataType::STRING, true)));
    let mut b = action_read_schema()
        .build_on()
        .with_nullable(FSR_JOIN_KEY_COL, join_key_type);
    if with_version {
        b = b.with_not_null("version", DataType::LONG);
    }
    b.build().map_err(|e| e.into_delta_default())
}

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
    use std::sync::Arc;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::UnaryExpressionOp;
    use crate::utils::test_utils::string_array_to_engine_data;
    use crate::Engine;

    #[test]
    fn fsr_dedup_key_eval_add_row_carries_path_and_dv_components() {
        // dv_unique_id is now `ToJson(Array(storageType, pathOrInlineDv))` rather than a
        // Plus-concatenated `unique_id_from_parts` string. Assert the encoded JSON carries the
        // path and DV identity components verbatim (any consumer that reduces over equality of
        // full JSON strings still dedups identical DVs).
        let line = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"dvpath","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let engine = SyncEngine::new();
        let parsed = engine
            .json_handler()
            .parse_json(
                string_array_to_engine_data(StringArray::from(vec![line])),
                action_read_schema(),
            )
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            batch,
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
        let engine = SyncEngine::new();
        let schema = action_read_schema();
        let check = |line: &str, subs: &[&str]| {
            let parsed = engine
                .json_handler()
                .parse_json(
                    string_array_to_engine_data(StringArray::from(vec![line])),
                    Arc::clone(&schema),
                )
                .unwrap();
            let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
            let batch = arrow.record_batch();
            let out = evaluate_expression(
                &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
                batch,
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
        let engine = SyncEngine::new();
        let schema = action_read_schema();
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
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), Arc::clone(&schema))
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let out = evaluate_expression(
            &fsr_row_has_identity_predicate().into(),
            batch,
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
