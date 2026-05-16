//! Schema helpers, action-column path constants, and predicate/projection builders shared
//! by the FSR plan and replay-scan modules.
//!
//! Centralizes the column-path string constants (`ADD_PATH`, `REMOVE_PATH`, ...), the action
//! schema builders (`action_schema`, `action_read_schema`, `action_output_schema` with
//! `LazyLock` caches), the per-plan projection helpers, and the row-identity / retention
//! predicates that the rest of the FSR pipeline composes.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
    SIDECAR_NAME,
};
use crate::delta_error;
use crate::expressions::{
    BinaryExpressionOp, ColumnName, Expression, IntoColumnName, Predicate, Scalar,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::schema::{
    ArrayType, DataType, MetadataColumnSpec, SchemaRef, StructField, StructType, ToSchema,
};
use crate::snapshot::Snapshot;

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

/// Action schema for the action-replay stream.
///
/// When `relaxed_nesting` is true (transport schema used while replaying / unioning action rows
/// from heterogeneous sources), nested fields are made all-nullable to avoid planner-time cast
/// failures during engine materialization. When false, the strict per-action `ToSchema` is used
/// (kernel-visible output schema).
fn action_schema(relaxed_nesting: bool) -> SchemaRef {
    let relax = |s: StructType| {
        if relaxed_nesting {
            schema_with_all_fields_nullable(&s).unwrap_or(s)
        } else {
            s
        }
    };
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, relax(Add::to_schema())),
        StructField::nullable(REMOVE_NAME, relax(Remove::to_schema())),
        StructField::nullable(PROTOCOL_NAME, relax(Protocol::to_schema())),
        StructField::nullable(METADATA_NAME, relax(Metadata::to_schema())),
        StructField::nullable(DOMAIN_METADATA_NAME, relax(DomainMetadata::to_schema())),
        StructField::nullable(SET_TRANSACTION_NAME, relax(SetTransaction::to_schema())),
    ]))
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
    Arc::new(StructType::new_unchecked(fields))
}

pub(super) fn scan_partition_values_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Option<SchemaRef> {
    let partition_columns = snapshot.table_configuration().partition_columns();
    if partition_columns.is_empty() {
        return None;
    }
    let fields = logical_schema
        .fields()
        .filter(|f| partition_columns.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    if fields.is_empty() {
        None
    } else {
        Some(Arc::new(StructType::new_unchecked(fields)))
    }
}

pub(super) fn scan_partition_values_physical_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Result<Option<SchemaRef>, DeltaError> {
    let Some(logical_partition_schema) = scan_partition_values_schema(snapshot, logical_schema)
    else {
        return Ok(None);
    };
    let mode = snapshot.table_configuration().column_mapping_mode();
    let physical_partition_schema = logical_partition_schema
        .as_ref()
        .make_physical(mode)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())?;
    Ok(Some(physical_partition_schema))
}

pub(super) fn scan_live_actions_schema(
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let partition_values = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    if let Some(schema) = partition_values_parsed_schema {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            schema.as_ref().clone(),
        ));
    }
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(
            FILE_CONSTANT_VALUES_NAME,
            StructType::new_unchecked(file_constant_fields),
        ),
    ]))
}

pub(super) fn scan_live_actions_projection(
    with_partition_values_parsed: bool,
) -> Vec<Arc<Expression>> {
    let mut file_constant_exprs = vec![
        Expression::column(["add", "partitionValues"]),
        Expression::column(["add", "baseRowId"]),
        Expression::column(["add", "defaultRowCommitVersion"]),
        Expression::column(["add", "tags"]),
        Expression::column(["add", "clusteringProvider"]),
    ];
    if with_partition_values_parsed {
        file_constant_exprs.push(Expression::map_to_struct(Expression::column([
            "add",
            "partitionValues",
        ])));
    }
    vec![
        Arc::new(Expression::column(["add", "path"])),
        Arc::new(Expression::column(["add", "size"])),
        Arc::new(Expression::column(["add", "deletionVector"])),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ]
}

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
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, StructType::new_unchecked(add_fields)),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ]))
}

pub(super) fn scan_actions_with_parsed_projection(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Vec<Arc<Expression>> {
    let mut add_exprs = vec![
        Arc::new(Expression::column(["add", "path"])),
        Arc::new(Expression::column(["add", "partitionValues"])),
        Arc::new(Expression::column(["add", "size"])),
        Arc::new(Expression::column(["add", "modificationTime"])),
        Arc::new(Expression::column(["add", "dataChange"])),
        Arc::new(Expression::column(["add", "stats"])),
        Arc::new(Expression::column(["add", "tags"])),
        Arc::new(Expression::column(["add", "deletionVector"])),
        Arc::new(Expression::column(["add", "baseRowId"])),
        Arc::new(Expression::column(["add", "defaultRowCommitVersion"])),
        Arc::new(Expression::column(["add", "clusteringProvider"])),
    ];
    if let Some(schema) = stats_parsed_schema {
        add_exprs.push(Arc::new(Expression::parse_json(
            Expression::column(["add", "stats"]),
            schema.clone(),
        )));
    }
    if partition_values_parsed_schema.is_some() {
        add_exprs.push(Arc::new(Expression::map_to_struct(Expression::column([
            "add",
            "partitionValues",
        ]))));
    }
    vec![
        Arc::new(Expression::struct_from(add_exprs)),
        Arc::new(Expression::column([REMOVE_NAME])),
        Arc::new(Expression::column([PROTOCOL_NAME])),
        Arc::new(Expression::column([METADATA_NAME])),
        Arc::new(Expression::column([DOMAIN_METADATA_NAME])),
        Arc::new(Expression::column([SET_TRANSACTION_NAME])),
    ]
}

pub(super) fn scan_data_file_schema(
    physical_schema: &SchemaRef,
    logical_schema: &SchemaRef,
) -> Result<SchemaRef, DeltaError> {
    if logical_schema.contains_metadata_column(&MetadataColumnSpec::RowId)
        && !physical_schema.contains_metadata_column(&MetadataColumnSpec::RowIndex)
    {
        let row_index_field = StructField::default_row_index_column().clone();
        if physical_schema.contains(row_index_field.name.as_str()) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::scan::scan_data_file_schema: row-id projection requires row-index metadata field `{}` but schema already contains a field with that name",
                row_index_field.name,
            ));
        }
        return physical_schema
            .as_ref()
            .add([row_index_field])
            .map(Arc::new)
            .map_err(|e| e.into_delta_default());
    }
    Ok(physical_schema.clone())
}

pub(super) fn scan_data_projection(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    column_mapping_mode: crate::table_features::ColumnMappingMode,
) -> Result<Vec<Arc<Expression>>, DeltaError> {
    let row_index_name = StructField::default_row_index_column().name.clone();
    let mut physical_fields = physical_schema.fields();
    logical_schema
        .fields()
        .map(|field| {
            let expr = match field.get_metadata_column_spec() {
                Some(MetadataColumnSpec::RowIndex) => Expression::column([row_index_name.as_str()]),
                Some(MetadataColumnSpec::RowCommitVersion) => {
                    Expression::column([FILE_CONSTANT_VALUES_NAME, "defaultRowCommitVersion"])
                }
                Some(MetadataColumnSpec::RowId) => Expression::binary(
                    BinaryExpressionOp::Plus,
                    Expression::column([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
                    Expression::column([row_index_name.as_str()]),
                ),
                Some(MetadataColumnSpec::FilePath) => Expression::column(["path"]),
                None if partition_columns.contains(field.name()) => Expression::column([
                    FILE_CONSTANT_VALUES_NAME,
                    "partitionValues_parsed",
                    field.physical_name(column_mapping_mode),
                ]),
                None => {
                    let physical_field = physical_fields.next().ok_or_else(|| {
                        delta_error!(
                            DeltaErrorCode::DeltaCommandInvariantViolation,
                            "fsr::scan_data_projection: missing physical field for logical field `{}`",
                            field.name(),
                        )
                    })?;
                    Expression::column([physical_field.name().as_str()])
                }
            };
            Ok(Arc::new(expr))
        })
        .collect()
}

pub(super) fn sidecar_only_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        SIDECAR_NAME,
        Sidecar::to_schema(),
    )]))
}

pub(super) fn checkpoint_manifest_scan_schema(include_sidecar: bool) -> SchemaRef {
    let mut fields: Vec<StructField> = action_read_schema().fields().cloned().collect();
    if include_sidecar {
        fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    }
    Arc::new(StructType::new_unchecked(fields))
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

pub(super) fn action_identity_projection() -> Vec<Arc<Expression>> {
    action_read_schema()
        .fields()
        .map(|f| Arc::new(Expression::column([f.name().as_str()])))
        .collect()
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
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::UnaryExpressionOp;
    use crate::schema::MetadataColumnSpec;
    use crate::utils::test_utils::string_array_to_engine_data;
    use crate::Engine;

    #[test]
    fn scan_data_projection_maps_metadata_columns() {
        let logical = Arc::new(StructType::new_unchecked([
            StructField::not_null("value", DataType::LONG),
            StructField::create_metadata_column(
                "_metadata.row_index",
                MetadataColumnSpec::RowIndex,
            ),
            StructField::create_metadata_column("_metadata.row_id", MetadataColumnSpec::RowId),
            StructField::create_metadata_column(
                "_metadata.row_commit_version",
                MetadataColumnSpec::RowCommitVersion,
            ),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        ]));
        let projection = scan_data_projection(
            &logical,
            &logical,
            &HashSet::new(),
            crate::table_features::ColumnMappingMode::None,
        )
        .unwrap();
        assert_eq!(projection.len(), 5);
        assert!(matches!(
            projection[0].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["value"])
        ));
        assert!(matches!(
            projection[1].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["_metadata.row_index"])
        ));
        assert!(matches!(
            projection[3].as_ref(),
            Expression::Column(c) if c == &ColumnName::new([FILE_CONSTANT_VALUES_NAME, "defaultRowCommitVersion"])
        ));
        assert!(matches!(
            projection[4].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["path"])
        ));
    }

    #[rstest::rstest]
    #[case::synthesizes_when_missing(false)]
    #[case::dedups_when_already_present(true)]
    fn scan_data_file_schema_emits_exactly_one_row_index_for_row_id(
        #[case] row_index_already_present: bool,
    ) {
        let mut fields = vec![
            StructField::not_null("value", DataType::LONG),
            StructField::create_metadata_column("_metadata.row_id", MetadataColumnSpec::RowId),
        ];
        if row_index_already_present {
            fields.push(StructField::create_metadata_column(
                "_metadata.row_index",
                MetadataColumnSpec::RowIndex,
            ));
        }
        let logical = Arc::new(StructType::new_unchecked(fields));
        let file_schema = scan_data_file_schema(&logical, &logical).unwrap();
        assert_eq!(
            file_schema
                .fields()
                .filter(|f| f.get_metadata_column_spec() == Some(MetadataColumnSpec::RowIndex))
                .count(),
            1
        );
    }

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
