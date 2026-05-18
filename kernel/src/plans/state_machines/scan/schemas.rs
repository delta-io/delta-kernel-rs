//! Action schema constructors used by the FSR plan-builders and the replay-scan plans.
//!
//! Centralizes the action-schema builder (`action_schema`) and the augmented-schema helpers
//! (`augmented_action_schema`, `action_schema_with_augmented_add`, `path_size_schema`)
//! plus the [`load_materialized_schema`] helper used by Load-sink builders.

use std::sync::{Arc, LazyLock};

use super::dedup::FSR_JOIN_KEY_COL;
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::schema::{
    arc_schema, ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema,
};

/// Top-level action fields in canonical order, used as the base of [`action_schema`] and
/// [`augmented_action_schema`]. Each variant points at the typed `ToSchema` of its action.
pub(super) fn all_action_fields() -> Vec<StructField> {
    vec![
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ]
}

/// Strict action schema for the action-replay stream: each top-level action slot is nullable
/// (a row carries one variant), but the inner per-action shape preserves the typed `ToSchema`
/// nullability. The engine is responsible for realigning relaxed file-source output to this
/// contract at the scan boundary (see `NullabilityEnforcingTableProvider`).
pub(super) fn action_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| arc_schema(all_action_fields()));
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

/// Compose a materialized-relation schema from a `LoadSink`'s `file_schema` plus the
/// upstream-row columns it broadcasts onto each emitted row. The returned schema lists
/// the file columns first, then every `passthrough` name resolved against `upstream`
/// preserving the original type and nullability.
///
/// Returns [`DeltaErrorCode::DeltaCommandInvariantViolation`] if any requested
/// `passthrough` name is absent from `upstream`.
pub(super) fn load_materialized_schema(
    file_schema: &SchemaRef,
    upstream: &SchemaRef,
    passthrough: &[&str],
) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
    let up = upstream.as_ref();
    for name in passthrough {
        let field = up.fields().find(|f| f.name() == *name).ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::load_materialized_schema: upstream schema {:?} missing passthrough `{name}`",
                upstream,
            )
        })?;
        fields.push(StructField::new(
            *name,
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

/// Action schema augmented with the [`Add`] slot replaced by an augmented struct that
/// carries optionally-parsed `stats_parsed` / `partitionValues_parsed` sub-fields. Used by
/// [`super::file_scan`] when a predicate forces data-skipping projection.
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

/// Action schema augmented with the FSR join key column (`__fsr_join_k: ARRAY<STRING>?`), and
/// optionally a per-commit `version: LONG` column. The version column is required only by
/// `build_commit_dedup_plan`'s row_number window (`ORDER BY version DESC`) and is dropped
/// before the relation is persisted, so its presence in the schema is plan-internal.
pub(super) fn augmented_action_schema(with_version: bool) -> Result<SchemaRef, DeltaError> {
    augmented_action_schema_with_stats(with_version, None)
}

/// Like [`augmented_action_schema`] but with the `add` slot replaced by an augmented struct
/// carrying `add.stats_parsed` when `stats_parsed_schema` is `Some`. Used by FSR commit_dedup
/// and the FSR results union sources when `with_stats` is requested.
pub(super) fn augmented_action_schema_with_stats(
    with_version: bool,
    stats_parsed_schema: Option<&SchemaRef>,
) -> Result<SchemaRef, DeltaError> {
    let join_key_type = DataType::Array(Box::new(ArrayType::new(DataType::STRING, true)));
    let mut b = fsr_action_schema(stats_parsed_schema)
        .build_on()
        .with_nullable(FSR_JOIN_KEY_COL, join_key_type);
    if with_version {
        b = b.with_not_null("version", DataType::LONG);
    }
    b.build().map_err(|e| e.into_delta_default())
}

/// FSR-side action schema: [`action_schema`] when `stats_parsed_schema` is `None`, or
/// [`action_schema_with_augmented_add(stats, None)`] when stats are requested.
pub(super) fn fsr_action_schema(stats_parsed_schema: Option<&SchemaRef>) -> SchemaRef {
    match stats_parsed_schema {
        None => action_schema(),
        Some(_) => action_schema_with_augmented_add(stats_parsed_schema, None),
    }
}
