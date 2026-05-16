//! Action schema constructors used by the FSR plan-builders and the replay-scan plans.
//!
//! Centralizes the action-schema builders (`action_schema`, `action_read_schema`,
//! `action_output_schema` with `LazyLock` caches) and the augmented-schema helpers
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
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
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
