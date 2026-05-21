//! Action schema constructors for the FSR plan-builders and the replay-scan plans.

use std::sync::LazyLock;

use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::schema::{arc_schema, SchemaRef, StructField, ToSchema};

/// Top-level action fields in canonical order — base of [`action_schema`]. Each variant
/// points at the typed `ToSchema` of its action.
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

/// Strict action schema for the action-replay stream: each top-level action slot is nullable
/// (a row carries one variant), but the inner per-action shape preserves the typed `ToSchema`
/// nullability. The engine is responsible for realigning relaxed file-source output to this
/// contract at the scan boundary (see `NullabilityEnforcingTableProvider`).
pub(super) fn action_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| arc_schema(all_action_fields()));
    SCHEMA.clone()
}
