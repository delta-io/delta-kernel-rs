//! Action schema constructors for the FSR plan-builders and the replay-scan plans.

use std::sync::LazyLock;

use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::schema::{arc_schema, SchemaRef, StructField, StructType, ToSchema};

/// Top-level action fields in canonical order — base of [`action_schema`]. Each variant
/// points at the typed `ToSchema` of its action.
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

/// Action schema with the `add` slot replaced by a struct carrying optional `stats_parsed` /
/// `partitionValues_parsed` sub-fields. Produced when FSR and replay-scan projections need
/// stats / partition-values wiring.
///
/// Add fields are forced **nullable** here. Downstream chains rebuild `add` via
/// `Expression::struct_from(...)` from streams that include non-`add` rows; for those rows
/// the children read as `NULL` regardless of `Add::to_schema()`'s strict declarations.
/// Declaring strict here would force a `Cast(nullable -> non-null)` between project and
/// union / anti-join / sink, which DataFusion rejects at planning time. Strict types are
/// still enforced at the parquet/JSON file-scan boundary.
pub(super) fn action_schema_with_augmented_add(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let mut add_fields: Vec<StructField> = Add::to_schema()
        .fields()
        .map(|f| StructField::nullable(f.name(), f.data_type().clone()))
        .collect();
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

/// FSR-side action schema: plain [`action_schema`] when `stats_parsed_schema` is `None`,
/// else [`action_schema_with_augmented_add`] with stats.
pub(super) fn fsr_action_schema(stats_parsed_schema: Option<&SchemaRef>) -> SchemaRef {
    match stats_parsed_schema {
        None => action_schema(),
        Some(_) => action_schema_with_augmented_add(stats_parsed_schema, None),
    }
}
