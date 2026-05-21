//! Action-stream (schema, projection) pair builders for `execute_reconciliation`.
//!
//! Each reconciliation stage projects through a `Pair = (SchemaRef, Vec<Arc<Expression>>)`. Both
//! scan and FSR pipelines start from a base ([`SCAN_BASE`] or [`FSR_BASE`]) and optionally apply
//! [`augment_add`] to splice typed `stats_parsed` / `partitionValues_parsed` sub-fields into the
//! nested `add` struct. Carrying schema and projection as one value keeps them in lockstep.

use std::sync::Arc;

use super::dedup::FSR_JOIN_KEY_COL;
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::expressions::{col, Expression};
use crate::schema::{
    arc_schema, ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema,
};

/// Paired output of any action-stream stage: the row schema and the matching per-column
/// projection expressions in declaration order.
pub(super) type Pair = (SchemaRef, Vec<Arc<Expression>>);

// === Base pairs ===========================================================================
//
// Both bases are `LazyLock<Pair>` built once on first access and then cheaply cloned (the
// `Arc<SchemaRef>` + `Vec<Arc<Expression>>` clones are O(small)). All the augmentation helpers
// below take a `Pair` by value, so the typical call shape is
// `augment_add(SCAN_BASE.clone(), ..)`.

/// Scan pipeline base: `{add, remove}` only. The scan path never reconciles
/// protocol/metaData/txn/domainMetadata (the snapshot reads those separately).
pub(super) static SCAN_BASE: std::sync::LazyLock<Pair> = std::sync::LazyLock::new(|| {
    (
        arc_schema([
            StructField::nullable(ADD_NAME, Add::to_schema()),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        ]),
        vec![col([ADD_NAME]).into(), col([REMOVE_NAME]).into()],
    )
});

/// FSR pipeline base: all six action slots.
pub(super) static FSR_BASE: std::sync::LazyLock<Pair> = std::sync::LazyLock::new(|| {
    (
        arc_schema([
            StructField::nullable(ADD_NAME, Add::to_schema()),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
            StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
            StructField::nullable(METADATA_NAME, Metadata::to_schema()),
            StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
            StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        ]),
        vec![
            col([ADD_NAME]).into(),
            col([REMOVE_NAME]).into(),
            col([PROTOCOL_NAME]).into(),
            col([METADATA_NAME]).into(),
            col([DOMAIN_METADATA_NAME]).into(),
            col([SET_TRANSACTION_NAME]).into(),
        ],
    )
});

// === Extra-column field constants =========================================================

/// Synthetic dedup-key column added before windowing and re-projected through the antijoin.
pub(super) static JOIN_KEY_FIELD: std::sync::LazyLock<StructField> =
    std::sync::LazyLock::new(|| {
        StructField::nullable(
            FSR_JOIN_KEY_COL,
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
        )
    });

/// Per-commit `version` column broadcast through the commit-load LoadSink and used as the
/// window's order-by key. Dropped after the post-window projection.
pub(super) static VERSION_FIELD: std::sync::LazyLock<StructField> =
    std::sync::LazyLock::new(|| StructField::not_null("version", DataType::LONG));

// === Add augmentation =====================================================================

/// Augment the nested `add` struct with optional `stats_parsed` and / or `partitionValues_parsed`
/// sub-fields. Identity when both are `None`.
///
/// Schema: gains `stats_parsed` after `stats` and / or `partitionValues_parsed` after
/// `partitionValues`. Inner `add` fields are normalized to nullable because the outer slot is
/// nullable; declaring strict here forces nullable-to-non-null casts that DataFusion rejects.
///
/// Projection: `projection[0]` is rewritten as a [`Expression::struct_from`] over the augmented
/// inner fields. Existing fields are bare column refs; `stats_parsed` is either `parse_json` or
/// the `add.stats_parsed` column (per `has_parsed_stats`); `partitionValues_parsed` is always
/// `map_to_struct(add.partitionValues)` — checkpoints never carry it natively.
///
/// We use `struct_from` rather than `Expression::Transform` because the datafusion translator
/// does not yet support non-identity transforms; switching is a future optimization.
pub(super) fn augment_add(
    pair: Pair,
    stats: Option<(SchemaRef, /* has_parsed_stats= */ bool)>,
    parts: Option<SchemaRef>,
) -> Pair {
    if stats.is_none() && parts.is_none() {
        return pair;
    }
    let (schema, mut projection) = pair;
    let Some(add_field) = schema.field(ADD_NAME) else {
        unreachable!("bases always place an `add` slot at index 0");
    };
    let DataType::Struct(add_struct) = add_field.data_type() else {
        unreachable!("bases always declare `add` as a struct");
    };

    // Build the augmented inner struct via fluent inserts.
    let mut augmented = StructType::new_unchecked(
        add_struct
            .fields()
            .map(|f| StructField::nullable(f.name(), f.data_type().clone())),
    );
    if let Some((stats_schema, _)) = &stats {
        augmented = augmented
            .with_field_inserted_after(
                Some("stats"),
                StructField::nullable("stats_parsed", stats_schema.as_ref().clone()),
            )
            .unwrap_or_else(|_| unreachable!("`Add` carries a `stats` field by Delta protocol"));
    }
    if let Some(parts_schema) = &parts {
        augmented = augmented
            .with_field_inserted_after(
                Some("partitionValues"),
                StructField::nullable("partitionValues_parsed", parts_schema.as_ref().clone()),
            )
            .unwrap_or_else(|_| {
                unreachable!("`Add` carries a `partitionValues` field by Delta protocol")
            });
    }

    // Build the matching projection by walking the augmented fields once. The augmented field
    // names are unique tags that imply their context (e.g. `stats_parsed` is only present iff
    // `stats.is_some()`); we still match defensively via `if let`.
    let inner_exprs: Vec<Arc<Expression>> = augmented
        .fields()
        .map(|f| {
            let expr = match (f.name().as_str(), &stats) {
                ("stats_parsed", Some((_, true))) => col([ADD_NAME, "stats_parsed"]),
                ("stats_parsed", Some((s, false))) => {
                    Expression::parse_json(col([ADD_NAME, "stats"]), s.clone())
                }
                ("partitionValues_parsed", _) => {
                    Expression::map_to_struct(col([ADD_NAME, "partitionValues"]))
                }
                (name, _) => col([ADD_NAME, name]),
            };
            Arc::new(expr)
        })
        .collect();

    let mut new_fields: Vec<StructField> = schema.fields().cloned().collect();
    new_fields[0] = StructField::nullable(ADD_NAME, augmented);
    projection[0] = Arc::new(Expression::struct_from(inner_exprs));
    (arc_schema(new_fields), projection)
}

// === Scan-specific terminal builder =======================================================

/// Post-FSR flat file row built by the scan terminal: `{path, size, deletionVector,
/// fileConstantValues: {...}}`. When `partition_values_parsed_schema` is `Some(parts)`, an
/// extra `partitionValues_parsed: parts` field is appended inside `fileConstantValues`.
///
/// **Invariant:** when `partition_values_parsed_schema` is `Some`, the upstream reconciliation
/// pipeline must have already augmented `add` with a matching `partitionValues_parsed` via
/// [`augment_add`]. The terminal projection therefore reads `col(["add",
/// "partitionValues_parsed"])` directly instead of re-deriving the struct via `map_to_struct` per
/// row.
pub(super) fn scan_file_row_pair(partition_values_parsed_schema: Option<&SchemaRef>) -> Pair {
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
    let partition_values = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    let mut file_constant_exprs: Vec<Arc<Expression>> = vec![
        col(["add", "partitionValues"]).into(),
        col(["add", "baseRowId"]).into(),
        col(["add", "defaultRowCommitVersion"]).into(),
        col(["add", "tags"]).into(),
        col(["add", "clusteringProvider"]).into(),
    ];
    if let Some(parts) = partition_values_parsed_schema {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            parts.as_ref().clone(),
        ));
        // Passthrough: the upstream `augment_add(.., Some(parts))` already materialized this
        // sub-field on every row. Re-deriving via `map_to_struct(add.partitionValues)` here
        // would be a wasted per-row map walk.
        file_constant_exprs.push(col(["add", "partitionValues_parsed"]).into());
    }
    let schema = Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(
            FILE_CONSTANT_VALUES_NAME,
            StructType::new_unchecked(file_constant_fields),
        ),
    ]));
    let projection: Vec<Arc<Expression>> = vec![
        col(["add", "path"]).into(),
        col(["add", "size"]).into(),
        col(["add", "deletionVector"]).into(),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ];
    (schema, projection)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(fields: impl Iterator<Item = &'static str>) -> Vec<String> {
        fields.map(String::from).collect()
    }

    /// Lookup the projection expression for an augmented inner field by name.
    fn inner_expr<'a>(pair: &'a Pair, field: &str) -> &'a Expression {
        let DataType::Struct(add) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add must be a struct");
        };
        let idx = add.fields().position(|f| f.name() == field).unwrap();
        let Expression::Struct(exprs, _) = pair.1[0].as_ref() else {
            panic!("projection[0] must be struct_from when augmented");
        };
        exprs[idx].as_ref()
    }

    /// Pin canonical top-level action lists — load-bearing for every downstream stage.
    #[test]
    fn bases_pin_canonical_action_slots() {
        let scan: Vec<_> = SCAN_BASE.0.fields().map(|f| f.name().clone()).collect();
        let fsr: Vec<_> = FSR_BASE.0.fields().map(|f| f.name().clone()).collect();
        assert_eq!(scan, names([ADD_NAME, REMOVE_NAME].into_iter()));
        assert_eq!(
            fsr,
            names(
                [
                    ADD_NAME,
                    REMOVE_NAME,
                    PROTOCOL_NAME,
                    METADATA_NAME,
                    DOMAIN_METADATA_NAME,
                    SET_TRANSACTION_NAME
                ]
                .into_iter()
            )
        );
    }

    /// `augment_add(_, None, None)` is identity; projection[0] stays a bare column ref.
    #[test]
    fn augment_add_is_identity_when_no_args() {
        let pair = augment_add(SCAN_BASE.clone(), None, None);
        assert_eq!(pair.0.fields().count(), SCAN_BASE.0.fields().count());
        assert!(matches!(pair.1[0].as_ref(), Expression::Column(_)));
    }

    /// Exhaustively check the augmented projection: `partitionValues_parsed` is always
    /// `map_to_struct`; `stats_parsed` is `parse_json` when `has_parsed_stats=false` and a bare
    /// column ref when `has_parsed_stats=true`.
    #[test]
    fn augment_add_inner_projection_matches_augmented_fields() {
        let stats = arc_schema([StructField::not_null("numRecords", DataType::LONG)]);
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);

        // Non-native stats + parts: both augmented, parse_json + map_to_struct.
        let derived = augment_add(
            SCAN_BASE.clone(),
            Some((stats.clone(), /* has_parsed_stats= */ false)),
            Some(parts.clone()),
        );
        assert!(matches!(
            inner_expr(&derived, "stats_parsed"),
            Expression::ParseJson(_)
        ));
        assert!(matches!(
            inner_expr(&derived, "partitionValues_parsed"),
            Expression::MapToStruct(_)
        ));

        // Native stats: stats_parsed becomes a passthrough column.
        let native = augment_add(
            SCAN_BASE.clone(),
            Some((stats, /* has_parsed_stats= */ true)),
            None,
        );
        assert!(matches!(
            inner_expr(&native, "stats_parsed"),
            Expression::Column(_)
        ));
    }

    /// `scan_file_row_pair(Some(parts))` reads `partitionValues_parsed` as a passthrough column
    /// instead of re-deriving via `map_to_struct` per row. Top-level shape is unchanged.
    #[test]
    fn scan_file_row_pair_passes_parsed_partition_values_through() {
        let plain = scan_file_row_pair(None);
        let plain_fields: Vec<_> = plain.0.fields().map(|f| f.name().clone()).collect();
        assert_eq!(
            plain_fields,
            ["path", "size", "deletionVector", "fileConstantValues"]
        );

        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let with_parts = scan_file_row_pair(Some(&parts));
        let with_fields: Vec<_> = with_parts.0.fields().map(|f| f.name().clone()).collect();
        assert_eq!(with_fields, plain_fields);

        let Expression::Struct(fc_exprs, _) = with_parts.1[3].as_ref() else {
            panic!("projection[3] must be a Struct expression");
        };
        let last = fc_exprs.last().unwrap();
        assert!(
            matches!(last.as_ref(), Expression::Column(_)),
            "partitionValues_parsed must be a bare column ref (passthrough); got {last:?}",
        );
    }
}
