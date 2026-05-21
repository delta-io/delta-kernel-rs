//! Action-stream (schema, projection) pair builders for `execute_reconciliation`.
//!
//! This module exposes a small toolkit for assembling the
//! `Pair = (SchemaRef, Vec<Arc<Expression>>)` values that the reconciliation pipeline projects
//! through at each stage. Both scan and FSR pipelines start from one of two compile-time bases
//! ([`SCAN_BASE`] for add/remove only, [`FSR_BASE`] for the full six-slot action set), and
//! optionally apply an [`augment_add`] augmentation (typed `stats_parsed` and / or
//! `partitionValues_parsed` sub-fields) before the pair is handed to
//! `PlanBuilder::project_pair(...)`.
//!
//! Zero new types beyond the [`Pair`] tuple alias. The augmentation uses [`Expression::Transform`]
//! for sparse projection over the nested `add` struct (so each augmented field is one
//! `with_inserted_field` call) and [`StructType`] fluent builders for the matching schema mutation
//! (so schema and projection stay in sync field-by-field).
//!
//! ## Why pairs (not separate schema + projection helpers)?
//!
//! The schema and the projection must agree, field-by-field, at every projecting stage of the IR.
//! Carrying them as one value built by a single helper makes schema-projection drift impossible:
//! [`augment_add`] mutates the `add` slot of both halves in lockstep.

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

/// Augment the nested `add` struct of `pair` with optional `stats_parsed` and / or
/// `partitionValues_parsed` sub-fields.
///
/// Returns `pair` unchanged when both `stats` and `parts` are `None`. Otherwise:
///
/// - The `add` slot's struct gains a nullable `stats_parsed` field (after `stats`) and / or a
///   nullable `partitionValues_parsed` field (after `partitionValues`).
/// - `projection[0]` is rewritten as an [`Expression::struct_from`] over the nested `add` struct
///   with one expression per inner field:
///   - existing inner fields: passthrough `col(["add", <field>])`.
///   - `stats_parsed`: `col(["add", "stats_parsed"])` if `has_parsed_stats` (the leaf already
///     materializes the column), otherwise `parse_json(col(["add", "stats"]), stats_schema)`.
///   - `partitionValues_parsed`: always `map_to_struct(col(["add", "partitionValues"]))` — Delta
///     checkpoint files never carry `add.partitionValues_parsed` natively, so there is no
///     passthrough variant.
///
/// Inner `add` fields are normalized to `nullable=true`. The outer `add` slot is nullable —
/// any row may have `add IS NULL`, which makes every inner field transitively nullable in
/// downstream evaluators. Declaring strict here would force a `Cast(nullable -> non-null)`
/// between project / union / anti-join / sink that DataFusion rejects at planning time.
///
/// Implementation note: we build the projection with `struct_from` (rather than the sparser
/// [`Expression::Transform::with_inserted_field`](Transform::with_inserted_field)) because the
/// `delta-kernel-datafusion-engine` translator does not yet support non-identity transforms;
/// switching the projection to a Transform here is a future optimization gated on engine
/// support.
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

    // Schema: start from the base `add` struct with all inner fields normalized to nullable.
    // The fluent `with_field_inserted_after` builders thread the augmentations through.
    let mut augmented = normalize_to_nullable(add_struct);

    // Projection: one expression per inner field of the augmented `add` struct. Existing
    // fields are bare column refs; the augmented fields are the typed expressions described in
    // the doc-comment. We mirror `with_field_inserted_after`'s positional semantics by tracking
    // the inserted field's index and splicing the matching expression at the same position.
    let mut inner_exprs: Vec<Arc<Expression>> = augmented
        .fields()
        .map(|f| Arc::new(col([ADD_NAME, f.name()])))
        .collect();

    if let Some((stats_schema, has_parsed_stats)) = &stats {
        let Ok(next) = augmented.with_field_inserted_after(
            Some("stats"),
            StructField::nullable("stats_parsed", stats_schema.as_ref().clone()),
        ) else {
            unreachable!("Add::to_schema() carries a `stats` field by Delta protocol invariant");
        };
        augmented = next;
        let Some(stats_idx) = augmented.fields().position(|f| f.name() == "stats_parsed") else {
            unreachable!("with_field_inserted_after(stats_parsed) succeeded above");
        };
        let stats_expr: Expression = if *has_parsed_stats {
            col([ADD_NAME, "stats_parsed"])
        } else {
            Expression::parse_json(col([ADD_NAME, "stats"]), stats_schema.clone())
        };
        inner_exprs.insert(stats_idx, Arc::new(stats_expr));
    }
    if let Some(parts_schema) = &parts {
        let Ok(next) = augmented.with_field_inserted_after(
            Some("partitionValues"),
            StructField::nullable("partitionValues_parsed", parts_schema.as_ref().clone()),
        ) else {
            unreachable!(
                "Add::to_schema() carries a `partitionValues` field by Delta protocol invariant",
            );
        };
        augmented = next;
        let Some(parts_idx) = augmented
            .fields()
            .position(|f| f.name() == "partitionValues_parsed")
        else {
            unreachable!("with_field_inserted_after(partitionValues_parsed) succeeded above");
        };
        let parts_expr = Expression::map_to_struct(col([ADD_NAME, "partitionValues"]));
        inner_exprs.insert(parts_idx, Arc::new(parts_expr));
    }

    let mut new_fields: Vec<StructField> = schema.fields().cloned().collect();
    new_fields[0] = StructField::nullable(ADD_NAME, augmented);
    projection[0] = Arc::new(Expression::struct_from(inner_exprs));
    (arc_schema(new_fields), projection)
}

/// Rebuild `add_struct` as a `StructType` whose top-level fields are all `nullable`. See the
/// `augment_add` doc comment for why this normalization is load-bearing.
fn normalize_to_nullable(add_struct: &StructType) -> StructType {
    StructType::new_unchecked(
        add_struct
            .fields()
            .map(|f| StructField::nullable(f.name(), f.data_type().clone())),
    )
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

    /// Helper: extract field names from a schema.
    fn field_names(schema: &SchemaRef) -> Vec<String> {
        schema.fields().map(|f| f.name().clone()).collect()
    }

    /// Helper: extract field names from a StructType.
    fn struct_field_names(s: &StructType) -> Vec<String> {
        s.fields().map(|f| f.name().clone()).collect()
    }

    /// Pin the canonical top-level action lists for both pipelines. These shapes are
    /// load-bearing for every downstream stage (commit_load schema, dedup-key arm coverage,
    /// terminal projection).
    #[test]
    fn bases_pin_canonical_action_slots() {
        assert_eq!(field_names(&SCAN_BASE.0), [ADD_NAME, REMOVE_NAME]);
        assert_eq!(
            field_names(&FSR_BASE.0),
            [
                ADD_NAME,
                REMOVE_NAME,
                PROTOCOL_NAME,
                METADATA_NAME,
                DOMAIN_METADATA_NAME,
                SET_TRANSACTION_NAME,
            ],
        );
    }

    /// `augment_add(_, None, None)` is the identity.
    #[test]
    fn augment_add_identity_when_no_args() {
        let pair = augment_add(SCAN_BASE.clone(), None, None);
        assert_eq!(field_names(&pair.0), field_names(&SCAN_BASE.0));
        // Projection[0] is still a bare column reference, not a Transform.
        assert!(matches!(pair.1[0].as_ref(), Expression::Column(_)));
    }

    /// Helper: extract the per-inner-field projection from `augment_add`'s `projection[0]`
    /// (a `struct_from` expression).
    fn add_inner_exprs(pair: &Pair) -> &[Arc<Expression>] {
        let Expression::Struct(exprs, _) = pair.1[0].as_ref() else {
            panic!(
                "projection[0] must be a struct_from over add when augment_add is non-identity; \
                 got {:?}",
                pair.1[0]
            );
        };
        exprs
    }

    /// Lookup an inner expression by the augmented add struct's field index.
    fn inner_expr_for_field<'a>(pair: &'a Pair, field_name: &str) -> &'a Expression {
        let DataType::Struct(add) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add slot must be a struct");
        };
        let idx = add
            .fields()
            .position(|f| f.name() == field_name)
            .unwrap_or_else(|| panic!("field {field_name} not present"));
        add_inner_exprs(pair)[idx].as_ref()
    }

    /// With stats only, `add` gains `stats_parsed` and the projection's matching inner slot
    /// is a `parse_json(add.stats, schema)` expression.
    #[test]
    fn augment_add_stats_inserts_into_add_struct() {
        let stats = arc_schema([StructField::not_null("numRecords", DataType::LONG)]);
        let pair = augment_add(
            SCAN_BASE.clone(),
            Some((stats, /* has_parsed_stats= */ false)),
            None,
        );
        let DataType::Struct(add) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add slot must be a struct");
        };
        assert!(struct_field_names(add).contains(&"stats_parsed".to_string()));
        assert!(matches!(
            inner_expr_for_field(&pair, "stats_parsed"),
            Expression::ParseJson(_)
        ));
    }

    /// With native stats (`has_parsed_stats=true`) the stats_parsed slot is a bare column
    /// reference (passthrough) instead of `parse_json`.
    #[test]
    fn augment_add_stats_native_passthrough() {
        let stats = arc_schema([StructField::not_null("numRecords", DataType::LONG)]);
        let pair = augment_add(
            SCAN_BASE.clone(),
            Some((stats, /* has_parsed_stats= */ true)),
            None,
        );
        assert!(matches!(
            inner_expr_for_field(&pair, "stats_parsed"),
            Expression::Column(_)
        ));
    }

    /// Partition values are always derived (no `has_parsed` variant) — the matching inner
    /// slot is `map_to_struct(add.partitionValues)` regardless of any other flag.
    #[test]
    fn augment_add_parts_always_derived() {
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let pair = augment_add(SCAN_BASE.clone(), None, Some(parts));
        let DataType::Struct(add) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add slot must be a struct");
        };
        assert!(struct_field_names(add).contains(&"partitionValues_parsed".to_string()));
        assert!(matches!(
            inner_expr_for_field(&pair, "partitionValues_parsed"),
            Expression::MapToStruct(_)
        ));
    }

    /// Stats and parts compose into a single `add` struct, both inner-field exprs present.
    #[test]
    fn augment_add_composes_stats_and_parts() {
        let stats = arc_schema([StructField::not_null("numRecords", DataType::LONG)]);
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let pair = augment_add(
            SCAN_BASE.clone(),
            Some((stats, /* has_parsed_stats= */ false)),
            Some(parts),
        );
        // Top-level slots are unchanged.
        assert_eq!(field_names(&pair.0), [ADD_NAME, REMOVE_NAME]);
        let DataType::Struct(add) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add slot must be a struct");
        };
        let names = struct_field_names(add);
        assert!(names.contains(&"stats_parsed".to_string()));
        assert!(names.contains(&"partitionValues_parsed".to_string()));
        // Inner-projection length matches the augmented struct's field count.
        assert_eq!(add_inner_exprs(&pair).len(), add.num_fields());
        // Spot-check the two augmented slots.
        assert!(matches!(
            inner_expr_for_field(&pair, "stats_parsed"),
            Expression::ParseJson(_)
        ));
        assert!(matches!(
            inner_expr_for_field(&pair, "partitionValues_parsed"),
            Expression::MapToStruct(_)
        ));
    }

    /// `scan_file_row_pair(None)` is the four-column flat row; `Some(parts)` only changes
    /// the *inner* `fileConstantValues` struct, never the top-level shape. With `Some`, the
    /// `partitionValues_parsed` slot is a passthrough column, not a `map_to_struct` re-derivation.
    #[test]
    fn scan_file_row_pair_passes_parsed_partition_values_through() {
        let plain = scan_file_row_pair(None);
        assert_eq!(
            field_names(&plain.0),
            ["path", "size", "deletionVector", "fileConstantValues"],
        );
        assert_eq!(plain.1.len(), 4);

        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let with_parts = scan_file_row_pair(Some(&parts));
        assert_eq!(field_names(&with_parts.0), field_names(&plain.0));
        assert_eq!(with_parts.1.len(), 4);
        let DataType::Struct(fc) = with_parts
            .0
            .field("fileConstantValues")
            .expect("fileConstantValues present")
            .data_type()
        else {
            panic!("fileConstantValues must be a struct");
        };
        assert!(fc.contains("partitionValues_parsed"));

        // The terminal projection must read partitionValues_parsed as a passthrough column.
        let Expression::Struct(fc_exprs, _) = with_parts.1[3].as_ref() else {
            panic!("projection[3] must be a Struct expression");
        };
        // partitionValues_parsed is the last entry pushed when parts is Some.
        let last = fc_exprs.last().unwrap();
        assert!(
            matches!(last.as_ref(), Expression::Column(_)),
            "partitionValues_parsed must be a bare column ref (passthrough), not a derived \
             expression; got {:?}",
            last,
        );
    }
}
