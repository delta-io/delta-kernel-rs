//! Action-stream (schema, projection) pair builders used by `execute_reconciliation`.
//!
//! This module exposes a small toolkit for assembling the
//! `Pair = (SchemaRef, Vec<Arc<Expression>>)` values that the reconciliation pipeline projects
//! through at each stage. Both scan and FSR pipelines start from one of two compile-time bases
//! ([`SCAN_BASE`] for add/remove only, [`FSR_BASE`] for the full six-slot action set), and
//! optionally apply nested `add`-struct augmentations ([`with_stats`], [`with_partition_values`])
//! before the pair is handed to `PlanBuilder::project_pair(...)`.
//!
//! Zero new types beyond the [`Pair`] tuple alias. The macro-free design keeps the public
//! surface a flat list of `LazyLock` statics + plain functions.
//!
//! ## Why pairs (not separate schema + projection helpers)?
//!
//! The schema and the projection must agree, field-by-field, at every projecting stage of
//! the IR. Carrying them as one value built by a single helper makes schema-projection drift
//! impossible: every augmentation function takes a `Pair` and returns a `Pair`, so the two
//! halves stay in lockstep by construction.

use std::sync::{Arc, LazyLock};

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
// below take a `Pair` by value, so the typical call shape is `with_stats(SCAN_BASE.clone(), ..)`.

/// Scan pipeline base: `{add, remove}` only. The scan path never reconciles
/// protocol/metaData/txn/domainMetadata (the snapshot reads those separately).
pub(super) static SCAN_BASE: LazyLock<Pair> = LazyLock::new(|| {
    (
        arc_schema([
            StructField::nullable(ADD_NAME, Add::to_schema()),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        ]),
        vec![col([ADD_NAME]).into(), col([REMOVE_NAME]).into()],
    )
});

/// FSR pipeline base: all six action slots.
pub(super) static FSR_BASE: LazyLock<Pair> = LazyLock::new(|| {
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
pub(super) static JOIN_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::nullable(
        FSR_JOIN_KEY_COL,
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
    )
});

/// Per-commit `version` column broadcast through the commit-load LoadSink and used as the
/// window's order-by key. Dropped after the post-window projection.
pub(super) static VERSION_FIELD: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("version", DataType::LONG));

// === Augmentation helpers =================================================================

/// Augment the nested `add` struct with a typed `stats_parsed` sub-field.
///
/// - When `stats` is `None`: no-op.
/// - When `stats` is `Some(schema)`:
///   - The `add` slot's struct gains a nullable `stats_parsed` field with the requested schema.
///   - The `add` projection expression is rebuilt as `struct_from(add.path, ..., stats_expr)` where
///     `stats_expr` is either the native `col(["add", "stats_parsed"])` (when `native` is true,
///     e.g. when the leaf parquet has `add.stats_parsed` and we want to passthrough) or
///     `parse_json(col(["add", "stats"]), schema)` otherwise.
pub(super) fn with_stats(pair: Pair, stats: Option<SchemaRef>, native: bool) -> Pair {
    let Some(stats) = stats else {
        return pair;
    };
    rewrite_add(pair, |inner_fields, inner_exprs| {
        inner_fields.push(StructField::nullable(
            "stats_parsed",
            stats.as_ref().clone(),
        ));
        let stats_expr = if native {
            col([ADD_NAME, "stats_parsed"])
        } else {
            Expression::parse_json(col([ADD_NAME, "stats"]), stats.clone())
        };
        inner_exprs.push(stats_expr.into());
    })
}

/// Augment the nested `add` struct with a typed `partitionValues_parsed` sub-field built by
/// `map_to_struct(add.partitionValues)`. No-op when `parts` is `None`.
pub(super) fn with_partition_values(pair: Pair, parts: Option<SchemaRef>) -> Pair {
    let Some(parts) = parts else {
        return pair;
    };
    rewrite_add(pair, |inner_fields, inner_exprs| {
        inner_fields.push(StructField::nullable(
            "partitionValues_parsed",
            parts.as_ref().clone(),
        ));
        inner_exprs.push(Expression::map_to_struct(col([ADD_NAME, "partitionValues"])).into());
    })
}

/// Internal: rewrite the `add` slot of `pair` by destructuring its struct and letting the
/// caller mutate the inner `(fields, exprs)` lists before reassembling. The `add` slot is at
/// index 0 in every base (and in every output of this helper), so we index directly.
///
/// Each inner field is normalized to `nullable=true` because the outer `add` slot itself is
/// nullable — any row may have `add IS NULL`, which makes every inner field transitively
/// nullable in downstream evaluators.
fn rewrite_add(
    (schema, mut projection): Pair,
    augment: impl FnOnce(&mut Vec<StructField>, &mut Vec<Arc<Expression>>),
) -> Pair {
    let mut fields: Vec<StructField> = schema.fields().cloned().collect();
    let DataType::Struct(add_struct) = fields[0].data_type() else {
        unreachable!("bases place `add` as a struct at index 0");
    };
    let mut inner_fields: Vec<StructField> = add_struct
        .fields()
        .map(|f| StructField::nullable(f.name(), f.data_type().clone()))
        .collect();
    // Two shapes: a fresh base passes `add` through as `col([add])` — bootstrap by
    // re-projecting every inner field explicitly. A previously-augmented pair already has
    // `struct_from([...])` here — reuse its children.
    let mut inner_exprs: Vec<Arc<Expression>> = match projection[0].as_ref() {
        Expression::Struct(children, _) => children.clone(),
        _ => inner_fields
            .iter()
            .map(|f| col([ADD_NAME, f.name().as_str()]).into())
            .collect(),
    };

    augment(&mut inner_fields, &mut inner_exprs);
    fields[0] = StructField::nullable(ADD_NAME, StructType::new_unchecked(inner_fields));
    projection[0] = Arc::new(Expression::struct_from(inner_exprs));
    (arc_schema(fields), projection)
}

// === Scan-specific terminal builder =======================================================

/// Post-FSR flat file row built by the scan terminal: `{path, size, deletionVector,
/// fileConstantValues: {...}}`. When `partition_values_parsed_schema` is `Some(parts)`, an
/// extra `partitionValues_parsed: parts` field is appended INSIDE `fileConstantValues` and
/// the projection emits `map_to_struct(add.partitionValues)` for it.
///
/// Returns the paired `(schema, projection)` for the terminal project that converts a
/// (post-reconciliation) action-stream row with an `add` slot into a flat file row.
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
    let mut file_constant_exprs = vec![
        col(["add", "partitionValues"]),
        col(["add", "baseRowId"]),
        col(["add", "defaultRowCommitVersion"]),
        col(["add", "tags"]),
        col(["add", "clusteringProvider"]),
    ];
    if let Some(parts) = partition_values_parsed_schema {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            parts.as_ref().clone(),
        ));
        file_constant_exprs.push(Expression::map_to_struct(col(["add", "partitionValues"])));
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
    let projection = vec![
        col(["add", "path"]).into(),
        col(["add", "size"]).into(),
        col(["add", "deletionVector"]).into(),
        Expression::struct_from(file_constant_exprs).into(),
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

    /// Both add-side augmentations compose without clobbering each other: `stats_parsed`
    /// and `partitionValues_parsed` both land inside `add`, the original `add`/`remove`
    /// slots remain. This single test replaces the per-helper sanity checks because the
    /// `None` branches are trivial `if` guards on the helper's input.
    #[test]
    fn with_helpers_compose_into_one_pair() {
        let stats = arc_schema([StructField::not_null("numRecords", DataType::LONG)]);
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let pair = with_partition_values(
            with_stats(SCAN_BASE.clone(), Some(stats), /* native= */ false),
            Some(parts),
        );
        assert_eq!(field_names(&pair.0), [ADD_NAME, REMOVE_NAME]);
        let DataType::Struct(add_struct) = pair.0.field(ADD_NAME).unwrap().data_type() else {
            panic!("add slot must be a struct");
        };
        assert!(add_struct.contains("stats_parsed"));
        assert!(add_struct.contains("partitionValues_parsed"));
        // Non-native stats must rebuild the add expression (parse_json wrap); only assert
        // it's no longer a bare column.
        assert!(!matches!(pair.1[0].as_ref(), Expression::Column(_)));
    }

    /// `scan_file_row_pair(None)` is the four-column flat row; `Some(parts)` only changes
    /// the *inner* `fileConstantValues` struct, never the top-level shape.
    #[test]
    fn scan_file_row_pair_shape_is_stable_across_parts() {
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
    }
}
