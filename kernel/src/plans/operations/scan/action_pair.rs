//! Shared base schemas and the scan-side terminal projection.
//!
//! [`SCAN_BASE`] and [`FSR_BASE`] pin the per-pipeline `Add`/`Remove`/... slot list as a
//! `SchemaRef`, used by reconciliation as the JSON commit-load file schema.
//! [`project_scan_file_row`] folds a reconciled [`Cursor`] into the flat post-FSR
//! `scan_file_row` shape that the data stage consumes.

use std::sync::Arc;

use super::dedup::FSR_JOIN_KEY_COL;
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::expressions::{col, Expression};
use crate::plans::errors::DeltaError;
use crate::plans::operations::framework::plan_context::Cursor;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::schema::{
    arc_schema, ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema,
};

// === Base schemas =========================================================================

/// Scan pipeline base: `{add, remove}` only. The scan path never reconciles
/// protocol/metaData/txn/domainMetadata (the snapshot reads those separately).
pub(super) static SCAN_BASE: std::sync::LazyLock<SchemaRef> = std::sync::LazyLock::new(|| {
    arc_schema([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ])
});

/// FSR pipeline base: all six action slots.
pub(super) static FSR_BASE: std::sync::LazyLock<SchemaRef> = std::sync::LazyLock::new(|| {
    arc_schema([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ])
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

// === Scan terminal: reconciled cursor -> flat scan_file_row ===============================

/// Project the reconciled action stream into the flat `scan_file_row` shape consumed by
/// the SSA scan data stage:
///
/// ```text
/// {
///   path: STRING NOT NULL,
///   size: LONG NOT NULL,
///   deletionVector: DV?,
///   fileConstantValues: STRUCT<
///     baseRowId: LONG?,
///     defaultRowCommitVersion: LONG?,
///     tags: MAP<STRING,STRING>?,
///     clusteringProvider: STRING?,
///     partitionValues_parsed?: STRUCT<...>,  // present iff `partitions` is Some
///   >?,
/// }
/// ```
///
/// **Invariant:** when `partitions` is `Some(parts)`, the upstream reconciliation pipeline
/// must have already replaced `add.partitionValues` with `add.partitionValues_parsed`
/// (see `ssa_reconciliation::ReconciliationCursor::with_partitions_parsed`). The terminal
/// reads `col(["add", "partitionValues_parsed"])` directly -- no per-row
/// `map_to_struct(add.partitionValues)` here. The raw Map form has no downstream consumer
/// in the SSA path, so it is omitted from `fileConstantValues` entirely (parsing happens
/// once upstream, not per file row in the data phase).
pub(super) fn project_scan_file_row(
    cursor: Cursor,
    partitions: Option<&SchemaRef>,
) -> Result<Cursor, DeltaError> {
    use crate::actions::deletion_vector::DeletionVectorDescriptor;

    let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    let mut file_constant_exprs: Vec<Arc<Expression>> = vec![
        col([ADD_NAME, "baseRowId"]).into(),
        col([ADD_NAME, "defaultRowCommitVersion"]).into(),
        col([ADD_NAME, "tags"]).into(),
        col([ADD_NAME, "clusteringProvider"]).into(),
    ];
    if let Some(parts) = partitions {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            parts.as_ref().clone(),
        ));
        file_constant_exprs.push(col([ADD_NAME, "partitionValues_parsed"]).into());
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
    let exprs: Vec<Arc<Expression>> = vec![
        col([ADD_NAME, "path"]).into(),
        col([ADD_NAME, "size"]).into(),
        col([ADD_NAME, "deletionVector"]).into(),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ];
    cursor.project_with_schema(exprs, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::operations::framework::plan_context::Context;

    fn names(fields: impl Iterator<Item = &'static str>) -> Vec<String> {
        fields.map(String::from).collect()
    }

    /// Pin canonical top-level action lists -- load-bearing for every downstream stage.
    #[test]
    fn bases_pin_canonical_action_slots() {
        let scan: Vec<_> = SCAN_BASE.fields().map(|f| f.name().clone()).collect();
        let fsr: Vec<_> = FSR_BASE.fields().map(|f| f.name().clone()).collect();
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

    /// Build a synthetic input cursor whose schema mimics the reconciled action stream
    /// (post-`with_partitions_parsed`): `add` carries `path`/`size`/`deletionVector`/
    /// `baseRowId`/etc., plus `partitionValues_parsed` when partitions are present. The
    /// terminal projection reads only those field names regardless of how the rest of the
    /// upstream looks.
    fn make_input_cursor(parts: Option<&SchemaRef>) -> (Context, Cursor) {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;
        let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
        let mut add_fields = vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
            StructField::nullable("baseRowId", DataType::LONG),
            StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            StructField::nullable("tags", tags),
            StructField::nullable("clusteringProvider", DataType::STRING),
        ];
        if let Some(p) = parts {
            add_fields.push(StructField::nullable(
                "partitionValues_parsed",
                p.as_ref().clone(),
            ));
        }
        let schema = arc_schema([
            StructField::nullable(ADD_NAME, StructType::new_unchecked(add_fields)),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        ]);
        let ctx = Context::new();
        let cursor = ctx
            .values(schema, Vec::<Vec<crate::expressions::Scalar>>::new())
            .unwrap();
        (ctx, cursor)
    }

    /// Without partitions: the terminal emits the four-field `fileConstantValues` and
    /// drops the `partitionValues` Map slot. The previous Pair-based helper retained a
    /// Map-typed slot here.
    #[test]
    fn project_scan_file_row_drops_partition_values_map_when_no_parts() {
        let (_ctx, cursor) = make_input_cursor(None);
        let out = project_scan_file_row(cursor, None).unwrap();
        let schema = out.schema().unwrap();
        let fields: Vec<_> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(
            fields,
            ["path", "size", "deletionVector", FILE_CONSTANT_VALUES_NAME]
        );
        let DataType::Struct(fcv) = schema.field(FILE_CONSTANT_VALUES_NAME).unwrap().data_type()
        else {
            panic!("fileConstantValues must be a struct");
        };
        let fcv_fields: Vec<_> = fcv.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(
            fcv_fields,
            [
                "baseRowId",
                "defaultRowCommitVersion",
                "tags",
                "clusteringProvider",
            ],
            "partitionValues Map slot must be dropped when no partitions",
        );
    }

    /// With partitions: `fileConstantValues.partitionValues_parsed` is appended as a
    /// passthrough column ref (not `map_to_struct`); the Map slot remains absent.
    #[test]
    fn project_scan_file_row_appends_partitions_parsed_when_some() {
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let (_ctx, cursor) = make_input_cursor(Some(&parts));
        let out = project_scan_file_row(cursor, Some(&parts)).unwrap();
        let schema = out.schema().unwrap();
        let DataType::Struct(fcv) = schema.field(FILE_CONSTANT_VALUES_NAME).unwrap().data_type()
        else {
            panic!("fileConstantValues must be a struct");
        };
        let fcv_fields: Vec<_> = fcv.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(
            fcv_fields,
            [
                "baseRowId",
                "defaultRowCommitVersion",
                "tags",
                "clusteringProvider",
                "partitionValues_parsed",
            ],
            "partitions present => partitionValues_parsed appended; Map slot stays absent",
        );
    }
}
