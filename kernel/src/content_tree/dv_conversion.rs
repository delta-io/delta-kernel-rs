use std::sync::{Arc, LazyLock};

use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::content_tree::DeletionVectorInfo;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ArrayData, Scalar};
use crate::schema::{
    column_name, ArrayType, ColumnName, DataType, SchemaRef, StructField, StructType,
};
use crate::{DeltaResult, EngineData, Error};

/// Extracts deletion vector content from a DeletionVectorDescriptor.
///
/// This function decodes the `path_or_inline_dv` field based on the storage type:
///
/// - `PersistedRelative`: The format is `<random prefix - optional><base85 encoded uuid>`. The UUID
///   is 20 characters (base85 encoded), and any characters before that are the optional random
///   prefix. Decodes to the table-root-relative path `<prefix>/deletion_vector_<uuid>.bin`.
///
/// - `PersistedAbsolute`: The `path_or_inline_dv` contains the absolute path to the DV file.
///
/// - `Inline`: Currently not supported - returns an error. Inline DVs would need to be persisted
///   first before being added to metadata.
pub(crate) fn extract_deletion_vector_content(
    dv: &DeletionVectorDescriptor,
) -> DeltaResult<DeletionVectorInfo> {
    let location = match dv.storage_type {
        DeletionVectorStorageType::PersistedAbsolute => {
            // Use absolute path as-is
            dv.path_or_inline_dv.clone()
        }
        DeletionVectorStorageType::PersistedRelative => {
            // Decode to relative path
            dv.relative_path()?
        }
        DeletionVectorStorageType::Inline => {
            return Err(Error::DeletionVector(
                "Inline deletion vectors are not supported. They must be persisted first."
                    .to_string(),
            ));
        }
    };
    // Add 8 bytes to convert from Delta's size to Iceberg's size (full blob): Delta's
    // `sizeInBytes` counts the 4-byte magic + bitmap; Iceberg's full-blob size adds the 4-byte
    // length prefix and the 4-byte trailing CRC.
    Ok(DeletionVectorInfo {
        location,
        // Absent offset defaults to 1: a persisted DV file opens with a 1-byte version header, so
        // the first blob starts at byte 1. Matches `DeletionVectorDescriptor::read`.
        offset: dv.offset.map_or(1, i64::from),
        size_in_bytes: dv.size_in_bytes as i64 + 8,
        cardinality: dv.cardinality,
    })
}

/// Intermediate flat decoded-DV columns: path resolved (base85-decoded for relative DVs, verbatim
/// for absolute), sizes widened to LONG, `+8` bytes for Iceberg framing.
const DV_LOCATION: &str = "_dv_location";
const DV_OFFSET: &str = "_dv_offset";
const DV_SIZE_IN_BYTES: &str = "_dv_size_in_bytes";
const DV_CARDINALITY: &str = "_dv_cardinality";

/// Schema of [`DV_LOCATION`] etc., for [`EngineData::append_columns`].
static DV_DECODED_FLAT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::nullable(DV_LOCATION, DataType::STRING),
        StructField::nullable(DV_OFFSET, DataType::LONG),
        StructField::nullable(DV_SIZE_IN_BYTES, DataType::LONG),
        StructField::nullable(DV_CARDINALITY, DataType::LONG),
    ]))
});

/// Types of the DV descriptor leaves [`DecodedDvVisitor`] decodes, in getter order. Shared across
/// all source layouts: only the *names* (the projection path to the DV struct) vary per layout, so
/// they live on the visitor as [`DecodedDvVisitor::names`] rather than here.
static DV_LEAF_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| {
    vec![
        DataType::STRING,
        DataType::STRING,
        DataType::INTEGER,
        DataType::INTEGER,
        DataType::LONG,
    ]
});

/// Projection paths locating the DV descriptor leaves in scan-transform rows, where the DV struct
/// is projected at `deletionVector`. Pass to [`DecodedDvVisitor::new`].
static SCAN_ROW_DV_COLUMNS: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
    vec![
        column_name!("deletionVector.storageType"),
        column_name!("deletionVector.pathOrInlineDv"),
        column_name!("deletionVector.offset"),
        column_name!("deletionVector.sizeInBytes"),
        column_name!("deletionVector.cardinality"),
    ]
});

/// Projection paths locating the DV descriptor leaves in raw log batches, where the DV struct is
/// nested under `add.deletionVector`. Pass to [`DecodedDvVisitor::new`].
static ADD_DV_COLUMNS: LazyLock<Vec<ColumnName>> = LazyLock::new(|| {
    vec![
        column_name!("add.deletionVector.storageType"),
        column_name!("add.deletionVector.pathOrInlineDv"),
        column_name!("add.deletionVector.offset"),
        column_name!("add.deletionVector.sizeInBytes"),
        column_name!("add.deletionVector.cardinality"),
    ]
});

/// Visits rows in one pass, accumulating decoded DV columns.
///
/// For rows with a DV: decodes path (base85 UUID -> relative path), widens offset/sizeInBytes
/// to LONG, adds 8 to sizeInBytes (Delta -> Iceberg framing), stores cardinality. Rows without a
/// DV push a null in every column.
///
/// Columns accumulate directly in their final [`Scalar`] form so they can be moved into
/// [`ArrayData`] without a second pass to translate them.
///
/// One batch per visitor: the accumulated columns are appended to a single [`EngineData`] via
/// [`Self::append_decoded_dv_columns`], so visit exactly the batch you pass there. Reusing one
/// visitor across batches would append the concatenated columns onto only the last batch, with
/// mismatched row counts.
///
/// The visitor is layout-agnostic: it reads getters positionally and shares the leaf *types* (via
/// [`DV_LEAF_TYPES`]). The projection locating the DV struct in the source layout is injected at
/// construction as [`Self::names`] (e.g. [`SCAN_ROW_DV_COLUMNS`] or [`ADD_DV_COLUMNS`]), so the
/// visitor is self-describing and drives [`RowVisitor::visit_rows_of`] directly.
struct DecodedDvVisitor {
    /// Projection paths to the DV descriptor leaves, in getter order, matching [`DV_LEAF_TYPES`].
    names: &'static [ColumnName],
    decoded_paths: Vec<Scalar>,
    decoded_offsets: Vec<Scalar>,
    decoded_sizes: Vec<Scalar>,
    decoded_cardinalities: Vec<Scalar>,
}

impl DecodedDvVisitor {
    /// Builds a visitor reading the DV descriptor leaves at `names` (see [`SCAN_ROW_DV_COLUMNS`] /
    /// [`ADD_DV_COLUMNS`]), pre-sized for `n` rows.
    fn new(names: &'static [ColumnName], n: usize) -> Self {
        Self {
            names,
            decoded_paths: Vec::with_capacity(n),
            decoded_offsets: Vec::with_capacity(n),
            decoded_sizes: Vec::with_capacity(n),
            decoded_cardinalities: Vec::with_capacity(n),
        }
    }

    /// Appends one row to every column, so the columns can only advance in lockstep. Both arms
    /// build the same tuple shape, which is what ties a new column to a compile error in each arm
    /// rather than to a silently short column.
    fn push_row(&mut self, decoded: Option<DeletionVectorInfo>) {
        let (location, offset, size_in_bytes, cardinality) = match decoded {
            Some(dv) => (
                Scalar::String(dv.location),
                Scalar::Long(dv.offset),
                Scalar::Long(dv.size_in_bytes),
                Scalar::Long(dv.cardinality),
            ),
            None => (
                Scalar::Null(DataType::STRING),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
            ),
        };
        self.decoded_paths.push(location);
        self.decoded_offsets.push(offset);
        self.decoded_sizes.push(size_in_bytes);
        self.decoded_cardinalities.push(cardinality);
    }

    fn has_any_dv(&self) -> bool {
        self.decoded_paths.iter().any(|s| !s.is_null())
    }

    fn append_decoded_dv_columns(self, data: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        data.append_columns(
            DV_DECODED_FLAT_SCHEMA.clone(),
            vec![
                ArrayData::try_new(ArrayType::new(DataType::STRING, true), self.decoded_paths)?,
                ArrayData::try_new(ArrayType::new(DataType::LONG, true), self.decoded_offsets)?,
                ArrayData::try_new(ArrayType::new(DataType::LONG, true), self.decoded_sizes)?,
                ArrayData::try_new(
                    ArrayType::new(DataType::LONG, true),
                    self.decoded_cardinalities,
                )?,
            ],
        )
    }
}

impl RowVisitor for DecodedDvVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        (self.names, &DV_LEAF_TYPES)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // `storageType` is a required (non-null) field of the DV descriptor, so it is null
            // for a row iff the whole `deletionVector` struct is null (the visitor unions parent
            // null masks into leaves). We use it as the presence check, then `get` the other
            // required fields infallibly.
            // Labels are the bare leaf field names so error messages are correct for both the
            // scan-row (`deletionVector.*`) and log-batch (`add.deletionVector.*`) shapes.
            let storage_type_opt: Option<String> = getters[0].get_opt(i, "storageType")?;
            let decoded = match storage_type_opt {
                Some(storage_type_str) => {
                    let dv = DeletionVectorDescriptor {
                        storage_type: storage_type_str.parse()?,
                        path_or_inline_dv: getters[1].get(i, "pathOrInlineDv")?,
                        offset: getters[2].get_opt(i, "offset")?,
                        size_in_bytes: getters[3].get(i, "sizeInBytes")?,
                        cardinality: getters[4].get(i, "cardinality")?,
                    };
                    Some(extract_deletion_vector_content(&dv)?)
                }
                None => None,
            };
            self.push_row(decoded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;
    use crate::schema::{ColumnNamesAndTypes, ToSchema};
    use crate::Engine;

    /// Decoded DV columns read back from an augmented batch, one entry per row.
    #[derive(Default)]
    struct DecodedColumnsVisitor {
        locations: Vec<Option<String>>,
        offsets: Vec<Option<i64>>,
        sizes: Vec<Option<i64>>,
        cardinalities: Vec<Option<i64>>,
    }

    impl RowVisitor for DecodedColumnsVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                let names = vec![
                    column_name!(DV_LOCATION),
                    column_name!(DV_OFFSET),
                    column_name!(DV_SIZE_IN_BYTES),
                    column_name!(DV_CARDINALITY),
                ];
                let types = vec![
                    DataType::STRING,
                    DataType::LONG,
                    DataType::LONG,
                    DataType::LONG,
                ];
                (names, types).into()
            });
            COLUMNS.as_ref()
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                self.locations.push(getters[0].get_opt(i, DV_LOCATION)?);
                self.offsets.push(getters[1].get_opt(i, DV_OFFSET)?);
                self.sizes.push(getters[2].get_opt(i, DV_SIZE_IN_BYTES)?);
                self.cardinalities
                    .push(getters[3].get_opt(i, DV_CARDINALITY)?);
            }
            Ok(())
        }
    }

    /// A `PersistedRelative` DV whose z85 UUID decodes to
    /// `ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin`.
    fn sample_dv() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    /// The absolute path an [`absolute_dv`] descriptor must pass through verbatim.
    const ABSOLUTE_DV_PATH: &str =
        "s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";

    /// A `PersistedAbsolute` DV, whose path is used as-is rather than z85-decoded.
    fn absolute_dv() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv: ABSOLUTE_DV_PATH.to_string(),
            offset: Some(9),
            size_in_bytes: 12,
            cardinality: 3,
        }
    }

    /// Builds the DV descriptor struct scalar, with leaves ordered to match
    /// [`DeletionVectorDescriptor::to_schema`].
    fn dv_scalar(dv: &DeletionVectorDescriptor) -> Scalar {
        let offset = dv
            .offset
            .map(Scalar::Integer)
            .unwrap_or(Scalar::Null(DataType::INTEGER));
        Scalar::Struct(
            StructData::try_new(
                DeletionVectorDescriptor::to_schema()
                    .fields()
                    .cloned()
                    .collect(),
                vec![
                    Scalar::String(dv.storage_type.to_string()),
                    Scalar::String(dv.path_or_inline_dv.clone()),
                    offset,
                    Scalar::Integer(dv.size_in_bytes),
                    Scalar::Long(dv.cardinality),
                ],
            )
            .unwrap(),
        )
    }

    /// Column shape the [`DecodedDvVisitor`] reads from: scan-transform rows expose the DV struct
    /// at `deletionVector`, raw log batches nest it under `add.deletionVector`.
    #[derive(Clone, Copy)]
    enum DvColumnShape {
        ScanRow,
        LogBatch,
    }

    impl DvColumnShape {
        /// Schema projecting `dv_schema` at this shape's location.
        fn schema(self, dv_schema: StructType) -> SchemaRef {
            let dv_field = StructField::nullable("deletionVector", dv_schema);
            let root = match self {
                DvColumnShape::ScanRow => StructType::new_unchecked([dv_field]),
                DvColumnShape::LogBatch => StructType::new_unchecked([StructField::nullable(
                    "add",
                    StructType::new_unchecked([dv_field]),
                )]),
            };
            Arc::new(root)
        }

        /// Wraps a DV struct scalar (or a null placeholder) at this shape's root field, matching
        /// [`Self::schema`] for the same `dv_schema`.
        fn root_scalar(self, dv_schema: StructType, dv: Scalar) -> Scalar {
            match self {
                DvColumnShape::ScanRow => dv,
                DvColumnShape::LogBatch => Scalar::Struct(
                    StructData::try_new(
                        vec![StructField::nullable("deletionVector", dv_schema)],
                        vec![dv],
                    )
                    .unwrap(),
                ),
            }
        }

        /// Builds a single-row `EngineData` holding `dv` under this shape's `deletionVector` field,
        /// typed as `dv_schema`.
        fn engine_data_for(self, dv_schema: StructType, dv: Scalar) -> Box<dyn EngineData> {
            let row = vec![self.root_scalar(dv_schema.clone(), dv)];
            SyncEngine::new()
                .evaluation_handler()
                .create_many(self.schema(dv_schema), &[row.as_slice()])
                .unwrap()
        }

        /// Builds an `EngineData` with one row per entry in `dvs`; `None` yields a row whose DV
        /// field is null, `Some(dv)` embeds the given descriptor.
        fn engine_data(self, dvs: &[Option<DeletionVectorDescriptor>]) -> Box<dyn EngineData> {
            let dv_schema = DeletionVectorDescriptor::to_schema();
            let dv_type = DataType::from(dv_schema.clone());
            let rows: Vec<Vec<Scalar>> = dvs
                .iter()
                .map(|dv| {
                    let scalar = match dv {
                        Some(dv) => dv_scalar(dv),
                        None => Scalar::Null(dv_type.clone()),
                    };
                    vec![self.root_scalar(dv_schema.clone(), scalar)]
                })
                .collect();
            let row_refs: Vec<&[Scalar]> = rows.iter().map(Vec::as_slice).collect();
            SyncEngine::new()
                .evaluation_handler()
                .create_many(self.schema(dv_schema), &row_refs)
                .unwrap()
        }

        /// Projection paths locating the DV struct leaves for this shape, matching
        /// [`Self::schema`].
        fn columns(self) -> &'static [ColumnName] {
            match self {
                DvColumnShape::ScanRow => &SCAN_ROW_DV_COLUMNS,
                DvColumnShape::LogBatch => &ADD_DV_COLUMNS,
            }
        }
    }

    /// Decodes `dvs` through the shape's visitor, appends the decoded columns, and reads them
    /// back with [`DecodedColumnsVisitor`]. A row's decoded columns are all `None` exactly when
    /// its source `deletionVector` struct was null, so the visited columns carry the actual
    /// per-row nullability directly -- no separate presence flag is needed.
    fn decode(
        shape: DvColumnShape,
        dvs: &[Option<DeletionVectorDescriptor>],
    ) -> Result<DecodedColumnsVisitor, Box<dyn std::error::Error>> {
        let data = shape.engine_data(dvs);
        let mut decoder = DecodedDvVisitor::new(shape.columns(), dvs.len());
        decoder.visit_rows_of(data.as_ref())?;

        let augmented = decoder.append_decoded_dv_columns(data.as_ref())?;
        let mut columns = DecodedColumnsVisitor::default();
        columns.visit_rows_of(augmented.as_ref())?;
        Ok(columns)
    }

    #[rstest]
    fn test_visitor_decodes_dv_row(
        #[values(DvColumnShape::ScanRow, DvColumnShape::LogBatch)] shape: DvColumnShape,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let columns = decode(shape, &[Some(sample_dv())])?;
        assert_eq!(
            columns.locations,
            vec![Some(
                "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string()
            )]
        );
        assert_eq!(columns.offsets, vec![Some(4)]);
        assert_eq!(columns.sizes, vec![Some(48)]); // 40 + 8 for Iceberg framing
        assert_eq!(columns.cardinalities, vec![Some(6)]);
        Ok(())
    }

    #[rstest]
    fn test_visitor_row_without_dv_is_null(
        #[values(DvColumnShape::ScanRow, DvColumnShape::LogBatch)] shape: DvColumnShape,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // A null source struct decodes to nulls across all four columns.
        let columns = decode(shape, &[None])?;
        assert_eq!(columns.locations, vec![None]);
        assert_eq!(columns.offsets, vec![None]);
        assert_eq!(columns.sizes, vec![None]);
        assert_eq!(columns.cardinalities, vec![None]);
        Ok(())
    }

    #[rstest]
    fn test_visitor_mixed_rows(
        #[values(DvColumnShape::ScanRow, DvColumnShape::LogBatch)] shape: DvColumnShape,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // One batch mixing all three row kinds: a relative DV (z85-decoded), an absolute DV (path
        // verbatim), and a row with no DV at all.
        let columns = decode(shape, &[Some(sample_dv()), Some(absolute_dv()), None])?;
        assert_eq!(
            columns.locations,
            vec![
                Some("ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string()),
                Some(ABSOLUTE_DV_PATH.to_string()),
                None,
            ]
        );
        assert_eq!(columns.offsets, vec![Some(4), Some(9), None]);
        assert_eq!(columns.sizes, vec![Some(48), Some(20), None]);
        assert_eq!(columns.cardinalities, vec![Some(6), Some(3), None]);
        Ok(())
    }

    #[rstest]
    // Empty and all-null batches carry no DV; any non-null row flips `has_any_dv`.
    #[case(&[], false)]
    #[case(&[None], false)]
    #[case(&[Some(()), None], true)]
    #[case(&[Some(())], true)]
    fn test_has_any_dv(
        #[case] presence: &[Option<()>],
        #[case] expected: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dvs: Vec<Option<DeletionVectorDescriptor>> =
            presence.iter().map(|p| p.map(|()| sample_dv())).collect();
        let data = DvColumnShape::ScanRow.engine_data(&dvs);
        let mut decoder = DecodedDvVisitor::new(DvColumnShape::ScanRow.columns(), dvs.len());
        decoder.visit_rows_of(data.as_ref())?;
        assert_eq!(decoder.has_any_dv(), expected);
        Ok(())
    }

    /// A row whose `storageType` is set but whose required `pathOrInlineDv` leaf is null must
    /// error, not be silently treated as a DV-less row. `ensure_data_types` deliberately skips
    /// nullability, so a malformed log can present this even though the derived schema marks the
    /// leaf non-null -- hence the all-nullable schema variant here.
    #[rstest]
    fn test_visitor_storage_type_present_with_null_required_leaf_errors(
        #[values(DvColumnShape::ScanRow, DvColumnShape::LogBatch)] shape: DvColumnShape,
    ) {
        let nullable_dv_schema = StructType::new_unchecked(
            DeletionVectorDescriptor::to_schema()
                .fields()
                .map(|f| StructField::nullable(f.name(), f.data_type().clone())),
        );
        let partial_dv = Scalar::Struct(
            StructData::try_new(
                nullable_dv_schema.fields().cloned().collect(),
                vec![
                    Scalar::String(DeletionVectorStorageType::PersistedRelative.to_string()),
                    Scalar::Null(DataType::STRING), // pathOrInlineDv missing
                    Scalar::Integer(4),
                    Scalar::Integer(40),
                    Scalar::Long(6),
                ],
            )
            .unwrap(),
        );
        let data = shape.engine_data_for(nullable_dv_schema, partial_dv);

        let mut decoder = DecodedDvVisitor::new(shape.columns(), 1);
        assert_result_error_with_message(decoder.visit_rows_of(data.as_ref()), "pathOrInlineDv");
    }

    #[rstest]
    fn test_visitor_empty_batch_yields_no_rows(
        #[values(DvColumnShape::ScanRow, DvColumnShape::LogBatch)] shape: DvColumnShape,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // A zero-row batch appends four empty decoded columns without error.
        let columns = decode(shape, &[])?;
        assert!(columns.locations.is_empty());
        assert!(columns.offsets.is_empty());
        assert!(columns.sizes.is_empty());
        assert!(columns.cardinalities.is_empty());
        Ok(())
    }

    /// Builds a descriptor from `(storage_type, path_or_inline_dv, offset, size_in_bytes)`,
    /// with `cardinality` fixed at 6.
    fn dv(input: (DeletionVectorStorageType, &str, Option<i32>, i32)) -> DeletionVectorDescriptor {
        let (storage_type, path_or_inline_dv, offset, size_in_bytes) = input;
        DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv: path_or_inline_dv.to_string(),
            offset,
            size_in_bytes,
            cardinality: 6,
        }
    }

    // `size_in_bytes` gains +8 (4-byte size prefix + 4-byte CRC) to convert Delta's bitmap-only
    // size to Iceberg's full-blob size. `expected` is (location, offset, size_in_bytes).
    #[rstest]
    // Persisted-relative with a "ab" prefix; z85 UUID decodes to d2c639aa-...-d3fe2512ff61.
    #[case::relative_with_prefix(
        (DeletionVectorStorageType::PersistedRelative, "ab^-aqEH.-t@S}K{vb[*k^", Some(4), 40),
        ("ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", 4, 48)
    )]
    // Persisted-relative with no prefix (bare 20-char z85 UUID).
    #[case::relative_no_prefix(
        (DeletionVectorStorageType::PersistedRelative, "vBn[lx{q8@P<9BNH/isA", Some(1), 36),
        ("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin", 1, 44)
    )]
    // Persisted-absolute preserves the path verbatim.
    #[case::absolute(
        (
            DeletionVectorStorageType::PersistedAbsolute,
            "s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin",
            Some(4),
            40,
        ),
        ("s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", 4, 48)
    )]
    // Absent offset defaults to 1 (the byte after the DV file's version header).
    #[case::relative_no_offset_defaults_to_one(
        (DeletionVectorStorageType::PersistedRelative, "vBn[lx{q8@P<9BNH/isA", None, 36),
        ("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin", 1, 44)
    )]
    fn test_extract_deletion_vector_content(
        #[case] input: (DeletionVectorStorageType, &str, Option<i32>, i32),
        #[case] expected: (&str, i64, i64),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (expected_location, expected_offset, expected_size_in_bytes) = expected;
        let deletion_vector = extract_deletion_vector_content(&dv(input))?;

        assert_eq!(deletion_vector.location, expected_location);
        assert_eq!(deletion_vector.offset, expected_offset);
        assert_eq!(deletion_vector.size_in_bytes, expected_size_in_bytes);
        assert_eq!(deletion_vector.cardinality, 6);
        Ok(())
    }

    #[rstest]
    // Inline DVs must be persisted before conversion.
    #[case::inline_not_supported(
        (
            DeletionVectorStorageType::Inline,
            "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
            None,
            44,
        ),
        "Inline deletion vectors are not supported"
    )]
    // Persisted-relative path shorter than the 20-char z85 UUID suffix.
    #[case::invalid_relative_path(
        (DeletionVectorStorageType::PersistedRelative, "short", Some(1), 36),
        "Invalid length"
    )]
    // Non-ASCII byte straddling the trailing-20-byte window must error, not panic (byte-slicing).
    #[case::non_ascii_relative_path(
        (DeletionVectorStorageType::PersistedRelative, "éaaaaaaaaaaaaaaaaaaa", Some(1), 36),
        "Failed to decode DV uuid"
    )]
    fn test_extract_deletion_vector_content_error(
        #[case] input: (DeletionVectorStorageType, &str, Option<i32>, i32),
        #[case] expected_error: &str,
    ) {
        let err = extract_deletion_vector_content(&dv(input))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(expected_error),
            "error {err:?} did not contain {expected_error:?}"
        );
    }
}
