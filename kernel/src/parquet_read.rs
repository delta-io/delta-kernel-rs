//! Options for exact, single-file Parquet reads.

use std::ops::Range;
use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::schema::{DataType, SchemaRef, StructType};
use crate::transforms::{SchemaTransform, transform_output_type};
use crate::{CancellationTokenRef, KernelError, KernelResult, PredicateRef};

/// Eligible rows in original, zero-based physical file coordinates.
///
/// Engines inspect a selection through [`Self::ranges`], using the file footer's row count.
/// Empty ranges or a keep mask with no set bits keep no rows. An empty exclusion set keeps all
/// rows, as does omitting a selection from [`ParquetReadOptions`]. Result offsets count matching
/// rows and do not change these coordinates.
#[derive(Debug, Clone)]
pub struct FileRowSelection(Selection);

#[derive(Debug, Clone)]
enum Selection {
    Ranges(Arc<[Range<u64>]>),
    KeepMask { bytes: Arc<[u8]>, row_count: u64 },
    Exclusions(Arc<RoaringTreemap>),
}

impl FileRowSelection {
    /// Selects ordered, disjoint half-open ranges. Rejects reversed or overlapping ranges.
    pub fn from_ranges(ranges: impl IntoIterator<Item = Range<u64>>) -> KernelResult<Self> {
        let ranges: Vec<_> = ranges.into_iter().collect();
        let mut end = 0;
        for range in &ranges {
            if range.start < end || range.start > range.end {
                return Err(KernelError::generic(
                    "Row ranges must be ordered and disjoint",
                ));
            }
            end = range.end;
        }
        Ok(Self(Selection::Ranges(ranges.into())))
    }

    /// Selects set bits in a packed mask, with the least significant bit first in each byte.
    ///
    /// The mask must cover exactly `row_count` rows; unused bits in the last byte must be zero.
    /// The declared count must equal the file's row count when read.
    pub fn from_keep_mask(bytes: Vec<u8>, row_count: u64) -> KernelResult<Self> {
        if bytes.len() as u64 != row_count.div_ceil(8)
            || (!row_count.is_multiple_of(8)
                && bytes
                    .last()
                    .is_some_and(|last| last >> (row_count % 8) != 0))
        {
            return Err(KernelError::generic(
                "Keep mask length or padding is invalid",
            ));
        }
        Ok(Self(Selection::KeepMask {
            bytes: bytes.into(),
            row_count,
        }))
    }

    /// Excludes the given physical row positions, retaining compressed deletion-vector storage.
    /// Rows after the last exclusion remain eligible.
    pub fn from_exclusions(exclusions: RoaringTreemap) -> Self {
        Self(Selection::Exclusions(Arc::new(exclusions)))
    }

    /// Validates this selection against the file's row count without expanding it.
    /// Rejects ranges or exclusions beyond `row_count`, and masks with a different row count.
    pub fn validate(&self, row_count: u64) -> KernelResult<()> {
        match &self.0 {
            Selection::Ranges(ranges)
                if ranges.last().is_some_and(|range| range.end > row_count) =>
            {
                Err(KernelError::generic(
                    "Row selection exceeds the file row count",
                ))
            }
            Selection::KeepMask {
                row_count: mask_rows,
                ..
            } if *mask_rows != row_count => Err(KernelError::generic(
                "Keep mask must cover the complete file",
            )),
            Selection::Exclusions(exclusions)
                if exclusions.max().is_some_and(|last| last >= row_count) =>
            {
                Err(KernelError::generic(
                    "Excluded row exceeds the file row count",
                ))
            }
            _ => Ok(()),
        }
    }

    /// Iterates eligible ranges without expanding them to row indexes.
    /// Checks file bounds with [`Self::validate`] before constructing the iterator.
    pub fn ranges(
        &self,
        row_count: u64,
    ) -> KernelResult<Box<dyn Iterator<Item = Range<u64>> + Send + '_>> {
        self.validate(row_count)?;
        match &self.0 {
            Selection::Ranges(ranges) => Ok(Box::new(
                ranges.iter().filter(|range| !range.is_empty()).cloned(),
            )),
            Selection::KeepMask { bytes, .. } => {
                let mut position = 0;
                Ok(Box::new(std::iter::from_fn(move || {
                    let keep = |index: u64| bytes[(index / 8) as usize] & (1 << (index % 8)) != 0;
                    while position < row_count && !keep(position) {
                        position += 1;
                    }
                    let start = position;
                    while position < row_count && keep(position) {
                        position += 1;
                    }
                    (start < position).then_some(start..position)
                })))
            }
            Selection::Exclusions(exclusions) => {
                let mut excluded = exclusions.iter();
                let mut position = 0;
                Ok(Box::new(std::iter::from_fn(move || {
                    loop {
                        let end = excluded.next().unwrap_or(row_count);
                        let start = position;
                        position = end.saturating_add(1);
                        if start < end {
                            return Some(start..end);
                        }
                        if end == row_count {
                            return None;
                        }
                    }
                })))
            }
        }
    }

    /// Borrows packed keep-mask bytes and their declared row count, when available.
    /// Bits use the layout described by [`Self::from_keep_mask`]. Use [`Self::validate`] to
    /// check file bounds and [`Self::ranges`] for representation-independent access.
    pub fn as_keep_mask(&self) -> Option<(&[u8], u64)> {
        match &self.0 {
            Selection::KeepMask { bytes, row_count } => Some((bytes, *row_count)),
            _ => None,
        }
    }
}

/// Configures an exact read through [`crate::ParquetHandler::read_parquet_file_with_options`].
///
/// Physical selection precedes exact filtering, then result offset and limit. Output columns are
/// materialized for surviving rows. Defaults select all rows and all columns in the read schema.
/// Empty nested structs in output or predicate values are unsupported; a zero-column output
/// schema remains valid.
#[derive(Default, Clone)]
pub struct ParquetReadOptions {
    output_schema: Option<SchemaRef>,
    row_filter: Option<PredicateRef>,
    row_selection: Option<FileRowSelection>,
    result_offset: u64,
    result_limit: Option<u64>,
    cancellation_token: Option<CancellationTokenRef>,
}

impl ParquetReadOptions {
    /// Returns only this projection of the read schema, in its specified order.
    /// Retained fields must preserve nullability and metadata. Struct fields may be recursively
    /// projected; other types must match apart from nested field order.
    /// An empty top-level schema returns batches with no columns and the correct row count.
    /// Empty nested structs are unsupported.
    ///
    /// Clone parent fields when projecting nested children to preserve their metadata and
    /// nullability:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use delta_kernel::ParquetReadOptions;
    /// use delta_kernel::schema::{DataType, MetadataValue, StructField, StructType, schema_ref};
    ///
    /// let children = schema_ref! { nullable "payload": STRING, nullable "unused": INTEGER };
    /// let parent = StructField::not_null("s", children)
    ///     .with_metadata([("annotation", MetadataValue::String("preserve".into()))]);
    /// let read_schema = Arc::new(StructType::try_new([parent.clone()])?);
    /// let DataType::Struct(children) = parent.data_type() else { unreachable!() };
    /// let projected_parent = StructField {
    ///     data_type: children.project_as_struct(&["payload"])?.into(),
    ///     ..parent.clone()
    /// };
    /// let output = Arc::new(StructType::try_new([projected_parent])?);
    /// let options = ParquetReadOptions::default().with_output_schema(output);
    /// options.validate(&read_schema)?;
    /// let projected_parent = options.output_schema().unwrap().field("s").unwrap();
    /// assert_eq!(projected_parent.metadata(), parent.metadata());
    /// assert!(!projected_parent.is_nullable());
    /// # Ok::<(), delta_kernel::KernelError>(())
    /// ```
    pub fn with_output_schema(mut self, schema: SchemaRef) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Keeps only rows for which the predicate evaluates to true; false and NULL exclude a row.
    /// References and literal types must agree with the physical read schema. FILE leaf predicates
    /// validate their parent locator; the built-in Arrow engines read the complete FILE value
    /// for this validation.
    pub fn with_row_filter(mut self, predicate: PredicateRef) -> Self {
        self.row_filter = Some(predicate);
        self
    }

    /// Restricts eligible rows using original physical file positions.
    pub fn with_row_selection(mut self, selection: FileRowSelection) -> Self {
        self.row_selection = Some(selection);
        self
    }

    /// Skips this many rows after physical selection and exact filtering.
    pub fn with_result_offset(mut self, offset: u64) -> Self {
        self.result_offset = offset;
        self
    }

    /// Returns at most this many rows after selection, filtering, and result offset.
    pub fn with_result_limit(mut self, limit: u64) -> Self {
        self.result_limit = Some(limit);
        self
    }

    /// Applies the [engine cancellation contract](crate::cancellation#engine-operation-contract).
    pub fn with_cancellation_token(mut self, token: CancellationTokenRef) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Returns the output projection, or `None` to return the complete read schema.
    pub fn output_schema(&self) -> Option<&SchemaRef> {
        self.output_schema.as_ref()
    }

    /// Returns the exact predicate, or `None` to retain every eligible row.
    pub fn row_filter(&self) -> Option<&PredicateRef> {
        self.row_filter.as_ref()
    }

    /// Returns the physical selection, or `None` to select the complete file.
    pub fn row_selection(&self) -> Option<&FileRowSelection> {
        self.row_selection.as_ref()
    }

    /// Returns the number of matching rows to skip.
    pub fn result_offset(&self) -> u64 {
        self.result_offset
    }

    /// Returns the maximum number of matching rows to return after the offset.
    pub fn result_limit(&self) -> Option<u64> {
        self.result_limit
    }

    /// Returns this read's cancellation token, when supplied.
    pub fn cancellation_token(&self) -> Option<&CancellationTokenRef> {
        self.cancellation_token.as_ref()
    }

    /// Checks that output fields are a compatible projection and filter references resolve.
    /// Engines must also validate predicate support and selection bounds before returning data.
    pub fn validate(&self, read_schema: &StructType) -> KernelResult<()> {
        let output = self.output_schema.as_deref().unwrap_or(read_schema);
        validate_projection(read_schema, output)?;
        for field in output.fields() {
            NonEmptyStructs.transform_struct_field(field)?;
        }
        if let Some(predicate) = &self.row_filter {
            for column in predicate.references() {
                NonEmptyStructs.transform_struct_field(read_schema.field_at(column)?)?;
            }
        }
        Ok(())
    }
}

struct NonEmptyStructs;

impl<'a> SchemaTransform<'a> for NonEmptyStructs {
    transform_output_type!(|'a, T| KernelResult<()>);

    fn transform_struct(&mut self, schema: &'a StructType) -> KernelResult<()> {
        if schema.num_fields() == 0 {
            return Err(KernelError::unsupported(
                "Empty nested structs in Parquet reads",
            ));
        }
        self.recurse_into_struct(schema)
    }
}

fn validate_projection(read_schema: &StructType, output_schema: &StructType) -> KernelResult<()> {
    for output in output_schema.fields() {
        let read = read_schema
            .field(output.name())
            .ok_or_else(|| KernelError::missing_column(output.name()))?;
        let compatible = match (read.data_type(), output.data_type()) {
            (DataType::Struct(read), DataType::Struct(output)) => {
                validate_projection(read, output)?;
                true
            }
            (read, output) => read == output,
        };
        if !compatible
            || read.is_nullable() != output.is_nullable()
            || read.metadata() != output.metadata()
        {
            return Err(KernelError::generic(format!(
                "Output field {} is not a compatible read-schema projection",
                output.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::expressions::col;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, MapType, MetadataValue, StructField, schema_ref,
    };

    #[rstest]
    fn output_projection_preserves_field_attributes(
        #[values(false, true)] nested: bool,
        #[values("identity", "nullability", "field_id", "metadata")] change: &str,
    ) {
        let make_schema = |changed: bool| {
            let field = StructField::new(
                "x",
                DataType::INTEGER,
                !(changed && change == "nullability"),
            )
            .with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(if changed && change == "field_id" {
                        2
                    } else {
                        1
                    }),
                ),
                (
                    "annotation",
                    MetadataValue::String(
                        if changed && change == "metadata" {
                            "b"
                        } else {
                            "a"
                        }
                        .into(),
                    ),
                ),
            ]);
            let schema = StructType::try_new([field]).unwrap();
            Arc::new(if nested {
                StructType::try_new([StructField::nullable("s", schema)]).unwrap()
            } else {
                schema
            })
        };
        let result = ParquetReadOptions::default()
            .with_output_schema(make_schema(true))
            .validate(&make_schema(false));
        assert_eq!(result.is_ok(), change == "identity");
    }

    #[rstest]
    fn output_projection_preserves_complete_container_types(
        #[values(
            "struct",
            "array",
            "map",
            "map_key",
            "nested_array",
            "array_map",
            "map_array"
        )]
        container: &str,
        #[values("identity", "subset", "reorder")] projection: &str,
    ) {
        let children = schema_ref! { nullable "a": INTEGER, nullable "b": STRING };
        let projected = match projection {
            "subset" => children.project(&["b"]).unwrap(),
            "reorder" => children.project(&["b", "a"]).unwrap(),
            _ => children.clone(),
        };
        let wrap = |children: &SchemaRef| {
            let child_type = DataType::from(children.as_ref().clone());
            let data_type = match container {
                "array" => DataType::from(ArrayType::new(child_type, true)),
                "map" => DataType::from(MapType::new(DataType::STRING, child_type, true)),
                "map_key" => DataType::from(MapType::new(child_type, DataType::STRING, true)),
                "nested_array" => {
                    DataType::from(ArrayType::new(ArrayType::new(child_type, true), true))
                }
                "array_map" => DataType::from(ArrayType::new(
                    MapType::new(DataType::STRING, child_type, true),
                    true,
                )),
                "map_array" => DataType::from(MapType::new(
                    DataType::STRING,
                    ArrayType::new(child_type, true),
                    true,
                )),
                _ => child_type,
            };
            Arc::new(StructType::try_new([StructField::nullable("value", data_type)]).unwrap())
        };
        let result = ParquetReadOptions::default()
            .with_output_schema(wrap(&projected))
            .validate(&wrap(&children));
        assert_eq!(
            result.is_ok(),
            container == "struct" || projection != "subset"
        );
    }

    #[rstest]
    #[case::separated_runs(vec![0b10001001, 0b00000101], 11, vec![0..1, 3..4, 7..9, 10..11])]
    #[case::no_kept_rows(vec![0, 0], 11, vec![])]
    #[case::empty_file(vec![], 0, vec![])]
    fn packed_mask_ranges(
        #[case] bytes: Vec<u8>,
        #[case] row_count: u64,
        #[case] expected: Vec<Range<u64>>,
    ) {
        let selection = FileRowSelection::from_keep_mask(bytes, row_count).unwrap();
        assert_eq!(
            selection.ranges(row_count).unwrap().collect::<Vec<_>>(),
            expected
        );
    }

    #[rstest]
    fn rejects_empty_nested_structs_before_decoding(
        #[values(false, true)] nullable: bool,
        #[values("output", "default", "predicate")] use_site: &str,
    ) {
        let nested = |schema: StructType| {
            Arc::new(StructType::try_new([StructField::new("s", schema, nullable)]).unwrap())
        };
        let complete =
            nested(StructType::try_new([StructField::nullable("key", DataType::INTEGER)]).unwrap());
        let empty = nested(StructType::try_new([]).unwrap());
        let (read_schema, options) = match use_site {
            "output" => (
                complete,
                ParquetReadOptions::default().with_output_schema(empty),
            ),
            "predicate" => (
                empty,
                ParquetReadOptions::default()
                    .with_output_schema(schema_ref! {})
                    .with_row_filter(Arc::new(col!("s").is_null())),
            ),
            _ => (empty, ParquetReadOptions::default()),
        };
        assert!(matches!(options.validate(&read_schema),
            Err(error) if matches!(error.without_backtrace(), KernelError::Unsupported(_))));
    }

    #[test]
    fn selections_validate_bounds_and_preserve_live_tail() {
        assert!(FileRowSelection::from_ranges(vec![4..8, 7..9]).is_err());
        assert!(FileRowSelection::from_keep_mask(vec![255], 7).is_err());
        assert!(FileRowSelection::from_keep_mask(vec![], 1).is_err());
        assert!(
            FileRowSelection::from_exclusions(RoaringTreemap::from_iter([12]))
                .ranges(12)
                .is_err()
        );
        let selection = FileRowSelection::from_exclusions(RoaringTreemap::from_iter([0, 1, 4]));
        assert_eq!(
            selection.ranges(10).unwrap().collect::<Vec<_>>(),
            vec![2..4, 5..10]
        );
        assert!(
            FileRowSelection::from_exclusions(RoaringTreemap::new())
                .ranges(0)
                .unwrap()
                .next()
                .is_none()
        );
    }
}
