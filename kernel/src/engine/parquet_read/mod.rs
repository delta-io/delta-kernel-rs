//! Arrow decoding support for exact Parquet reads.

use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::arrow::array::{Array, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch};
use crate::arrow::buffer::{BooleanBuffer, Buffer};
use crate::arrow::datatypes::{DataType, Field};
use crate::arrow::error::ArrowError;
use crate::engine::arrow_conversion::TryIntoArrow as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_expression::evaluate_expression::{
    evaluate_predicate, validate_predicate,
};
use crate::engine::arrow_utils::{
    ReorderIndex, fixup_parquet_read, ordering_needs_row_indexes, ordering_preserves_values,
    projected_parquet_read_plan,
};
use crate::engine::parquet_row_group_skipping::RowGroupFilter;
use crate::engine::reader_options;
use crate::expressions::{ColumnName, Scalar};
use crate::parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderBuilder, ArrowReaderMetadata, RowFilter, RowSelection, RowSelector,
};
use crate::parquet::arrow::{ProjectionMask, RowNumber};
use crate::schema::{
    ColumnMetadataKey, DataType as KernelDataType, PrimitiveType, SchemaRef, StructField,
    StructType,
};
use crate::transforms::{ExpressionTransform, SchemaTransform, transform_output_type};
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, ParquetReadOptions};

/// A decoder configuration and schema reconciliation plan for one exact file read.
#[internal_api]
pub(crate) struct ParquetReadPlan {
    metadata: ArrowReaderMetadata,
    output_schema: SchemaRef,
    output_ordering: Vec<ReorderIndex>,
    projection: ProjectionMask,
    filter: Option<RowFilter>,
    selection: Option<RowSelection>,
    offset: usize,
    limit: Option<usize>,
    virtual_row_indexes: bool,
    file_location: String,
}

impl ParquetReadPlan {
    /// Validates options and prepares decoding, including any required virtual row indexes.
    #[internal_api]
    pub(crate) fn new(
        metadata: ArrowReaderMetadata,
        read_schema: SchemaRef,
        options: ParquetReadOptions,
        file_location: String,
    ) -> KernelResult<Self> {
        options.validate(&read_schema)?;
        let output_schema = options
            .output_schema()
            .cloned()
            .unwrap_or(read_schema.clone());
        let (output_ordering, projection) =
            projected_parquet_read_plan(&read_schema, &output_schema, &metadata)?;
        let (total_rows, row_group_ranges) = validated_row_group_ranges(
            metadata.metadata().file_metadata().num_rows(),
            metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|group| group.num_rows()),
        )?;
        let row_count = total_rows as u64;
        let mut selection = options
            .row_selection()
            .map(|selection| {
                if let Some((bytes, _)) = selection.as_keep_mask() {
                    selection.validate(row_count)?;
                    return Ok(RowSelection::from_boolean_buffer(BooleanBuffer::new(
                        Buffer::from_slice_ref(bytes),
                        0,
                        total_rows,
                    )));
                }
                let ranges = selection.ranges(row_count)?;
                Ok::<_, KernelError>(row_selection_from_ranges(
                    ranges.map(|range| range.start as usize..range.end as usize),
                    total_rows,
                ))
            })
            .transpose()?;

        let mut virtual_row_indexes = ordering_needs_row_indexes(&output_ordering);
        let filter = if let Some(predicate) = options.row_filter() {
            let mut projection = PredicateProjection {
                references: predicate.references(),
                path: Vec::new(),
            };
            let predicate_schema = Arc::new(match projection.transform_struct(&read_schema) {
                Some(schema) => schema.into_owned(),
                None => StructType::try_new([])?,
            });
            let empty =
                RecordBatch::new_empty(Arc::new(predicate_schema.as_ref().try_into_arrow()?));
            validate_predicate(predicate, &empty)?;
            let (ordering, mask) =
                projected_parquet_read_plan(&read_schema, &predicate_schema, &metadata)?;
            virtual_row_indexes |= ordering_needs_row_indexes(&ordering);

            // Footer scalar comparisons equate signed zeros, and its bounds omit NaNs. Field-ID
            // remapping, casts, and generated values can also invalidate bounds.
            let mut floating_literals = HasFloatingPointLiteral::default();
            floating_literals.transform_pred(predicate);
            let mut can_use_footer_stats =
                !floating_literals.0 && ordering_preserves_values(&ordering);
            for column in projection.references {
                read_schema.visit_fields_of_path(column, |field| {
                    can_use_footer_stats &= field
                        .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                        .is_none()
                        && !matches!(
                            field.data_type().physical_type(),
                            KernelDataType::Primitive(PrimitiveType::Float | PrimitiveType::Double)
                        );
                })?;
            }
            if can_use_footer_stats {
                let ranges: Vec<_> = metadata
                    .metadata()
                    .row_groups()
                    .iter()
                    .zip(row_group_ranges)
                    .filter_map(|(group, range)| {
                        RowGroupFilter::apply(group, predicate).then_some(range)
                    })
                    .collect();
                if ranges.len() != metadata.metadata().num_row_groups() {
                    selection = Some(match selection {
                        _ if ranges.is_empty() => {
                            RowSelection::from_consecutive_ranges(std::iter::empty(), total_rows)
                        }
                        Some(rows) => intersect_row_selection(rows, ranges.into_iter(), total_rows),
                        None => row_selection_from_ranges(ranges.into_iter(), total_rows),
                    });
                }
            }

            let predicate = predicate.clone();
            let location = file_location.clone();
            let row_indexes = virtual_row_indexes;
            let callback =
                ArrowPredicateFn::new(mask.unwrap_or_else(ProjectionMask::all), move |batch| {
                    let evaluate = || -> KernelResult<BooleanArray> {
                        let batch =
                            reconcile(batch, &ordering, &predicate_schema, row_indexes, &location)?;
                        let keep = evaluate_predicate(&predicate, batch.record_batch(), false)?;
                        if keep.len() == 1 {
                            return Ok(BooleanArray::from_iter(std::iter::repeat_n(
                                Some(keep.value(0) && keep.is_valid(0)),
                                batch.record_batch().num_rows(),
                            )));
                        }
                        Ok(keep)
                    };
                    evaluate().map_err(|error| ArrowError::ExternalError(Box::new(error)))
                });
            Some(RowFilter::new(vec![Box::new(callback)]))
        } else {
            None
        };

        let metadata = if virtual_row_indexes {
            let field = Arc::new(
                Field::new("__kernel_row_number", DataType::Int64, false)
                    .with_extension_type(RowNumber),
            );
            // RowNumber requires complete, unique ordinals, which Parquet does not require.
            // Our reader retains footer order, so physical positions define these identifiers.
            let row_groups = metadata
                .metadata()
                .row_groups()
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    let ordinal = index.try_into().map_err(KernelError::generic_err)?;
                    Ok(group.clone().into_builder().set_ordinal(ordinal).build()?)
                })
                .collect::<KernelResult<Vec<_>>>()?;
            let parquet_metadata = metadata
                .metadata()
                .as_ref()
                .clone()
                .into_builder()
                .set_row_groups(row_groups)
                .build();
            ArrowReaderMetadata::try_new(
                Arc::new(parquet_metadata),
                reader_options().with_virtual_columns(vec![field])?,
            )?
        } else {
            metadata
        };
        Ok(Self {
            metadata,
            output_schema,
            output_ordering,
            projection: projection.unwrap_or_else(ProjectionMask::all),
            filter,
            selection,
            offset: usize::try_from(options.result_offset()).map_err(KernelError::generic_err)?,
            limit: options
                .result_limit()
                .map(usize::try_from)
                .transpose()
                .map_err(KernelError::generic_err)?,
            virtual_row_indexes,
            file_location,
        })
    }

    /// Constructs and configures a reader with this plan's metadata, returning the reconciliation
    /// function to apply to each decoded batch.
    #[internal_api]
    pub(crate) fn configure<T>(
        self,
        make_builder: impl FnOnce(ArrowReaderMetadata) -> ArrowReaderBuilder<T>,
    ) -> (
        ArrowReaderBuilder<T>,
        impl Fn(RecordBatch) -> KernelResult<ArrowEngineData> + 'static,
    ) {
        let builder = make_builder(self.metadata)
            .with_projection(self.projection)
            .with_offset(self.offset)
            .fold_with(self.limit, ArrowReaderBuilder::with_limit)
            .fold_with(self.selection, ArrowReaderBuilder::with_row_selection)
            .fold_with(self.filter, ArrowReaderBuilder::with_row_filter);
        (builder, move |batch| {
            reconcile(
                batch,
                &self.output_ordering,
                &self.output_schema,
                self.virtual_row_indexes,
                &self.file_location,
            )
        })
    }
}

fn row_selection_from_ranges(
    mut ranges: impl Iterator<Item = Range<usize>>,
    total_rows: usize,
) -> RowSelection {
    // Each range needs at most a skip and a select run. Stop collecting once a bitmap is smaller.
    let max_ranges = (total_rows.div_ceil(8) / (2 * size_of::<RowSelector>())).max(1);
    let mut prefix: Vec<Range<usize>> = Vec::new();
    for range in ranges.by_ref() {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = prefix.last_mut().filter(|last| last.end == range.start) {
            last.end = range.end;
        } else {
            prefix.push(range);
        }
        if prefix.len() > max_ranges {
            return mask_selection(prefix.into_iter().chain(ranges), total_rows, None);
        }
    }
    RowSelection::from_consecutive_ranges(prefix.into_iter(), total_rows)
}

fn intersect_row_selection(
    rows: RowSelection,
    ranges: impl Iterator<Item = Range<usize>>,
    total_rows: usize,
) -> RowSelection {
    // Mixed mask/run intersection expands the mask into runs.
    if let Some(mask) = rows.as_mask() {
        return mask_selection(ranges, total_rows, Some(mask));
    }
    let pruned = row_selection_from_ranges(ranges, total_rows);
    if let Some(mask) = pruned.as_mask() {
        let mut end = 0;
        let ranges = rows.iter().filter_map(|selector| {
            let start = end;
            end += selector.row_count;
            (!selector.skip).then_some(start..end)
        });
        mask_selection(ranges, total_rows, Some(mask))
    } else {
        rows.intersection(&pruned)
    }
}

fn mask_selection(
    ranges: impl Iterator<Item = Range<usize>>,
    total_rows: usize,
    source: Option<&BooleanBuffer>,
) -> RowSelection {
    let mut mask = BooleanBufferBuilder::new(total_rows);
    for range in ranges {
        mask.append_n(range.start - mask.len(), false);
        match source {
            Some(source) => mask.append_buffer(&source.slice(range.start, range.len())),
            None => mask.append_n(range.len(), true),
        }
    }
    mask.append_n(total_rows - mask.len(), false);
    RowSelection::from_boolean_buffer(mask.finish())
}

fn validated_row_group_ranges(
    file_rows: i64,
    group_rows: impl IntoIterator<Item = i64>,
) -> KernelResult<(usize, Vec<Range<usize>>)> {
    let total_rows = usize::try_from(file_rows).map_err(KernelError::generic_err)?;
    let mut end = 0usize;
    let ranges = group_rows
        .into_iter()
        .map(|rows| {
            let start = end;
            let rows = usize::try_from(rows).map_err(KernelError::generic_err)?;
            end = end
                .checked_add(rows)
                .ok_or_else(|| KernelError::generic("Parquet row-group counts overflow"))?;
            Ok(start..end)
        })
        .collect::<KernelResult<Vec<_>>>()?;
    if end != total_rows {
        return Err(KernelError::generic(
            "Parquet row-group counts do not match the file row count",
        ));
    }
    Ok((total_rows, ranges))
}

#[derive(Default)]
struct HasFloatingPointLiteral(bool);

impl<'a> ExpressionTransform<'a> for HasFloatingPointLiteral {
    transform_output_type!(|'a, T| ());

    fn transform_expr_literal(&mut self, value: &'a Scalar) {
        self.0 |= matches!(value, Scalar::Float(_) | Scalar::Double(_));
    }
}

struct PredicateProjection<'a> {
    references: HashSet<&'a ColumnName>,
    path: Vec<String>,
}

impl<'a> SchemaTransform<'a> for PredicateProjection<'_> {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.path.push(field.name().clone());
        let projected = if self
            .references
            .iter()
            .any(|column| column.path() == self.path)
        {
            Some(Cow::Borrowed(field))
        } else if self
            .references
            .iter()
            .any(|column| column.path().starts_with(&self.path))
        {
            if matches!(field.data_type(), KernelDataType::File(_)) {
                // FILE locator validation depends on siblings of the referenced leaf.
                Some(Cow::Borrowed(field))
            } else {
                self.recurse_into_struct_field(field)
            }
        } else {
            None
        };
        self.path.pop();
        projected
    }
}

fn reconcile(
    mut batch: RecordBatch,
    ordering: &[ReorderIndex],
    schema: &SchemaRef,
    virtual_row_indexes: bool,
    file_location: &str,
) -> KernelResult<ArrowEngineData> {
    let row_numbers = if virtual_row_indexes {
        let column = batch
            .num_columns()
            .checked_sub(1)
            .ok_or_else(|| KernelError::generic("Missing virtual row indexes"))?;
        let array = batch.remove_column(column);
        Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| KernelError::generic("Invalid virtual row indexes"))?
                .clone(),
        )
    } else {
        None
    };
    fixup_parquet_read(
        batch,
        ordering,
        row_numbers,
        Some(file_location),
        Some(schema),
    )
}

#[cfg(test)]
mod tests;
