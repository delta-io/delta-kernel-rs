//! Arrow decoding support for exact Parquet reads.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use crate::arrow::buffer::{BooleanBuffer, Buffer};
use crate::arrow::datatypes::{DataType, Field};
use crate::arrow::error::ArrowError;
use crate::engine::arrow_conversion::TryIntoArrow as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use crate::engine::arrow_utils::{
    ReorderIndex, fixup_parquet_read, ordering_needs_row_indexes, parquet_read_plan,
};
use crate::engine::reader_options;
use crate::expressions::ColumnName;
use crate::parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderBuilder, ArrowReaderMetadata, RowFilter, RowSelection,
};
use crate::parquet::arrow::{ProjectionMask, RowNumber};
use crate::schema::{DataType as KernelDataType, SchemaRef, StructField, StructType};
use crate::transforms::{SchemaTransform, transform_output_type};
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
        for field in output_schema.fields() {
            NonEmptyStructs.transform_struct_field(field)?;
        }
        // TODO: Reconcile field-ID name fallback and resolve projections against the complete
        // read schema before scan adoption.
        // TODO: Preserve nullable parents with only missing children and reconcile nested
        // collections and reordered FILE fields in the shared reader helpers.
        let (output_ordering, projection) = parquet_read_plan(&output_schema, &metadata)?;
        let total_rows = validated_row_count(
            metadata.metadata().file_metadata().num_rows(),
            metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|group| group.num_rows()),
        )?;
        let row_count = total_rows as u64;
        // TODO: Keep scattered range/exclusion selections compact during decoder planning.
        let selection = options
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
                Ok::<_, KernelError>(RowSelection::from_consecutive_ranges(
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
            for field in predicate_schema.fields() {
                NonEmptyStructs.transform_struct_field(field)?;
            }
            let empty =
                RecordBatch::new_empty(Arc::new(predicate_schema.as_ref().try_into_arrow()?));
            // TODO: Validate skipped conditional branches and fix IN/JSON evaluator edge cases
            // before Scan::execute adopts exact filtering.
            evaluate_predicate(predicate, &empty, false)?;
            let (ordering, mask) = parquet_read_plan(&predicate_schema, &metadata)?;
            virtual_row_indexes |= ordering_needs_row_indexes(&ordering);

            // TODO: Add conservative footer pruning for exact predicates.
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
            // TODO(#3966): Support Arrow 59 row indexes without the i16 row-group ordinal limit.
            #[cfg(all(feature = "arrow-59", not(feature = "arrow-60")))]
            if metadata.metadata().num_row_groups() > i16::MAX as usize + 1 {
                return Err(KernelError::unsupported(
                    "Exact Parquet reads with Arrow 59 do not support row indexes in files \
                     with more than 32,768 row groups",
                ));
            }
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

fn validated_row_count(
    file_rows: i64,
    group_rows: impl IntoIterator<Item = i64>,
) -> KernelResult<usize> {
    let total_rows = usize::try_from(file_rows).map_err(KernelError::generic_err)?;
    let mut sum = 0usize;
    for rows in group_rows {
        let rows = usize::try_from(rows).map_err(KernelError::generic_err)?;
        sum = sum
            .checked_add(rows)
            .ok_or_else(|| KernelError::generic("Parquet row-group counts overflow"))?;
    }
    if sum != total_rows {
        return Err(KernelError::generic(
            "Parquet row-group counts do not match the file row count",
        ));
    }
    Ok(total_rows)
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
