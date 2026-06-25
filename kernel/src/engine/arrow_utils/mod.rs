//! Some utilities for working with arrow data types

pub(crate) mod apply_schema;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use delta_kernel_derive::internal_api;
use itertools::Itertools;
use tracing::debug;

use self::apply_schema::apply_schema_to_struct;
use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{
    make_array, new_null_array, Array as ArrowArray, ArrayRef as ArrowArrayRef, GenericListArray,
    MapArray, OffsetSizeTrait, PrimitiveArray, RecordBatch, RecordBatchOptions, StringArray,
    StructArray,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::compute::{cast_with_options, CastOptions};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Fields as ArrowFields, Int64Type, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use crate::arrow::json::writer::{make_encoder, LineDelimited, NullableEncoder};
use crate::arrow::json::{Encoder, EncoderFactory, EncoderOptions, ReaderBuilder, WriterBuilder};
use crate::engine::arrow_conversion::{TryFromKernel as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::ensure_data_types::DataTypeCompat;
use crate::engine_data::FilteredEngineData;
use crate::parquet::arrow::{ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use crate::parquet::file::metadata::RowGroupMetaData;
use crate::parquet::schema::types::SchemaDescriptor;
use crate::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataColumnSpec, MetadataValue,
    PrimitiveType, Schema, SchemaRef, StructField, StructType,
};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

macro_rules! prim_array_cmp {
    ( $left_arr: ident, $right_arr: ident, $(($data_ty: pat, $prim_ty: ty)),+ ) => {

        return match $left_arr.data_type() {
        $(
            $data_ty => {
                let prim_array = $left_arr.as_primitive_opt::<$prim_ty>()
                        .ok_or(Error::invalid_expression(
                            format!("Cannot cast to primitive array: {}", $left_arr.data_type()))
                        )?;
                    let list_array = $right_arr.as_list_opt::<i32>()
                        .ok_or(Error::invalid_expression(
                            format!("Cannot cast to list array: {}", $right_arr.data_type()))
                        )?;
                crate::arrow::compute::kernels::comparison::in_list(prim_array, list_array)
            }
        )+
            _ => Err(ArrowError::CastError(
                        format!("Bad Comparison between: {:?} and {:?}",
                            $left_arr.data_type(),
                            $right_arr.data_type())
                        )
                )
        }.map_err(Error::generic_err);
    };
}

pub(crate) use prim_array_cmp;

type FieldIndex = usize;
type FlattenedRangeIterator<T> = std::iter::Flatten<std::vec::IntoIter<Range<T>>>;

/// contains information about a StructField matched to a parquet struct field
///
/// # Lifetime Parameters
/// * `'k` - The lifetime of the referenced kernel StructField
struct KernelFieldInfo<'k> {
    /// The index of the struct field in its parent struct
    parquet_index: FieldIndex,
    /// A reference to the struct field
    field: &'k StructField,
}

/// Contains a information about a parquet field and the matching `KernelFieldInfo` if one
/// exists. Parquet struct fields are matched to Kernel fields in [`match_parquet_fields`].
///
/// # Lifetime Parameters
/// * `'k` - The lifetime of the referenced kernel StructField
/// * `'p` - The lifetime of the referenced parquet ArrowField
struct MatchedParquetField<'p, 'k> {
    /// The index of the parquet field
    parquet_index: FieldIndex,
    /// A reference to the parquet field in the arrow schema
    parquet_field: &'p ArrowField,
    /// If present, this is a `KernelFieldInfo` belonging to a matching kernel `StructField`
    kernel_field_info: Option<KernelFieldInfo<'k>>,
}

/// Create an [`Error::Arrow`] with a backtrace from the given message.
#[internal_api]
pub(crate) fn make_arrow_error(s: impl Into<String>) -> Error {
    Error::Arrow(crate::arrow::error::ArrowError::InvalidArgumentError(
        s.into(),
    ))
    .with_backtrace()
}

/// Prepares to enumerate row indexes of rows in a parquet file, accounting for row group skipping.
#[internal_api]
pub(crate) struct RowIndexBuilder {
    row_group_row_index_ranges: Vec<Range<i64>>,
    row_group_ordinals: Option<Vec<usize>>,
}

impl RowIndexBuilder {
    #[internal_api]
    pub(crate) fn new(row_groups: &[RowGroupMetaData]) -> Self {
        let mut row_group_row_index_ranges = Vec::with_capacity(row_groups.len());
        let mut offset = 0;
        for row_group in row_groups {
            let num_rows = row_group.num_rows();
            row_group_row_index_ranges.push(offset..offset + num_rows);
            offset += num_rows;
        }
        Self {
            row_group_row_index_ranges,
            row_group_ordinals: None,
        }
    }

    /// Only produce row indexes for the row groups specified by the ordinals that survived row
    /// group skipping. The ordinals must be in 0..num_row_groups.
    #[internal_api]
    pub(crate) fn select_row_groups(&mut self, ordinals: &[usize]) {
        // NOTE: Don't apply the filtering until we actually build the iterator, because the
        // filtering is not idempotent and `with_row_groups` could be called more than once.
        self.row_group_ordinals = Some(ordinals.to_vec())
    }

    /// Build an iterator of row indexes, filtering out row groups that were skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if there are duplicate or out of bounds row group ordinals.
    #[internal_api]
    pub(crate) fn build(self) -> DeltaResult<FlattenedRangeIterator<i64>> {
        let starting_offsets = match self.row_group_ordinals {
            Some(ordinals) => {
                let mut seen_ordinals = HashSet::with_capacity(ordinals.len());
                ordinals
                    .iter()
                    .map(|&i| {
                        // We verify that there are no duplicate or out of bounds ordinals
                        if !seen_ordinals.insert(i) {
                            return Err(Error::generic("Found duplicate row group ordinal"));
                        }
                        // We have to clone here to avoid modifying the original vector in each
                        // iteration
                        self.row_group_row_index_ranges
                            .get(i)
                            .cloned()
                            .ok_or_else(|| {
                                Error::generic(format!("Row group ordinal {i} is out of bounds"))
                            })
                    })
                    .try_collect()?
            }
            None => self.row_group_row_index_ranges,
        };
        Ok(starting_offsets.into_iter().flatten())
    }
}

/// Applies post-processing to data read from parquet files. This includes `reorder_struct_array` to
/// ensure schema compatibility, as well as `fix_nested_null_masks` to ensure that leaf columns have
/// accurate null masks that row visitors rely on for correctness.
/// `row_indexes` are passed through to `reorder_struct_array`.
/// `file_location` is used to populate file metadata columns if requested.
///
/// If `target_schema` is provided, rewrites the batch's schema wrappers to match the kernel
/// schema via `apply_schema_to_struct`. Specifically, at every nesting level (struct child, list
/// element, map key/value):
///
/// - field names are taken from the kernel schema (producer names are kept only for list element
///   and map key/value positions, where the kernel `ArrayType`/`MapType` is unnamed);
/// - field nullability is taken from the kernel schema;
/// - field metadata is replaced wholesale with kernel-derived metadata (translating
///   `parquet.field.id` to `PARQUET:field_id` and propagating kernel-only annotations such as
///   `delta.typeChanges`);
/// - if both the source and kernel fields carry a `PARQUET:field_id` and they disagree, the call
///   errors (defense against malformed inputs);
/// - top-level `RecordBatch::schema().metadata()` is not preserved (the rebuilt schema is created
///   via `ArrowSchema::new`).
///
/// **Type validation.** `apply_schema_to_struct` runs `ensure_data_types(.., Full)` at every
/// primitive leaf. This is safe because `reorder_struct_array` above has already resolved every
/// `DataTypeCompat::NeedsCast` into an actual `arrow::compute::cast`, so post-reorder leaf types
/// are `Identical` to the kernel target.
///
/// **Cost.** O(F) per batch where F is the total number of fields (including nested). Row data
/// (Arrow buffers, offsets, null buffers) is shared via `Arc` and never copied.
#[internal_api]
pub(crate) fn fixup_parquet_read(
    batch: RecordBatch,
    requested_ordering: &[ReorderIndex],
    row_indexes: Option<&mut FlattenedRangeIterator<i64>>,
    file_location: Option<&str>,
    target_schema: Option<&SchemaRef>,
) -> DeltaResult<ArrowEngineData> {
    let data = reorder_struct_array(batch.into(), requested_ordering, row_indexes, file_location)?;
    let data = fix_nested_null_masks(data);
    let data = if let Some(schema) = target_schema {
        apply_schema_to_struct(&data, schema)?
    } else {
        data
    };
    Ok(data.into())
}

/*
* The code below implements proper pruning of columns when reading parquet, reordering of columns to
* match the specified schema, and insertion of null columns if the requested schema includes a
* nullable column that isn't included in the parquet file.
*
* At a high level there are three schemas/concepts to worry about:
*  - The parquet file's physical schema (= the columns that are actually available), called
*    "parquet_schema" below
*  - The requested logical schema from the engine (= the columns we actually want), called
*    "requested_schema" below
*  - The Read schema (and intersection of 1. and 2., in logical schema order). This is never
*    materialized, but is useful to be able to refer to here
*  - A `ProjectionMask` that goes to the parquet reader which specifies which subset of columns from
*    the file schema to actually read. (See "Example" below)
*
* In other words, the ProjectionMask is the intersection of the parquet schema and logical schema,
* and then mapped to indices in the parquet file. Columns unique to the file schema need to be
* masked out (= ignored), while columns unique to the logical schema need to be backfilled with
* nulls.
*
* We also have to worry about field ordering differences between the read schema and logical
* schema. We represent any reordering needed as a tree. Each level of the tree is a vec of
* `ReorderIndex`s. Each element's index represents a column that will be in the read parquet data
* (as an arrow StructArray) at that level and index. The `ReorderIndex::index` field of the element
* is the position that the column should appear in the final output.

* The algorithm has three parts, handled by `get_requested_indices`, `generate_mask` and
* `reorder_struct_array` respectively.

* `get_requested_indices` generates indices to select, along with reordering information:
* 1. Loop over each field in parquet_schema, keeping track of how many physical fields (i.e. leaf
*    columns) we have seen so far
* 2. If a requested field matches the physical field, push the index of the field onto the mask.

* 3. Also push a ReorderIndex element that indicates where this item should be in the final output,
*    and if it needs any transformation (i.e. casting, create null column)
* 4. If a nested element (struct/map/list) is encountered, recurse into it, pushing indices onto
*    the same vector, but producing a new reorder level, which is added to the parent with a `Nested`
*    transform
*
* `generate_mask` is simple, and just calls `ProjectionMask::leaves` in the parquet crate with the
* indices computed by `get_requested_indices`
*
* `reorder_struct_array` handles reordering and data transforms:
* 1. First check if we need to do any transformations (see doc comment for
*    `ordering_needs_transform`)
* 2. If nothing is required we're done (return); otherwise:
* 3. Create a Vec[None, ..., None] of placeholders that will hold the correctly ordered columns
* 4. Deconstruct the existing struct array and then loop over the `ReorderIndex` list
* 5. Use the `ReorderIndex::index` value to put the column at the correct location
* 6. Additionally, if `ReorderIndex::transform` is not `Identity`, then if it is:
*      - `Cast`: cast the column to the specified type
*      - `Missing`: put a column of `null` at the correct location
*      - `Nested([child_order])` and the data is a `StructArray`: recursively call
*         `reorder_struct_array` on the column with `child_order` to correctly ordered the child
*         array
*      - `Nested` and the data is a `List<StructArray>`: get the inner struct array out of the list,
*         reorder it recursively as above, rebuild the list, and the put the column at the correct
*         location
*      - `Nested` and the data is a `Map`. We expect the child order to contain two elements. The
*         first specifies any needed reordering in the keys (i.e. if the key contains a struct),
*         and the second any reordering needed in the values.
*
* Example:
* The parquet crate `ProjectionMask::leaves` method only considers leaf columns -- a "flat" schema --
* so a struct column is purely a schema level thing and doesn't "count" wrt. column indices.
*
* So if we have the following file physical schema:
*
*  a
*    d
*    x
*  b
*    y
*      z
*    e
*    f
*  c
*
* and a logical requested schema of:
*
*  b
*    f
*    e
*  a
*    x
*  c
*
* The mask is [1, 3, 4, 5] because a, b, and y don't contribute to the column indices.
*
* The reorder tree is:
* [
*   // col a is at position 0 in the struct array, and should be moved to position 1
*   { index: 1, Nested([{ index: 0 }]) },
*   // col b is at position 1 in the struct array, and should be moved to position 0
*   //   also, the inner struct array needs to be reordered to swap 'f' and 'e'
*   { index: 0, Nested([{ index: 1 }, {index: 0}]) },
*   // col c is at position 2 in the struct array, and should stay there
*   { index: 2 }
* ]
*/

/// Reordering is specified as a tree. Each level is a vec of `ReorderIndex`s. Each element's
/// position represents a column that will be in the read parquet data at that level and
/// position. The `index` of the element is the position that the column should appear in the final
/// output. The `transform` indicates what, if any, transforms are needed. See the docs for
/// [`ReorderIndexTransform`] for the meaning.
#[derive(Debug, PartialEq)]
#[internal_api]
pub(crate) struct ReorderIndex {
    pub index: usize,
    transform: ReorderIndexTransform,
}

#[derive(Debug, PartialEq)]
#[internal_api]
pub(crate) enum ReorderIndexTransform {
    /// For a non-nested type, indicates that we need to cast to the contained type
    Cast(ArrowDataType),
    /// Used for struct/list/map. Potentially transform child fields using contained reordering
    Nested(Vec<ReorderIndex>),
    /// No work needed to transform this data
    Identity,
    /// Data is missing, fill in with a null column
    Missing(ArrowFieldRef),
    /// Row index column requested, compute it
    RowIndex(ArrowFieldRef),
    /// File path column requested, populate with file path
    FilePath(ArrowFieldRef),
}

impl ReorderIndex {
    fn new(index: usize, transform: ReorderIndexTransform) -> Self {
        ReorderIndex { index, transform }
    }

    fn cast(index: usize, target: ArrowDataType) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::Cast(target))
    }

    fn nested(index: usize, children: Vec<ReorderIndex>) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::Nested(children))
    }

    fn identity(index: usize) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::Identity)
    }

    fn missing(index: usize, field: ArrowFieldRef) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::Missing(field))
    }

    fn row_index(index: usize, field: ArrowFieldRef) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::RowIndex(field))
    }

    fn file_path(index: usize, field: ArrowFieldRef) -> Self {
        ReorderIndex::new(index, ReorderIndexTransform::FilePath(field))
    }

    /// Check if this reordering requires a transformation anywhere. See comment below on
    /// [`ordering_needs_transform`] to understand why this is needed.
    fn needs_transform(&self) -> bool {
        match self.transform {
            // if we're casting, inserting null, or generating row index/file path, we need to
            // transform
            ReorderIndexTransform::Cast(_)
            | ReorderIndexTransform::Missing(_)
            | ReorderIndexTransform::RowIndex(_)
            | ReorderIndexTransform::FilePath(_) => true,
            // if our nested ordering needs a transform, we need a transform
            ReorderIndexTransform::Nested(ref children) => ordering_needs_transform(children),
            // no transform needed
            ReorderIndexTransform::Identity => false,
        }
    }
}

// count the number of physical columns, including nested ones in an `ArrowField`
fn count_cols(field: &ArrowField) -> usize {
    _count_cols(field.data_type())
}

fn _count_cols(dt: &ArrowDataType) -> usize {
    match dt {
        ArrowDataType::Struct(fields) => fields.iter().map(|f| count_cols(f)).sum(),
        ArrowDataType::Union(fields, _) => fields.iter().map(|(_, f)| count_cols(f)).sum(),
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _)
        | ArrowDataType::Map(field, _) => count_cols(field),
        ArrowDataType::Dictionary(_, value_field) => _count_cols(value_field.as_ref()),
        _ => 1, // other types are "real" fields, so count
    }
}

/// Validate that a given field in a parquet file which is presumed to represent data of the
/// `VARIANT` type is represented as `STRUCT<metadata: BINARY, value: BINARY>`. This is to make
/// sure that the default engine does not try to read shredded Variants, which it currently does
/// not support.
fn validate_parquet_variant(field: &ArrowField) -> DeltaResult<()> {
    fn variant_parquet_error(field_name: &String) -> Error {
        Error::Generic(format!(
            "The field {field_name} presumed to be of Variant type might be \
            shredded in the parquet file. The default engine does not support \
            shredded reads yet."
        ))
    }
    match field.data_type() {
        ArrowDataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err(variant_parquet_error(field.name()));
            }
            if !matches!(
                (fields[0].name().as_str(), fields[1].name().as_str()),
                ("value", "metadata") | ("metadata", "value")
            ) {
                return Err(variant_parquet_error(field.name()));
            }
            Ok(())
        }
        _ => Err(variant_parquet_error(field.name())),
    }
}

/// helper function, does the same as `get_requested_indices` but at an offset. used to recurse into
/// structs, lists, and maps. `parquet_offset` is how many parquet fields exist before processing
/// this potentially nested schema. returns the number of parquet fields in `fields` (regardless of
/// if they are selected or not) and reordering information for the requested fields.
fn get_indices(
    start_parquet_offset: usize,
    requested_schema: &Schema,
    fields: &ArrowFields,
    mask_indices: &mut Vec<usize>,
) -> DeltaResult<(usize, Vec<ReorderIndex>)> {
    let mut found_fields = HashSet::with_capacity(requested_schema.num_fields());
    let mut reorder_indices = Vec::with_capacity(requested_schema.num_fields());
    // Missing entries for structs found in parquet but with no selected leaves. These must
    // be appended after all input-consuming entries (Identity/Nested/Cast) because
    // `reorder_struct_array` uses vec position as the index into the parquet reader output.
    let mut deferred_missing = Vec::new();
    let mut parquet_offset = start_parquet_offset;
    // for each field, get its position in the parquet (via enumerate), a reference to the arrow
    // field, and info about where it appears in the requested_schema, or None if the field is not
    // requested
    let matched_parquet_fields = match_parquet_fields(requested_schema, fields);
    for MatchedParquetField {
        parquet_index,
        parquet_field: field,
        kernel_field_info,
    } in matched_parquet_fields
    {
        debug!(
            "Getting indices for field {} with offset {parquet_offset}, with index {parquet_index}",
            field.name()
        );
        if let Some(KernelFieldInfo {
            parquet_index: index,
            field: requested_field,
            ..
        }) = kernel_field_info
        {
            // If the field is a variant, make sure the parquet schema matches the unshredded
            // variant representation. This is to ensure that shredded reads are not
            // performed.
            if requested_field.data_type == DataType::unshredded_variant() {
                validate_parquet_variant(field)?;
            }
            // A UDT is read as its physical `sql_type`, so resolve through it for the structural
            // checks below. The leaf arm passes the UDT to `ensure_data_types`, which delegates.
            let requested_data_type = match &requested_field.data_type {
                DataType::UserDefined(udt) => udt.sql_type.as_ref(),
                dt => dt,
            };
            match field.data_type() {
                ArrowDataType::Struct(fields) => {
                    if let DataType::Struct(requested_schema)
                    | DataType::Variant(requested_schema) = requested_data_type
                    {
                        let mask_before = mask_indices.len();
                        let (parquet_advance, children) = get_indices(
                            parquet_index + parquet_offset,
                            requested_schema.as_ref(),
                            fields,
                            mask_indices,
                        )?;
                        // advance the number of parquet fields, but subtract 1 because the
                        // struct will be counted by the `enumerate` call but doesn't count as
                        // an actual index. Use saturating_sub to handle empty structs (0 fields).
                        parquet_offset += parquet_advance.saturating_sub(1);
                        // If no leaf columns were selected (mask unchanged), the parquet
                        // reader will omit this struct entirely. We cannot create a Nested
                        // entry because it would index into a column that doesn't exist.
                        // The recursive call is still needed for the correct
                        // `parquet_advance` value.
                        found_fields.insert(requested_field.name());
                        if mask_indices.len() > mask_before {
                            reorder_indices.push(ReorderIndex::nested(index, children));
                        } else {
                            // The recursive call resolved all children (as nullable/missing
                            // or the struct is empty), but no parquet leaves were selected.
                            // Defer the Missing entry so it appears after all entries that
                            // consume parquet input columns.
                            debug_assert_eq!(children.len(), requested_schema.num_fields());
                            deferred_missing.push(ReorderIndex::missing(
                                index,
                                Arc::new(requested_field.try_into_arrow()?),
                            ));
                        }
                    } else {
                        return Err(Error::unexpected_column_type(field.name()));
                    }
                }
                ArrowDataType::List(list_field)
                | ArrowDataType::LargeList(list_field)
                | ArrowDataType::ListView(list_field) => {
                    // we just want to transparently recurse into lists, need to transform the
                    // kernel list data type into a schema
                    if let DataType::Array(array_type) = requested_data_type {
                        let requested_schema = StructType::new_unchecked([StructField::new(
                            list_field.name().clone(), // so we find it in the inner call
                            array_type.element_type.clone(),
                            array_type.contains_null,
                        )]);
                        let mask_before = mask_indices.len();
                        let (parquet_advance, mut children) = get_indices(
                            parquet_index + parquet_offset,
                            &requested_schema,
                            &[list_field.clone()].into(),
                            mask_indices,
                        )?;
                        // see comment above in struct match arm
                        parquet_offset += parquet_advance - 1;
                        found_fields.insert(requested_field.name());
                        if mask_indices.len() <= mask_before {
                            // No leaves selected inside this list. Defer a Missing entry.
                            deferred_missing.push(ReorderIndex::missing(
                                index,
                                Arc::new(requested_field.try_into_arrow()?),
                            ));
                        } else if children.len() != 1 {
                            return Err(Error::generic(
                                "List call should not have generated more than one reorder index",
                            ));
                        } else {
                            // safety, checked that we have 1 element
                            let mut children = children.swap_remove(0);
                            // the index is wrong, as it's the index from the inner schema.
                            // Adjust it to be our index
                            children.index = index;
                            reorder_indices.push(children);
                        }
                    } else {
                        return Err(Error::unexpected_column_type(list_field.name()));
                    }
                }
                ArrowDataType::Map(key_val_field, _) => {
                    match (key_val_field.data_type(), requested_data_type) {
                        (ArrowDataType::Struct(inner_fields), DataType::Map(map_type)) => {
                            let mut key_val_names =
                                inner_fields.iter().map(|f| f.name().to_string());
                            let key_name = key_val_names.next().ok_or_else(|| {
                                Error::generic("map fields didn't include a key col")
                            })?;
                            let val_name = key_val_names.next().ok_or_else(|| {
                                Error::generic("map fields didn't include a val col")
                            })?;
                            if key_val_names.next().is_some() {
                                return Err(Error::generic("map fields had more than 2 members"));
                            }
                            let inner_schema = map_type.as_struct_schema(key_name, val_name);
                            let mask_before = mask_indices.len();
                            let (parquet_advance, mut children) = get_indices(
                                parquet_index + parquet_offset,
                                &inner_schema,
                                inner_fields,
                                mask_indices,
                            )?;

                            // advance the number of parquet fields, but subtract 1 because the
                            // map will be counted by the `enumerate` call but doesn't count as
                            // an actual index.
                            parquet_offset += parquet_advance - 1;
                            found_fields.insert(requested_field.name());
                            if mask_indices.len() <= mask_before {
                                // No leaves selected inside this map. Defer a Missing entry.
                                deferred_missing.push(ReorderIndex::missing(
                                    index,
                                    Arc::new(requested_field.try_into_arrow()?),
                                ));
                            } else if children.len() != 2 {
                                return Err(Error::generic(
                                    "Map call should have generated exactly two reorder indices",
                                ));
                            } else {
                                // vec indexing is safe, we checked len above
                                let mut num_identity_transforms = 0;
                                if !children[0].needs_transform() {
                                    children[0] = ReorderIndex::identity(0);
                                    num_identity_transforms += 1;
                                }
                                if !children[1].needs_transform() {
                                    children[1] = ReorderIndex::identity(1);
                                    num_identity_transforms += 1;
                                }
                                let transform = match num_identity_transforms {
                                    2 => ReorderIndex::identity(index),
                                    _ => ReorderIndex::nested(index, children),
                                };
                                reorder_indices.push(transform);
                            }
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(field.name()));
                        }
                    }
                }
                _ => {
                    // We don't care about matching on nullability or metadata here. These can
                    // differ between the delta schema and the parquet schema without causing
                    // issues in reading the data. We fix them up in expression evaluation later.
                    match super::ensure_data_types::ensure_data_types(
                        &requested_field.data_type,
                        field.data_type(),
                        super::ensure_data_types::ValidationMode::TypesAndNames,
                    )? {
                        DataTypeCompat::Identical => {
                            reorder_indices.push(ReorderIndex::identity(index))
                        }
                        DataTypeCompat::NeedsCast(target) => {
                            reorder_indices.push(ReorderIndex::cast(index, target))
                        }
                        DataTypeCompat::Nested => {
                            return Err(Error::internal_error(
                                "Comparing nested types in get_indices",
                            ))
                        }
                    }
                    found_fields.insert(requested_field.name());
                    mask_indices.push(parquet_offset + parquet_index);
                }
            }
        } else {
            // We're NOT selecting this field, but we still need to track how many leaf columns we
            // skipped over
            debug!("Skipping over un-selected field: {}", field.name());
            // offset by number of inner fields. subtract one, because the enumerate still
            // counts this logical "parent" field
            parquet_offset += count_cols(field).saturating_sub(1);
        }
    }

    // Append deferred Missing entries after all input-consuming entries from the main loop.
    reorder_indices.extend(deferred_missing);

    if found_fields.len() != requested_schema.num_fields() {
        // some fields are missing, but they might be nullable or metadata columns, need to insert
        // them into the reorder_indices
        for (requested_position, field) in requested_schema.fields().enumerate() {
            if !found_fields.contains(field.name()) {
                match field.get_metadata_column_spec() {
                    Some(MetadataColumnSpec::RowIndex) => {
                        debug!("Inserting a row index column: {}", field.name());
                        reorder_indices.push(ReorderIndex::row_index(
                            requested_position,
                            Arc::new(field.try_into_arrow()?),
                        ));
                    }
                    Some(MetadataColumnSpec::FilePath) => {
                        debug!("Inserting a file path column: {}", field.name());
                        reorder_indices.push(ReorderIndex::file_path(
                            requested_position,
                            Arc::new(field.try_into_arrow()?),
                        ));
                    }
                    Some(metadata_spec) => {
                        return Err(Error::Generic(format!(
                            "Metadata column {metadata_spec:?} is not supported by the default parquet reader"
                        )));
                    }
                    None if field.nullable => {
                        debug!("Inserting missing and nullable field: {}", field.name());
                        reorder_indices.push(ReorderIndex::missing(
                            requested_position,
                            Arc::new(field.try_into_arrow()?),
                        ));
                    }
                    None => {
                        return Err(Error::Generic(format!(
                            "Requested field not found in parquet schema, and field is not nullable: {}",
                            field.name()
                        )));
                    }
                }
            }
        }
    }
    Ok((
        parquet_offset + fields.len() - start_parquet_offset,
        reorder_indices,
    ))
}

/// Constructs an iterator where each parquet Field in `fields` is matched
/// with a a kernel `KernelFieldInfo` representing a StructField.
///
/// The iterator returned has a [`MatchedParquetField`] for each element in `parquet_fields`.
fn match_parquet_fields<'k, 'p>(
    kernel_schema: &'k StructType,
    parquet_fields: &'p ArrowFields,
) -> impl Iterator<Item = MatchedParquetField<'p, 'k>> {
    type FieldId = i64;

    // Lazily construct a map from the field id to its StructField name.
    let field_id_to_name: OnceLock<HashMap<FieldId, &String>> = OnceLock::new();
    let init_field_map = || {
        kernel_schema
            .fields()
            .filter_map(
                |field| match field.get_config_value(&ColumnMetadataKey::ParquetFieldId) {
                    Some(MetadataValue::Number(fid)) => Some((*fid, field.name())),
                    _ => None,
                },
            )
            .collect()
    };

    parquet_fields
        .iter()
        .enumerate()
        // move is used to take ownership of the `get_matching_kernel_field` closure so that the
        // iterator can be returned
        .map(move |(parquet_index, parquet_field)| {
            // Get the parquet field id
            let parquet_field_id = parquet_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|x| x.parse::<FieldId>().ok());

            // Get kernel field name by parquet field id if present. Otherwise fallback to using
            // parquet name.
            let field_name = parquet_field_id
                .and_then(|field_id| {
                    // If the fid to name map hasn't been initialized, construct it and get the
                    // field name
                    field_id_to_name
                        .get_or_init(init_field_map)
                        .get(&field_id)
                        .copied()
                })
                .unwrap_or_else(|| parquet_field.name());

            // Map the parquet ArrowField to the matching kernel KernelFieldInfo if present.
            let kernel_field_info =
                kernel_schema
                    .field_with_index(field_name)
                    .and_then(|(idx, field)| {
                        (!field.is_metadata_column()).then_some(KernelFieldInfo {
                            parquet_index: idx,
                            field,
                        })
                    });

            MatchedParquetField {
                parquet_index,
                parquet_field,
                kernel_field_info,
            }
        })
}

/// Get the indices in `parquet_schema` of the specified columns in `requested_schema`. This returns
/// a tuple of (mask_indices: Vec<parquet_schema_index>, reorder_indices:
/// Vec<requested_index>). `mask_indices` is used for generating the mask for reading from the
/// parquet file, and simply contains an entry for each index we wish to select from the parquet
/// file set to the index of the requested column in the parquet. `reorder_indices` is used for
/// re-ordering. See the documentation for [`ReorderIndex`] to understand what each element in the
/// returned array means.
#[internal_api]
pub(crate) fn get_requested_indices(
    requested_schema: &SchemaRef,
    parquet_schema: &ArrowSchemaRef,
) -> DeltaResult<(Vec<usize>, Vec<ReorderIndex>)> {
    let mut mask_indices = vec![];
    let (_, reorder_indexes) = get_indices(
        0,
        requested_schema,
        parquet_schema.fields(),
        &mut mask_indices,
    )?;
    Ok((mask_indices, reorder_indexes))
}

/// Create a mask that will only select the specified indices from the parquet. `indices` can be
/// computed from a [`Schema`] using [`get_requested_indices`]
#[internal_api]
pub(crate) fn generate_mask(
    _requested_schema: &SchemaRef,
    _parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indices: &[usize],
) -> Option<ProjectionMask> {
    // TODO: Determine if it's worth checking if we're selecting everything and returning None in
    // that case
    Some(ProjectionMask::leaves(
        parquet_physical_schema,
        indices.to_owned(),
    ))
}

/// Check if an ordering requires transforming the data in any way. This is true if the indices are
/// NOT in ascending order (so we have to reorder things), or if we need to do any transformation on
/// the data read from parquet. We check the ordering here, and also call
/// `ReorderIndex::needs_transform` on each element to check for other transforms, and to check
/// `Nested` variants recursively.
fn ordering_needs_transform(requested_ordering: &[ReorderIndex]) -> bool {
    if requested_ordering.is_empty() {
        return false;
    }
    // we have >=1 element. check that the first element doesn't need a transform
    if requested_ordering[0].needs_transform() {
        return true;
    }
    // Check for all elements if we need a transform. This is true if any elements are not in order
    // (i.e. element[i].index < element[i+1].index), or any element needs a transform
    requested_ordering
        .windows(2)
        .any(|ri| (ri[0].index >= ri[1].index) || ri[1].needs_transform())
}

/// Check if an ordering requires row index computation.
///
/// The function only checks if a RowIndex transform is present at the top-level, since metadata
/// columns are not allowed to be nested.
#[internal_api]
pub(crate) fn ordering_needs_row_indexes(requested_ordering: &[ReorderIndex]) -> bool {
    requested_ordering
        .iter()
        .any(|reorder_index| matches!(&reorder_index.transform, ReorderIndexTransform::RowIndex(_)))
}

// we use this as a placeholder for an array and its associated field. We can fill in a Vec of None
// of this type and then set elements of the Vec to Some(FieldArrayOpt) for each column
type FieldArrayOpt = Option<(Arc<ArrowField>, Arc<dyn ArrowArray>)>;

/// Creates an array for a missing field. For non-nullable structs, produces a non-null struct
/// (no null buffer) with recursively missing children, preserving the non-null constraint at
/// every level. For all other types (or nullable structs), produces an all-null array.
fn new_missing_array(field: &ArrowField, num_rows: usize) -> Arc<dyn ArrowArray> {
    match (field.is_nullable(), field.data_type()) {
        (false, ArrowDataType::Struct(child_fields)) => {
            let child_arrays: Vec<Arc<dyn ArrowArray>> = child_fields
                .iter()
                .map(|f| new_missing_array(f, num_rows))
                .collect();
            Arc::new(StructArray::new(child_fields.clone(), child_arrays, None))
        }
        _ => new_null_array(field.data_type(), num_rows),
    }
}

/// Reorder a RecordBatch to match `requested_ordering`. For each non-zero value in
/// `requested_ordering`, the column at that index will be added in order to the returned batch.
///
/// If the requested ordering contains a [`ReorderIndexTransform::RowIndex`], `row_indexes`
/// must not be `None` to append a row index column to the output.
/// If the requested ordering contains a [`ReorderIndexTransform::FilePath`], `file_location`
/// must not be `None` to append a file path column to the output.
pub(crate) fn reorder_struct_array(
    input_data: StructArray,
    requested_ordering: &[ReorderIndex],
    mut row_indexes: Option<&mut FlattenedRangeIterator<i64>>,
    file_location: Option<&str>,
) -> DeltaResult<StructArray> {
    debug!("Reordering {input_data:?} with ordering: {requested_ordering:?}");
    if !ordering_needs_transform(requested_ordering) {
        // indices is already sorted, meaning we requested in the order that the columns were
        // stored in the parquet
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        debug!("Have requested reorder {requested_ordering:#?} on {input_data:?}");
        let num_rows = input_data.len();
        let num_cols = requested_ordering.len();
        let (input_fields, input_cols, null_buffer) = input_data.into_parts();
        let mut final_fields_cols: Vec<FieldArrayOpt> = vec![None; num_cols];
        for (parquet_position, reorder_index) in requested_ordering.iter().enumerate() {
            // for each item, reorder_index.index() tells us where to put it, and its position in
            // requested_ordering tells us where it is in the parquet data
            match &reorder_index.transform {
                ReorderIndexTransform::Cast(target) => {
                    let col = input_cols[parquet_position].as_ref();
                    let col = Arc::new(crate::arrow::compute::cast(col, target)?);
                    let new_field = Arc::new(
                        input_fields[parquet_position]
                            .as_ref()
                            .clone()
                            .with_data_type(col.data_type().clone()),
                    );
                    final_fields_cols[reorder_index.index] = Some((new_field, col));
                }
                ReorderIndexTransform::Nested(children) => {
                    let input_field_name = input_fields[parquet_position].name();
                    match input_cols[parquet_position].data_type() {
                        ArrowDataType::Struct(_) => {
                            let struct_array = input_cols[parquet_position].as_struct().clone();
                            let result_array = Arc::new(reorder_struct_array(
                                struct_array,
                                children,
                                None, /* Nested structures don't need row indexes since metadata
                                       * columns can't be nested */
                                None, /* No file_location passed since metadata columns can't be
                                       * nested */
                            )?);
                            // create the new field specifying the correct order for the struct
                            let new_field = Arc::new(ArrowField::new_struct(
                                input_field_name,
                                result_array.fields().clone(),
                                input_fields[parquet_position].is_nullable(),
                            ));
                            final_fields_cols[reorder_index.index] =
                                Some((new_field, result_array));
                        }
                        ArrowDataType::List(_) => {
                            let list_array = input_cols[parquet_position].as_list::<i32>().clone();
                            final_fields_cols[reorder_index.index] =
                                reorder_list(list_array, input_field_name, children)?;
                        }
                        ArrowDataType::LargeList(_) => {
                            let list_array = input_cols[parquet_position].as_list::<i64>().clone();
                            final_fields_cols[reorder_index.index] =
                                reorder_list(list_array, input_field_name, children)?;
                        }
                        ArrowDataType::Map(_, _) => {
                            let map_array = input_cols[parquet_position].as_map().clone();
                            final_fields_cols[reorder_index.index] =
                                reorder_map(map_array, input_field_name, children)?;
                        }
                        _ => {
                            return Err(Error::internal_error(
                                "Nested reorder can only apply to struct/list/map.",
                            ));
                        }
                    }
                }
                ReorderIndexTransform::Identity => {
                    final_fields_cols[reorder_index.index] = Some((
                        input_fields[parquet_position].clone(), // cheap Arc clone
                        input_cols[parquet_position].clone(),   // cheap Arc clone
                    ));
                }
                ReorderIndexTransform::Missing(field) => {
                    let array = new_missing_array(field, num_rows);
                    final_fields_cols[reorder_index.index] = Some((field.clone(), array));
                }
                ReorderIndexTransform::RowIndex(field) => {
                    let Some(ref mut row_index_iter) = row_indexes else {
                        return Err(Error::generic(
                            "Row index column requested but row index iterator not provided",
                        ));
                    };
                    let row_index_array: PrimitiveArray<Int64Type> =
                        row_index_iter.take(num_rows).collect();
                    require!(
                        row_index_array.len() == num_rows,
                        Error::internal_error(
                            "Row index iterator exhausted before reaching the end of the file"
                        )
                    );
                    final_fields_cols[reorder_index.index] =
                        Some((Arc::clone(field), Arc::new(row_index_array)));
                }
                ReorderIndexTransform::FilePath(field) => {
                    let Some(file_path) = file_location else {
                        return Err(Error::generic(
                            "File path column requested but file location not provided",
                        ));
                    };
                    let file_path_array = StringArray::from(vec![file_path; num_rows]);
                    final_fields_cols[reorder_index.index] =
                        Some((Arc::clone(field), Arc::new(file_path_array)));
                }
            }
        }
        let num_cols = final_fields_cols.len();
        let (field_vec, reordered_columns): (Vec<Arc<ArrowField>>, _) =
            final_fields_cols.into_iter().flatten().unzip();
        if field_vec.len() != num_cols {
            Err(Error::internal_error("Found a None in final_fields_cols."))
        } else {
            Ok(StructArray::try_new(
                field_vec.into(),
                reordered_columns,
                null_buffer,
            )?)
        }
    }
}

fn reorder_list<O: OffsetSizeTrait>(
    list_array: GenericListArray<O>,
    input_field_name: &str,
    children: &[ReorderIndex],
) -> DeltaResult<FieldArrayOpt> {
    let (list_field, offset_buffer, maybe_sa, null_buf) = list_array.into_parts();
    if let Some(struct_array) = maybe_sa.as_struct_opt() {
        let struct_array = struct_array.clone();
        let result_array = Arc::new(reorder_struct_array(
            struct_array,
            children,
            None, /* Nested structures don't need row indexes since metadata columns can't be
                   * nested */
            None, // No file_location passed since metadata columns can't be nested
        )?);
        let new_list_field = Arc::new(ArrowField::new_struct(
            list_field.name(),
            result_array.fields().clone(),
            result_array.is_nullable(),
        ));
        let new_field = Arc::new(ArrowField::new_list(
            input_field_name,
            new_list_field.clone(),
            list_field.is_nullable(),
        ));
        let list = Arc::new(GenericListArray::try_new(
            new_list_field,
            offset_buffer,
            result_array,
            null_buf,
        )?);
        Ok(Some((new_field, list)))
    } else {
        Err(Error::internal_error(
            "Nested reorder of list should have had struct child.",
        ))
    }
}

fn reorder_map(
    map_array: MapArray,
    input_field_name: &str,
    children: &[ReorderIndex],
) -> DeltaResult<FieldArrayOpt> {
    let (map_field, offset_buffer, struct_array, null_buf, ordered) = map_array.into_parts();
    let result_array = reorder_struct_array(
        struct_array,
        children,
        None, // Nested structures don't need row indexes since metadata columns can't be nested
        None, // No file_location passed since metadata columns can't be nested
    )?;
    let result_fields = result_array.fields();
    let new_map_field = Arc::new(ArrowField::new_struct(
        map_field.name(),
        result_fields.clone(),
        result_array.is_nullable(),
    ));
    let key_field = result_fields[0].clone();
    let val_field = result_fields[1].clone();
    let new_field = Arc::new(ArrowField::new_map(
        input_field_name,
        map_field.name(),
        key_field,
        val_field,
        ordered,
        map_field.is_nullable(),
    ));
    let map = Arc::new(MapArray::try_new(
        new_map_field,
        offset_buffer,
        result_array,
        null_buf,
        ordered,
    )?);
    Ok(Some((new_field, map)))
}

/// Use this function to recursively compute properly unioned null masks for all nested
/// columns of a record batch, making it safe to project out and consume nested columns.
///
/// Arrow does not guarantee that the null masks associated with nested columns are accurate --
/// instead, the reader must consult the union of logical null masks the column and all
/// ancestors. The parquet reader stopped doing this automatically as of arrow-53.3, for example.
pub fn fix_nested_null_masks(batch: StructArray) -> StructArray {
    compute_nested_null_masks(batch, None)
}

/// Splits a StructArray into its parts, unions in the parent null mask, and uses the result to
/// recursively update the children as well before putting everything back together.
fn compute_nested_null_masks(sa: StructArray, parent_nulls: Option<&NullBuffer>) -> StructArray {
    let (fields, columns, nulls) = sa.into_parts();
    let nulls = NullBuffer::union(parent_nulls, nulls.as_ref());
    let columns = columns
        .into_iter()
        .map(|column| match column.data_type() {
            // NullArray (void columns) does not accept a null buffer — all values are
            // already null by definition, so propagating the parent null mask is a no-op.
            ArrowDataType::Null => column,
            ArrowDataType::Struct(_) => {
                let sa = column.as_struct();
                Arc::new(compute_nested_null_masks(sa.clone(), nulls.as_ref())) as _
            }
            _ => {
                let data = column.to_data();
                let nulls = NullBuffer::union(nulls.as_ref(), data.nulls());
                let builder = data.into_builder().nulls(nulls);
                // Use an unchecked build to avoid paying a redundant O(k) validation cost for a
                // `RecordBatch` with k leaf columns.
                //
                // SAFETY: The builder was constructed from an `ArrayData` we extracted from the
                // column. The change we make is the null buffer, via `NullBuffer::union` with input
                // null buffers that were _also_ extracted from the column and its parent. A union
                // can only _grow_ the set of NULL rows, so data validity is preserved. Even if the
                // `parent_nulls` somehow had a length mismatch --- which it never should, having
                // also been extracted from our grandparent --- the mismatch would have already
                // caused `NullBuffer::union` to panic.
                let data = unsafe { builder.build_unchecked() };
                make_array(data)
            }
        })
        .collect();

    // Use an unchecked constructor to avoid paying O(n*k) a redundant null buffer validation cost
    // for a `RecordBatch` with n rows and k leaf columns.
    //
    // SAFETY: We are simply reassembling the input `StructArray` we previously broke apart, with
    // updated null buffers. See above for details about null buffer safety.
    unsafe { StructArray::new_unchecked(fields, columns, nulls) }
}

/// Parse a column of JSON strings into a typed `RecordBatch` matching `schema`. N input
/// rows produce N output rows.
///
/// Arrow lacks the functionality to json-parse a string column into a struct column, so we
/// implement it here.
///
/// Failure-prone primitive leaves (`Timestamp`, `TimestampNtz`, `Date`, `Decimal`) produce
/// per-cell NULL when the typed decoder rejects a value (extended-year timestamps,
/// decimals that overflow the declared precision, etc.). Other leaf type mismatches still
/// surface as batch-level errors.
#[internal_api]
pub(crate) fn parse_json(
    json_strings: Box<dyn EngineData>,
    schema: SchemaRef,
) -> DeltaResult<Box<dyn EngineData>> {
    let json_strings: RecordBatch = ArrowEngineData::try_from_engine_data(json_strings)?.into();
    let result = parse_json_impl(json_strings.column(0).as_ref(), schema)?;
    Ok(Box::new(ArrowEngineData::new(result)))
}

/// Raw implementation of [`parse_json`]; see there for the per-cell NULL contract.
///
/// Accepts any string array type (`StringArray`, `LargeStringArray`, `StringViewArray`) to
/// avoid narrowing casts that could overflow.
pub(crate) fn parse_json_impl(
    json_strings: &dyn ArrowArray,
    schema: SchemaRef,
) -> DeltaResult<RecordBatch> {
    let num_rows = json_strings.len();
    match json_strings.data_type() {
        ArrowDataType::Utf8 => {
            parse_json_inner(json_strings.as_string::<i32>().iter(), num_rows, schema)
        }
        ArrowDataType::LargeUtf8 => {
            parse_json_inner(json_strings.as_string::<i64>().iter(), num_rows, schema)
        }
        ArrowDataType::Utf8View => {
            parse_json_inner(json_strings.as_string_view().iter(), num_rows, schema)
        }
        dt => Err(Error::generic(format!(
            "Expected string array for JSON parsing, got {dt}"
        ))),
    }
}

fn parse_json_inner<'a>(
    json_strings: impl Iterator<Item = Option<&'a str>>,
    num_rows: usize,
    schema: SchemaRef,
) -> DeltaResult<RecordBatch> {
    // arrow-json's typed Timestamp/TimestampNtz/Date/Decimal decoders fail the entire batch
    // on a single bad cell, so rewrite those leaves to `String` first and safe-cast back to
    // the target type. `Cow::Borrowed` means nothing was rewritten; skip the cast pass.
    match StringifyFailureProneLeaves.transform_struct(schema.as_ref()) {
        Cow::Borrowed(_) => {
            let arrow_target = Arc::new(ArrowSchema::try_from_kernel(schema.as_ref())?);
            decode_with_arrow_json(json_strings, num_rows, arrow_target)
        }
        Cow::Owned(relaxed) => {
            let arrow_target = Arc::new(ArrowSchema::try_from_kernel(schema.as_ref())?);
            let arrow_relaxed = Arc::new(ArrowSchema::try_from_kernel(&relaxed)?);
            let decoded = decode_with_arrow_json(json_strings, num_rows, arrow_relaxed)?;
            safe_cast_back(decoded, &arrow_target)
        }
    }
}

/// Runs arrow-json's typed `Decoder` against the given schema and returns the resulting
/// `RecordBatch`. Each input string must contain exactly one JSON object; missing inputs
/// (`None`) decode as `{}` so the row stays present with all-NULL fields.
fn decode_with_arrow_json<'a>(
    json_strings: impl Iterator<Item = Option<&'a str>>,
    num_rows: usize,
    schema: ArrowSchemaRef,
) -> DeltaResult<RecordBatch> {
    if num_rows == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut decoder = ReaderBuilder::new(schema)
        .with_batch_size(num_rows)
        .with_coerce_primitive(true)
        .build_decoder()?;

    for (json, row_number) in json_strings.zip(1..) {
        let line = json.unwrap_or("{}");
        let consumed = decoder.decode(line.as_bytes())?;
        // did we fail to decode the whole line, or was the line partial
        if consumed != line.len() || decoder.has_partial_record() {
            return Err(Error::Generic(format!(
                "Malformed JSON: Multiple, partial, or 0 JSON objects on row {row_number}"
            )));
        }
        // did we decode exactly one record
        if decoder.len() != row_number {
            return Err(Error::Generic(format!(
                "Malformed JSON: Multiple, partial, or 0 JSON objects on row {row_number}"
            )));
        }
    }
    // Get the final batch out
    if let Some(batch) = decoder.flush()? {
        if batch.num_rows() != num_rows {
            return Err(Error::Generic(format!(
                "Unexpected number of rows decoded. Got {}, expected{}",
                batch.num_rows(),
                num_rows
            )));
        }
        return Ok(batch);
    }
    Err(Error::generic(
        "Malformed JSON: exited parse_json_impl without deserializing anything useful",
    ))
}

/// Rewrites failure-prone primitives (`Timestamp`, `TimestampNtz`, `Date`, `Decimal`) to
/// `String` so the typed decoder accepts any well-formed JSON string for those cells.
///
/// `Array`/`Map`/`Variant` are not visited: Delta doesn't track min/max stats for them,
/// so a failure-prone leaf only ever shows up inside a `Struct` for our callers.
struct StringifyFailureProneLeaves;

impl<'a> SchemaTransform<'a> for StringifyFailureProneLeaves {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Cow<'a, PrimitiveType> {
        use PrimitiveType::*;
        match ptype {
            Timestamp | TimestampNtz | Date | Decimal(_) => Cow::Owned(String),
            _ => Cow::Borrowed(ptype),
        }
    }

    fn transform_array(&mut self, atype: &'a ArrayType) -> Cow<'a, ArrayType> {
        Cow::Borrowed(atype)
    }

    fn transform_map(&mut self, mtype: &'a MapType) -> Cow<'a, MapType> {
        Cow::Borrowed(mtype)
    }

    fn transform_variant(&mut self, stype: &'a StructType) -> Cow<'a, StructType> {
        Cow::Borrowed(stype)
    }
}

/// Safe-casts each column of `decoded` back to its target type. `safe: true` produces
/// per-cell NULL on parse failure rather than failing the whole batch.
fn safe_cast_back(decoded: RecordBatch, target: &ArrowSchemaRef) -> DeltaResult<RecordBatch> {
    let opts = CastOptions {
        safe: true,
        ..Default::default()
    };
    let (_, columns, row_count) = decoded.into_parts();
    let columns = columns
        .into_iter()
        .zip(target.fields().iter())
        .map(|(arr, field)| safe_cast_array(arr, field.data_type(), &opts))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

/// Recursive worker for [`safe_cast_back`]. Recurses through `Struct` containers preserving
/// the parent null buffer; applies `cast_with_options` at the leaves.
///
/// Delta doesn't track min/max stats for `Array`/`Map`/`Variant`, so a failure-prone leaf
/// only ever reaches here inside a `Struct`.
fn safe_cast_array(
    array: ArrowArrayRef,
    target: &ArrowDataType,
    opts: &CastOptions<'_>,
) -> DeltaResult<ArrowArrayRef> {
    if array.data_type() == target {
        return Ok(array);
    }
    match target {
        ArrowDataType::Struct(target_fields) => {
            let s = array.as_struct();
            let nulls = s.nulls().cloned();
            let new_children = s
                .columns()
                .iter()
                .zip(target_fields.iter())
                .map(|(c, f)| safe_cast_array(c.clone(), f.data_type(), opts))
                .collect::<DeltaResult<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                new_children,
                nulls,
            )?))
        }
        _ => Ok(cast_with_options(&array, target, opts)?),
    }
}

pub(crate) fn filter_to_record_batch(
    filtered_data: FilteredEngineData,
) -> DeltaResult<RecordBatch> {
    let filtered = filtered_data.apply_selection_vector()?;
    let arrow_data = ArrowEngineData::try_from_engine_data(filtered)?;
    Ok((*arrow_data).into())
}

// we want to keep nulls in our partition map, so we end up with data in the log like:
// {partitionValues:{"foo": null}}, which is what is generally expected. Without this we would
// get: {partitionValues:{}}
struct NullValueMapEncoder<'a> {
    field: &'a ArrowFieldRef,
    array: &'a MapArray,
}

impl<'a> Encoder for NullValueMapEncoder<'a> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let options = EncoderOptions::default().with_explicit_nulls(true);
        // this unwrap is technically unsafe, but we _know_ that the array is a MapArray, and that
        // `make_encoder` won't return an error for that. It would still be nice if we could return
        // a `Result`, but we cannot
        #[allow(clippy::unwrap_used)]
        let mut encoder = make_encoder(self.field, self.array, &options).unwrap();
        encoder.encode(idx, out);
    }
}

/// This is a special encoder factory that will use the default encoder for all array types except
/// MapArrays. For MapArrays, it will make a `NullValueMapEncoder` which encodes the map preserving
/// keys that have null values.
#[derive(Debug)]
struct NullValueMapEncoderFactory;

impl EncoderFactory for NullValueMapEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a ArrowFieldRef,
        array: &'a dyn ArrowArray,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, crate::arrow::error::ArrowError> {
        // It would be tempting to use `make_encoder` below, but we can't because we have to create
        // a new `EncoderOptions` in order to set `with_explicit_nulls`. Then the lifetime of the
        // created encoder becomes tied to the lifetime of the `EncoderOptions`, and we cannot
        // return it from this method as the options would be freed here.  We _also_ can't put the
        // options inside the NullValueMapEncoderFactory, because this method takes `&self` not
        // `&'a self`, and we can't change that as it's part of the trait definition.
        match array.data_type() {
            ArrowDataType::Map(_, _) => {
                let array = array.as_map();
                let encoder = NullValueMapEncoder { field, array };
                let array_encoder = Box::new(encoder) as Box<dyn Encoder + 'a>;
                let nulls = array.nulls().cloned();
                Ok(Some(NullableEncoder::new(array_encoder, nulls)))
            }
            _ => Ok(None),
        }
    }
}

/// serialize an arrow RecordBatch to a JSON string by appending to a buffer.
// TODO (zach): this should stream data to the JSON writer and output an iterator.
#[internal_api]
pub(crate) fn to_json_bytes(
    data: impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send,
) -> DeltaResult<Vec<u8>> {
    let builder = WriterBuilder::new().with_encoder_factory(Arc::new(NullValueMapEncoderFactory));
    let mut writer = builder.build::<_, LineDelimited>(Vec::new());
    for chunk in data {
        let batch = filter_to_record_batch(chunk?)?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner())
}

/// Applies post-processing to data read from a JSON file. Inserts synthesized metadata columns
/// (e.g. [`MetadataColumnSpec::FilePath`]) at the positions specified by `reorder_indices`.
///
/// `reorder_indices` should be built once per schema via [`build_json_reorder_indices`] and
/// reused for every batch from the same file.
#[internal_api]
pub(crate) fn fixup_json_read(
    batch: RecordBatch,
    reorder_indices: &[ReorderIndex],
    file_location: &str,
) -> DeltaResult<ArrowEngineData> {
    let data = reorder_struct_array(batch.into(), reorder_indices, None, Some(file_location))?;
    Ok(data.into())
}

/// Builds the [`ReorderIndex`] vec for post-processing JSON read batches.
///
/// The JSON reader is given a schema with metadata columns stripped (see [`json_arrow_schema`]).
/// Its output therefore has non-metadata columns at contiguous indices 0..N in schema order.
/// This function maps those source indices -- and any metadata column specs -- into a
/// `Vec<ReorderIndex>` that `reorder_struct_array` can use to produce the final batch with
/// every column at its correct position.
///
/// Build the index vec once per schema (e.g. once per file); apply it to every batch produced
/// by the reader via `reorder_struct_array`.
///
/// # Companion function
/// - Use [`json_arrow_schema`] to strip metadata columns before passing the schema to the JSON
///   reader.
#[internal_api]
pub(crate) fn build_json_reorder_indices(schema: &StructType) -> DeltaResult<Vec<ReorderIndex>> {
    // Real columns: position in reorder_indices IS the source column index (0..N in schema
    // order), and reorder_index.index carries the output position.
    let mut reorder_indices = Vec::with_capacity(schema.num_fields());
    // Metadata columns are appended after all real columns. reorder_struct_array never reads
    // source data for metadata transforms, so their vec position doesn't correspond to a source
    // column. Unsupported specs use Missing (null fill); non-nullable violations surface
    // naturally via StructArray::try_new.
    let mut metadata_entries = Vec::new();

    for (output_pos, field) in schema.fields().enumerate() {
        match field.get_metadata_column_spec() {
            None => reorder_indices.push(ReorderIndex::identity(output_pos)),
            Some(spec) => metadata_entries.push((output_pos, field, spec)),
        }
    }

    for (output_pos, field, spec) in metadata_entries {
        let field = Arc::new(field.try_into_arrow()?);
        let rindex = match spec {
            MetadataColumnSpec::FilePath => ReorderIndex::file_path(output_pos, field),
            _ => ReorderIndex::missing(output_pos, field),
        };
        reorder_indices.push(rindex);
    }

    Ok(reorder_indices)
}

/// Builds an Arrow [`ArrowSchema`] from `schema` containing only the "real" JSON columns,
/// omitting any fields annotated with [`MetadataColumnSpec`].
///
/// Pass the returned schema to Arrow's JSON reader; then call [`build_json_reorder_indices`]
/// once on the same schema and apply `reorder_struct_array` to each resulting batch to
/// insert the synthesized metadata columns at their correct positions.
#[internal_api]
pub(crate) fn json_arrow_schema(schema: &StructType) -> DeltaResult<ArrowSchema> {
    let json_fields = schema.with_fields_filtered(|f| f.get_metadata_column_spec().is_none())?;
    Ok(ArrowSchema::try_from_kernel(&json_fields)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{
        Array, ArrayRef as ArrowArrayRef, AsArray, BooleanArray, GenericListArray, Int32Array,
        Int32Builder, Int64Array, MapArray, MapBuilder, StringArray, StringBuilder, StructArray,
        StructBuilder,
    };
    use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
    };
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataColumnSpec, MetadataValue,
        StructField, StructType,
    };
    use crate::table_features::ColumnMappingMode;
    use crate::utils::test_utils::assert_result_error_with_message;

    fn column_mapping_cases() -> [ColumnMappingMode; 3] {
        [
            ColumnMappingMode::Id,
            ColumnMappingMode::Name,
            ColumnMappingMode::None,
        ]
    }

    /// Generates the logical name for a field given its id.
    /// This is "logical-{fieldId}".
    fn logical_name(field_id: i64) -> String {
        format!("logical-{field_id}")
    }

    /// Generates the physical name for a field given its id.
    /// This is "physical-{fieldId}".
    fn physical_name(field_id: i64) -> String {
        format!("physical-{field_id}")
    }

    /// Generates the name that should be written to parquet from the field id.
    /// This is the physical name for Id/Name modes, and logical name for None mode.
    fn parquet_name(field_id: i64, mode: ColumnMappingMode) -> String {
        match mode {
            ColumnMappingMode::Id | ColumnMappingMode::Name => physical_name(field_id),
            ColumnMappingMode::None => logical_name(field_id),
        }
    }

    /// Generates the column mapping metadata for a logical struct field given the field id.
    /// Returns empty metadata for `None` mode, since no annotations should be present.
    fn column_mapping_metadata(
        field_id: i64,
        mode: ColumnMappingMode,
    ) -> HashMap<String, MetadataValue> {
        match mode {
            ColumnMappingMode::None => HashMap::new(),
            _ => kernel_fid_and_name(field_id, physical_name(field_id)),
        }
    }

    /// Generates metadata for a parquet field with id `field_id`.
    fn arrow_fid(field_id: i64) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())])
    }

    /// Generates appropriate column mapping metadata for a kernel struct field with column mapping
    /// id `field_id`.
    fn kernel_fid_and_name(field_id: i64, name: impl AsRef<str>) -> HashMap<String, MetadataValue> {
        HashMap::from([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                field_id.into(),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                name.as_ref().to_string().into(),
            ),
        ])
    }

    /// Helper function to create mock row group metadata for testing
    fn create_mock_row_group(num_rows: i64) -> RowGroupMetaData {
        use crate::parquet::basic::{Encoding, Type as PhysicalType};
        use crate::parquet::file::metadata::ColumnChunkMetaData;
        use crate::parquet::schema::types::Type;

        // Create a minimal schema descriptor
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("test_col", PhysicalType::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        )));

        // Create a minimal column chunk metadata
        let column_chunk = ColumnChunkMetaData::builder(schema.column(0))
            .set_encodings(vec![Encoding::PLAIN])
            .set_total_compressed_size(100)
            .set_total_uncompressed_size(100)
            .set_num_values(num_rows)
            .build()
            .unwrap();

        RowGroupMetaData::builder(schema)
            .set_num_rows(num_rows)
            .set_total_byte_size(100)
            .set_column_metadata(vec![column_chunk])
            .build()
            .unwrap()
    }

    #[test]
    fn test_json_parsing() {
        static EXPECTED_JSON_ERR_STR: &str = "Generic delta kernel error: Malformed JSON: Multiple, partial, or 0 JSON objects on row";
        fn check_parse_fails(input: Vec<Option<&str>>, schema: SchemaRef, expected_start: &str) {
            let result = parse_json_impl(&StringArray::from(input), schema);
            let err = result.expect_err("Expected an error");
            let msg = err.to_string();
            assert!(
                msg.starts_with(expected_start),
                "Error message was not what was expected"
            );
        }

        let requested_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
                StructField::nullable("c", DataType::INTEGER),
            ])
            .unwrap(),
        );
        let input: Vec<&str> = vec![];
        let result = parse_json_impl(&StringArray::from(input), requested_schema.clone()).unwrap();
        assert_eq!(result.num_rows(), 0);

        for input in [
            vec![Some("")],
            vec![Some(" \n\t")],
            vec![Some(r#"{ "a": 1"#)],
            vec![Some("{}{}")],
            vec![Some(r#"{} { "a": 1"#)],
            vec![Some(r#"{} { "a": 1"#), Some("}")],
            vec![Some(r#"{ "a": 1"#), Some(r#", "b": "b"}"#)],
        ] {
            check_parse_fails(input, requested_schema.clone(), EXPECTED_JSON_ERR_STR);
        }

        // this one is an error from within the tape decoder, so has a different format
        check_parse_fails(
            vec![Some(r#""a""#)],
            requested_schema.clone(),
            "Json error: expected { got \"a\"",
        );

        let input: Vec<Option<&str>> = vec![None, Some(r#"{"a": 1, "b": "2", "c": 3}"#), None];
        let result = parse_json_impl(&StringArray::from(input), requested_schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.column(0).null_count(), 2);
        assert_eq!(result.column(1).null_count(), 2);
        assert_eq!(result.column(2).null_count(), 2);
    }

    #[test]
    fn test_parse_json_with_long_strings() {
        // See issue#1139: https://github.com/delta-io/delta-kernel-rs/issues/1139
        let schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("long_val", DataType::STRING)]).unwrap(),
        );
        let long_string = "a".repeat(1_000_000); // 1MB string
        let json_string = format!(r#"{{"long_val": "{long_string}"}}"#);
        let input: Vec<Option<&str>> = vec![Some(&json_string)];

        let batch = parse_json_impl(&StringArray::from(input), schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let long_col = batch.column(0).as_string::<i32>();
        assert_eq!(long_col.value(0), long_string);
    }

    #[test]
    fn test_parse_json_large_string_array() {
        // See issue#1923: parse_json should handle LargeStringArray (64-bit offsets)
        use crate::arrow::array::LargeStringArray;
        use crate::engine::arrow_data::ArrowEngineData;

        let large_strings = LargeStringArray::from(vec![
            Some(r#"{"a": 1, "b": "hello"}"#),
            None,
            Some(r#"{"a": 3, "b": "world"}"#),
        ]);
        let field = Arc::new(ArrowField::new("s", ArrowDataType::LargeUtf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(large_strings) as ArrowArrayRef]).unwrap();
        let engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(batch));

        let output_schema: crate::schema::SchemaRef = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ]));
        let result = parse_json(engine_data, output_schema).unwrap();
        let result = ArrowEngineData::try_from_engine_data(result).unwrap();
        let batch: RecordBatch = result.into();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.column(0).null_count(), 1);
        assert_eq!(batch.column(1).null_count(), 1);
    }

    #[test]
    fn test_parse_json_string_view_array() {
        use crate::arrow::array::StringViewArray;
        use crate::engine::arrow_data::ArrowEngineData;

        let view_strings = StringViewArray::from(vec![
            Some(r#"{"a": 1, "b": "hello"}"#),
            None,
            Some(r#"{"a": 3, "b": "world"}"#),
        ]);
        let field = Arc::new(ArrowField::new("s", ArrowDataType::Utf8View, true));
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(view_strings) as ArrowArrayRef]).unwrap();
        let engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(batch));

        let output_schema: crate::schema::SchemaRef = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ]));
        let result = parse_json(engine_data, output_schema).unwrap();
        let result = ArrowEngineData::try_from_engine_data(result).unwrap();
        let batch: RecordBatch = result.into();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.column(0).null_count(), 1);
        assert_eq!(batch.column(1).null_count(), 1);
    }

    #[test]
    fn test_parse_json_rejects_non_string_array() {
        use crate::engine::arrow_data::ArrowEngineData;

        let int_array = Int32Array::from(vec![1, 2, 3]);
        let field = Arc::new(ArrowField::new("s", ArrowDataType::Int32, true));
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(int_array) as ArrowArrayRef]).unwrap();
        let engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(batch));

        let output_schema: crate::schema::SchemaRef =
            Arc::new(StructType::new_unchecked(vec![StructField::nullable(
                "a",
                DataType::INTEGER,
            )]));
        let err = match parse_json(engine_data, output_schema) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("Expected error for non-string array input"),
        };
        assert!(
            err.contains("Expected string array for JSON parsing"),
            "Unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_json_impl_strict_leaf_errors_propagate() {
        // Type mismatches on strict (non-failure-prone) leaves still surface as batch-level
        // errors, so the expression-level caller can fall back to its all-null backstop.
        let schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("a", DataType::LONG)]).unwrap(),
        );
        let input: Vec<Option<&str>> = vec![Some(r#"{"a": "not_a_number"}"#)];
        assert!(parse_json_impl(&StringArray::from(input), schema).is_err());
    }

    // === Per-cell NULL on failure-prone leaf parse failures ===

    /// Parses `inputs` against a single-column schema `{column_name: leaf_type}` and asserts
    /// that rows in `expected_null_rows` are NULL while every other row is non-null.
    fn assert_per_cell_null_isolation(
        column_name: &str,
        leaf_type: DataType,
        inputs: &[&str],
        expected_null_rows: &[usize],
    ) {
        let schema = Arc::new(
            StructType::try_new(vec![StructField::nullable(column_name, leaf_type)]).unwrap(),
        );
        let inputs: Vec<Option<&str>> = inputs.iter().copied().map(Some).collect();
        let batch = parse_json_impl(&StringArray::from(inputs.clone()), schema)
            .expect("parse_json_impl should not error on failure-prone leaf parse failures");
        assert_eq!(batch.num_rows(), inputs.len());
        let col = batch.column(0);
        assert_eq!(
            col.null_count(),
            expected_null_rows.len(),
            "unexpected null count for column {column_name}",
        );
        for row in 0..inputs.len() {
            let want_null = expected_null_rows.contains(&row);
            assert_eq!(
                col.is_null(row),
                want_null,
                "row {row} of column {column_name}: expected is_null={want_null}",
            );
        }
    }

    /// Per-cell NULL isolation across the four failure-prone leaf types. Each case feeds
    /// a 3-row single-column batch where row 1 is malformed in a way that defeats
    /// arrow-json's typed decoder; rows 0 and 2 must round-trip to valid typed cells.
    #[rstest]
    #[case::timestamp_extended_year(
        DataType::TIMESTAMP,
        &[
            r#"{"v": "2024-01-01T00:00:00Z"}"#,
            r#"{"v": "+48690-07-02T22:50:38.211Z"}"#,
            r#"{"v": "2025-06-01T00:00:00Z"}"#,
        ],
    )]
    #[case::timestamp_ntz_garbage(
        DataType::TIMESTAMP_NTZ,
        &[
            r#"{"v": "2024-01-01T00:00:00"}"#,
            r#"{"v": "not-a-timestamp"}"#,
            r#"{"v": "2025-06-01T00:00:00"}"#,
        ],
    )]
    #[case::date_out_of_range_month(
        DataType::DATE,
        &[
            r#"{"v": "2024-01-01"}"#,
            r#"{"v": "2024-13-01"}"#,
            r#"{"v": "2025-06-30"}"#,
        ],
    )]
    #[case::decimal_overflow(
        DataType::decimal(10, 2).unwrap(),
        &[
            r#"{"v": "10.50"}"#,
            r#"{"v": "99999999999.99"}"#,
            r#"{"v": "5.25"}"#,
        ],
    )]
    fn test_parse_json_safe_cast_per_cell_null(
        #[case] leaf_type: DataType,
        #[case] inputs: &[&str],
    ) {
        assert_per_cell_null_isolation("v", leaf_type, inputs, &[1]);
    }

    #[test]
    fn test_parse_json_safe_cast_intermixed_struct_per_cell_isolation() {
        // Single struct with mixed failure-prone and strict leaves. The bad EventTime on row 1
        // must not contaminate IngestTime / Price / UserId on the same row, nor any field on
        // rows 0 / 2. This is the per-cell isolation property end-to-end.
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("EventTime", DataType::TIMESTAMP),
                StructField::nullable("IngestTime", DataType::TIMESTAMP),
                StructField::nullable("Price", DataType::decimal(10, 2).unwrap()),
                StructField::nullable("UserId", DataType::LONG),
            ])
            .unwrap(),
        );
        let inputs: Vec<Option<&str>> = vec![
            Some(
                r#"{"EventTime": "2024-01-01T00:00:00Z", "IngestTime": "2024-01-01T00:00:01Z", "Price": "10.50", "UserId": 1}"#,
            ),
            Some(
                r#"{"EventTime": "+48690-07-02T22:50:38.211Z", "IngestTime": "2024-06-01T00:00:01Z", "Price": "20.00", "UserId": 2}"#,
            ),
            Some(
                r#"{"EventTime": "2025-06-01T00:00:00Z", "IngestTime": "2025-06-01T00:00:01Z", "Price": "30.75", "UserId": 3}"#,
            ),
        ];
        let batch = parse_json_impl(&StringArray::from(inputs.clone()), schema).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let event_time = batch.column_by_name("EventTime").unwrap();
        let ingest_time = batch.column_by_name("IngestTime").unwrap();
        let price = batch.column_by_name("Price").unwrap();
        let user_id = batch.column_by_name("UserId").unwrap();

        assert!(!event_time.is_null(0) && event_time.is_null(1) && !event_time.is_null(2));
        assert_eq!(event_time.null_count(), 1);

        for col in [&ingest_time, &price, &user_id] {
            assert_eq!(
                col.null_count(),
                0,
                "row 1's bad EventTime should not contaminate other fields in the same row"
            );
        }
    }

    #[test]
    fn test_parse_json_safe_cast_nested_stats_shape() {
        // Mirrors the Delta stats StructType:
        //   { numRecords: Long, nullCount: { EventTime: Long, UserId: Long },
        //     minValues: { EventTime: Timestamp, UserId: Long },
        //     maxValues: { EventTime: Timestamp, UserId: Long },
        //     tightBounds: Bool }
        let null_count_struct = StructType::try_new(vec![
            StructField::nullable("EventTime", DataType::LONG),
            StructField::nullable("UserId", DataType::LONG),
        ])
        .unwrap();
        let min_max_struct = StructType::try_new(vec![
            StructField::nullable("EventTime", DataType::TIMESTAMP),
            StructField::nullable("UserId", DataType::LONG),
        ])
        .unwrap();
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("numRecords", DataType::LONG),
                StructField::nullable("nullCount", null_count_struct),
                StructField::nullable("minValues", min_max_struct.clone()),
                StructField::nullable("maxValues", min_max_struct),
                StructField::nullable("tightBounds", DataType::BOOLEAN),
            ])
            .unwrap(),
        );

        let inputs: Vec<Option<&str>> = vec![
            Some(
                r#"{"numRecords": 10, "nullCount": {"EventTime": 0, "UserId": 0},
                    "minValues": {"EventTime": "2024-01-01T00:00:00Z", "UserId": 1},
                    "maxValues": {"EventTime": "2024-01-31T00:00:00Z", "UserId": 100},
                    "tightBounds": true}"#,
            ),
            Some(
                r#"{"numRecords": 20, "nullCount": {"EventTime": 0, "UserId": 0},
                    "minValues": {"EventTime": "+48690-07-02T22:50:38.211Z", "UserId": 5},
                    "maxValues": {"EventTime": "+48690-07-02T22:50:38.211Z", "UserId": 200},
                    "tightBounds": true}"#,
            ),
            Some(
                r#"{"numRecords": 30, "nullCount": {"EventTime": 0, "UserId": 0},
                    "minValues": {"EventTime": "2025-01-01T00:00:00Z", "UserId": 10},
                    "maxValues": {"EventTime": "2025-12-31T00:00:00Z", "UserId": 300},
                    "tightBounds": true}"#,
            ),
        ];
        let batch = parse_json_impl(&StringArray::from(inputs), schema).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let num_records = batch.column_by_name("numRecords").unwrap();
        assert_eq!(num_records.null_count(), 0);

        let min_values = batch.column_by_name("minValues").unwrap().as_struct();
        let max_values = batch.column_by_name("maxValues").unwrap().as_struct();
        let min_event = min_values.column_by_name("EventTime").unwrap();
        let max_event = max_values.column_by_name("EventTime").unwrap();
        let min_user = min_values.column_by_name("UserId").unwrap();
        let max_user = max_values.column_by_name("UserId").unwrap();

        // EventTime min/max for row 1 must be NULL; rows 0 and 2 must be populated.
        assert!(!min_event.is_null(0) && min_event.is_null(1) && !min_event.is_null(2));
        assert!(!max_event.is_null(0) && max_event.is_null(1) && !max_event.is_null(2));
        // UserId min/max stay populated on every row even when the sibling EventTime fails.
        assert_eq!(min_user.null_count(), 0);
        assert_eq!(max_user.null_count(), 0);
    }

    #[test]
    fn test_parse_json_safe_cast_all_null_input() {
        // Every row decodes to `{}` (the unwrap_or default for `None`), so every output cell
        // is NULL and no error surfaces from the safe-cast pass.
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("ts", DataType::TIMESTAMP),
                StructField::nullable("n", DataType::LONG),
            ])
            .unwrap(),
        );
        let inputs: Vec<Option<&str>> = vec![None, None, None];
        let batch = parse_json_impl(&StringArray::from(inputs), schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
        for col in batch.columns() {
            assert_eq!(col.null_count(), 3);
        }
    }

    #[test]
    fn simple_mask_indices() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::nullable(logical_name(1), DataType::STRING)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::nullable(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Utf8, true)
                    .with_metadata(arrow_fid(1)),
                ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, true)
                    .with_metadata(arrow_fid(2)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::identity(1),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn test_variant_masks() {
        fn unshredded_variant_parquet_schema() -> ArrowField {
            ArrowField::new(
                "v",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("metadata", ArrowDataType::Binary, false),
                        ArrowField::new("value", ArrowDataType::Binary, false),
                    ]
                    .into(),
                ),
                true,
            )
        }
        fn shredded_variant_parquet_schema() -> ArrowField {
            ArrowField::new(
                "v",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("metadata", ArrowDataType::Binary, false),
                        ArrowField::new("value", ArrowDataType::Binary, true),
                        ArrowField::new("typed_value", ArrowDataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            )
        }
        fn incorrect_variant_parquet_schema() -> ArrowField {
            ArrowField::new(
                "v",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("field1", ArrowDataType::Binary, false),
                        ArrowField::new("field2", ArrowDataType::Binary, false),
                    ]
                    .into(),
                ),
                true,
            )
        }
        fn scalar_variant_parquet_schema() -> ArrowField {
            ArrowField::new("v", ArrowDataType::Int16, true)
        }
        // Top level variant
        let requested_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "v",
            DataType::unshredded_variant(),
        )]));
        let unshredded_parquet_schema =
            Arc::new(ArrowSchema::new(vec![unshredded_variant_parquet_schema()]));
        let shredded_parquet_schema =
            Arc::new(ArrowSchema::new(vec![shredded_variant_parquet_schema()]));
        let incorrect_parquet_schema =
            Arc::new(ArrowSchema::new(vec![incorrect_variant_parquet_schema()]));
        let scalar_parquet_schema =
            Arc::new(ArrowSchema::new(vec![scalar_variant_parquet_schema()]));
        let result_unshredded =
            get_requested_indices(&requested_schema, &unshredded_parquet_schema);
        assert!(result_unshredded.is_ok());
        let result_shredded = get_requested_indices(&requested_schema, &shredded_parquet_schema);
        assert!(matches!(result_shredded,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));
        let result_incorrect = get_requested_indices(&requested_schema, &incorrect_parquet_schema);
        assert!(matches!(result_incorrect,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));
        let result_scalar = get_requested_indices(&requested_schema, &scalar_parquet_schema);
        assert!(matches!(result_scalar,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));

        // Struct of Variant
        let requested_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "struct_v",
            StructType::new_unchecked([StructField::nullable("v", DataType::unshredded_variant())]),
        )]));
        let unshredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "struct_v",
            ArrowDataType::Struct(vec![unshredded_variant_parquet_schema()].into()),
            true,
        )]));
        let shredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "struct_v",
            ArrowDataType::Struct(vec![shredded_variant_parquet_schema()].into()),
            true,
        )]));
        let result_unshredded =
            get_requested_indices(&requested_schema, &unshredded_parquet_schema);
        let result_shredded = get_requested_indices(&requested_schema, &shredded_parquet_schema);
        assert!(result_unshredded.is_ok());
        assert!(matches!(result_shredded,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));
        // Array of Variant
        let requested_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "array_v",
            ArrayType::new(DataType::unshredded_variant(), true),
        )]));
        let unshredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "array_v",
            ArrowDataType::List(Arc::new(unshredded_variant_parquet_schema())),
            true,
        )]));
        let shredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "array_v",
            ArrowDataType::List(Arc::new(shredded_variant_parquet_schema())),
            true,
        )]));
        let result_unshredded =
            get_requested_indices(&requested_schema, &unshredded_parquet_schema);
        let result_shredded = get_requested_indices(&requested_schema, &shredded_parquet_schema);
        assert!(result_unshredded.is_ok());
        assert!(matches!(result_shredded,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));

        // Map of Variant
        let requested_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "map_v",
            MapType::new(DataType::STRING, DataType::unshredded_variant(), true),
        )]));
        let unshredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new_map(
            "map_v",
            "struc_v",
            ArrowField::new("s", ArrowDataType::Utf8, false),
            unshredded_variant_parquet_schema(),
            false,
            false,
        )]));
        let shredded_parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new_map(
            "map_v",
            "struc_v",
            ArrowField::new("s", ArrowDataType::Utf8, false),
            shredded_variant_parquet_schema(),
            false,
            false,
        )]));
        let result_unshredded =
            get_requested_indices(&requested_schema, &unshredded_parquet_schema);
        let result_shredded = get_requested_indices(&requested_schema, &shredded_parquet_schema);
        assert!(result_unshredded.is_ok());
        assert!(matches!(result_shredded,
            Err(e) if e.to_string().contains("The default engine does not support shredded reads")));
    }

    #[test]
    fn ensure_data_types_fails_correctly() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::nullable(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Utf8, true)
                    .with_metadata(arrow_fid(1)),
            ]));
            let res = get_requested_indices(&requested_schema, &parquet_schema);
            assert_result_error_with_message(
                res,
                "Invalid argument error: Incorrect datatype. Expected integer, got Utf8",
            );

            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::nullable(logical_name(1), DataType::STRING)
                    .with_metadata(column_mapping_metadata(1, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false),
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, true),
            ]));
            let res = get_requested_indices(&requested_schema, &parquet_schema);
            assert_result_error_with_message(
                res,
                "Invalid argument error: Incorrect datatype. Expected Utf8, got Int32",
            );
        })
    }

    #[test]
    fn mask_with_map() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([StructField::not_null(
                logical_name(0),
                MapType::new(DataType::INTEGER, DataType::STRING, false),
            )
            .with_metadata(column_mapping_metadata(0, mode))])
            .make_physical(mode)
            .unwrap()
            .into();

            // The key and value may have field ids not present in the delta schema
            let parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new_map(
                parquet_name(0, mode),
                "entries",
                ArrowField::new("i", ArrowDataType::Int32, false),
                ArrowField::new("s", ArrowDataType::Utf8, false),
                false,
                false,
            )
            .with_metadata(arrow_fid(1))]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1];
            let expect_reorder = vec![ReorderIndex::identity(0)];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn simple_reorder_indices() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::nullable(logical_name(1), DataType::STRING)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::nullable(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, true)
                    .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Utf8, true)
                    .with_metadata(arrow_fid(1)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2];
            let expect_reorder = vec![
                ReorderIndex::identity(2),
                ReorderIndex::identity(0),
                ReorderIndex::identity(1),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        })
    }

    #[test]
    fn simple_nullable_field_missing() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::nullable(logical_name(1), DataType::STRING)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::nullable(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, true)
                    .with_metadata(arrow_fid(2)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1];
            let expected_arrow_field = requested_schema
                .field(parquet_name(1, mode))
                .unwrap()
                .try_into_arrow()
                .unwrap();
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::identity(2),
                ReorderIndex::missing(1, Arc::new(expected_arrow_field)),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn get_requested_indices_by_id_only() {
        let requested_schema = StructType::new_unchecked([
            StructField::not_null("i_logical", DataType::INTEGER)
                .with_metadata(kernel_fid_and_name(1, "i_physical")),
            StructField::nullable("s_logical", DataType::STRING)
                .with_metadata(kernel_fid_and_name(2, "s_physical")),
            StructField::nullable("i2_logical", DataType::INTEGER)
                .with_metadata(kernel_fid_and_name(3, "i2_physical")),
        ])
        .make_physical(ColumnMappingMode::Id)
        .unwrap()
        .into();
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("not-i", ArrowDataType::Int32, false).with_metadata(arrow_fid(1)),
            ArrowField::new("not-i2", ArrowDataType::Int32, true).with_metadata(arrow_fid(3)),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let expected_arrow_field = requested_schema
            .field("s_physical")
            .unwrap()
            .try_into_arrow()
            .unwrap();
        let expect_reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::identity(2),
            ReorderIndex::missing(1, Arc::new(expected_arrow_field)),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn get_requested_indices_by_id_falls_back_to_name() {
        let requested_schema = StructType::new_unchecked([
            StructField::not_null("i_logical", DataType::INTEGER)
                .with_metadata(kernel_fid_and_name(1, "i_physical")),
            StructField::nullable("s_logical", DataType::STRING)
                .with_metadata(kernel_fid_and_name(2, "s_physical")),
            StructField::nullable("i2_logical", DataType::INTEGER)
                .with_metadata(kernel_fid_and_name(3, "i2_physical")),
        ])
        .make_physical(ColumnMappingMode::Id)
        .unwrap()
        .into();
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i_logical", ArrowDataType::Int32, false).with_metadata(arrow_fid(1)),
            ArrowField::new("i2_physical", ArrowDataType::Int32, true).with_metadata(arrow_fid(3)),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let expected_arrow_field = requested_schema
            .field("s_physical")
            .unwrap()
            .try_into_arrow()
            .unwrap();
        let expect_reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::identity(2),
            ReorderIndex::missing(1, Arc::new(expected_arrow_field)),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    fn nested_parquet_schema(mode: ColumnMappingMode) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                .with_metadata(arrow_fid(1)),
            ArrowField::new(
                parquet_name(3, mode),
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new(parquet_name(4, mode), ArrowDataType::Int32, false)
                            .with_metadata(arrow_fid(4)),
                        ArrowField::new(parquet_name(5, mode), ArrowDataType::Utf8, false)
                            .with_metadata(arrow_fid(5)),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(arrow_fid(3)),
            ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, false)
                .with_metadata(arrow_fid(2)),
        ]))
    }

    #[test]
    fn test_match_parquet_fields_filters_metadata_columns() {
        let kernel_schema = StructType::new_unchecked([
            StructField::not_null("regular_field", DataType::INTEGER),
            StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex),
            StructField::nullable("another_field", DataType::STRING),
        ]);

        let parquet_fields: ArrowFields = vec![
            ArrowField::new("regular_field", ArrowDataType::Int32, false),
            ArrowField::new("row_index", ArrowDataType::Int64, false),
            ArrowField::new("another_field", ArrowDataType::Utf8, true),
        ]
        .into();

        let matched_fields: Vec<_> =
            match_parquet_fields(&kernel_schema, &parquet_fields).collect();

        assert_eq!(matched_fields.len(), 3);

        // First field (regular_field) should have kernel_field_info
        assert!(matched_fields[0].kernel_field_info.is_some());
        assert_eq!(matched_fields[0].parquet_field.name(), "regular_field");

        // Second field (row_index metadata column) should have None for kernel_field_info
        assert!(matched_fields[1].kernel_field_info.is_none());
        assert_eq!(matched_fields[1].parquet_field.name(), "row_index");

        // Third field (another_field) should have kernel_field_info
        assert!(matched_fields[2].kernel_field_info.is_some());
        assert_eq!(matched_fields[2].parquet_field.name(), "another_field");
    }

    #[test]
    fn test_ordering_needs_row_indexes() {
        // Test case 1: No row index needed
        let ordering_no_row_index = vec![
            ReorderIndex::identity(0),
            ReorderIndex::cast(1, ArrowDataType::Int64),
            ReorderIndex::missing(
                2,
                Arc::new(ArrowField::new("missing", ArrowDataType::Utf8, true)),
            ),
        ];
        assert!(!ordering_needs_row_indexes(&ordering_no_row_index));

        // Test case 2: Row index needed at top level
        let ordering_with_row_index = vec![
            ReorderIndex::identity(0),
            ReorderIndex::row_index(
                1,
                Arc::new(ArrowField::new("row_idx", ArrowDataType::Int64, false)),
            ),
        ];
        assert!(ordering_needs_row_indexes(&ordering_with_row_index));

        // Test case 3: Empty ordering
        assert!(!ordering_needs_row_indexes(&[]));
    }

    #[test]
    fn test_reorder_struct_array_missing_row_indexes() {
        // Test that we get a proper error when row indexes are needed but not provided
        let arry = make_struct_array();
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::row_index(
                1,
                Arc::new(ArrowField::new("row_idx", ArrowDataType::Int64, false)),
            ),
        ];

        let result = reorder_struct_array(arry, &reorder, None, None);
        assert_result_error_with_message(
            result,
            "Row index column requested but row index iterator not provided",
        );
    }

    #[test]
    fn test_reorder_struct_array_with_row_indexes() {
        // Test that row indexes work when properly provided
        let arry = make_struct_array();
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::row_index(
                1,
                Arc::new(ArrowField::new("row_idx", ArrowDataType::Int64, false)),
            ),
        ];

        // Create a mock row index iterator
        #[allow(clippy::single_range_in_vec_init)]
        let mut row_indexes = vec![(0..4)].into_iter().flatten();

        let ordered = reorder_struct_array(arry, &reorder, Some(&mut row_indexes), None).unwrap();
        assert_eq!(ordered.column_names(), vec!["b", "row_idx"]);

        // Verify the row index column contains the expected values
        let row_idx_col = ordered.column(1).as_primitive::<Int64Type>();
        assert_eq!(row_idx_col.values(), &[0, 1, 2, 3]);
    }

    #[test]
    fn simple_row_index_field() {
        let requested_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("i", DataType::INTEGER),
            StructField::create_metadata_column("my_row_index", MetadataColumnSpec::RowIndex),
            StructField::nullable("i2", DataType::INTEGER),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let mut arrow_row_index_field =
            ArrowField::new("my_row_index", ArrowDataType::Int64, false);
        arrow_row_index_field.set_metadata(HashMap::from([(
            "delta.metadataSpec".to_string(),
            "row_index".to_string(),
        )]));
        let expect_reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::identity(2),
            ReorderIndex::row_index(1, Arc::new(arrow_row_index_field)),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_file_path_field() {
        let requested_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("i", DataType::INTEGER),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
            StructField::nullable("i2", DataType::INTEGER),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let mut arrow_file_path_field = ArrowField::new("_file", ArrowDataType::Utf8, false);
        arrow_file_path_field.set_metadata(HashMap::from([(
            "delta.metadataSpec".to_string(),
            "_file".to_string(),
        )]));
        let expect_reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::identity(2),
            ReorderIndex::file_path(1, Arc::new(arrow_file_path_field)),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn test_reorder_struct_array_with_file_path() {
        // Test that file paths work when properly provided
        let arry = make_struct_array();
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::file_path(
                1,
                Arc::new(ArrowField::new("_file", ArrowDataType::Utf8, false)),
            ),
        ];

        let file_location = "s3://bucket/path/to/file.parquet";
        let ordered = reorder_struct_array(arry, &reorder, None, Some(file_location)).unwrap();
        assert_eq!(ordered.column_names(), vec!["b", "_file"]);

        // Verify the file path column is a plain StringArray with the path repeated for each row.
        let file_path_col = ordered.column(1);
        let string_array = file_path_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(string_array.len(), 4);
        assert!(string_array.iter().all(|v| v == Some(file_location)));
    }

    #[test]
    fn test_reorder_struct_array_missing_file_path() {
        // Test that error occurs when file path is requested but not provided
        let arry = make_struct_array();
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::file_path(
                1,
                Arc::new(ArrowField::new("_file", ArrowDataType::Utf8, false)),
            ),
        ];

        let result = reorder_struct_array(arry, &reorder, None, None);
        assert_result_error_with_message(
            result,
            "File path column requested but file location not provided",
        );
    }

    #[test]
    fn test_row_index_builder_no_skipping() {
        let row_groups = vec![
            create_mock_row_group(5), // 5 rows: indexes 0-4
            create_mock_row_group(3), // 3 rows: indexes 5-7
            create_mock_row_group(4), // 4 rows: indexes 8-11
        ];

        let builder = RowIndexBuilder::new(&row_groups);
        let row_indexes: Vec<i64> = builder.build().unwrap().collect();

        // Should produce consecutive indexes from 0 to 11
        assert_eq!(row_indexes, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn test_row_index_builder_with_skipping() {
        let row_groups = vec![
            create_mock_row_group(5), // 5 rows: indexes 0-4
            create_mock_row_group(3), // 3 rows: indexes 5-7 (will be skipped)
            create_mock_row_group(4), // 4 rows: indexes 8-11
            create_mock_row_group(2), // 2 rows: indexes 12-13 (will be skipped)
        ];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[0, 2]);

        let row_indexes: Vec<i64> = builder.build().unwrap().collect();

        // Should produce indexes from row groups 0 and 2: [0-4] and [8-11]
        assert_eq!(row_indexes, vec![0, 1, 2, 3, 4, 8, 9, 10, 11]);
    }

    #[test]
    fn test_row_index_builder_single_row_group() {
        let row_groups = vec![create_mock_row_group(7)];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[0]);

        let row_indexes: Vec<i64> = builder.build().unwrap().collect();

        assert_eq!(row_indexes, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_row_index_builder_empty_selection() {
        let row_groups = vec![create_mock_row_group(3), create_mock_row_group(2)];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[]);

        let row_indexes: Vec<i64> = builder.build().unwrap().collect();

        // Should produce no indexes
        assert_eq!(row_indexes, Vec::<i64>::new());
    }

    #[test]
    fn test_row_index_builder_out_of_order_selection() {
        let row_groups = vec![
            create_mock_row_group(2), // 2 rows: indexes 0-1
            create_mock_row_group(3), // 3 rows: indexes 2-4
            create_mock_row_group(1), // 1 row: index 5
        ];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[2, 0]);

        let row_indexes: Vec<i64> = builder.build().unwrap().collect();

        // Should produce indexes in the order specified: group 2 first, then group 0
        assert_eq!(row_indexes, vec![5, 0, 1]);
    }

    #[test]
    fn test_row_index_builder_out_of_bounds_row_group_ordinals() {
        let row_groups = vec![create_mock_row_group(2)];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[1]);

        let result = builder.build();
        assert_result_error_with_message(result, "Row group ordinal 1 is out of bounds");
    }

    #[test]
    fn test_row_index_builder_duplicate_row_group_ordinals() {
        let row_groups = vec![create_mock_row_group(2), create_mock_row_group(3)];

        let mut builder = RowIndexBuilder::new(&row_groups);
        builder.select_row_groups(&[1, 1]);

        let result = builder.build();
        assert_result_error_with_message(result, "Found duplicate row group ordinal");
    }

    #[test]
    fn nested_indices() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(
                    logical_name(3),
                    StructType::new_unchecked([
                        StructField::not_null(logical_name(4), DataType::INTEGER)
                            .with_metadata(column_mapping_metadata(4, mode)),
                        StructField::not_null(logical_name(5), DataType::STRING)
                            .with_metadata(column_mapping_metadata(5, mode)),
                    ]),
                )
                .with_metadata(column_mapping_metadata(3, mode)),
                StructField::not_null(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = nested_parquet_schema(mode);
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2, 3];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::nested(
                    1,
                    vec![ReorderIndex::identity(0), ReorderIndex::identity(1)],
                ),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }
    #[test]
    fn nested_indices_reorder() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(
                    logical_name(3),
                    StructType::new_unchecked([
                        StructField::not_null(logical_name(5), DataType::STRING)
                            .with_metadata(column_mapping_metadata(5, mode)),
                        StructField::not_null(logical_name(4), DataType::INTEGER)
                            .with_metadata(column_mapping_metadata(4, mode)),
                    ]),
                )
                .with_metadata(column_mapping_metadata(3, mode)),
                StructField::not_null(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = nested_parquet_schema(mode);
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2, 3];
            let expect_reorder = vec![
                ReorderIndex::identity(2),
                ReorderIndex::nested(
                    0,
                    vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
                ),
                ReorderIndex::identity(1),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn nested_indices_mask_inner() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(
                    logical_name(3),
                    StructType::new_unchecked([StructField::not_null(
                        logical_name(4),
                        DataType::INTEGER,
                    )
                    .with_metadata(column_mapping_metadata(4, mode))]),
                )
                .with_metadata(column_mapping_metadata(3, mode)),
                StructField::not_null(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = nested_parquet_schema(mode);
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 3];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::nested(1, vec![ReorderIndex::identity(0)]),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        })
    }

    #[test]
    fn unmatched_struct_before_selected_leaf_ordering() {
        // Regression: when a struct with no matching children appears BEFORE a selected
        // leaf in parquet order, the Missing entry must be deferred so the leaf's
        // Identity entry gets the correct parquet_position in reorder_struct_array.
        let requested_schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable(
                "stats",
                StructType::new_unchecked([StructField::nullable("age", DataType::LONG)]),
            ),
        ]));
        // Parquet has stats BEFORE a
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "stats",
                ArrowDataType::Struct(
                    vec![ArrowField::new("id", ArrowDataType::Int64, true)].into(),
                ),
                true,
            ),
            ArrowField::new("a", ArrowDataType::Int64, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        // Only "a" should be in the mask (leaf index 1, after stats.id at index 0)
        assert_eq!(mask_indices, vec![1]);
        let expected_stats_field = Arc::new(
            requested_schema
                .field("stats")
                .unwrap()
                .try_into_arrow()
                .unwrap(),
        );
        // Identity for "a" must come FIRST (parquet_position 0), then Missing for stats
        assert_eq!(
            reorder_indices,
            vec![
                ReorderIndex::identity(0),
                ReorderIndex::missing(1, expected_stats_field),
            ]
        );
    }

    #[test]
    fn simple_list_mask() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(logical_name(2), ArrayType::new(DataType::INTEGER, false))
                    .with_metadata(column_mapping_metadata(2, mode)),
                StructField::not_null(logical_name(3), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(3, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(1)),
                ArrowField::new(
                    parquet_name(2, mode),
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "nested",
                        ArrowDataType::Int32,
                        false,
                    ))),
                    false,
                )
                .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(3)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::identity(1),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn list_skip_earlier_element() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([StructField::not_null(
                logical_name(1),
                ArrayType::new(DataType::INTEGER, false),
            )
            .with_metadata(column_mapping_metadata(1, mode))])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(
                    parquet_name(1, mode),
                    ArrowDataType::List(Arc::new(
                        ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, false)
                            .with_metadata(arrow_fid(2)),
                    )),
                    false,
                )
                .with_metadata(arrow_fid(1)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![1];
            let expect_reorder = vec![ReorderIndex::identity(0)];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn nested_indices_list() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(0), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(0, mode)),
                StructField::not_null(
                    logical_name(1),
                    ArrayType::new(
                        StructType::new_unchecked([
                            StructField::not_null(logical_name(3), DataType::INTEGER)
                                .with_metadata(column_mapping_metadata(3, mode)),
                            StructField::not_null(logical_name(4), DataType::STRING)
                                .with_metadata(column_mapping_metadata(4, mode)),
                        ]),
                        false,
                    ),
                )
                .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(0, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(0)),
                ArrowField::new(
                    parquet_name(1, mode),
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "nested",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                                    .with_metadata(arrow_fid(3)),
                                ArrowField::new(parquet_name(4, mode), ArrowDataType::Utf8, false)
                                    .with_metadata(arrow_fid(4)),
                            ]
                            .into(),
                        ),
                        false,
                    ))),
                    false,
                )
                .with_metadata(arrow_fid(1)),
                ArrowField::new(parquet_name(2, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(2)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 2, 3];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::nested(
                    1,
                    vec![ReorderIndex::identity(0), ReorderIndex::identity(1)],
                ),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn nested_indices_unselected_list() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(logical_name(3), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(3, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(1)),
                ArrowField::new(
                    parquet_name(2, mode),
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "nested",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new(parquet_name(4, mode), ArrowDataType::Int32, false)
                                    .with_metadata(arrow_fid(4)),
                                ArrowField::new(parquet_name(5, mode), ArrowDataType::Utf8, false)
                                    .with_metadata(arrow_fid(5)),
                            ]
                            .into(),
                        ),
                        false,
                    ))),
                    false,
                )
                .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(3)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 3];
            let expect_reorder = vec![ReorderIndex::identity(0), ReorderIndex::identity(1)];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn nested_indices_list_mask_inner() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(
                    logical_name(2),
                    ArrayType::new(
                        StructType::new_unchecked([StructField::not_null(
                            logical_name(4),
                            DataType::INTEGER,
                        )
                        .with_metadata(column_mapping_metadata(4, mode))]),
                        false,
                    ),
                )
                .with_metadata(column_mapping_metadata(2, mode)),
                StructField::not_null(logical_name(3), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(3, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(1)),
                ArrowField::new(
                    parquet_name(2, mode),
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "nested",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new(parquet_name(4, mode), ArrowDataType::Int32, false)
                                    .with_metadata(arrow_fid(4)),
                                ArrowField::new(parquet_name(5, mode), ArrowDataType::Utf8, false)
                                    .with_metadata(arrow_fid(5)),
                            ]
                            .into(),
                        ),
                        false,
                    ))),
                    false,
                )
                .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(3)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 1, 3];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::nested(1, vec![ReorderIndex::identity(0)]),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn nested_indices_list_mask_inner_reorder() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(
                    logical_name(2),
                    ArrayType::new(
                        StructType::new_unchecked([
                            StructField::not_null(logical_name(6), DataType::STRING)
                                .with_metadata(column_mapping_metadata(6, mode)),
                            StructField::not_null(logical_name(5), DataType::INTEGER)
                                .with_metadata(column_mapping_metadata(5, mode)),
                        ]),
                        false,
                    ),
                )
                .with_metadata(column_mapping_metadata(2, mode)),
                StructField::not_null(logical_name(3), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(3, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(1)),
                ArrowField::new(
                    parquet_name(2, mode),
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "nested",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new(parquet_name(4, mode), ArrowDataType::Int32, false)
                                    .with_metadata(arrow_fid(4)),
                                ArrowField::new(parquet_name(5, mode), ArrowDataType::Int32, false)
                                    .with_metadata(arrow_fid(5)),
                                ArrowField::new(parquet_name(6, mode), ArrowDataType::Utf8, false)
                                    .with_metadata(arrow_fid(6)),
                            ]
                            .into(),
                        ),
                        false,
                    ))),
                    false,
                )
                .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(3)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![0, 2, 3, 4];
            let expect_reorder = vec![
                ReorderIndex::identity(0),
                ReorderIndex::nested(
                    1,
                    vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
                ),
                ReorderIndex::identity(2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn skipped_struct() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::not_null(logical_name(1), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::not_null(
                    logical_name(2),
                    StructType::new_unchecked([
                        StructField::not_null(logical_name(4), DataType::INTEGER)
                            .with_metadata(column_mapping_metadata(4, mode)),
                        StructField::not_null(logical_name(5), DataType::STRING)
                            .with_metadata(column_mapping_metadata(5, mode)),
                    ]),
                )
                .with_metadata(column_mapping_metadata(2, mode)),
                StructField::not_null(logical_name(3), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(3, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(
                    "skipped",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new(parquet_name(7, mode), ArrowDataType::Int32, false)
                                .with_metadata(arrow_fid(7)),
                            ArrowField::new(parquet_name(8, mode), ArrowDataType::Utf8, false)
                                .with_metadata(arrow_fid(8)),
                        ]
                        .into(),
                    ),
                    false,
                )
                .with_metadata(arrow_fid(6)),
                ArrowField::new(parquet_name(3, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(3)),
                ArrowField::new(
                    parquet_name(2, mode),
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new(parquet_name(4, mode), ArrowDataType::Int32, false)
                                .with_metadata(arrow_fid(4)),
                            ArrowField::new(parquet_name(5, mode), ArrowDataType::Utf8, false)
                                .with_metadata(arrow_fid(5)),
                        ]
                        .into(),
                    ),
                    false,
                )
                .with_metadata(arrow_fid(2)),
                ArrowField::new(parquet_name(1, mode), ArrowDataType::Int32, false)
                    .with_metadata(arrow_fid(1)),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask = vec![2, 3, 4, 5];
            let expect_reorder = vec![
                ReorderIndex::identity(2),
                ReorderIndex::nested(
                    1,
                    vec![ReorderIndex::identity(0), ReorderIndex::identity(1)],
                ),
                ReorderIndex::identity(0),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn reorder_map_with_structs() {
        let requested_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("i", DataType::INTEGER),
            StructField::not_null(
                "map",
                MapType::new(
                    StructType::new_unchecked([
                        StructField::not_null("k1", DataType::STRING),
                        StructField::not_null("k2", DataType::STRING),
                    ]),
                    StructType::new_unchecked([
                        StructField::not_null("v2", DataType::STRING),
                        StructField::not_null("v1", DataType::STRING),
                    ]),
                    false,
                ),
            ),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new_map(
                "map",
                "entries",
                ArrowField::new(
                    "i",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("k1", ArrowDataType::Utf8, false),
                            ArrowField::new("k2", ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                ArrowField::new(
                    "v",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("v1", ArrowDataType::Utf8, false),
                            ArrowField::new("v2", ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                false,
                false,
            ),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3, 4];
        let expect_reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::nested(
                1,
                vec![
                    ReorderIndex::identity(0), // key does not need re-ordering
                    ReorderIndex::nested(
                        1,
                        vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
                    ),
                ],
            ),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    fn make_struct_array() -> StructArray {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        StructArray::from(vec![
            (
                Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
                boolean.clone() as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
                int.clone() as ArrowArrayRef,
            ),
        ])
    }

    #[test]
    fn simple_reorder_struct() {
        let arry = make_struct_array();
        let reorder = vec![ReorderIndex::identity(1), ReorderIndex::identity(0)];
        let ordered = reorder_struct_array(arry, &reorder, None, None).unwrap();
        assert_eq!(ordered.column_names(), vec!["c", "b"]);
    }

    #[test]
    fn nested_reorder_struct() {
        let arry1 = Arc::new(make_struct_array());
        let arry2 = Arc::new(make_struct_array());
        let fields: ArrowFields = vec![
            Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
        ]
        .into();
        let nested = StructArray::from(vec![
            (
                Arc::new(ArrowField::new(
                    "struct1",
                    ArrowDataType::Struct(fields.clone()),
                    false,
                )),
                arry1 as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "struct2",
                    ArrowDataType::Struct(fields),
                    false,
                )),
                arry2 as ArrowArrayRef,
            ),
        ]);
        let reorder = vec![
            ReorderIndex::nested(
                1,
                vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
            ),
            ReorderIndex::nested(
                0,
                vec![
                    ReorderIndex::identity(0),
                    ReorderIndex::identity(1),
                    ReorderIndex::missing(
                        2,
                        Arc::new(ArrowField::new("s", ArrowDataType::Utf8, true)),
                    ),
                ],
            ),
        ];
        let ordered = reorder_struct_array(nested, &reorder, None, None).unwrap();
        assert_eq!(ordered.column_names(), vec!["struct2", "struct1"]);
        let ordered_s2 = ordered.column(0).as_struct();
        assert_eq!(ordered_s2.column_names(), vec!["b", "c", "s"]);
        let ordered_s1 = ordered.column(1).as_struct();
        assert_eq!(ordered_s1.column_names(), vec!["c", "b"]);
    }

    #[test]
    fn reorder_list_of_struct() {
        let boolean = Arc::new(BooleanArray::from(vec![
            false, false, true, true, false, true,
        ]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31, 0, 3]));
        let list_sa = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
                boolean.clone() as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
                int.clone() as ArrowArrayRef,
            ),
        ]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6]));
        let list_field = ArrowField::new("item", list_sa.data_type().clone(), false);
        let list = Arc::new(GenericListArray::new(
            Arc::new(list_field),
            offsets,
            Arc::new(list_sa),
            None,
        ));
        let fields: ArrowFields = vec![
            Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
        ]
        .into();
        let list_dt = Arc::new(ArrowField::new(
            "list",
            ArrowDataType::new_list(ArrowDataType::Struct(fields), false),
            false,
        ));
        let struct_array = StructArray::from(vec![(list_dt, list as ArrowArrayRef)]);
        let reorder = vec![ReorderIndex::nested(
            0,
            vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
        )];
        let ordered = reorder_struct_array(struct_array, &reorder, None, None).unwrap();
        let ordered_list_col = ordered.column(0).as_list::<i32>();
        for i in 0..ordered_list_col.len() {
            let array_item = ordered_list_col.value(i);
            let struct_item = array_item.as_struct();
            assert_eq!(struct_item.column_names(), vec!["c", "b"]);
        }
    }

    // boy howdy this is more complicated than expected
    fn build_arrow_map() -> MapArray {
        let key_struct_builder = StructBuilder::from_fields(
            ArrowFields::from(vec![
                ArrowField::new("k1", ArrowDataType::Int32, false),
                ArrowField::new("k2", ArrowDataType::Int32, false),
            ]),
            1,
        );
        let value_struct_builder = StructBuilder::from_fields(
            ArrowFields::from(vec![
                ArrowField::new("v1", ArrowDataType::Int32, false),
                ArrowField::new("v2", ArrowDataType::Int32, false),
            ]),
            1,
        );
        let mut map_builder = MapBuilder::new(None, key_struct_builder, value_struct_builder);

        let (key_builder, value_builder) = map_builder.entries();
        let key_k1_builder = key_builder.field_builder::<Int32Builder>(0).unwrap();
        key_k1_builder.append_value(1);
        let key_k2_builder = key_builder.field_builder::<Int32Builder>(1).unwrap();
        key_k2_builder.append_value(2);
        key_builder.append(true);

        let value_v1_builder = value_builder.field_builder::<Int32Builder>(0).unwrap();
        value_v1_builder.append_value(1);
        let value_v2_builder = value_builder.field_builder::<Int32Builder>(1).unwrap();
        value_v2_builder.append_value(2);
        value_builder.append(true);
        map_builder.append(true).unwrap();
        map_builder.finish()
    }

    #[test]
    fn reorder_map_of_struct() {
        let int_array = Arc::new(Int32Array::from(vec![42]));
        let int_dt = Arc::new(ArrowField::new("i", int_array.data_type().clone(), false));
        let map_array = Arc::new(build_arrow_map());
        let map_dt = Arc::new(ArrowField::new("map", map_array.data_type().clone(), false));
        let struct_array = StructArray::from(vec![
            (int_dt, int_array as ArrowArrayRef),
            (map_dt, map_array as ArrowArrayRef),
        ]);
        let reorder = vec![
            ReorderIndex::identity(1),
            ReorderIndex::nested(
                0,
                vec![
                    ReorderIndex::identity(0),
                    ReorderIndex::nested(
                        1,
                        vec![ReorderIndex::identity(1), ReorderIndex::identity(0)],
                    ),
                ],
            ),
        ];
        let ordered = reorder_struct_array(struct_array, &reorder, None, None).unwrap();
        assert_eq!(ordered.column_names(), vec!["map", "i"]);
        if let ArrowDataType::Map(field, _) = ordered.column(0).data_type() {
            if let ArrowDataType::Struct(fields) = field.data_type() {
                fn assert_col_order(field: &ArrowField, expected: Vec<&str>) {
                    if let ArrowDataType::Struct(fields) = field.data_type() {
                        let names: Vec<&str> =
                            fields.iter().map(|field| field.name().as_str()).collect();
                        assert_eq!(names, expected);
                    } else {
                        panic!("Expected struct field");
                    }
                }
                assert_col_order(&fields[0], vec!["k1", "k2"]);
                assert_col_order(&fields[1], vec!["v2", "v1"]);
            } else {
                panic!("Inner field should have been a struct");
            }
        } else {
            panic!("Column 0 should have been a map");
        }
    }

    #[test]
    fn no_matches() {
        column_mapping_cases().into_iter().for_each(|mode| {
            let requested_schema = StructType::new_unchecked([
                StructField::nullable(logical_name(1), DataType::STRING)
                    .with_metadata(column_mapping_metadata(1, mode)),
                StructField::nullable(logical_name(2), DataType::INTEGER)
                    .with_metadata(column_mapping_metadata(2, mode)),
            ])
            .make_physical(mode)
            .unwrap()
            .into();
            let nots_field =
                ArrowField::new("NOTs", ArrowDataType::Utf8, true).with_metadata(arrow_fid(3));
            let noti2_field =
                ArrowField::new("NOTi2", ArrowDataType::Int32, true).with_metadata(arrow_fid(4));
            let parquet_schema = Arc::new(ArrowSchema::new(vec![
                nots_field.clone(),
                noti2_field.clone(),
            ]));
            let (mask_indices, reorder_indices) =
                get_requested_indices(&requested_schema, &parquet_schema).unwrap();
            let expect_mask: Vec<usize> = vec![];

            // Build expected arrow fields using proper conversion
            let mut fields = requested_schema.fields();
            let expected_field1: Arc<ArrowField> =
                Arc::new(fields.next().unwrap().try_into_arrow().unwrap());
            let expected_field2: Arc<ArrowField> =
                Arc::new(fields.next().unwrap().try_into_arrow().unwrap());

            let expect_reorder = vec![
                ReorderIndex::missing(0, expected_field1),
                ReorderIndex::missing(1, expected_field2),
            ];
            assert_eq!(mask_indices, expect_mask);
            assert_eq!(reorder_indices, expect_reorder);
        });
    }

    #[test]
    fn empty_requested_schema() {
        let requested_schema = Arc::new(StructType::new_unchecked([]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask: Vec<usize> = vec![];
        let expect_reorder = vec![];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn test_write_json() -> DeltaResult<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "string",
            ArrowDataType::Utf8,
            true,
        )]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["string1", "string2"]))],
        )?;
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(data));
        let filtered_data = FilteredEngineData::with_all_rows_selected(data);
        let json = to_json_bytes(Box::new(std::iter::once(Ok(filtered_data))))?;
        assert_eq!(
            json,
            "{\"string\":\"string1\"}\n{\"string\":\"string2\"}\n".as_bytes()
        );
        Ok(())
    }

    #[test]
    fn test_to_json_bytes_filters_data() -> DeltaResult<()> {
        // Create test data with 4 rows
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            ArrowDataType::Utf8,
            true,
        )]));
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "row0", "row1", "row2", "row3",
            ]))],
        )?;

        // Helper function to create EngineData from the same record batch
        let create_engine_data =
            || -> Box<dyn EngineData> { Box::new(ArrowEngineData::new(record_batch.clone())) };

        // Test case 1: All rows selected (should include all 4 rows)
        let all_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![true, true, true, true])?;
        let json_all = to_json_bytes(Box::new(std::iter::once(Ok(all_selected))))?;
        assert_eq!(
            json_all,
            "{\"value\":\"row0\"}\n{\"value\":\"row1\"}\n{\"value\":\"row2\"}\n{\"value\":\"row3\"}\n".as_bytes()
        );

        // Test case 2: Only first and last rows selected (should include only 2 rows)
        let partial_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![true, false, false, true])?;
        let json_partial = to_json_bytes(Box::new(std::iter::once(Ok(partial_selected))))?;
        assert_eq!(
            json_partial,
            "{\"value\":\"row0\"}\n{\"value\":\"row3\"}\n".as_bytes()
        );

        // Test case 3: Only middle rows selected (should include only 2 rows)
        let middle_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![false, true, true, false])?;
        let json_middle = to_json_bytes(Box::new(std::iter::once(Ok(middle_selected))))?;
        assert_eq!(
            json_middle,
            "{\"value\":\"row1\"}\n{\"value\":\"row2\"}\n".as_bytes()
        );

        // Test case 4: No rows selected (should produce empty output)
        let none_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![false, false, false, false])?;
        let json_none = to_json_bytes(Box::new(std::iter::once(Ok(none_selected))))?;
        assert_eq!(json_none, "".as_bytes());

        // Test case 5: Only one row selected (should include only 1 row)
        let one_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![false, true, false, false])?;
        let json_one = to_json_bytes(Box::new(std::iter::once(Ok(one_selected))))?;
        assert_eq!(json_one, "{\"value\":\"row1\"}\n".as_bytes());

        // Test case 6: Only one row selected implicitly by short vector
        let one_selected =
            FilteredEngineData::try_new(create_engine_data(), vec![false, false, false])?;
        let json_one = to_json_bytes(Box::new(std::iter::once(Ok(one_selected))))?;
        assert_eq!(json_one, "{\"value\":\"row3\"}\n".as_bytes());

        Ok(())
    }

    #[test]
    fn test_arrow_broken_nested_null_masks() {
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_utils::fix_nested_null_masks;
        use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Parse some JSON into a nested schema
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(ArrowFields::from(vec![
                Field::new(
                    "inner_nullable",
                    DataType::Struct(ArrowFields::from(vec![
                        Field::new("leaf_non_null", DataType::Int32, false),
                        Field::new("leaf_nullable", DataType::Int32, true),
                    ])),
                    true,
                ),
                Field::new(
                    "inner_non_null",
                    DataType::Struct(ArrowFields::from(vec![
                        Field::new("leaf_non_null", DataType::Int32, false),
                        Field::new("leaf_nullable", DataType::Int32, true),
                    ])),
                    false,
                ),
            ])),
            true,
        )]));
        let json_string = r#"
{ }
{ "outer" : { "inner_non_null" : { "leaf_non_null" : 1 } } }
{ "outer" : { "inner_non_null" : { "leaf_non_null" : 2, "leaf_nullable" : 3 } } }
{ "outer" : { "inner_non_null" : { "leaf_non_null" : 4 }, "inner_nullable" : { "leaf_non_null" : 5 } } }
{ "outer" : { "inner_non_null" : { "leaf_non_null" : 6 }, "inner_nullable" : { "leaf_non_null" : 7, "leaf_nullable": 8 } } }
"#;
        let batch1 = crate::arrow::json::ReaderBuilder::new(schema.clone())
            .build(json_string.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        macro_rules! assert_nulls {
            ( $column: expr, $nulls: expr ) => {
                assert_eq!($column.nulls().unwrap(), &NullBuffer::from(&$nulls[..]));
            };
        }

        // If any of these tests ever fail, it means the arrow JSON reader started producing
        // incomplete nested NULL masks. If that happens, we need to update all JSON reads to call
        // `fix_nested_null_masks`.
        let outer_1 = batch1.column(0).as_struct();
        assert_nulls!(outer_1, [false, true, true, true, true]);
        let inner_nullable_1 = outer_1.column(0).as_struct();
        assert_nulls!(inner_nullable_1, [false, false, false, true, true]);
        let nullable_leaf_non_null_1 = inner_nullable_1.column(0);
        assert_nulls!(nullable_leaf_non_null_1, [false, false, false, true, true]);
        let nullable_leaf_nullable_1 = inner_nullable_1.column(1);
        assert_nulls!(nullable_leaf_nullable_1, [false, false, false, false, true]);
        let inner_non_null_1 = outer_1.column(1).as_struct();
        assert_nulls!(inner_non_null_1, [false, true, true, true, true]);
        let non_null_leaf_non_null_1 = inner_non_null_1.column(0);
        assert_nulls!(non_null_leaf_non_null_1, [false, true, true, true, true]);
        let non_null_leaf_nullable_1 = inner_non_null_1.column(1);
        assert_nulls!(non_null_leaf_nullable_1, [false, false, true, false, false]);

        // Write the batch to a parquet file and read it back
        let mut buffer = vec![];
        let mut writer =
            crate::parquet::arrow::ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
        writer.write(&batch1).unwrap();
        writer.close().unwrap(); // writer must be closed to write footer
        let batch2 = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buffer))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        // Starting from arrow-53.3, the parquet reader started returning broken nested NULL masks.
        let batch2 = RecordBatch::from(fix_nested_null_masks(batch2.into()));

        // Verify the data survived the round trip
        let outer_2 = batch2.column(0).as_struct();
        assert_eq!(outer_2, outer_1);
        let inner_nullable_2 = outer_2.column(0).as_struct();
        assert_eq!(inner_nullable_2, inner_nullable_1);
        let nullable_leaf_non_null_2 = inner_nullable_2.column(0);
        assert_eq!(nullable_leaf_non_null_2, nullable_leaf_non_null_1);
        let nullable_leaf_nullable_2 = inner_nullable_2.column(1);
        assert_eq!(nullable_leaf_nullable_2, nullable_leaf_nullable_1);
        let inner_non_null_2 = outer_2.column(1).as_struct();
        assert_eq!(inner_non_null_2, inner_non_null_1);
        let non_null_leaf_non_null_2 = inner_non_null_2.column(0);
        assert_eq!(non_null_leaf_non_null_2, non_null_leaf_non_null_1);
        let non_null_leaf_nullable_2 = inner_non_null_2.column(1);
        assert_eq!(non_null_leaf_nullable_2, non_null_leaf_nullable_1);
    }

    // --- Tests for build_json_reorder_indices and json_arrow_schema ---

    const FILE_PATH: &str = "s3://bucket/test.json";

    struct JsonInsertCase {
        /// Full schema; may include a `FilePath` metadata column at any position.
        schema: StructType,
        /// Field names that [`json_arrow_schema`] should expose (metadata columns stripped).
        expected_json_names: &'static [&'static str],
        /// Column names in the final output after [`reorder_struct_array`].
        expected_output_names: &'static [&'static str],
        /// Index of the `_file` column in the output, or `None` when the schema has no FilePath.
        file_path_col: Option<usize>,
    }

    /// Verifies that `json_arrow_schema` + `build_json_reorder_indices` + `reorder_struct_array`
    /// correctly insert (or omit) the `_file` column at the position declared in the schema.
    #[rstest]
    #[case::no_file_path(JsonInsertCase {
        schema: StructType::new_unchecked([
            StructField::not_null("a", DataType::INTEGER),
            StructField::nullable("b", DataType::INTEGER),
        ]),
        expected_json_names: &["a", "b"],
        expected_output_names: &["a", "b"],
        file_path_col: None,
    })]
    #[case::file_path_at_start(JsonInsertCase {
        schema: StructType::new_unchecked([
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
            StructField::not_null("a", DataType::INTEGER),
            StructField::nullable("b", DataType::INTEGER),
        ]),
        expected_json_names: &["a", "b"],
        expected_output_names: &["_file", "a", "b"],
        file_path_col: Some(0),
    })]
    #[case::file_path_in_middle(JsonInsertCase {
        schema: StructType::new_unchecked([
            StructField::not_null("a", DataType::INTEGER),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
            StructField::nullable("b", DataType::INTEGER),
        ]),
        expected_json_names: &["a", "b"],
        expected_output_names: &["a", "_file", "b"],
        file_path_col: Some(1),
    })]
    #[case::file_path_at_end(JsonInsertCase {
        schema: StructType::new_unchecked([
            StructField::not_null("a", DataType::INTEGER),
            StructField::nullable("b", DataType::INTEGER),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        ]),
        expected_json_names: &["a", "b"],
        expected_output_names: &["a", "b", "_file"],
        file_path_col: Some(2),
    })]
    fn test_json_file_path_insertion(#[case] case: JsonInsertCase) {
        // json_arrow_schema exposes only the non-metadata fields.
        let json_schema = json_arrow_schema(&case.schema).unwrap();
        let json_names: Vec<_> = json_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(json_names, case.expected_json_names);

        // Build an input batch with the JSON schema (real columns only, each an Int32Array).
        let arrow_schema = Arc::new(json_schema);
        let cols: Vec<ArrowArrayRef> = (0..arrow_schema.fields().len())
            .map(|_| Arc::new(Int32Array::from(vec![1i32, 2, 3])) as _)
            .collect();
        let batch = RecordBatch::try_new(arrow_schema, cols).unwrap();

        // build_json_reorder_indices + reorder_struct_array inserts the _file column.
        let indices = build_json_reorder_indices(&case.schema).unwrap();
        let result = RecordBatch::from(
            reorder_struct_array(batch.into(), &indices, None, Some(FILE_PATH)).unwrap(),
        );

        // Verify output column order and row count.
        let schema = result.schema();
        let output_names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(output_names, case.expected_output_names);
        assert_eq!(result.num_rows(), 3);

        // When FilePath is in the schema, verify a plain StringArray with the path for every row.
        // When absent, verify no _file column leaked into the output.
        if let Some(idx) = case.file_path_col {
            let arr = result
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("_file column should be a StringArray");
            assert!(arr.iter().all(|v| v == Some(FILE_PATH)));
        } else {
            assert!(
                result.schema().fields().iter().all(|f| f.name() != "_file"),
                "_file should not appear when not declared in the schema"
            );
        }
    }

    #[test]
    fn test_build_json_reorder_indices_unsupported_metadata_column_errors() {
        // RowIndex is not supported for JSON reads. All metadata column specs are non-nullable,
        // so the Missing transform inserts a null array — reorder_struct_array errors because
        // the field is declared non-nullable.
        let schema = StructType::new_unchecked([
            StructField::not_null("a", DataType::INTEGER),
            StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex),
        ]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let indices = build_json_reorder_indices(&schema).unwrap();
        assert!(reorder_struct_array(batch.into(), &indices, None, None).is_err());
    }

    #[test]
    fn ensure_we_encode_maps_with_null_values() {
        let schema = ArrowSchema::new(vec![
            ArrowField::new("str_col", ArrowDataType::Utf8, false),
            ArrowField::new(
                "map_col",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "entries",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("keys", ArrowDataType::Utf8, false),
                                ArrowField::new("values", ArrowDataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false, // sorted
                ),
                false,
            ),
        ]);
        let s_array = StringArray::from(vec!["foo"]);

        let string_builder = StringBuilder::new();
        let string_builder2 = StringBuilder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, string_builder2);

        // Append one entry: "bar" -> null
        map_builder.keys().append_value("bar");
        map_builder.values().append_null();
        map_builder.append(true).unwrap(); // finish the map row

        let map_array: MapArray = map_builder.finish();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(s_array), Arc::new(map_array)],
        )
        .unwrap();

        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(batch));
        let filtered_data = FilteredEngineData::with_all_rows_selected(data);
        let json = to_json_bytes(Box::new(std::iter::once(Ok(filtered_data)))).unwrap();
        assert_eq!(
            json,
            "{\"str_col\":\"foo\",\"map_col\":{\"bar\":null}}\n".as_bytes()
        );
    }

    #[rstest]
    fn struct_with_all_nullable_children_unmatched_is_missing(
        #[values(true, false)] struct_nullable: bool,
    ) {
        // When a struct exists in parquet but none of its children match the requested
        // schema, the struct should be treated as missing regardless of its nullability.
        let info_field = if struct_nullable {
            StructField::nullable(
                "info",
                StructType::new_unchecked([StructField::nullable("z", DataType::LONG)]),
            )
        } else {
            StructField::not_null(
                "info",
                StructType::new_unchecked([StructField::nullable("z", DataType::LONG)]),
            )
        };
        let requested_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            info_field,
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int64, true),
            ArrowField::new(
                "info",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("x", ArrowDataType::Int64, true),
                        ArrowField::new("y", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                !struct_nullable,
            ),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        assert_eq!(mask_indices, vec![0]);
        let expected_info_field = Arc::new(
            requested_schema
                .field("info")
                .unwrap()
                .try_into_arrow()
                .unwrap(),
        );
        assert_eq!(
            reorder_indices,
            vec![
                ReorderIndex::identity(0),
                ReorderIndex::missing(1, expected_info_field),
            ]
        );
    }

    #[test]
    fn reorder_non_nullable_missing_struct_produces_non_null_struct() {
        // The Missing transform for a non-nullable struct should produce a struct with
        // null_count == 0 and all-null children.
        let a_array: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let input = StructArray::from(vec![(
            Arc::new(ArrowField::new("a", ArrowDataType::Int64, false)),
            a_array,
        )]);
        let missing_field = Arc::new(ArrowField::new(
            "info",
            ArrowDataType::Struct(vec![ArrowField::new("z", ArrowDataType::Int64, true)].into()),
            false,
        ));
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::missing(1, missing_field),
        ];
        let ordered = reorder_struct_array(input, &reorder, None, None).unwrap();
        assert_eq!(ordered.column_names(), vec!["a", "info"]);
        let info = ordered.column(1).as_struct();
        assert_eq!(info.null_count(), 0);
        assert_eq!(info.column(0).null_count(), 3);
    }

    #[test]
    fn reorder_nested_non_nullable_missing_struct_recurses() {
        // Non-nullable struct containing a non-nullable struct child: both levels should
        // have null_count == 0, with the leaf nullable child being all-null.
        let a_array: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(vec![1, 2]));
        let input = StructArray::from(vec![(
            Arc::new(ArrowField::new("a", ArrowDataType::Int64, false)),
            a_array,
        )]);
        let inner_struct =
            ArrowDataType::Struct(vec![ArrowField::new("leaf", ArrowDataType::Int64, true)].into());
        let missing_field = Arc::new(ArrowField::new(
            "outer",
            ArrowDataType::Struct(vec![ArrowField::new("inner", inner_struct, false)].into()),
            false,
        ));
        let reorder = vec![
            ReorderIndex::identity(0),
            ReorderIndex::missing(1, missing_field),
        ];
        let ordered = reorder_struct_array(input, &reorder, None, None).unwrap();
        let outer = ordered.column(1).as_struct();
        assert_eq!(outer.null_count(), 0);
        let inner = outer.column(0).as_struct();
        assert_eq!(inner.null_count(), 0);
        assert_eq!(inner.column(0).null_count(), 2);
    }

    #[test]
    fn empty_struct_is_matched() {
        // Delta protocol allows empty structs. An empty struct has no children, so no
        // leaf columns are selected, but the struct itself should still be matched.
        let requested_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("empty", StructType::new_unchecked([])),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int64, true),
            ArrowField::new("empty", ArrowDataType::Struct(ArrowFields::empty()), false),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        assert_eq!(mask_indices, vec![0]);
        let expected_empty_field = Arc::new(
            requested_schema
                .field("empty")
                .unwrap()
                .try_into_arrow()
                .unwrap(),
        );
        assert_eq!(
            reorder_indices,
            vec![
                ReorderIndex::identity(0),
                ReorderIndex::missing(1, expected_empty_field),
            ]
        );
    }

    // === fixup_parquet_read coercion behavior ===

    /// Verifies that `fixup_parquet_read` rewrites the batch's nullability flags at every
    /// nesting level (struct field, list element, map value) to match the kernel schema, while
    /// preserving the Arrow-mandated non-nullability of map entries and map keys.
    #[test]
    fn test_fixup_parquet_read_coerces_nullability_at_all_levels() {
        // Source: outer (non-null) {
        //   lst: List<Int32 (non-null)> (non-null),
        //   mp:  Map<Utf8, Int32 (non-null)> (non-null),
        // }
        let src_list_elem = Arc::new(ArrowField::new("element", ArrowDataType::Int32, false));
        let src_list = ArrowField::new("lst", ArrowDataType::List(src_list_elem.clone()), false);
        let src_map_entries = Arc::new(ArrowField::new(
            "entries",
            ArrowDataType::Struct(ArrowFields::from(vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Int32, false),
            ])),
            false,
        ));
        let src_map = ArrowField::new(
            "mp",
            ArrowDataType::Map(src_map_entries.clone(), false),
            false,
        );
        let src_outer = ArrowField::new(
            "outer",
            ArrowDataType::Struct(ArrowFields::from(vec![src_list.clone(), src_map.clone()])),
            false,
        );

        let offsets_1 = || OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1]));
        let list_col: ArrowArrayRef = Arc::new(
            GenericListArray::<i32>::try_new(
                src_list_elem,
                offsets_1(),
                Arc::new(Int32Array::from(vec![1])),
                None,
            )
            .unwrap(),
        );
        let entries_fields = match src_map_entries.data_type() {
            ArrowDataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        };
        let entries = StructArray::try_new(
            entries_fields,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int32Array::from(vec![10])) as _,
            ],
            None,
        )
        .unwrap();
        let map_col: ArrowArrayRef = Arc::new(
            MapArray::try_new(src_map_entries, offsets_1(), entries, None, false).unwrap(),
        );
        let outer_col: ArrowArrayRef = Arc::new(
            StructArray::try_new(
                ArrowFields::from(vec![src_list, src_map]),
                vec![list_col, map_col],
                None,
            )
            .unwrap(),
        );

        let src_schema = Arc::new(ArrowSchema::new(vec![src_outer]));
        let batch = RecordBatch::try_new(src_schema, vec![outer_col]).unwrap();

        // Kernel target: all nullable where Arrow allows it.
        let target_schema: SchemaRef =
            Arc::new(StructType::new_unchecked([StructField::nullable(
                "outer",
                StructType::new_unchecked([
                    StructField::nullable("lst", ArrayType::new(DataType::INTEGER, true)),
                    StructField::nullable(
                        "mp",
                        MapType::new(DataType::STRING, DataType::INTEGER, true),
                    ),
                ]),
            )]));

        let ordering = [ReorderIndex::identity(0)];
        let result =
            fixup_parquet_read(batch, &ordering, None, None, Some(&target_schema)).unwrap();
        let result_batch: RecordBatch = result.into();

        let schema = result_batch.schema();
        let outer_field = schema.field(0);
        assert!(outer_field.is_nullable(), "outer should be nullable");

        let outer_children = match outer_field.data_type() {
            ArrowDataType::Struct(f) => f,
            other => panic!("expected Struct, got {other:?}"),
        };

        let lst_field = &outer_children[0];
        assert!(lst_field.is_nullable(), "lst should be nullable");
        let lst_element = match lst_field.data_type() {
            ArrowDataType::List(e) => e,
            other => panic!("expected List, got {other:?}"),
        };
        assert!(lst_element.is_nullable(), "list element should be nullable");

        let mp_field = &outer_children[1];
        assert!(mp_field.is_nullable(), "mp should be nullable");
        let (mp_entries, mp_key, mp_value) = match mp_field.data_type() {
            ArrowDataType::Map(entries, _) => {
                let inner = match entries.data_type() {
                    ArrowDataType::Struct(f) => f,
                    other => panic!("expected Struct entries, got {other:?}"),
                };
                (entries.as_ref(), inner[0].clone(), inner[1].clone())
            }
            other => panic!("expected Map, got {other:?}"),
        };
        assert!(
            !mp_entries.is_nullable(),
            "map entries field must remain non-null per Arrow spec"
        );
        assert!(
            !mp_key.is_nullable(),
            "map key field must remain non-null per Arrow spec"
        );
        assert!(mp_value.is_nullable(), "map value should be nullable");
    }

    /// Verifies that `fixup_parquet_read` rewrites nested struct child names to match the kernel
    /// schema. Complements the engine-level `test_read_parquet_with_field_id_matching` (which
    /// only renames the top-level fields) by exercising the nested rename path directly at the
    /// `fixup_parquet_read` boundary.
    #[test]
    fn test_fixup_parquet_read_renames_nested_struct_fields() {
        // Source: outer { src_x: Int32, src_y: Struct { src_z: Int32 } }
        let src_inner_fields =
            ArrowFields::from(vec![ArrowField::new("src_z", ArrowDataType::Int32, false)]);
        let src_inner = ArrowField::new(
            "src_y",
            ArrowDataType::Struct(src_inner_fields.clone()),
            false,
        );
        let src_outer_fields = ArrowFields::from(vec![
            ArrowField::new("src_x", ArrowDataType::Int32, false),
            src_inner,
        ]);
        let src_outer = ArrowField::new(
            "outer",
            ArrowDataType::Struct(src_outer_fields.clone()),
            false,
        );

        let inner_z: ArrowArrayRef = Arc::new(Int32Array::from(vec![42, 43, 44]));
        let inner_struct: ArrowArrayRef =
            Arc::new(StructArray::try_new(src_inner_fields, vec![inner_z], None).unwrap());
        let outer_x: ArrowArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let outer_col: ArrowArrayRef = Arc::new(
            StructArray::try_new(src_outer_fields, vec![outer_x, inner_struct], None).unwrap(),
        );

        let src_schema = Arc::new(ArrowSchema::new(vec![src_outer]));
        let batch = RecordBatch::try_new(src_schema, vec![outer_col]).unwrap();

        // Kernel target with different names at every level: tgt_x, tgt_y, tgt_z.
        let target_schema: SchemaRef =
            Arc::new(StructType::new_unchecked([StructField::not_null(
                "outer",
                StructType::new_unchecked([
                    StructField::not_null("tgt_x", DataType::INTEGER),
                    StructField::not_null(
                        "tgt_y",
                        StructType::new_unchecked([StructField::not_null(
                            "tgt_z",
                            DataType::INTEGER,
                        )]),
                    ),
                ]),
            )]));

        let ordering = [ReorderIndex::identity(0)];
        let result =
            fixup_parquet_read(batch, &ordering, None, None, Some(&target_schema)).unwrap();
        let result_batch: RecordBatch = result.into();

        let schema = result_batch.schema();
        let outer_field = schema.field(0);
        assert_eq!(outer_field.name(), "outer");

        let outer_children = match outer_field.data_type() {
            ArrowDataType::Struct(f) => f,
            other => panic!("expected Struct, got {other:?}"),
        };
        assert_eq!(outer_children[0].name(), "tgt_x");
        assert_eq!(outer_children[1].name(), "tgt_y");

        let nested_children = match outer_children[1].data_type() {
            ArrowDataType::Struct(f) => f,
            other => panic!("expected nested Struct, got {other:?}"),
        };
        assert_eq!(nested_children[0].name(), "tgt_z");
    }

    /// Verify that `fix_nested_null_masks` handles a struct with a NullArray child column
    /// (void type) when the parent struct has non-trivial nulls. NullArray does not accept
    /// a null buffer, so the propagation must skip it without panicking.
    #[test]
    fn test_nested_null_masks_with_null_array_child() {
        use crate::arrow::array::NullArray;
        use crate::arrow::datatypes::{DataType, Field};
        use crate::engine::arrow_utils::fix_nested_null_masks;

        // Build: struct<val: int32, void_col: null> with 4 rows, parent null at row 0
        let int_field = Field::new("val", DataType::Int32, true);
        let null_field = Field::new("void_col", DataType::Null, true);
        let fields = ArrowFields::from(vec![int_field, null_field]);

        let int_col: ArrowArrayRef = Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
        ]));
        let null_col: ArrowArrayRef = Arc::new(NullArray::new(4));

        // Parent struct has row 0 null
        let parent_nulls = NullBuffer::from(&[false, true, true, true][..]);
        let sa = StructArray::new(fields.clone(), vec![int_col, null_col], Some(parent_nulls));

        // Wrap in an outer struct (as fix_nested_null_masks expects a top-level StructArray)
        let outer_field = Field::new("outer", DataType::Struct(fields), true);
        let outer = StructArray::new(
            ArrowFields::from(vec![outer_field]),
            vec![Arc::new(sa)],
            None,
        );

        // This should NOT panic — previously it would crash because NullArray rejects null buffers
        let result = fix_nested_null_masks(outer);

        // Verify the NullArray child survived unchanged
        let inner = result.column(0).as_struct();
        assert_eq!(inner.len(), 4);

        let void_col = inner.column(1);
        assert_eq!(*void_col.data_type(), DataType::Null);
        assert_eq!(void_col.len(), 4);
        // NullArray has no null bitmap, so null_count() returns 0 in Arrow even though
        // all values are conceptually null. Verify the quirk explicitly.
        assert_eq!(void_col.null_count(), 0);

        // Verify the int column got the parent null propagated (row 0 is now null)
        let val_col = inner.column(0);
        assert!(val_col.is_null(0), "row 0 should be null (parent null)");
        assert!(!val_col.is_null(1));
        assert!(!val_col.is_null(2));
        assert!(!val_col.is_null(3));
    }
}
