//! Arrow utilities used by the DataFusion executor.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, GenericListViewArray, MapArray,
    OffsetSizeTrait, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use itertools::Itertools;

type ParquetFieldId = i64;

/// Relabels a physical Parquet schema using Delta's field-identity rule.
///
/// At every struct level, physical fields match requested fields by Parquet field ID first and
/// exact name otherwise. The result retains the physical shape and types while applying logical
/// names to matched fields.
///
/// # Parameters
///
/// - `requested_schema`: logical schema containing any requested Parquet field IDs.
/// - `physical_schema`: footer-derived schema for one Parquet file.
///
/// # Returns
///
/// The physical schema with logical names applied to identity matches.
///
/// # Errors
///
/// Returns an error when map entries have an invalid Arrow shape.
pub(crate) fn resolve_parquet_field_ids(
    requested_schema: &Schema,
    physical_schema: &Schema,
) -> DataFusionResult<Schema> {
    Ok(Schema::new_with_metadata(
        resolve_parquet_fields(requested_schema.fields(), physical_schema.fields())?,
        physical_schema.metadata().clone(),
    ))
}

/// Tightens nested physical fields that the requested schema declares non-nullable.
///
/// Top-level nullability remains physical. Nested fields are validated when their Arrow wrappers
/// are rebuilt by [`relabel_arrow_array`].
///
/// # Parameters
///
/// - `requested_schema`: schema containing Kernel's requested nested nullability.
/// - `physical_schema`: field-resolved physical schema to align.
///
/// # Returns
///
/// The physical schema with compatible nested nullability tightened.
pub(crate) fn align_nested_nullability(
    requested_schema: &Schema,
    physical_schema: &Schema,
) -> Schema {
    let fields = align_fields(requested_schema.fields(), physical_schema.fields(), false);
    Schema::new_with_metadata(fields, physical_schema.metadata().clone())
}

/// Rebuilds nested Arrow wrappers with `target_type` without copying their buffers.
///
/// The source and target must have the same container shape and primitive leaf types. Field names,
/// nullability, and metadata may differ at any nesting level.
///
/// # Parameters
///
/// - `array`: source Arrow array whose buffers are retained.
/// - `target_type`: data type containing the replacement nested field descriptors.
///
/// # Returns
///
/// An array with the target field descriptors and the source buffers.
///
/// # Errors
///
/// Returns an error when the container shapes or leaf types are incompatible, or when an Arrow
/// wrapper cannot be rebuilt.
pub(crate) fn relabel_arrow_array(
    array: &ArrayRef,
    target_type: &DataType,
) -> DataFusionResult<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(Arc::clone(array));
    }

    match (array.data_type(), target_type) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let source = array.as_struct();
            if source_fields.len() != target_fields.len() {
                return Err(execution_error(format!(
                    "Cannot relabel struct with {} fields as struct with {} fields",
                    source_fields.len(),
                    target_fields.len()
                )));
            }
            let columns = source
                .columns()
                .iter()
                .zip(target_fields)
                .map(|(column, field)| relabel_arrow_array(column, field.data_type()))
                .try_collect()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(target_field)) => {
            relabel_list_array::<i32>(array, target_field)
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            relabel_list_array::<i64>(array, target_field)
        }
        (DataType::ListView(_), DataType::ListView(target_field)) => {
            relabel_list_view_array::<i32>(array, target_field)
        }
        (DataType::LargeListView(_), DataType::LargeListView(target_field)) => {
            relabel_list_view_array::<i64>(array, target_field)
        }
        (
            DataType::FixedSizeList(_, source_size),
            DataType::FixedSizeList(target_field, target_size),
        ) if source_size == target_size => {
            let source = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| execution_error("Arrow fixed-size-list downcast failed"))?;
            let values = relabel_arrow_array(source.values(), target_field.data_type())?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(target_field),
                *target_size,
                values,
                source.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, source_ordered), DataType::Map(target_field, target_ordered))
            if source_ordered == target_ordered =>
        {
            let source = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| execution_error("Arrow map downcast failed"))?;
            let entries = relabel_arrow_array(
                &(Arc::new(source.entries().clone()) as ArrayRef),
                target_field.data_type(),
            )?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| execution_error("Map entries must be a struct"))?
                .clone();
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                entries,
                source.nulls().cloned(),
                *target_ordered,
            )?))
        }
        (source, target) => Err(execution_error(format!(
            "Cannot relabel Arrow array from {source} to {target}"
        ))),
    }
}

fn align_data_type(requested_type: &DataType, physical_type: &DataType) -> DataType {
    match (requested_type, physical_type) {
        (DataType::Struct(requested_fields), DataType::Struct(physical_fields)) => {
            DataType::Struct(align_fields(requested_fields, physical_fields, true))
        }
        (DataType::List(requested), DataType::List(physical)) => {
            DataType::List(align_field(requested, physical))
        }
        (DataType::List(requested), DataType::LargeList(physical)) => {
            DataType::LargeList(align_field(requested, physical))
        }
        (DataType::List(requested), DataType::ListView(physical)) => {
            DataType::ListView(align_field(requested, physical))
        }
        (DataType::List(requested), DataType::LargeListView(physical)) => {
            DataType::LargeListView(align_field(requested, physical))
        }
        (DataType::LargeList(requested), DataType::LargeList(physical)) => {
            DataType::LargeList(align_field(requested, physical))
        }
        (DataType::ListView(requested), DataType::ListView(physical)) => {
            DataType::ListView(align_field(requested, physical))
        }
        (DataType::LargeListView(requested), DataType::LargeListView(physical)) => {
            DataType::LargeListView(align_field(requested, physical))
        }
        (
            DataType::FixedSizeList(requested, requested_size),
            DataType::FixedSizeList(physical, physical_size),
        ) if requested_size == physical_size => {
            DataType::FixedSizeList(align_field(requested, physical), *physical_size)
        }
        (DataType::Map(requested, _), DataType::Map(physical, ordered)) => {
            DataType::Map(align_field(requested, physical), *ordered)
        }
        _ => physical_type.clone(),
    }
}

fn align_field(requested_field: &FieldRef, physical_field: &FieldRef) -> FieldRef {
    Arc::new(
        physical_field
            .as_ref()
            .clone()
            .with_data_type(align_data_type(
                requested_field.data_type(),
                physical_field.data_type(),
            ))
            .with_nullable(physical_field.is_nullable() && requested_field.is_nullable()),
    )
}

fn align_fields(
    requested_fields: &Fields,
    physical_fields: &Fields,
    tighten_nullability: bool,
) -> Fields {
    physical_fields
        .iter()
        .map(|physical_field| {
            let Some((_, requested_field)) = requested_fields.find(physical_field.name()) else {
                return Arc::clone(physical_field);
            };
            if tighten_nullability {
                align_field(requested_field, physical_field)
            } else {
                Arc::new(
                    physical_field
                        .as_ref()
                        .clone()
                        .with_data_type(align_data_type(
                            requested_field.data_type(),
                            physical_field.data_type(),
                        )),
                )
            }
        })
        .collect()
}

fn parquet_field_id(field: &Field) -> Option<ParquetFieldId> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|id| id.parse().ok())
}

fn requested_name_by_parquet_id<'a>(
    requested_by_id: &HashMap<ParquetFieldId, &'a String>,
    physical_field: &Field,
) -> Option<&'a String> {
    parquet_field_id(physical_field).and_then(|id| requested_by_id.get(&id).copied())
}

fn resolve_parquet_fields(
    requested_fields: &Fields,
    physical_fields: &Fields,
) -> DataFusionResult<Fields> {
    let requested_by_id: HashMap<ParquetFieldId, &String> = requested_fields
        .iter()
        .filter_map(|field| parquet_field_id(field).map(|id| (id, field.name())))
        .collect();

    physical_fields
        .iter()
        .map(|physical_field| {
            let requested_name = requested_name_by_parquet_id(&requested_by_id, physical_field)
                .unwrap_or_else(|| physical_field.name());
            let requested_field = requested_fields
                .iter()
                .find(|field| field.name() == requested_name);

            requested_field
                .map(|requested_field| resolve_parquet_field(requested_field, physical_field))
                .transpose()
                .map(|field| field.unwrap_or_else(|| Arc::clone(physical_field)))
        })
        .try_collect::<_, Vec<_>, _>()
        .map(Into::into)
}

fn resolve_parquet_field(
    requested_field: &Field,
    physical_field: &Field,
) -> DataFusionResult<FieldRef> {
    let data_type =
        resolve_parquet_data_type(requested_field.data_type(), physical_field.data_type())?;
    Ok(Arc::new(
        physical_field
            .clone()
            .with_name(requested_field.name())
            .with_data_type(data_type),
    ))
}

fn resolve_parquet_data_type(
    requested_type: &DataType,
    physical_type: &DataType,
) -> DataFusionResult<DataType> {
    let resolved = match (requested_type, physical_type) {
        (DataType::Struct(requested), DataType::Struct(physical)) => {
            DataType::Struct(resolve_parquet_fields(requested, physical)?)
        }
        (DataType::List(requested), DataType::List(physical)) => {
            DataType::List(resolve_parquet_field(requested, physical)?)
        }
        (DataType::List(requested), DataType::LargeList(physical)) => {
            DataType::LargeList(resolve_parquet_field(requested, physical)?)
        }
        (DataType::List(requested), DataType::ListView(physical)) => {
            DataType::ListView(resolve_parquet_field(requested, physical)?)
        }
        (DataType::List(requested), DataType::LargeListView(physical)) => {
            DataType::LargeListView(resolve_parquet_field(requested, physical)?)
        }
        (
            DataType::FixedSizeList(requested, requested_size),
            DataType::FixedSizeList(physical, physical_size),
        ) if requested_size == physical_size => {
            DataType::FixedSizeList(resolve_parquet_field(requested, physical)?, *physical_size)
        }
        (DataType::Map(requested_entries, _), DataType::Map(physical_entries, ordered)) => {
            DataType::Map(
                resolve_map_entries(requested_entries, physical_entries)?,
                *ordered,
            )
        }
        _ => physical_type.clone(),
    };
    Ok(resolved)
}

fn resolve_map_entries(
    requested_entries: &Field,
    physical_entries: &Field,
) -> DataFusionResult<FieldRef> {
    let (DataType::Struct(requested_fields), DataType::Struct(physical_fields)) =
        (requested_entries.data_type(), physical_entries.data_type())
    else {
        return Err(execution_error("Map entries must be struct fields"));
    };
    if requested_fields.len() != 2 || physical_fields.len() != 2 {
        return Err(execution_error(format!(
            "Map entries must contain exactly two fields, got {} requested and {} physical",
            requested_fields.len(),
            physical_fields.len()
        )));
    }

    let fields: Vec<_> = requested_fields
        .iter()
        .zip(physical_fields)
        .map(|(requested, physical)| resolve_parquet_field(requested, physical))
        .try_collect()?;
    Ok(Arc::new(
        physical_entries
            .clone()
            .with_data_type(DataType::Struct(fields.into())),
    ))
}

fn relabel_list_array<O: OffsetSizeTrait>(
    array: &ArrayRef,
    target_field: &FieldRef,
) -> DataFusionResult<ArrayRef> {
    let source = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| execution_error("Arrow list downcast failed"))?;
    let values = relabel_arrow_array(source.values(), target_field.data_type())?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(target_field),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

fn relabel_list_view_array<O: OffsetSizeTrait>(
    array: &ArrayRef,
    target_field: &FieldRef,
) -> DataFusionResult<ArrayRef> {
    let source = array
        .as_any()
        .downcast_ref::<GenericListViewArray<O>>()
        .ok_or_else(|| execution_error("Arrow list-view downcast failed"))?;
    let values = relabel_arrow_array(source.values(), target_field.data_type())?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        Arc::clone(target_field),
        source.offsets().clone(),
        source.sizes().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};

    use super::*;

    fn field_id(id: i64) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    }

    #[test]
    fn resolve_field_ids_relabels_nested_matches_and_preserves_physical_shape() {
        let requested_child = Arc::new(
            Field::new("logical_child", DataType::Int64, false).with_metadata(field_id(2)),
        );
        let requested = Schema::new(vec![
            Field::new(
                "logical_parent",
                DataType::Struct(
                    vec![
                        requested_child,
                        Arc::new(Field::new("missing", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(field_id(1)),
            Field::new("by_name", DataType::Utf8, true),
        ]);
        let physical_child = Arc::new(
            Field::new("physical_child", DataType::Int32, false).with_metadata(field_id(2)),
        );
        let physical = Schema::new_with_metadata(
            vec![
                Field::new(
                    "physical_parent",
                    DataType::Struct(
                        vec![
                            physical_child,
                            Arc::new(Field::new("extra", DataType::Boolean, true)),
                        ]
                        .into(),
                    ),
                    false,
                )
                .with_metadata(field_id(1)),
                Field::new("by_name", DataType::LargeUtf8, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "malformed".to_string(),
                )])),
            ],
            HashMap::from([("source".to_string(), "physical".to_string())]),
        );

        let resolved = resolve_parquet_field_ids(&requested, &physical).unwrap();

        assert_eq!(resolved.metadata(), physical.metadata());
        assert_eq!(resolved.field(0).name(), "logical_parent");
        let DataType::Struct(children) = resolved.field(0).data_type() else {
            panic!("expected a struct")
        };
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].name(), "logical_child");
        assert_eq!(children[0].data_type(), &DataType::Int32);
        assert_eq!(children[1].name(), "extra");
        assert_eq!(resolved.field(1).name(), "by_name");
        assert_eq!(resolved.field(1).data_type(), &DataType::LargeUtf8);
    }

    #[test]
    fn relabel_array_rebuilds_nested_wrappers_without_copying_leaf_arrays() {
        let source_child = Arc::new(Field::new("physical_child", DataType::Int32, false));
        let leaf: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let values: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![Arc::clone(&source_child)].into(),
                vec![Arc::clone(&leaf)],
                None,
            )
            .unwrap(),
        );
        let source: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "physical_element",
                DataType::Struct(vec![source_child].into()),
                false,
            )),
            OffsetBuffer::from_lengths([2]),
            values,
            None,
        ));
        let target_type = DataType::List(Arc::new(Field::new(
            "logical_element",
            DataType::Struct(vec![Field::new("logical_child", DataType::Int32, false)].into()),
            false,
        )));

        let relabeled = relabel_arrow_array(&source, &target_type).unwrap();

        assert_eq!(relabeled.data_type(), &target_type);
        let relabeled_values = relabeled.as_list::<i32>().values().as_struct();
        assert!(Arc::ptr_eq(&leaf, relabeled_values.column(0)));
        assert_eq!(
            source.as_list::<i32>().offsets().as_ptr(),
            relabeled.as_list::<i32>().offsets().as_ptr()
        );
    }

    #[test]
    fn alignment_only_tightens_nested_fields() {
        let requested = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, false),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, false)],
                    true,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                    true,
                ),
                Field::new("properties", map_data_type("key_value", false), true),
            ],
            false,
        )]);
        let physical = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, true),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, true)],
                    false,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
                Field::new("properties", map_data_type("entries", true), true),
            ],
            true,
        )]);

        let aligned = align_nested_nullability(&requested, &physical);

        assert!(aligned.field(0).is_nullable());
        let DataType::Struct(fields) = aligned.field(0).data_type() else {
            panic!("expected a struct")
        };
        assert!(!fields[0].is_nullable());
        assert!(!fields[1].is_nullable());
        let DataType::Struct(nested_fields) = fields[1].data_type() else {
            panic!("expected a nested struct")
        };
        assert!(!nested_fields[0].is_nullable());
        let DataType::List(element) = fields[2].data_type() else {
            panic!("expected a list")
        };
        assert_eq!(element.name(), "item");
        assert!(!element.is_nullable());
        let DataType::Map(entries, _) = fields[3].data_type() else {
            panic!("expected a map")
        };
        assert_eq!(entries.name(), "entries");
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("expected map entries")
        };
        assert!(!entry_fields.find("value").unwrap().1.is_nullable());
    }

    #[test]
    fn relabeling_nested_required_child_checks_parent_validity() {
        let source_children: Fields = vec![Field::new("child", DataType::Int32, true)].into();
        let source_fields: Fields =
            vec![Field::new_struct("nested", source_children.clone(), true)].into();
        let target_children: Fields = vec![Field::new("child", DataType::Int32, false)].into();
        let target_type =
            DataType::Struct(vec![Field::new_struct("nested", target_children, true)].into());
        let nested = |nulls| -> ArrayRef {
            let child: ArrayRef = Arc::new(Int32Array::from(vec![None]));
            let nested = StructArray::try_new(source_children.clone(), vec![child], nulls).unwrap();
            Arc::new(nested)
        };
        let outer = |nested| -> ArrayRef {
            let outer = StructArray::try_new(source_fields.clone(), vec![nested], None).unwrap();
            Arc::new(outer)
        };

        let masked = outer(nested(Some(NullBuffer::from(vec![false]))));
        relabel_arrow_array(&masked, &target_type).unwrap();

        let unmasked = outer(nested(None));
        let error = relabel_arrow_array(&unmasked, &target_type).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(r#"Found unmasked nulls for non-nullable StructArray field "child""#),
            "{error}"
        );
    }

    fn map_data_type(entries_name: &str, value_nullable: bool) -> DataType {
        let fields: Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, value_nullable),
        ]
        .into();
        DataType::Map(
            Arc::new(Field::new(entries_name, DataType::Struct(fields), false)),
            false,
        )
    }
}
