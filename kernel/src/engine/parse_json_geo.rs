//! GeoArrow WKT-to-WKB conversion helpers for JSON stats parsing.
//!
//! Delta JSON stats store geometry bounds as WKT strings, but GeoArrow WKB fields have
//! DataType::Binary. The Arrow JSON reader cannot parse WKT into binary, so we temporarily
//! replace geo fields with Utf8 for parsing, then convert the resulting WKT strings back to
//! WKB binary afterward. Tables with no geo columns skip both passes.

use std::sync::Arc;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{
    Array as ArrowArray, GenericListArray, MapArray, RecordBatch, StructArray,
};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Fields as ArrowFields, Schema as ArrowSchema,
};
use crate::geoarrow_array::array::WktArray;
use crate::geoarrow_array::{cast as geo_cast, GeoArrowArray};
use crate::geoarrow_schema::{GeoArrowType, WktType};
use crate::{DeltaResult, Error};

fn is_geoarrow_field(field: &ArrowField) -> bool {
    GeoArrowType::try_from(field).is_ok()
}

/// Builds a copy of schema with every GeoArrow field replaced by Utf8, recursing through
/// Struct, List, and Map children. The flag in the return is true iff any GeoArrow leaf
/// was found; when false, the caller can skip the conversion pass entirely.
pub(super) fn replace_geo_fields_with_utf8(schema: &ArrowSchema) -> (ArrowSchema, bool) {
    let (new_fields, has_geo) = rebuild_fields_replacing_geo(schema.fields());
    (
        ArrowSchema::new_with_metadata(new_fields, schema.metadata().clone()),
        has_geo,
    )
}

fn rebuild_fields_replacing_geo(fields: &ArrowFields) -> (ArrowFields, bool) {
    let mut has_geo = false;
    let new_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let (new_field, child_has_geo) = rebuild_field_replacing_geo(f);
            if child_has_geo {
                has_geo = true;
            }
            new_field
        })
        .collect();
    (ArrowFields::from(new_fields), has_geo)
}

// When no geo leaf is found in a subtree, returns the original Arc unchanged to avoid
// cloning the field tree.
fn rebuild_field_replacing_geo(field: &ArrowFieldRef) -> (ArrowFieldRef, bool) {
    if is_geoarrow_field(field) {
        let new_field = ArrowField::new(field.name(), ArrowDataType::Utf8, field.is_nullable());
        return (Arc::new(new_field), true);
    }
    match field.data_type() {
        ArrowDataType::Struct(children) => {
            let (new_children, child_has_geo) = rebuild_fields_replacing_geo(children);
            if !child_has_geo {
                return (Arc::clone(field), false);
            }
            let new_field = ArrowField::new(
                field.name(),
                ArrowDataType::Struct(new_children),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone());
            (Arc::new(new_field), true)
        }
        ArrowDataType::List(element) => {
            let (new_element, child_has_geo) = rebuild_field_replacing_geo(element);
            if !child_has_geo {
                return (Arc::clone(field), false);
            }
            let new_field = ArrowField::new(
                field.name(),
                ArrowDataType::List(new_element),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone());
            (Arc::new(new_field), true)
        }
        ArrowDataType::Map(entries_field, sorted) => {
            let ArrowDataType::Struct(entry_children) = entries_field.data_type() else {
                return (Arc::clone(field), false);
            };
            let (new_entry_children, child_has_geo) = rebuild_fields_replacing_geo(entry_children);
            if !child_has_geo {
                return (Arc::clone(field), false);
            }
            let new_entries = Arc::new(
                ArrowField::new(
                    entries_field.name(),
                    ArrowDataType::Struct(new_entry_children),
                    entries_field.is_nullable(),
                )
                .with_metadata(entries_field.metadata().clone()),
            );
            let new_field = ArrowField::new(
                field.name(),
                ArrowDataType::Map(new_entries, *sorted),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone());
            (Arc::new(new_field), true)
        }
        _ => (Arc::clone(field), false),
    }
}

/// Converts a Utf8 array of WKT strings to a Binary array of WKB bytes, preserving the CRS
/// from original_field's GeoArrow extension metadata. Errors if original_field is not a
/// GeoArrow WKB field.
fn convert_wkt_array_to_wkb(
    array: &Arc<dyn ArrowArray>,
    original_field: &ArrowField,
) -> DeltaResult<Arc<dyn ArrowArray>> {
    let metadata = match GeoArrowType::try_from(original_field) {
        Ok(GeoArrowType::Wkb(wkb_type)) => wkb_type.metadata().clone(),
        _ => {
            return Err(Error::generic(format!(
                "convert_wkt_array_to_wkb called on non-GeoArrow-WKB field '{}'",
                original_field.name()
            )));
        }
    };
    let wkt_array = WktArray::from((array.as_string::<i32>().clone(), WktType::new(metadata)));
    let wkb_array = geo_cast::to_wkb::<i32>(&wkt_array)
        .map_err(|e| Error::generic(format!("WKT to WKB conversion failed: {e}")))?;
    Ok(wkb_array.into_array_ref())
}

/// Walks batch and target_schema in parallel, converting WKT Utf8 columns to WKB Binary at
/// every GeoArrow leaf and recursing through Struct, List, and Map containers. Non-geo
/// non-container columns pass through unchanged.
pub(super) fn convert_geo_columns(
    batch: RecordBatch,
    target_schema: &ArrowSchema,
) -> DeltaResult<RecordBatch> {
    let (_, columns, _) = batch.into_parts();
    let new_columns: Vec<_> = columns
        .into_iter()
        .zip(target_schema.fields().iter())
        .map(|(col, target_field)| convert_geo_column(col, target_field))
        .collect::<DeltaResult<_>>()?;
    Ok(RecordBatch::try_new(
        Arc::new(target_schema.clone()),
        new_columns,
    )?)
}

fn convert_geo_column(
    column: Arc<dyn ArrowArray>,
    target_field: &ArrowField,
) -> DeltaResult<Arc<dyn ArrowArray>> {
    if is_geoarrow_field(target_field) {
        return convert_wkt_array_to_wkb(&column, target_field);
    }
    match target_field.data_type() {
        ArrowDataType::Struct(target_children) => {
            let (_, child_arrays, nulls) = column.as_struct().clone().into_parts();
            let new_children: Vec<_> = child_arrays
                .into_iter()
                .zip(target_children.iter())
                .map(|(c, f)| convert_geo_column(c, f))
                .collect::<DeltaResult<_>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_children.clone(),
                new_children,
                nulls,
            )?))
        }
        ArrowDataType::List(target_element) => {
            let (_, offsets, values, nulls) = column.as_list::<i32>().clone().into_parts();
            let new_values = convert_geo_column(values, target_element)?;
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                Arc::clone(target_element),
                offsets,
                new_values,
                nulls,
            )?))
        }
        ArrowDataType::Map(target_entries, sorted) => {
            let ArrowDataType::Struct(target_entry_children) = target_entries.data_type() else {
                return Err(Error::generic("Map entries field is not a struct"));
            };
            let (_, offsets, entries, nulls, _) = column.as_map().clone().into_parts();
            let (_, entry_arrays, entry_nulls) = entries.into_parts();
            let new_entry_children: Vec<_> = entry_arrays
                .into_iter()
                .zip(target_entry_children.iter())
                .map(|(c, f)| convert_geo_column(c, f))
                .collect::<DeltaResult<_>>()?;
            let new_entries = StructArray::try_new(
                target_entry_children.clone(),
                new_entry_children,
                entry_nulls,
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(target_entries),
                offsets,
                new_entries,
                nulls,
                *sorted,
            )?))
        }
        _ => Ok(column),
    }
}
