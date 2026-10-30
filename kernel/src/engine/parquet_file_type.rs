//! Parquet schema for writing `file` columns.
//!
//! A `file` column is written as a group annotated with the Parquet `FILE` logical type. Under
//! column mapping, each of its sub-fields gets the field id the column reserves for it: the
//! column's own field id plus the sub-field's offset (`uri` +1, `offset` +2, `size` +3,
//! `content_type` +4, `checksum` +5, `inline` +6). parquet 59 has no `FILE` logical type, so a
//! `file` column can only be written with parquet 60.

#[cfg(feature = "arrow-60")]
use std::collections::HashMap;
#[cfg(feature = "arrow-60")]
use std::sync::Arc;

use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::engine::arrow_utils::file_values::FilePaths;
use crate::expressions::ColumnName;
use crate::parquet::arrow::arrow_writer::ArrowWriterOptions;
#[cfg(feature = "arrow-60")]
use crate::parquet::arrow::ArrowSchemaConverter;
#[cfg(feature = "arrow-60")]
use crate::parquet::basic::LogicalType;
#[cfg(feature = "arrow-60")]
use crate::parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
#[cfg(feature = "arrow-60")]
use crate::schema::DataType;
use crate::schema::StructType;
use crate::{KernelError, KernelResult};

/// Returns `options` set up to write batches of `arrow_schema` as a data file with
/// `physical_kernel_schema`: each `file` column is written as a `FILE`-annotated group carrying
/// its sub-field field ids.
#[cfg(feature = "arrow-60")]
pub(super) fn with_file_columns(
    options: ArrowWriterOptions,
    arrow_schema: &ArrowSchema,
    physical_kernel_schema: &StructType,
) -> KernelResult<ArrowWriterOptions> {
    let Some(FilePaths::Struct(file_fields)) = FilePaths::of_struct(physical_kernel_schema) else {
        return Ok(options);
    };
    let descriptor = ArrowSchemaConverter::new().convert(arrow_schema)?;
    let root = descriptor.root_schema();
    let fields = annotate_struct(root.get_fields(), &file_fields, &[])?;
    let root = Type::group_type_builder(root.name())
        .with_fields(fields)
        .build()?;
    Ok(options.with_parquet_schema(SchemaDescriptor::new(Arc::new(root))))
}

/// Returns `options` unchanged, or an error if `physical_kernel_schema` has a `file` column, which
/// parquet 59 cannot write.
#[cfg(not(feature = "arrow-60"))]
pub(super) fn with_file_columns(
    options: ArrowWriterOptions,
    _arrow_schema: &ArrowSchema,
    physical_kernel_schema: &StructType,
) -> KernelResult<ArrowWriterOptions> {
    match FilePaths::of_struct(physical_kernel_schema) {
        Some(file_paths) => Err(KernelError::unsupported(format!(
            "Writing `file` column '{}' requires the `arrow-60` feature.",
            first_column(&file_paths)
        ))),
        None => Ok(options),
    }
}

/// Returns the path of the first `file` column in `file_paths`, following struct fields down to
/// an array, a map or the `file` itself.
#[cfg(not(feature = "arrow-60"))]
fn first_column(mut file_paths: &FilePaths) -> ColumnName {
    let mut path = Vec::new();
    while let FilePaths::Struct(fields) = file_paths {
        let (name, inner) = &fields[0];
        path.push(name.clone());
        file_paths = inner;
    }
    ColumnName::new(path)
}

/// Annotates the `file` columns among `fields`, the Parquet types of a struct whose fields leading
/// to `file` values are `file_fields`, and whose path from the root is `path`.
#[cfg(feature = "arrow-60")]
fn annotate_struct(
    fields: &[TypePtr],
    file_fields: &[(String, FilePaths)],
    path: &[String],
) -> KernelResult<Vec<TypePtr>> {
    let column_path = |name: &str| ColumnName::new(path.iter().cloned().chain([name.to_string()]));
    let file_paths_by_name: HashMap<&str, &FilePaths> = file_fields
        .iter()
        .map(|(name, file_paths)| (name.as_str(), file_paths))
        .collect();
    // A `file` column the batch lacks would otherwise be written unannotated, or not at all.
    if let Some((name, _)) = file_fields
        .iter()
        .find(|(name, _)| !fields.iter().any(|field| field.name() == name))
    {
        return Err(KernelError::generic(format!(
            "`file` column '{}' is missing from the data",
            column_path(name)
        )));
    }
    fields
        .iter()
        .map(|field| match file_paths_by_name.get(field.name()) {
            Some(file_paths) => annotate(field, file_paths, &column_path(field.name())),
            None => Ok(field.clone()),
        })
        .collect()
}

/// Annotates the `file` values at `file_paths` in `field`, the Parquet type written for `column`.
#[cfg(feature = "arrow-60")]
fn annotate(field: &TypePtr, file_paths: &FilePaths, column: &ColumnName) -> KernelResult<TypePtr> {
    let fields = match file_paths {
        FilePaths::File => return annotate_file(field, column),
        FilePaths::Struct(file_fields) => {
            annotate_struct(group_fields(field)?, file_fields, column.path())?
        }
        // A list is a `LIST` group holding a repeated group holding the element.
        FilePaths::Array(element) => annotate_repeated(field, &[Some(element)], column)?,
        // A map is a `MAP` group holding a repeated group holding the key and value.
        FilePaths::Map { key, value } => {
            annotate_repeated(field, &[key.as_deref(), value.as_deref()], column)?
        }
    };
    rebuild_group(
        field,
        field.get_basic_info().logical_type_ref().cloned(),
        fields,
    )
}

/// Annotates the children of the single repeated group in the list or map group `field`, where
/// `file_paths` gives, for each child, the paths to its `file` values, if any.
#[cfg(feature = "arrow-60")]
fn annotate_repeated(
    field: &TypePtr,
    file_paths: &[Option<&FilePaths>],
    column: &ColumnName,
) -> KernelResult<Vec<TypePtr>> {
    let [repeated] = group_fields(field)? else {
        return Err(unexpected_layout(field));
    };
    let children = group_fields(repeated)?;
    if children.len() != file_paths.len() {
        return Err(unexpected_layout(field));
    }
    let children = children
        .iter()
        .zip(file_paths)
        .map(|(child, file_paths)| match file_paths {
            Some(file_paths) => annotate(child, file_paths, column),
            None => Ok(child.clone()),
        })
        .collect::<KernelResult<_>>()?;
    Ok(vec![rebuild_group(repeated, None, children)?])
}

/// Annotates the group `field`, written for the `file` column `column`, with the `FILE` logical
/// type and, if it has a field id, gives each sub-field the id reserved for it.
#[cfg(feature = "arrow-60")]
fn annotate_file(field: &TypePtr, column: &ColumnName) -> KernelResult<TypePtr> {
    let DataType::File(file_fields) = DataType::file_type() else {
        return Err(KernelError::internal_error("`file_type()` is not a `file`"));
    };
    let sub_fields = group_fields(field)?;
    // A `FILE` group must have exactly the canonical sub-fields, which readers require.
    let mut names: Vec<_> = sub_fields
        .iter()
        .map(|sub_field| sub_field.name())
        .collect();
    let mut expected: Vec<_> = file_fields.fields().map(|f| f.name().as_str()).collect();
    names.sort_unstable();
    expected.sort_unstable();
    if names != expected {
        return Err(KernelError::generic(format!(
            "`file` column '{column}' must have the sub-fields [{}], got [{}]",
            expected.join(", "),
            names.join(", ")
        )));
    }
    let info = field.get_basic_info();
    let id = info.has_id().then(|| info.id());
    let sub_fields = sub_fields
        .iter()
        .map(|sub_field| {
            // Sub-field ids follow the canonical sub-field order: `uri` is +1, `inline` is +6.
            let offset = file_fields
                .index_of(sub_field.name())
                .ok_or_else(|| unexpected_layout(field))?;
            // Without a field id on the column, no sub-field gets one either, even if the
            // batch carries ids for them.
            let sub_field_id = id
                .map(|id| {
                    i32::try_from(offset + 1)
                        .ok()
                        .and_then(|offset| id.checked_add(offset))
                        .ok_or_else(|| {
                            KernelError::generic(format!(
                                "The sub-field ids of `file` column '{column}' (field id {id}) \
                                 overflow"
                            ))
                        })
                })
                .transpose()?;
            with_id(sub_field, sub_field_id)
        })
        .collect::<KernelResult<_>>()?;
    rebuild_group(field, Some(LogicalType::File), sub_fields)
}

#[cfg(feature = "arrow-60")]
fn group_fields(field: &TypePtr) -> KernelResult<&[TypePtr]> {
    if field.is_group() {
        Ok(field.get_fields())
    } else {
        Err(unexpected_layout(field))
    }
}

#[cfg(feature = "arrow-60")]
fn rebuild_group(
    group: &TypePtr,
    logical_type: Option<LogicalType>,
    fields: Vec<TypePtr>,
) -> KernelResult<TypePtr> {
    let info = group.get_basic_info();
    let mut builder = Type::group_type_builder(info.name())
        .with_converted_type(info.converted_type())
        .with_logical_type(logical_type)
        .with_id(info.has_id().then(|| info.id()))
        .with_fields(fields);
    if info.has_repetition() {
        builder = builder.with_repetition(info.repetition());
    }
    Ok(Arc::new(builder.build()?))
}

#[cfg(feature = "arrow-60")]
fn with_id(field: &TypePtr, id: Option<i32>) -> KernelResult<TypePtr> {
    let Type::PrimitiveType {
        basic_info,
        physical_type,
        type_length,
        scale,
        precision,
    } = field.as_ref()
    else {
        return Err(unexpected_layout(field));
    };
    let primitive = Type::primitive_type_builder(basic_info.name(), *physical_type)
        .with_repetition(basic_info.repetition())
        .with_converted_type(basic_info.converted_type())
        .with_logical_type(basic_info.logical_type_ref().cloned())
        .with_length(*type_length)
        .with_scale(*scale)
        .with_precision(*precision)
        .with_id(id)
        .build()?;
    Ok(Arc::new(primitive))
}

#[cfg(feature = "arrow-60")]
fn unexpected_layout(field: &TypePtr) -> KernelError {
    KernelError::internal_error(format!(
        "Unexpected Parquet layout for field '{}' while writing a `file` column",
        field.name()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::writer_options;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    #[cfg(feature = "arrow-60")]
    use crate::parquet::file::reader::{FileReader as _, SerializedFileReader};
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
    };

    fn with_field_ids(
        field: StructField,
        id: Option<i64>,
        nested_ids: &[(&str, i64)],
    ) -> StructField {
        let field = match id {
            Some(id) => field.add_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(id),
            )]),
            None => field,
        };
        if nested_ids.is_empty() {
            return field;
        }
        let nested_ids: serde_json::Map<_, _> = nested_ids
            .iter()
            .map(|(path, id)| (path.to_string(), (*id).into()))
            .collect();
        field.add_metadata([(
            ColumnMetadataKey::ColumnMappingNestedIds.as_ref(),
            MetadataValue::Other(nested_ids.into()),
        )])
    }

    fn file_field(name: &str, id: Option<i64>) -> StructField {
        with_field_ids(StructField::nullable(name, DataType::file_type()), id, &[])
    }

    fn top_level(id: Option<i64>) -> StructType {
        StructType::new_unchecked([file_field("f", id)])
    }

    fn in_struct() -> StructType {
        let inner = StructType::new_unchecked([file_field("f", Some(2))]);
        StructType::new_unchecked([with_field_ids(
            StructField::nullable("s", inner),
            Some(1),
            &[],
        )])
    }

    fn array_element() -> StructType {
        let array = ArrayType::new(DataType::file_type(), true);
        StructType::new_unchecked([with_field_ids(
            StructField::nullable("a", array),
            Some(1),
            &[("a.element", 5)],
        )])
    }

    fn map_value() -> StructType {
        let map = MapType::new(DataType::STRING, DataType::file_type(), true);
        StructType::new_unchecked([with_field_ids(
            StructField::nullable("m", map),
            Some(1),
            &[("m.key", 2), ("m.value", 3)],
        )])
    }

    /// Writes an empty batch of `physical_schema` with [`with_file_columns`].
    fn write(physical_schema: &StructType) -> KernelResult<Vec<u8>> {
        write_arrow(physical_schema.try_into_arrow()?, physical_schema)
    }

    /// Writes an empty batch of `arrow_schema` with [`with_file_columns`] for `physical_schema`.
    fn write_arrow(
        arrow_schema: ArrowSchema,
        physical_schema: &StructType,
    ) -> KernelResult<Vec<u8>> {
        let options = with_file_columns(writer_options(), &arrow_schema, physical_schema)?;
        let batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new_with_options(&mut buffer, batch.schema(), options)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    /// Returns the path and sub-field ids of each `FILE`-annotated group in `schema`.
    #[cfg(feature = "arrow-60")]
    fn file_groups(schema: &Type) -> Vec<(String, Vec<Option<i32>>)> {
        fn visit(field: &Type, path: String, out: &mut Vec<(String, Vec<Option<i32>>)>) {
            if !field.is_group() {
                return;
            }
            if field.get_basic_info().logical_type_ref() == Some(&LogicalType::File) {
                let names: Vec<_> = field.get_fields().iter().map(|f| f.name()).collect();
                assert_eq!(
                    names,
                    [
                        "uri",
                        "offset",
                        "size",
                        "content_type",
                        "checksum",
                        "inline"
                    ]
                );
                let ids = field
                    .get_fields()
                    .iter()
                    .map(|f| f.get_basic_info().has_id().then(|| f.get_basic_info().id()))
                    .collect();
                out.push((path.clone(), ids));
            }
            for child in field.get_fields() {
                let child_path = match path.as_str() {
                    "" => child.name().to_string(),
                    _ => format!("{path}.{}", child.name()),
                };
                visit(child, child_path, out);
            }
        }
        let mut out = Vec::new();
        visit(schema, String::new(), &mut out);
        out
    }

    /// A `file` column is written as a `FILE`-annotated group and, when it has a field id, each
    /// sub-field gets the column's field id plus its offset.
    #[cfg(feature = "arrow-60")]
    #[rstest]
    #[case::no_field_ids(top_level(None), "f", None)]
    #[case::top_level(top_level(Some(10)), "f", Some(11))]
    #[case::in_struct(in_struct(), "s.f", Some(3))]
    #[case::array_element(array_element(), "a.list.element", Some(6))]
    #[case::map_value(map_value(), "m.key_value.value", Some(4))]
    fn writes_annotated_file_column(
        #[case] physical_schema: StructType,
        #[case] path: &str,
        #[case] first_sub_field_id: Option<i32>,
    ) {
        let buffer = write(&physical_schema).unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let root = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema_ptr();
        let sub_field_ids = (0..6)
            .map(|offset| first_sub_field_id.map(|id| id + offset))
            .collect();
        assert_eq!(file_groups(&root), vec![(path.to_string(), sub_field_ids)]);
    }

    /// Without the `FILE` logical type in parquet 59, a `file` column cannot be written wherever
    /// it appears.
    #[cfg(not(feature = "arrow-60"))]
    #[rstest]
    #[case::top_level(top_level(None), "f")]
    #[case::in_struct(in_struct(), "s.f")]
    #[case::array_element(array_element(), "a")]
    #[case::map_value(map_value(), "m")]
    fn rejects_file_column(#[case] physical_schema: StructType, #[case] column: &str) {
        let error = write(&physical_schema).unwrap_err().to_string();
        assert!(
            error.contains(&format!(
                "Writing `file` column '{column}' requires the `arrow-60` feature"
            )),
            "got: {error}"
        );
    }

    /// Without a field id on the `file` column, sub-field ids the batch carries are dropped.
    #[cfg(feature = "arrow-60")]
    #[test]
    fn drops_stale_sub_field_ids_without_field_id() {
        use std::collections::HashMap;

        use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
        use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let physical_schema = top_level(None);
        let arrow_schema: ArrowSchema = (&physical_schema).try_into_arrow().unwrap();
        let ArrowDataType::Struct(sub_fields) = arrow_schema.field(0).data_type() else {
            panic!("a file is physically a struct");
        };
        let stale: Vec<_> = sub_fields
            .iter()
            .map(|f| {
                f.as_ref().clone().with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "99".to_string(),
                )]))
            })
            .collect();
        let field = ArrowField::new("f", ArrowDataType::Struct(stale.into()), true);
        let buffer = write_arrow(ArrowSchema::new(vec![field]), &physical_schema).unwrap();

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let root = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema_ptr();
        assert_eq!(file_groups(&root), vec![("f".to_string(), vec![None; 6])]);
    }

    /// A batch whose columns are in another order than `physical_schema` is annotated by name, and
    /// one missing a `file` column fails.
    #[cfg(feature = "arrow-60")]
    #[test]
    fn matches_columns_by_name() {
        let physical_schema = StructType::new_unchecked([
            file_field("f", Some(10)),
            StructField::nullable("x", DataType::LONG),
        ]);
        let reordered = StructType::new_unchecked([
            StructField::nullable("x", DataType::LONG),
            file_field("f", Some(10)),
        ]);
        let buffer = write_arrow((&reordered).try_into_arrow().unwrap(), &physical_schema).unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let root = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema_ptr();
        let ids = (11..=16).map(Some).collect();
        assert_eq!(file_groups(&root), vec![("f".to_string(), ids)]);

        let without_file = StructType::new_unchecked([StructField::nullable("x", DataType::LONG)]);
        let error = write_arrow((&without_file).try_into_arrow().unwrap(), &physical_schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("`file` column 'f' is missing from the data"),
            "got: {error}"
        );
    }

    /// A `file` struct without every canonical sub-field fails the write.
    #[cfg(feature = "arrow-60")]
    #[test]
    fn rejects_missing_sub_field() {
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

        let physical_schema = top_level(None);
        let arrow_schema: ArrowSchema = (&physical_schema).try_into_arrow().unwrap();
        let ArrowDataType::Struct(sub_fields) = arrow_schema.field(0).data_type() else {
            panic!("a file is physically a struct");
        };
        let without_checksum: Vec<_> = sub_fields
            .iter()
            .filter(|f| f.name() != "checksum")
            .cloned()
            .collect();
        let field = ArrowField::new("f", ArrowDataType::Struct(without_checksum.into()), true);
        let error = write_arrow(ArrowSchema::new(vec![field]), &physical_schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("`file` column 'f' must have the sub-fields"),
            "got: {error}"
        );
    }

    /// A missing nested `file` column is named by its full path.
    #[cfg(feature = "arrow-60")]
    #[test]
    fn names_missing_nested_column_by_path() {
        let without_file = StructType::new_unchecked([StructField::nullable(
            "s",
            StructType::new_unchecked([StructField::nullable("x", DataType::LONG)]),
        )]);
        let error = write_arrow((&without_file).try_into_arrow().unwrap(), &in_struct())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("`file` column 's.f' is missing from the data"),
            "got: {error}"
        );
    }

    /// Sub-field ids that would pass the 32-bit field id limit fail the write.
    #[cfg(feature = "arrow-60")]
    #[test]
    fn rejects_overflowing_sub_field_ids() {
        let error = write(&top_level(Some(i64::from(i32::MAX))))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("sub-field ids of `file` column 'f'"),
            "got: {error}"
        );
    }

    /// A schema without a `file` column is written with the writer's own schema.
    #[test]
    fn writes_schema_without_file_column() {
        let schema = StructType::new_unchecked([StructField::nullable("x", DataType::LONG)]);
        write(&schema).unwrap();
    }
}
