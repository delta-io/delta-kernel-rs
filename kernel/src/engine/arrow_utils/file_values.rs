//! Validation of `file` values read from Parquet.

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{Array, StructArray};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::engine::arrow_utils::make_arrow_error;
use crate::expressions::ColumnName;
use crate::schema::{DataType, StructType};
use crate::{KernelError, KernelResult};

/// Fails if a non-null `file` value in `data` does not resolve to any referent, which the File
/// type requires readers to reject: neither `uri` nor `inline` is set, or `offset` is set without
/// `uri`. Any `uri`, absolute or relative, is accepted; a relative one resolves against the table
/// root, and reading does not resolve it. `data` must already match `schema`, as
/// [`super::apply_schema::apply_schema_to_struct`] ensures.
pub(crate) fn validate_file_values(data: &StructArray, schema: &StructType) -> KernelResult<()> {
    validate_struct(data, schema, &mut Vec::new())
}

fn validate_struct(
    array: &StructArray,
    schema: &StructType,
    path: &mut Vec<String>,
) -> KernelResult<()> {
    for (field, column) in schema.fields().zip(array.columns()) {
        path.push(field.name().clone());
        validate(column.as_ref(), field.data_type(), path)?;
        path.pop();
    }
    Ok(())
}

fn validate(array: &dyn Array, data_type: &DataType, path: &mut Vec<String>) -> KernelResult<()> {
    match data_type {
        DataType::File(_) => validate_file(as_struct(array)?, path),
        DataType::Struct(schema) => validate_struct(as_struct(array)?, schema, path),
        DataType::Array(array_type) => {
            let values = match array.data_type() {
                ArrowDataType::LargeList(_) => array.as_list_opt::<i64>().map(|l| l.values()),
                _ => array.as_list_opt::<i32>().map(|l| l.values()),
            }
            .ok_or_else(|| make_arrow_error("Arrow claimed to be a list but isn't a ListArray"))?;
            validate(values.as_ref(), array_type.element_type(), path)
        }
        DataType::Map(map_type) => {
            let map = array.as_map_opt().ok_or_else(|| {
                make_arrow_error("Arrow claimed to be a map but isn't a MapArray")
            })?;
            validate(map.keys().as_ref(), map_type.key_type(), path)?;
            validate(map.values().as_ref(), map_type.value_type(), path)
        }
        _ => Ok(()),
    }
}

fn as_struct(array: &dyn Array) -> KernelResult<&StructArray> {
    array
        .as_struct_opt()
        .ok_or_else(|| make_arrow_error("Arrow claimed to be a struct but isn't a StructArray"))
}

fn validate_file(file: &StructArray, path: &[String]) -> KernelResult<()> {
    let field = |name: &str| {
        file.column_by_name(name)
            .ok_or_else(|| make_arrow_error(format!("`file` value is missing field '{name}'")))
    };
    let (uri, offset, inline) = (field("uri")?, field("offset")?, field("inline")?);
    for row in (0..file.len()).filter(|&row| file.is_valid(row) && uri.is_null(row)) {
        let problem = if inline.is_null(row) {
            "neither `uri` nor `inline` is set"
        } else if offset.is_valid(row) {
            "`offset` is set without `uri`"
        } else {
            continue;
        };
        return Err(KernelError::generic(format!(
            "Invalid value in `file` column '{}': {problem}",
            ColumnName::new(path.iter().cloned())
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{
        ArrayRef, BinaryArray, Int64Array, ListArray, MapArray, StringArray,
    };
    use crate::arrow::buffer::{NullBuffer, OffsetBuffer};
    use crate::arrow::datatypes::{Field, Fields};
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::schema::{ArrayType, MapType, StructField};

    type Row<'a> = Option<(Option<&'a str>, Option<i64>, Option<&'a [u8]>)>;

    /// Builds a `file` array from `(uri, offset, inline)` rows; `None` is a null `file` value.
    fn file_array(rows: &[Row<'_>]) -> StructArray {
        let ArrowDataType::Struct(fields) = (&DataType::file_type()).try_into_arrow().unwrap()
        else {
            panic!("a file is physically a struct");
        };
        let uri: StringArray = rows.iter().map(|row| row.and_then(|r| r.0)).collect();
        let offset: Int64Array = rows.iter().map(|row| row.and_then(|r| r.1)).collect();
        let inline: BinaryArray = rows.iter().map(|row| row.and_then(|r| r.2)).collect();
        let nulls = |len| Arc::new(StringArray::from(vec![None::<&str>; len])) as ArrayRef;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(uri),
            Arc::new(offset),
            Arc::new(Int64Array::from(vec![None; rows.len()])),
            nulls(rows.len()),
            nulls(rows.len()),
            Arc::new(inline),
        ];
        let validity = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
        StructArray::try_new(fields, columns, Some(validity)).unwrap()
    }

    fn top_level(file: StructArray) -> (StructArray, StructType) {
        let data = StructArray::from(vec![(
            Arc::new(Field::new("f", file.data_type().clone(), true)),
            Arc::new(file) as ArrayRef,
        )]);
        let schema = StructType::new_unchecked([StructField::nullable("f", DataType::file_type())]);
        (data, schema)
    }

    #[rstest]
    #[case::uri(Some((Some("s3://b/a"), None, None)))]
    #[case::uri_with_offset(Some((Some("s3://b/a"), Some(4), None)))]
    #[case::inline(Some((None, None, Some(b"x".as_slice()))))]
    #[case::uri_and_inline(Some((Some("s3://b/a"), None, Some(b"x".as_slice()))))]
    #[case::null_value(None)]
    fn valid_values_pass(#[case] row: Row<'_>) {
        let (data, schema) = top_level(file_array(&[row]));
        validate_file_values(&data, &schema).unwrap();
    }

    #[rstest]
    #[case::no_referent((None, None, None), "neither `uri` nor `inline` is set")]
    #[case::offset_without_uri((None, Some(4), Some(b"x".as_slice())), "`offset` is set without `uri`")]
    fn invalid_values_fail(
        #[case] row: (Option<&str>, Option<i64>, Option<&[u8]>),
        #[case] problem: &str,
    ) {
        let (data, schema) = top_level(file_array(&[
            Some((Some("s3://b/a"), None, None)),
            Some(row),
        ]));
        let error = validate_file_values(&data, &schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("Invalid value in `file` column 'f': {problem}")),
            "got: {error}"
        );
    }

    /// An invalid value is found in a struct field, an array element, and a map value, and the
    /// error names the enclosing column.
    #[rstest]
    #[case::in_struct("s", "s.f")]
    #[case::array_element("a", "a")]
    #[case::map_value("m", "m")]
    fn invalid_nested_values_fail(#[case] column: &str, #[case] path: &str) {
        let file = file_array(&[Some((None, None, None))]);
        let file_field = Arc::new(Field::new("f", file.data_type().clone(), true));
        let (array, data_type): (ArrayRef, DataType) = match column {
            "s" => (
                Arc::new(StructArray::from(vec![(
                    file_field,
                    Arc::new(file) as ArrayRef,
                )])),
                StructType::new_unchecked([StructField::nullable("f", DataType::file_type())])
                    .into(),
            ),
            "a" => (
                Arc::new(ListArray::new(
                    Arc::new(Field::new("element", file.data_type().clone(), true)),
                    OffsetBuffer::from_lengths([1]),
                    Arc::new(file),
                    None,
                )),
                ArrayType::new(DataType::file_type(), true).into(),
            ),
            _ => {
                let entries = StructArray::from(vec![
                    (
                        Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                        Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("value", file.data_type().clone(), true)),
                        Arc::new(file) as ArrayRef,
                    ),
                ]);
                let entries_field = Arc::new(Field::new(
                    "key_value",
                    ArrowDataType::Struct(Fields::from(entries.fields().to_vec())),
                    false,
                ));
                (
                    Arc::new(MapArray::new(
                        entries_field,
                        OffsetBuffer::from_lengths([1]),
                        entries,
                        None,
                        false,
                    )),
                    MapType::new(DataType::STRING, DataType::file_type(), true).into(),
                )
            }
        };
        let data = StructArray::from(vec![(
            Arc::new(Field::new(column, array.data_type().clone(), true)),
            array,
        )]);
        let schema = StructType::new_unchecked([StructField::nullable(column, data_type)]);

        let error = validate_file_values(&data, &schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("Invalid value in `file` column '{path}'")),
            "got: {error}"
        );
    }
}
