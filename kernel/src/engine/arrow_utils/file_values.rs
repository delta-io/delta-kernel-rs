//! Validation of `file` values read from Parquet.

use std::ops::Range;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{Array, OffsetSizeTrait, StructArray};
use crate::arrow::datatypes::{DataType as ArrowDataType, Int64Type};
use crate::engine::arrow_utils::make_arrow_error;
use crate::expressions::ColumnName;
use crate::schema::{DataType, StructType};
use crate::{KernelError, KernelResult};

/// Fails if a non-null `file` value in `data` is invalid under the Parquet `FILE` specification,
/// which requires readers to reject it: neither `uri` nor `inline` is set, `offset` or `size` is
/// set without `uri`, `offset` is set without `size`, or `offset` or `size` is negative. Any
/// `uri`, absolute or relative, is accepted; a relative one resolves against the table root, and
/// reading does not resolve it.
///
/// Columns are matched to `kernel_schema` by name, and a `file` column missing from `data` is an
/// error. A value is checked only if it is present: not null, and not under a null struct, list or
/// map entry, nor outside the ranges a list or map uses.
pub(crate) fn validate_file_values(
    data: &StructArray,
    kernel_schema: &StructType,
) -> KernelResult<()> {
    let Some(file_paths) = FilePaths::of_struct(kernel_schema) else {
        return Ok(());
    };
    validate(
        data,
        &file_paths,
        &rows_present(data, None),
        &mut Vec::new(),
    )
}

/// The parts of a schema that lead to `file` values: each `file`, and the structs, arrays and maps
/// that contain one. Other fields are left out, so validating walks only these paths.
#[derive(Debug)]
pub(crate) enum FilePaths {
    File,
    /// The fields of a struct that contain a `file`.
    Struct(Vec<(String, FilePaths)>),
    Array(Box<FilePaths>),
    Map {
        key: Option<Box<FilePaths>>,
        value: Option<Box<FilePaths>>,
    },
}

impl FilePaths {
    /// Returns the paths to the `file` values in `schema`, or `None` if it has none. Each field is
    /// visited once.
    pub(crate) fn of_struct(schema: &StructType) -> Option<Self> {
        let fields: Vec<_> = schema
            .fields()
            .filter_map(|field| Some((field.name().clone(), Self::of(field.data_type())?)))
            .collect();
        (!fields.is_empty()).then_some(Self::Struct(fields))
    }

    fn of(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::File(_) => Some(Self::File),
            DataType::Struct(schema) => Self::of_struct(schema),
            DataType::Array(array_type) => {
                Some(Self::Array(Box::new(Self::of(array_type.element_type())?)))
            }
            DataType::Map(map_type) => {
                let key = Self::of(map_type.key_type()).map(Box::new);
                let value = Self::of(map_type.value_type()).map(Box::new);
                (key.is_some() || value.is_some()).then_some(Self::Map { key, value })
            }
            _ => None,
        }
    }
}

/// Returns, for each row of `array`, whether it is present: valid itself, and present in its
/// parent (`parent_present`, if any, has one entry per row of `array`).
fn rows_present(array: &dyn Array, parent_present: Option<&[bool]>) -> Vec<bool> {
    (0..array.len())
        .map(|row| array.is_valid(row) && parent_present.is_none_or(|present| present[row]))
        .collect()
}

/// Returns, for each of the `len` child values of a list or map, whether a present row uses it.
/// `ranges` gives the child values each row uses.
fn entries_present(
    ranges: impl Iterator<Item = Range<usize>>,
    rows_present: &[bool],
    len: usize,
) -> Vec<bool> {
    let mut entries = vec![false; len];
    for (range, _) in ranges.zip(rows_present).filter(|(_, present)| **present) {
        entries[range].fill(true);
    }
    entries
}

/// The child values each row of a list or map uses, from its offsets.
fn offset_ranges<O: OffsetSizeTrait>(offsets: &[O]) -> impl Iterator<Item = Range<usize>> + '_ {
    offsets.windows(2).map(|w| w[0].as_usize()..w[1].as_usize())
}

/// The child values each row of a list view uses, from its offsets and sizes.
fn view_ranges<'a, O: OffsetSizeTrait>(
    offsets: &'a [O],
    sizes: &'a [O],
) -> impl Iterator<Item = Range<usize>> + 'a {
    offsets
        .iter()
        .zip(sizes)
        .map(|(offset, size)| offset.as_usize()..offset.as_usize() + size.as_usize())
}

/// Returns the child values of the list-like `array` and which of them a present row uses.
fn list_entries<'a>(
    array: &'a dyn Array,
    present: &[bool],
) -> KernelResult<(&'a dyn Array, Vec<bool>)> {
    let entries = |values: &'a dyn Array, ranges: &mut dyn Iterator<Item = Range<usize>>| {
        let entries = entries_present(ranges, present, values.len());
        (values, entries)
    };
    let list = match array.data_type() {
        ArrowDataType::List(_) => array.as_list_opt::<i32>().map(|list| {
            entries(
                list.values().as_ref(),
                &mut offset_ranges(list.value_offsets()),
            )
        }),
        ArrowDataType::LargeList(_) => array.as_list_opt::<i64>().map(|list| {
            entries(
                list.values().as_ref(),
                &mut offset_ranges(list.value_offsets()),
            )
        }),
        ArrowDataType::ListView(_) => array.as_list_view_opt::<i32>().map(|list| {
            let mut ranges = view_ranges(list.value_offsets(), list.value_sizes());
            entries(list.values().as_ref(), &mut ranges)
        }),
        ArrowDataType::LargeListView(_) => array.as_list_view_opt::<i64>().map(|list| {
            let mut ranges = view_ranges(list.value_offsets(), list.value_sizes());
            entries(list.values().as_ref(), &mut ranges)
        }),
        ArrowDataType::FixedSizeList(..) => array.as_fixed_size_list_opt().map(|list| {
            let length = list.value_length() as usize;
            let mut ranges = (0..list.len()).map(|row| {
                let start = list.value_offset(row) as usize;
                start..start + length
            });
            entries(list.values().as_ref(), &mut ranges)
        }),
        _ => None,
    };
    list.ok_or_else(|| {
        make_arrow_error(format!(
            "Expected a list for an array of `file` values, got {}",
            array.data_type()
        ))
    })
}

/// Validates the `file` values at `file_paths` in `array`, whose rows are present in its parent
/// per `parent_present`.
fn validate(
    array: &dyn Array,
    file_paths: &FilePaths,
    parent_present: &[bool],
    path: &mut Vec<String>,
) -> KernelResult<()> {
    let current_present = rows_present(array, Some(parent_present));
    match file_paths {
        FilePaths::File => validate_file(as_struct(array)?, &current_present, path),
        FilePaths::Struct(fields) => {
            let array = as_struct(array)?;
            for (name, inner) in fields {
                path.push(name.clone());
                let column = array.column_by_name(name).ok_or_else(|| {
                    make_arrow_error(format!(
                        "`file` column '{}' is missing from the data",
                        ColumnName::new(path.iter().cloned())
                    ))
                })?;
                validate(column.as_ref(), inner, &current_present, path)?;
                path.pop();
            }
            Ok(())
        }
        FilePaths::Array(element) => {
            let (values, entries) = list_entries(array, &current_present)?;
            validate(values, element, &entries, path)
        }
        FilePaths::Map { key, value } => {
            let map = array.as_map_opt().ok_or_else(|| {
                make_arrow_error("Arrow claimed to be a map but isn't a MapArray")
            })?;
            let entries = entries_present(
                offset_ranges(map.value_offsets()),
                &current_present,
                map.entries().len(),
            );
            if let Some(key) = key {
                validate(map.keys().as_ref(), key, &entries, path)?;
            }
            if let Some(value) = value {
                validate(map.values().as_ref(), value, &entries, path)?;
            }
            Ok(())
        }
    }
}

fn as_struct(array: &dyn Array) -> KernelResult<&StructArray> {
    array
        .as_struct_opt()
        .ok_or_else(|| make_arrow_error("Arrow claimed to be a struct but isn't a StructArray"))
}

fn validate_file(file: &StructArray, present: &[bool], path: &[String]) -> KernelResult<()> {
    let field = |name: &str| {
        file.column_by_name(name)
            .ok_or_else(|| make_arrow_error(format!("`file` value is missing field '{name}'")))
    };
    let int64 = |name: &str| {
        field(name)?
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| make_arrow_error(format!("`file` field '{name}' is not an INT64")))
    };
    let (uri, inline) = (field("uri")?, field("inline")?);
    let (offset, size) = (int64("offset")?, int64("size")?);
    for row in (0..file.len()).filter(|&row| present[row]) {
        let problem = if uri.is_null(row) && inline.is_null(row) {
            "neither `uri` nor `inline` is set"
        } else if uri.is_null(row) && offset.is_valid(row) {
            "`offset` is set without `uri`"
        } else if uri.is_null(row) && size.is_valid(row) {
            "`size` is set without `uri`"
        } else if offset.is_valid(row) && size.is_null(row) {
            "`offset` is set without `size`"
        } else if offset.is_valid(row) && offset.value(row) < 0 {
            "`offset` is negative"
        } else if size.is_valid(row) && size.value(row) < 0 {
            "`size` is negative"
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
        ArrayRef, BinaryArray, FixedSizeListArray, Int64Array, LargeListArray, ListArray,
        ListViewArray, MapArray, StringArray,
    };
    use crate::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use crate::arrow::datatypes::{Field, Fields};
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::schema::{ArrayType, MapType, StructField};

    /// The `(uri, offset, size, inline)` fields of a `file` value.
    type Value<'a> = (Option<&'a str>, Option<i64>, Option<i64>, Option<&'a [u8]>);
    type Row<'a> = Option<Value<'a>>;

    /// Builds a `file` array from `(uri, offset, size, inline)` rows; `None` is a null `file`
    /// value.
    fn file_array(rows: &[Row<'_>]) -> StructArray {
        let ArrowDataType::Struct(fields) = (&DataType::file_type()).try_into_arrow().unwrap()
        else {
            panic!("a file is physically a struct");
        };
        let uri: StringArray = rows.iter().map(|row| row.and_then(|r| r.0)).collect();
        let offset: Int64Array = rows.iter().map(|row| row.and_then(|r| r.1)).collect();
        let size: Int64Array = rows.iter().map(|row| row.and_then(|r| r.2)).collect();
        let inline: BinaryArray = rows.iter().map(|row| row.and_then(|r| r.3)).collect();
        let nulls = |len| Arc::new(StringArray::from(vec![None::<&str>; len])) as ArrayRef;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(uri),
            Arc::new(offset),
            Arc::new(size),
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
    #[case::uri(Some((Some("s3://b/a"), None, None, None)))]
    #[case::uri_with_size(Some((Some("s3://b/a"), None, Some(10), None)))]
    #[case::uri_with_range(Some((Some("s3://b/a"), Some(4), Some(10), None)))]
    #[case::inline(Some((None, None, None, Some(b"x".as_slice()))))]
    #[case::uri_and_inline(Some((Some("s3://b/a"), None, None, Some(b"x".as_slice()))))]
    #[case::empty_range(Some((Some("s3://b/a"), Some(0), Some(0), None)))]
    #[case::inline_with_locator(Some((Some("s3://b/a"), Some(4), Some(10), Some(b"x".as_slice()))))]
    #[case::null_value(None)]
    fn valid_values_pass(#[case] row: Row<'_>) {
        let (data, schema) = top_level(file_array(&[row]));
        validate_file_values(&data, &schema).unwrap();
    }

    #[rstest]
    #[case::no_referent((None, None, None, None), "neither `uri` nor `inline` is set")]
    #[case::offset_without_uri(
        (None, Some(4), Some(10), Some(b"x".as_slice())),
        "`offset` is set without `uri`"
    )]
    #[case::offset_without_size(
        (Some("s3://b/a"), Some(4), None, None),
        "`offset` is set without `size`"
    )]
    #[case::size_without_uri(
        (None, None, Some(10), Some(b"x".as_slice())),
        "`size` is set without `uri`"
    )]
    #[case::negative_offset((Some("s3://b/a"), Some(-1), Some(10), None), "`offset` is negative")]
    #[case::negative_size((Some("s3://b/a"), None, Some(-1), None), "`size` is negative")]
    fn invalid_values_fail(#[case] row: Value<'_>, #[case] problem: &str) {
        let (data, schema) = top_level(file_array(&[
            Some((Some("s3://b/a"), None, None, None)),
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
        let file = file_array(&[Some((None, None, None, None))]);
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

    const INVALID: Row<'static> = Some((None, None, None, None));
    const VALID: Row<'static> = Some((Some("s3://b/a"), None, None, None));

    fn list_of_files(
        file: StructArray,
        offsets: &[i32],
        nulls: Option<NullBuffer>,
    ) -> (StructArray, StructType) {
        let list = ListArray::new(
            Arc::new(Field::new("element", file.data_type().clone(), true)),
            OffsetBuffer::new(offsets.to_vec().into()),
            Arc::new(file),
            nulls,
        );
        let data = StructArray::from(vec![(
            Arc::new(Field::new("a", list.data_type().clone(), true)),
            Arc::new(list) as ArrayRef,
        )]);
        let schema = StructType::new_unchecked([StructField::nullable(
            "a",
            ArrayType::new(DataType::file_type(), true),
        )]);
        (data, schema)
    }

    /// An invalid value is not checked where no present row uses it: in the range of a null list
    /// slot, or outside the ranges of a sliced list.
    #[test]
    fn values_no_present_row_uses_pass() {
        // Slot 1 is null but still spans the invalid element.
        let nulls = NullBuffer::from(vec![true, false]);
        let (data, schema) = list_of_files(file_array(&[VALID, INVALID]), &[0, 1, 2], Some(nulls));
        validate_file_values(&data, &schema).unwrap();

        // The slice keeps only the first slot, which spans the valid element.
        let (data, schema) = list_of_files(file_array(&[VALID, INVALID]), &[0, 1, 2], None);
        validate_file_values(&data.slice(0, 1), &schema).unwrap();
        validate_file_values(&data, &schema).unwrap_err();
    }

    /// Wraps `list` as the only column, `a`, of an array of `file` values.
    fn array_column(list: ArrayRef) -> (StructArray, StructType) {
        let data = StructArray::from(vec![(
            Arc::new(Field::new("a", list.data_type().clone(), true)),
            list,
        )]);
        let schema = StructType::new_unchecked([StructField::nullable(
            "a",
            ArrayType::new(DataType::file_type(), true),
        )]);
        (data, schema)
    }

    /// In a list view, a row uses the values its offset and size give, in any order, so an invalid
    /// value is checked only where a present row's view covers it.
    #[test]
    fn list_view_values_follow_present_rows() {
        let list_view = |offsets: Vec<i32>, nulls| {
            let file = file_array(&[VALID, INVALID]);
            let list = ListViewArray::new(
                Arc::new(Field::new("element", file.data_type().clone(), true)),
                ScalarBuffer::from(offsets),
                ScalarBuffer::from(vec![1, 1]),
                Arc::new(file),
                nulls,
            );
            array_column(Arc::new(list))
        };
        // Row 0 is null and covers the invalid value; row 1 covers the valid one.
        let (data, schema) = list_view(vec![1, 0], Some(NullBuffer::from(vec![false, true])));
        validate_file_values(&data, &schema).unwrap();
        let (data, schema) = list_view(vec![1, 0], None);
        validate_file_values(&data, &schema).unwrap_err();
    }

    /// In a fixed-size list, an invalid value is checked only where a present row uses it,
    /// including in a slice.
    #[test]
    fn fixed_size_list_values_follow_present_rows() {
        let fixed_size_list = |nulls| {
            let file = file_array(&[VALID, INVALID]);
            let list = FixedSizeListArray::new(
                Arc::new(Field::new("element", file.data_type().clone(), true)),
                1,
                Arc::new(file),
                nulls,
            );
            array_column(Arc::new(list))
        };
        let (data, schema) = fixed_size_list(Some(NullBuffer::from(vec![true, false])));
        validate_file_values(&data, &schema).unwrap();
        let (data, schema) = fixed_size_list(None);
        validate_file_values(&data, &schema).unwrap_err();
        validate_file_values(&data.slice(0, 1), &schema).unwrap();
    }

    /// In a `LargeList` and a map, an invalid value is checked only where a present row uses it.
    #[test]
    fn large_list_and_map_values_follow_present_rows() {
        let large_list = |nulls| {
            let file = file_array(&[VALID, INVALID]);
            let list = LargeListArray::new(
                Arc::new(Field::new("element", file.data_type().clone(), true)),
                OffsetBuffer::new(vec![0i64, 1, 2].into()),
                Arc::new(file),
                nulls,
            );
            let data = StructArray::from(vec![(
                Arc::new(Field::new("a", list.data_type().clone(), true)),
                Arc::new(list) as ArrayRef,
            )]);
            let schema = StructType::new_unchecked([StructField::nullable(
                "a",
                ArrayType::new(DataType::file_type(), true),
            )]);
            (data, schema)
        };
        let (data, schema) = large_list(Some(NullBuffer::from(vec![true, false])));
        validate_file_values(&data, &schema).unwrap();
        let (data, schema) = large_list(None);
        validate_file_values(&data, &schema).unwrap_err();

        let map = |nulls| {
            let file = file_array(&[VALID, INVALID]);
            let entries = StructArray::from(vec![
                (
                    Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                    Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
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
            let map = MapArray::new(
                entries_field,
                OffsetBuffer::from_lengths([1, 1]),
                entries,
                nulls,
                false,
            );
            let data = StructArray::from(vec![(
                Arc::new(Field::new("m", map.data_type().clone(), true)),
                Arc::new(map) as ArrayRef,
            )]);
            let schema = StructType::new_unchecked([StructField::nullable(
                "m",
                MapType::new(DataType::STRING, DataType::file_type(), true),
            )]);
            (data, schema)
        };
        let (data, schema) = map(Some(NullBuffer::from(vec![true, false])));
        validate_file_values(&data, &schema).unwrap();
        let (data, schema) = map(None);
        validate_file_values(&data, &schema).unwrap_err();
    }

    /// An invalid value under a null struct inside a list is not checked.
    #[test]
    fn value_under_null_struct_in_list_passes() {
        let file = file_array(&[VALID, INVALID]);
        let file_field = Arc::new(Field::new("f", file.data_type().clone(), true));
        let element = StructArray::try_new(
            vec![file_field].into(),
            vec![Arc::new(file) as ArrayRef],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap();
        let list = ListArray::new(
            Arc::new(Field::new("element", element.data_type().clone(), true)),
            OffsetBuffer::from_lengths([2]),
            Arc::new(element),
            None,
        );
        let data = StructArray::from(vec![(
            Arc::new(Field::new("a", list.data_type().clone(), true)),
            Arc::new(list) as ArrayRef,
        )]);
        let inner = StructType::new_unchecked([StructField::nullable("f", DataType::file_type())]);
        let schema = StructType::new_unchecked([StructField::nullable(
            "a",
            ArrayType::new(DataType::from(inner), true),
        )]);
        validate_file_values(&data, &schema).unwrap();
    }

    /// Columns are matched by name, and a missing `file` column is an error.
    #[test]
    fn columns_are_matched_by_name() {
        let other = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
        let file = Arc::new(file_array(&[VALID, INVALID])) as ArrayRef;
        let data = StructArray::from(vec![
            (
                Arc::new(Field::new("f", file.data_type().clone(), true)),
                file,
            ),
            (
                Arc::new(Field::new("s", ArrowDataType::Utf8, true)),
                other.clone(),
            ),
        ]);
        let schema = StructType::new_unchecked([
            StructField::nullable("s", DataType::STRING),
            StructField::nullable("f", DataType::file_type()),
        ]);
        let error = validate_file_values(&data, &schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Invalid value in `file` column 'f'"),
            "got: {error}"
        );

        let data = StructArray::from(vec![(
            Arc::new(Field::new("s", ArrowDataType::Utf8, true)),
            other,
        )]);
        let error = validate_file_values(&data, &schema)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("`file` column 'f' is missing from the data"),
            "got: {error}"
        );
    }
}
