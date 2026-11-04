//! Utility functions for the file type and file-related table features.

use crate::schema::{DataType, Schema, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::{SchemaTransform, transform_output_type};
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Schema visitor that checks if any column in the schema uses FILE type
pub(crate) struct UsesFile;

impl<'a> SchemaTransform<'a> for UsesFile {
    transform_output_type!(|'a, T| KernelResult<(), ()>);

    fn transform_file(&mut self, _: &'a StructType) -> KernelResult<(), ()> {
        Err(())
    }
}

/// Checks if any column in the schema (including nested columns) has FILE type.
pub(crate) fn schema_contains_file_type(schema: &Schema) -> bool {
    UsesFile.transform_struct(schema).is_err()
}

/// Schema visitor that checks if any map in the schema has a key type that contains a FILE.
struct FileInMapKey;

impl<'a> SchemaTransform<'a> for FileInMapKey {
    transform_output_type!(|'a, T| KernelResult<(), ()>);

    fn transform_map_key(&mut self, ktype: &'a DataType) -> KernelResult<(), ()> {
        if UsesFile.transform(ktype).is_err() {
            return Err(());
        }
        // Keep descending: the key may itself contain maps.
        self.transform(ktype)
    }
}

/// Checks if any map in the schema (including nested maps) has a key type containing a FILE. A
/// `file` is not a comparable type, so it cannot be a map key; it is allowed as an array element
/// and as a map value.
pub(crate) fn schema_has_file_in_map_key(schema: &Schema) -> bool {
    FileInMapKey.transform_struct(schema).is_err()
}

pub(crate) fn validate_file_type_feature_support(tc: &TableConfiguration) -> KernelResult<()> {
    // A `file` is not comparable, so it can never be a map key, regardless of the table's features.
    require!(
        !schema_has_file_in_map_key(&tc.logical_schema()),
        KernelError::schema(
            "A map key type must not contain a FILE column: FILE is not comparable"
        )
    );
    // Both the reader and writer need to have either the FileType or the FileTypePreview features.
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::FileType)
        && !protocol.has_table_feature(&TableFeature::FileTypePreview)
    {
        require!(
            !schema_contains_file_type(&tc.logical_schema()),
            KernelError::unsupported(
                "Table contains FILE columns but does not have the required 'fileType' or 'fileType-preview' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// The parts of a schema that lead to `file` values: each `file`, and the structs, arrays and maps
/// that contain one. Other fields are left out, so validating and writing `file` values walk only
/// these paths.
// Validating and writing `file` values, the only users, need `default-engine-base`.
#[cfg(feature = "default-engine-base")]
#[derive(Debug)]
pub(crate) enum FilePaths {
    File,
    /// The fields of a struct that contain a `file`.
    Struct(Vec<(String, FilePaths)>),
    Array(Box<FilePaths>),
    /// The value of a map. A `file` cannot be in a map key.
    Map(Box<FilePaths>),
}

#[cfg(feature = "default-engine-base")]
impl FilePaths {
    /// Returns the paths to the `file` values in `schema`, or `None` if it has none. Each field is
    /// visited once. Fails if a map key contains a `file`.
    pub(crate) fn from_schema(schema: &StructType) -> KernelResult<Option<Self>> {
        let mut fields = Vec::new();
        for field in schema.fields() {
            if let Some(paths) = Self::from_data_type(field.data_type())? {
                fields.push((field.name().clone(), paths));
            }
        }
        Ok((!fields.is_empty()).then_some(Self::Struct(fields)))
    }

    fn from_data_type(data_type: &DataType) -> KernelResult<Option<Self>> {
        Ok(match data_type {
            DataType::File(_) => Some(Self::File),
            DataType::Struct(schema) => Self::from_schema(schema)?,
            DataType::Array(array_type) => Self::from_data_type(array_type.element_type())?
                .map(|element| Self::Array(Box::new(element))),
            DataType::Map(map_type) => {
                // Table schemas with a `file` in a map key are rejected when they are loaded.
                if Self::from_data_type(map_type.key_type())?.is_some() {
                    return Err(KernelError::internal_error(
                        "A map key type must not contain a `file`",
                    ));
                }
                Self::from_data_type(map_type.value_type())?.map(|value| Self::Map(Box::new(value)))
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::schema_has_file_in_map_key;
    use crate::actions::Protocol;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType, schema};
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        assert_result_error_with_message, assert_schema_feature_validation,
    };

    /// A `file` can be an array element or a map value, but never (at any depth) part of a map key.
    #[rstest]
    #[case::map_value_ok(
        MapType::new(DataType::STRING, DataType::file_type(), true).into(), false)]
    #[case::array_element_ok(ArrayType::new(DataType::file_type(), true).into(), false)]
    #[case::file_key(MapType::new(DataType::file_type(), DataType::STRING, true).into(), true)]
    #[case::array_of_file_key(
        MapType::new(ArrayType::new(DataType::file_type(), true), DataType::STRING, true).into(),
        true
    )]
    #[case::struct_with_file_key(
        MapType::new(DataType::from(schema! { nullable "f": (DataType::file_type()) }),
                     DataType::STRING, true).into(),
        true
    )]
    #[case::nested_map_with_file_key(
        MapType::new(
            DataType::STRING,
            MapType::new(DataType::file_type(), DataType::STRING, true),
            true
        )
        .into(),
        true
    )]
    fn test_file_in_map_key(#[case] column_type: DataType, #[case] rejected: bool) {
        let schema = StructType::new_unchecked([StructField::nullable("c", column_type)]);
        assert_eq!(schema_has_file_in_map_key(&schema), rejected);
    }

    #[test]
    fn test_file_feature_validation() {
        let schema_with = schema! {
            not_null "id": INTEGER,
            nullable "f": (DataType::file_type()),
        };
        let schema_without = schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        };
        let nested_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "nested": { nullable "inner_f": (DataType::file_type()) },
        };
        let protocol_without =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();
        let err_msg = "Table contains FILE columns but does not have the required 'fileType' or 'fileType-preview' feature in reader and writer features";

        for (reader, writer) in [
            (TableFeature::FileType, TableFeature::FileType),
            (TableFeature::FileTypePreview, TableFeature::FileTypePreview),
        ] {
            let protocol_with = Protocol::try_new_modern([&reader], [&writer]).unwrap();

            // ReaderWriter features must be listed on both sides
            assert_result_error_with_message(
                Protocol::try_new_modern([&reader], TableFeature::EMPTY_LIST),
                "Reader features must contain only ReaderWriter features that are also listed in writer features",
            );
            assert_result_error_with_message(
                Protocol::try_new_modern(TableFeature::EMPTY_LIST, [&writer]),
                "Writer features must be Writer-only or also listed in reader features",
            );

            assert_schema_feature_validation(
                &schema_with,
                &schema_without,
                &protocol_with,
                &protocol_without,
                &[&nested_schema_with],
                err_msg,
            );
        }
    }

    /// Building the paths to `file` values fails for a map whose key contains a `file`, which a
    /// table schema never has.
    #[cfg(feature = "default-engine-base")]
    #[rstest]
    #[case::file_key(MapType::new(DataType::file_type(), DataType::STRING, true))]
    #[case::array_of_files_key(MapType::new(
        ArrayType::new(DataType::file_type(), true),
        DataType::STRING,
        true
    ))]
    #[case::struct_key(MapType::new(
        StructType::new_unchecked([StructField::nullable("f", DataType::file_type())]),
        DataType::STRING,
        true
    ))]
    #[case::key_of_map_in_value(MapType::new(
        DataType::STRING,
        MapType::new(DataType::file_type(), DataType::STRING, true),
        true
    ))]
    fn file_paths_reject_file_in_map_key(#[case] map: MapType) {
        let schema = StructType::new_unchecked([StructField::nullable("m", map)]);
        assert_result_error_with_message(
            super::FilePaths::from_schema(&schema),
            "A map key type must not contain a `file`",
        );
    }
}
