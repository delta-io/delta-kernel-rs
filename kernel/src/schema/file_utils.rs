//! Utility functions for the file type and file-related table features.

use crate::schema::{DataType, Schema, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{KernelError, KernelResult, Result};

/// Schema visitor that checks if any column in the schema uses FILE type
pub(crate) struct UsesFile;

impl<'a> SchemaTransform<'a> for UsesFile {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_file(&mut self, _: &'a StructType) -> Result<(), ()> {
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
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_map_key(&mut self, ktype: &'a DataType) -> Result<(), ()> {
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
                "Table contains FILE columns but does not have the required 'fileType' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::schema_has_file_in_map_key;
    use crate::actions::Protocol;
    use crate::schema::{schema, ArrayType, DataType, MapType, StructField, StructType};
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
        let err_msg = "Table contains FILE columns but does not have the required 'fileType' feature in reader and writer features";

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
}
