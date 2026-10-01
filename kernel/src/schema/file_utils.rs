//! Utility functions for the file type and file-related table features.

use crate::schema::{Schema, StructType};
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

pub(crate) fn validate_file_type_feature_support(tc: &TableConfiguration) -> KernelResult<()> {
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
    use crate::actions::Protocol;
    use crate::schema::{schema, DataType};
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        assert_result_error_with_message, assert_schema_feature_validation,
    };

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
