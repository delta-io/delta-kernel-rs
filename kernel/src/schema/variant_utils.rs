//! Utility functions for the variant type and variant-related table features.

use crate::table_features::{ReaderFeature, WriterFeature};
use crate::actions::Protocol;
use crate::{DeltaResult, Error};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use crate::engine::arrow_conversion::TryFromKernel;
use crate::schema::{DataType, PrimitiveType, Schema, SchemaTransform, StructField};
use crate::utils::require;
use std::borrow::Cow;
use std::collections::HashMap;
use delta_kernel_derive::internal_api;

pub const VARIANT_METADATA: &str = "__VARIANT__";

/// The variant type for arrow is a struct where where the `metadata` field is tagged with some
/// additional metadata saying `__VARIANT__ = true`.
pub fn variant_arrow_type() -> ArrowDataType {
    TryFromKernel::try_from_kernel(&variant_struct_schema()).unwrap()
}

/// Variant is represented as a `STRUCT<value: BINARY, metadata: BINARY>` where the metadata field
/// has some additional metadata saying `__VARIANT__ = true`. This makes it easier for the parquet
/// reader to understand variant.
pub fn variant_struct_schema() -> DataType {
    DataType::struct_type([
        StructField::nullable("value", DataType::BINARY),
        StructField::nullable("metadata", DataType::BINARY)
            .with_metadata([(VARIANT_METADATA, "true")]),
    ])
}

pub(crate) fn validate_variant_type_feature_support(
    schema: &Schema,
    protocol: &Protocol,
) -> DeltaResult<()> {
    // Both the reader and writer need to have either the VariantType or the VariantTypePreview
    // features.
    if (!protocol.has_reader_feature(&ReaderFeature::VariantType)
        && !protocol.has_reader_feature(&ReaderFeature::VariantTypePreview)) ||
        (!protocol.has_writer_feature(&WriterFeature::VariantType)
        && !protocol.has_writer_feature(&WriterFeature::VariantTypePreview))
    {
        let mut uses_variant = UsesVariant::default();
        let _ = uses_variant.transform_struct(schema);
        require!(
            !uses_variant.0,
            Error::unsupported(
                "Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Schema visitor that checks if any column in the schema uses VARIANT type
#[derive(Debug, Default)]
pub(crate) struct UsesVariant(pub(crate) bool);

impl<'a> SchemaTransform<'a> for UsesVariant {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if *ptype == PrimitiveType::Variant {
            self.0 = true;
        }
        None
    }
}

/// Utility to make it easier for third-party engines to replace nested Variants with TAGGED
/// `STRUCT<value: BINARY, metadata: BINARY>` to it is easier for engines to construct variant read
/// schemas.
#[allow(dead_code)]
pub struct ReplaceVariantWithStructRepresentation();

impl<'a> SchemaTransform<'a> for ReplaceVariantWithStructRepresentation {
    fn should_transform_primitive_to_data_type(&self) -> bool { true }

    fn transform_primitive_to_data_type(&mut self,
        ptype: &'a PrimitiveType) -> Option<Cow<'a, DataType>> {
        if *ptype == PrimitiveType::Variant {
            Some(Cow::Owned(variant_struct_schema()))
        } else {
            Some(Cow::Owned(DataType::Primitive(ptype.clone())))
        }
    }
}

/// Variant arrow type without metadata tag for testing purposes
#[allow(dead_code)]
#[internal_api]
pub(crate) fn variant_arrow_type_without_tag() -> ArrowDataType {
    let value_field = ArrowField::new("value", ArrowDataType::Binary, true);
    let metadata_field = ArrowField::new("metadata", ArrowDataType::Binary, true);
    let fields = vec![value_field, metadata_field];
    ArrowDataType::Struct(fields.into())
}

// Only used for testing purposes. Here, the arrow variant type is constructed manually instead of
// relying on the Kernel Struct -> ArrowStruct translation. This ensures that the metadata in
// Variant's arrow representation is added correctly.
#[allow(dead_code)]
#[internal_api]
pub(crate) fn hard_coded_variant_arrow_type() -> ArrowDataType {
    let mut tag = HashMap::new();
    tag.insert(VARIANT_METADATA.to_string(), "true".to_string());
    let value_field = ArrowField::new("value", ArrowDataType::Binary, true);
    let metadata_field = ArrowField::new("metadata", ArrowDataType::Binary, true)
        .with_metadata(tag);
    let fields = vec![value_field, metadata_field];
    ArrowDataType::Struct(fields.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::Protocol;
    use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
    use crate::table_features::{ReaderFeature, WriterFeature};

    #[test]
    fn test_variant_schema_replace_top_level() {
        let dt = DataType::VARIANT;
        let mut replace_variant =
            ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        assert_eq!(transformed.unwrap().into_owned(), variant_struct_schema());
    }

    #[test]
    fn test_variant_schema_replace_array() {
        let dt: DataType = ArrayType::new(DataType::VARIANT, false).into();
        let mut replace_variant =
            ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType = ArrayType::new(variant_struct_schema(), false).into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_schema_replace_struct() {
        let dt: DataType = DataType::struct_type([
            StructField::nullable("i", DataType::INTEGER),
            StructField::nullable("v", DataType::VARIANT),
            StructField::nullable("s", DataType::struct_type([
                StructField::nullable("v1", DataType::VARIANT),
                StructField::nullable("i1", DataType::STRING),
            ])),
            StructField::nullable("not_variant", variant_struct_schema()),
        ]).into();
        // let dt: DataType = ArrayType::new(DataType::VARIANT, false).into();
        let mut replace_variant =
            ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType = DataType::struct_type([
            StructField::nullable("i", DataType::INTEGER),
            StructField::nullable("v", variant_struct_schema()),
            StructField::nullable("s", DataType::struct_type([
                StructField::nullable("v1", variant_struct_schema()),
                StructField::nullable("i1", DataType::STRING),
            ])),
            StructField::nullable("not_variant", variant_struct_schema()),
        ]).into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_schema_replace_map() {
        let dt: DataType = MapType::new(DataType::STRING, DataType::VARIANT, false).into();
        let mut replace_variant =
            ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType = MapType::new(DataType::STRING, variant_struct_schema(), false).into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_feature_validation() {
        let features = vec![
            (ReaderFeature::VariantType, WriterFeature::VariantType),
            (ReaderFeature::VariantTypePreview, WriterFeature::VariantTypePreview)
        ];
        let schema_with_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", DataType::Primitive(PrimitiveType::Variant), true),
        ]);

        let schema_without_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        // Nested schema with VARIANT
        let nested_schema_with_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new([StructField::new(
                    "inner_v",
                    DataType::Primitive(PrimitiveType::Variant),
                    true,
                )]))),
                true,
            ),
        ]);
        features.iter().for_each(|(variant_reader, variant_writer)| {
            // Protocol with variantType features
            let protocol_with_features = Protocol::try_new(
                3,
                7,
                Some([variant_reader]),
                Some([variant_writer]),
            )
            .unwrap();

            // Protocol without variantType features
            let protocol_without_features = Protocol::try_new(
                3,
                7,
                Some::<Vec<String>>(vec![]),
                Some::<Vec<String>>(vec![]),
            )
            .unwrap();

            // Protocol without variantType writer feature
            let protocol_without_writer_feature = Protocol::try_new(
                3,
                7,
                Some([variant_reader]),
                Some::<Vec<String>>(vec![]),
            )
            .unwrap();

            // Protocol without variantType reader feature
            let protocol_without_reader_feature = Protocol::try_new(
                3,
                7,
                Some::<Vec<String>>(vec![]),
                Some([variant_writer]),
            )
            .unwrap();

            // Schema with VARIANT + Protocol with features = OK
            validate_variant_type_feature_support(&schema_with_variant, &protocol_with_features)
                .expect("Should succeed when features are present");

            // Schema without VARIANT + Protocol without features = OK
            validate_variant_type_feature_support(
                &schema_without_variant,
                &protocol_without_features,
            )
            .expect("Should succeed when no VARIANT columns are present");

            // Schema without VARIANT + Protocol with features = OK
            validate_variant_type_feature_support(
                &schema_without_variant,
                &protocol_with_features,
            )
            .expect("Should succeed when no VARIANT columns are present, even with features");

            // Schema with VARIANT + Protocol without features = ERROR
            let result = validate_variant_type_feature_support(
                &schema_with_variant,
                &protocol_without_features,
            );
            assert!(
                result.is_err(),
                "Should fail when VARIANT columns are present but features are missing"
            );
            assert!(result.unwrap_err().to_string().contains("variantType"));

            let result = validate_variant_type_feature_support(
                &nested_schema_with_variant,
                &protocol_without_features,
            );
            assert!(
                result.is_err(),
                "Should fail for nested VARIANT columns when features are missing"
            );

            // Schema with VARIANT + Protocol without writer feature = ERROR
            let result = validate_variant_type_feature_support(
                &schema_with_variant,
                &protocol_without_writer_feature,
            );
            assert!(
                result.is_err(),
                "Should fail when VARIANT columns are present but writer feature is missing"
            );
            assert!(result.unwrap_err().to_string().contains("variantType"));

            // Schema with VARIANT + Protocol without reader feature = ERROR
            let result = validate_variant_type_feature_support(
                &schema_with_variant,
                &protocol_without_reader_feature,
            );
            assert!(
                result.is_err(),
                "Should fail when VARIANT columns are present but reader feature is missing"
            );
            assert!(result.unwrap_err().to_string().contains("variantType"));
        });
    }
}
