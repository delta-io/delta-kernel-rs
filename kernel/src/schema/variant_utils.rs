//! Utility functions for the variant type and variant-related table features.

use crate::actions::Protocol;
use crate::schema::{DataType, PrimitiveType, Schema, SchemaTransform, StructField, StructType};
use crate::table_features::{ReaderFeature, WriterFeature};
use crate::utils::require;
use crate::{DeltaResult, Error};
use std::borrow::Cow;

pub const VARIANT_METADATA: &str = "__VARIANT__";

/// Variant is represented as a `STRUCT<value: BINARY, metadata: BINARY>` where the metadata field
/// has some additional metadata saying `__VARIANT__ = true`. This makes it easier for the parquet
/// reader to understand variant.
pub fn unshredded_variant_struct_schema() -> DataType {
    DataType::struct_type([
        StructField::nullable("value", DataType::BINARY),
        StructField::nullable("metadata", DataType::BINARY)
            .with_metadata([(VARIANT_METADATA, "true")]),
    ])
}

pub fn is_unshredded_variant(s: &StructType) -> bool {
    if let DataType::Struct(boxed_schema) = unshredded_variant_struct_schema() {
        s == boxed_schema.as_ref()
    } else {
        unreachable!("unshredded_variant_struct_schema must return DataType::Struct");
    }
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

pub(crate) fn validate_variant_type_feature_support(
    schema: &Schema,
    protocol: &Protocol,
) -> DeltaResult<()> {
    // Both the reader and writer need to have either the VariantType or the VariantTypePreview
    // features.
    if (!protocol.has_reader_feature(&ReaderFeature::VariantType)
        && !protocol.has_reader_feature(&ReaderFeature::VariantTypePreview))
        || (!protocol.has_writer_feature(&WriterFeature::VariantType)
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

/// Utility to make it easier for third-party engines to replace nested Variants with TAGGED
/// `STRUCT<value: BINARY, metadata: BINARY>` to it is easier for engines to construct variant read
/// schemas.
#[allow(dead_code)]
pub struct ReplaceVariantWithStructRepresentation();

impl<'a> SchemaTransform<'a> for ReplaceVariantWithStructRepresentation {
    fn should_transform_primitive_to_data_type(&self) -> bool {
        true
    }

    fn transform_primitive_to_data_type(
        &mut self,
        ptype: &'a PrimitiveType,
    ) -> Option<Cow<'a, DataType>> {
        if *ptype == PrimitiveType::Variant {
            Some(Cow::Owned(unshredded_variant_struct_schema()))
        } else {
            Some(Cow::Owned(DataType::Primitive(ptype.clone())))
        }
    }
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
        let mut replace_variant = ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        assert_eq!(
            transformed.unwrap().into_owned(),
            unshredded_variant_struct_schema()
        );
    }

    #[test]
    fn test_variant_schema_replace_array() {
        let dt: DataType = ArrayType::new(DataType::VARIANT, false).into();
        let mut replace_variant = ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType = ArrayType::new(unshredded_variant_struct_schema(), false).into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_schema_replace_struct() {
        let dt: DataType = DataType::struct_type([
            StructField::nullable("i", DataType::INTEGER),
            StructField::nullable("v", DataType::VARIANT),
            StructField::nullable(
                "s",
                DataType::struct_type([
                    StructField::nullable("v1", DataType::VARIANT),
                    StructField::nullable("i1", DataType::STRING),
                ]),
            ),
            StructField::nullable("not_variant", unshredded_variant_struct_schema()),
        ])
        .into();
        // let dt: DataType = ArrayType::new(DataType::VARIANT, false).into();
        let mut replace_variant = ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType = DataType::struct_type([
            StructField::nullable("i", DataType::INTEGER),
            StructField::nullable("v", unshredded_variant_struct_schema()),
            StructField::nullable(
                "s",
                DataType::struct_type([
                    StructField::nullable("v1", unshredded_variant_struct_schema()),
                    StructField::nullable("i1", DataType::STRING),
                ]),
            ),
            StructField::nullable("not_variant", unshredded_variant_struct_schema()),
        ])
        .into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_schema_replace_map() {
        let dt: DataType = MapType::new(DataType::STRING, DataType::VARIANT, false).into();
        let mut replace_variant = ReplaceVariantWithStructRepresentation();
        let transformed = replace_variant.transform(&dt);
        let expected: DataType =
            MapType::new(DataType::STRING, unshredded_variant_struct_schema(), false).into();
        assert_eq!(transformed.unwrap().into_owned(), expected);
    }

    #[test]
    fn test_variant_to_physical_maintains_metadata() {
        let var_field = StructField::nullable("v", unshredded_variant_struct_schema())
            .with_metadata([("delta.columnMapping.physicalName", "col1")])
            .add_metadata([("delta.columnMapping.id", 1)]);

        let expected = StructField::nullable("col1", unshredded_variant_struct_schema())
            .with_metadata([("delta.columnMapping.physicalName", "col1")])
            .add_metadata([("delta.columnMapping.id", 1)]);

        assert_eq!(var_field.make_physical(), expected);

        fn unshredded_variant_struct_schema_no_meta() -> DataType {
            DataType::struct_type([
                StructField::nullable("value", DataType::BINARY),
                StructField::nullable("metadata", DataType::BINARY),
            ])
        }

        let not_expected =
            StructField::nullable("col1", unshredded_variant_struct_schema_no_meta())
                .with_metadata([("delta.columnMapping.physicalName", "col1")])
                .add_metadata([("delta.columnMapping.id", 1)]);

        assert_ne!(var_field.make_physical(), not_expected);
    }

    #[test]
    fn test_is_unshredded_variant() {
        assert!(is_unshredded_variant(&StructType::new([
            StructField::nullable("value", DataType::BINARY),
            StructField::nullable("metadata", DataType::BINARY)
                .with_metadata([(VARIANT_METADATA, "true")]),
        ])));
        assert!(!is_unshredded_variant(&StructType::new([
            StructField::nullable("value", DataType::BINARY),
            StructField::nullable("metadata", DataType::BINARY),
        ])));
    }

    #[test]
    fn test_variant_feature_validation() {
        let features = vec![
            (ReaderFeature::VariantType, WriterFeature::VariantType),
            (
                ReaderFeature::VariantTypePreview,
                WriterFeature::VariantTypePreview,
            ),
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
        features
            .iter()
            .for_each(|(variant_reader, variant_writer)| {
                // Protocol with variantType features
                let protocol_with_features =
                    Protocol::try_new(3, 7, Some([variant_reader]), Some([variant_writer]))
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
                let protocol_without_writer_feature =
                    Protocol::try_new(3, 7, Some([variant_reader]), Some::<Vec<String>>(vec![]))
                        .unwrap();

                // Protocol without variantType reader feature
                let protocol_without_reader_feature =
                    Protocol::try_new(3, 7, Some::<Vec<String>>(vec![]), Some([variant_writer]))
                        .unwrap();

                // Schema with VARIANT + Protocol with features = OK
                validate_variant_type_feature_support(
                    &schema_with_variant,
                    &protocol_with_features,
                )
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
