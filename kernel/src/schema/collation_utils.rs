//! Detection and structural validation of field-relative collation annotations.

use crate::schema::{
    ColumnMetadataKey, DataType, FieldMetadataKeyChecker, MetadataValue, Schema, StructField,
};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Returns whether any field, including fields inside containers, carries collation metadata.
pub(crate) fn schema_has_collations(schema: &Schema) -> bool {
    FieldMetadataKeyChecker(ColumnMetadataKey::Collations)
        .transform_struct(schema)
        .is_err()
}

/// Validates annotation objects, STRING targets and `Provider.Name[.Version]` identifiers.
///
/// Returns a schema error for malformed annotations. Provider and version support is not checked.
pub(crate) fn validate_collation_annotations(schema: &Schema) -> KernelResult<()> {
    if !schema_has_collations(schema) {
        return Ok(());
    }
    CollationAnnotationValidator.transform_struct(schema)
}

/// Validates annotations and requires a declared stable or preview feature on annotated schemas.
///
/// This schema/protocol consistency check also applies to reads of this writer-only feature.
/// Returns a schema error for malformed annotations or an unsupported error for a missing feature.
pub(crate) fn validate_collations_feature_support(
    table_config: &TableConfiguration,
) -> KernelResult<()> {
    let schema = table_config.logical_schema_ref();
    validate_collation_annotations(schema)?;
    require!(
        !schema_has_collations(schema)
            || table_config
                .protocol()
                .has_table_feature(&TableFeature::Collations)
            || table_config
                .protocol()
                .has_table_feature(&TableFeature::CollationsPreview),
        KernelError::unsupported(
            "Table contains collation metadata but requires the 'collations' or \
             'collations-preview' table feature"
        )
    );
    Ok(())
}

struct CollationAnnotationValidator;

impl<'a> SchemaTransform<'a> for CollationAnnotationValidator {
    transform_output_type!(|'a, T| KernelResult<()>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> KernelResult<()> {
        if field.has_collations() {
            let Some(MetadataValue::Other(serde_json::Value::Object(collations))) =
                field.get_config_value(&ColumnMetadataKey::Collations)
            else {
                return Err(KernelError::schema(format!(
                    "__COLLATIONS on '{}' must be a JSON object",
                    field.name()
                )));
            };
            for (path, value) in collations {
                let Some(identifier) = value.as_str() else {
                    return Err(KernelError::schema(format!(
                        "__COLLATIONS entry '{path}' must have a string identifier"
                    )));
                };
                require!(
                    valid_collation_identifier(identifier),
                    KernelError::schema(format!(
                        "__COLLATIONS entry '{path}' has invalid identifier '{identifier}'; \
                         expected Provider.Name[.Version]"
                    ))
                );
                require!(
                    path.strip_prefix(field.name())
                        .is_some_and(|suffix| is_string_target(field.data_type(), suffix)),
                    KernelError::schema(format!(
                        "__COLLATIONS path '{path}' on '{}' must identify a STRING target \
                         owned by this field",
                        field.name()
                    ))
                );
            }
        }
        self.recurse_into_struct_field(field)
    }
}

fn valid_collation_identifier(identifier: &str) -> bool {
    let mut parts = identifier.splitn(3, '.');
    parts.next().is_some_and(|provider| !provider.is_empty())
        && parts.next().is_some_and(|name| !name.is_empty())
        && parts.next().is_none_or(|version| !version.is_empty())
}

fn is_string_target(data_type: &DataType, suffix: &str) -> bool {
    match data_type {
        DataType::Array(array) => suffix
            .strip_prefix(".element")
            .is_some_and(|suffix| is_string_target(array.element_type(), suffix)),
        DataType::Map(map) => {
            suffix
                .strip_prefix(".key")
                .is_some_and(|suffix| is_string_target(map.key_type(), suffix))
                || suffix
                    .strip_prefix(".value")
                    .is_some_and(|suffix| is_string_target(map.value_type(), suffix))
        }
        // Nested structs own their annotations on their child StructFields.
        _ => data_type == &DataType::STRING && suffix.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::schema::{schema, ArrayType, MapType, StructType};

    fn annotated(
        name: &str,
        data_type: impl Into<DataType>,
        value: serde_json::Value,
    ) -> StructField {
        StructField::nullable(name, data_type).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::Other(value),
        )])
    }

    #[rstest]
    #[case::string(DataType::STRING, "field", true)]
    #[case::literal_dot(DataType::STRING, "a.b", false)]
    #[case::array(ArrayType::new(DataType::STRING, true).into(), "field.element", true)]
    #[case::nested_array(ArrayType::new(ArrayType::new(DataType::STRING, true), true).into(),
        "field.element.element", true)]
    #[case::map_key(MapType::new(DataType::STRING, DataType::INTEGER, true).into(), "field.key", true)]
    #[case::map_value(MapType::new(DataType::INTEGER, DataType::STRING, true).into(), "field.value", true)]
    #[case::array_map(ArrayType::new(MapType::new(DataType::STRING, DataType::STRING, true), true).into(),
        "field.element.value", true)]
    #[case::not_string(DataType::INTEGER, "field", false)]
    #[case::wrong_container_path(ArrayType::new(DataType::STRING, true).into(), "field.value", false)]
    #[case::wrong_field(DataType::STRING, "other", false)]
    #[case::not_nearest(schema! { nullable "child": STRING }.into(), "field.child", false)]
    fn annotation_paths_resolve_to_owned_strings(
        #[case] data_type: DataType,
        #[case] path: &str,
        #[case] valid: bool,
    ) {
        let schema =
            StructType::try_new([annotated("field", data_type, json!({path: "custom.name"}))])
                .unwrap();
        assert_eq!(validate_collation_annotations(&schema).is_ok(), valid);
    }

    #[rstest]
    #[case(json!(null))]
    #[case(json!([]))]
    #[case(json!("text"))]
    #[case(json!({"field": 12}))]
    #[case(json!({"field": ""}))]
    #[case(json!({"field": "provider"}))]
    #[case(json!({"field": ".name"}))]
    #[case(json!({"field": "provider."}))]
    #[case(json!({"field": "provider.name."}))]
    fn malformed_annotations_are_rejected(#[case] annotation: serde_json::Value) {
        let schema =
            StructType::try_new([annotated("field", DataType::STRING, annotation)]).unwrap();
        assert!(validate_collation_annotations(&schema)
            .unwrap_err()
            .to_string()
            .contains("__COLLATIONS"));
    }

    #[rstest]
    #[case("unknown.name")]
    #[case("unknown.name.72")]
    #[case("unknown.name.75.1")]
    fn identifiers_do_not_require_known_providers_or_versions(#[case] identifier: &str) {
        let schema = StructType::try_new([annotated(
            "field",
            DataType::STRING,
            json!({"field": identifier}),
        )])
        .unwrap();
        validate_collation_annotations(&schema).unwrap();
    }

    #[rstest]
    #[case::nested_struct(DataType::from(schema! { (annotated("name", DataType::STRING, json!({"name":"spark.UTF8_LCASE"}))) }))]
    #[case::array(ArrayType::new(schema! { (annotated("name", DataType::STRING, json!({"name":"spark.UTF8_LCASE"}))) }, true).into())]
    #[case::map(MapType::new(DataType::STRING, schema! { (annotated("name", DataType::STRING, json!({"name":"spark.UTF8_LCASE"}))) }, true).into())]
    fn detection_traverses_unannotated_containers(#[case] data_type: DataType) {
        let schema = schema! { nullable "outer": (data_type) };
        assert!(schema_has_collations(&schema));
        validate_collation_annotations(&schema).unwrap();
        assert!(!schema_has_collations(&schema! { nullable "name": STRING }));
    }

    #[test]
    fn dotted_logical_names_and_mapping_metadata_are_preserved() {
        let field = annotated(
            "a.b",
            ArrayType::new(DataType::STRING, true),
            json!({"a.b.element": "custom.name.1.2"}),
        )
        .add_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("physical".into()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
        ]);
        validate_collation_annotations(&StructType::try_new([field]).unwrap()).unwrap();
    }

    #[test]
    fn json_text_is_not_an_annotation_object() {
        let field = StructField::nullable("field", DataType::STRING).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::String(r#"{"field":"custom.name"}"#.into()),
        )]);
        assert!(validate_collation_annotations(&StructType::try_new([field]).unwrap()).is_err());
    }
}
