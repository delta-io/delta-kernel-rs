//! Schema decoding with typed diagnostics for persisted metadata.
//!
//! The same visitors serve ordinary serde decoding. A diagnostic is recorded only at the
//! failing validation, so nested failures retain serde's traversal order and original source.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};

/// Validation facts retained before serde erases the concrete failure type.
pub(crate) enum SchemaDiagnostic {
    /// The logical field name rejected by case-insensitive struct validation.
    DuplicateField(String),
    /// The serialized primitive or complex type token rejected by type decoding.
    UnsupportedType(String),
}

/// Carries one operation's diagnostic through nested schema decoding.
pub(super) struct SchemaSeed<'a, T> {
    diagnostic: &'a RefCell<Option<SchemaDiagnostic>>,
    marker: PhantomData<T>,
}

impl<'a, T> SchemaSeed<'a, T> {
    /// Creates a seed that records a failing validation in `diagnostic`.
    pub(super) fn new(diagnostic: &'a RefCell<Option<SchemaDiagnostic>>) -> Self {
        Self {
            diagnostic,
            marker: PhantomData,
        }
    }
}

/// Decodes `json` and returns its original serde failure together with the typed validation.
pub(crate) fn decode_schema(
    json: &str,
) -> Result<StructType, (serde_json::Error, Option<SchemaDiagnostic>)> {
    let diagnostic = RefCell::new(None);
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let result = SchemaSeed::<StructType>::new(&diagnostic)
        .deserialize(&mut deserializer)
        .and_then(|schema| {
            deserializer.end()?;
            Ok(schema)
        });
    result.map_err(|error| (error, diagnostic.into_inner()))
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, StructType> {
    type Value = StructType;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("StructTypeSerDeHelper", &["type", "fields"], self)
    }
}

impl<'de> Visitor<'de> for SchemaSeed<'_, StructType> {
    type Value = StructType;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a StructType")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut type_name: Option<String> = None;
        let mut fields: Option<Vec<StructField>> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    if type_name.is_some() {
                        return Err(A::Error::duplicate_field("type"));
                    }
                    type_name = Some(map.next_value()?);
                }
                "fields" => {
                    if fields.is_some() {
                        return Err(A::Error::duplicate_field("fields"));
                    }
                    fields = Some(
                        map.next_value_seed(SchemaSeed::<Vec<StructField>>::new(self.diagnostic))?,
                    );
                }
                _ => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let type_name = type_name.ok_or_else(|| A::Error::missing_field("type"))?;
        let fields = fields.ok_or_else(|| A::Error::missing_field("fields"))?;
        let _ = type_name;
        StructType::try_new_with_duplicate_callback(fields, |name| {
            *self.diagnostic.borrow_mut() = Some(SchemaDiagnostic::DuplicateField(name.to_owned()));
        })
        .map_err(A::Error::custom)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let type_name: String = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let fields: Vec<StructField> = seq
            .next_element_seed(SchemaSeed::<Vec<StructField>>::new(self.diagnostic))?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        let _ = type_name;
        StructType::try_new_with_duplicate_callback(fields, |name| {
            *self.diagnostic.borrow_mut() = Some(SchemaDiagnostic::DuplicateField(name.to_owned()));
        })
        .map_err(A::Error::custom)
    }
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, StructField> {
    type Value = StructField;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct(
            "StructField",
            &["name", "type", "nullable", "metadata"],
            self,
        )
    }
}

impl<'de> Visitor<'de> for SchemaSeed<'_, StructField> {
    type Value = StructField;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a StructField")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut name: Option<String> = None;
        let mut data_type: Option<DataType> = None;
        let mut nullable: Option<bool> = None;
        let mut metadata: Option<std::collections::HashMap<String, super::MetadataValue>> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(A::Error::duplicate_field("name"));
                    }
                    name = Some(map.next_value()?);
                }
                "type" => {
                    if data_type.is_some() {
                        return Err(A::Error::duplicate_field("type"));
                    }
                    data_type =
                        Some(map.next_value_seed(SchemaSeed::<DataType>::new(self.diagnostic))?);
                }
                "nullable" => {
                    if nullable.is_some() {
                        return Err(A::Error::duplicate_field("nullable"));
                    }
                    nullable = Some(map.next_value()?);
                }
                "metadata" => {
                    if metadata.is_some() {
                        return Err(A::Error::duplicate_field("metadata"));
                    }
                    metadata = Some(map.next_value()?);
                }
                _ => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let name = name.ok_or_else(|| A::Error::missing_field("name"))?;
        let data_type = data_type.ok_or_else(|| A::Error::missing_field("type"))?;
        let nullable = nullable.ok_or_else(|| A::Error::missing_field("nullable"))?;
        let metadata = metadata.ok_or_else(|| A::Error::missing_field("metadata"))?;
        Ok(StructField {
            name,
            data_type,
            nullable,
            metadata,
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let name: String = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let data_type: DataType = seq
            .next_element_seed(SchemaSeed::<DataType>::new(self.diagnostic))?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        let nullable: bool = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
        let metadata: std::collections::HashMap<String, super::MetadataValue> = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(3, &self))?;
        Ok(StructField {
            name,
            data_type,
            nullable,
            metadata,
        })
    }
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, ArrayType> {
    type Value = ArrayType;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("ArrayType", &["type", "elementType", "containsNull"], self)
    }
}

impl<'de> Visitor<'de> for SchemaSeed<'_, ArrayType> {
    type Value = ArrayType;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a ArrayType")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut type_name: Option<String> = None;
        let mut element_type: Option<DataType> = None;
        let mut contains_null: Option<bool> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    if type_name.is_some() {
                        return Err(A::Error::duplicate_field("type"));
                    }
                    type_name = Some(map.next_value()?);
                }
                "elementType" => {
                    if element_type.is_some() {
                        return Err(A::Error::duplicate_field("elementType"));
                    }
                    element_type =
                        Some(map.next_value_seed(SchemaSeed::<DataType>::new(self.diagnostic))?);
                }
                "containsNull" => {
                    if contains_null.is_some() {
                        return Err(A::Error::duplicate_field("containsNull"));
                    }
                    contains_null = Some(map.next_value()?);
                }
                _ => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let type_name = type_name.ok_or_else(|| A::Error::missing_field("type"))?;
        let element_type = element_type.ok_or_else(|| A::Error::missing_field("elementType"))?;
        let contains_null = contains_null.ok_or_else(|| A::Error::missing_field("containsNull"))?;
        Ok(ArrayType {
            type_name,
            element_type,
            contains_null,
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let type_name: String = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let element_type: DataType = seq
            .next_element_seed(SchemaSeed::<DataType>::new(self.diagnostic))?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        let contains_null: bool = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
        Ok(ArrayType {
            type_name,
            element_type,
            contains_null,
        })
    }
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, MapType> {
    type Value = MapType;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct(
            "MapType",
            &["type", "keyType", "valueType", "valueContainsNull"],
            self,
        )
    }
}

impl<'de> Visitor<'de> for SchemaSeed<'_, MapType> {
    type Value = MapType;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a MapType")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut type_name: Option<String> = None;
        let mut key_type: Option<DataType> = None;
        let mut value_type: Option<DataType> = None;
        let mut value_contains_null: Option<bool> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    if type_name.is_some() {
                        return Err(A::Error::duplicate_field("type"));
                    }
                    type_name = Some(map.next_value()?);
                }
                "keyType" => {
                    if key_type.is_some() {
                        return Err(A::Error::duplicate_field("keyType"));
                    }
                    key_type =
                        Some(map.next_value_seed(SchemaSeed::<DataType>::new(self.diagnostic))?);
                }
                "valueType" => {
                    if value_type.is_some() {
                        return Err(A::Error::duplicate_field("valueType"));
                    }
                    value_type =
                        Some(map.next_value_seed(SchemaSeed::<DataType>::new(self.diagnostic))?);
                }
                "valueContainsNull" => {
                    if value_contains_null.is_some() {
                        return Err(A::Error::duplicate_field("valueContainsNull"));
                    }
                    value_contains_null = Some(map.next_value()?);
                }
                _ => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let type_name = type_name.ok_or_else(|| A::Error::missing_field("type"))?;
        let key_type = key_type.ok_or_else(|| A::Error::missing_field("keyType"))?;
        let value_type = value_type.ok_or_else(|| A::Error::missing_field("valueType"))?;
        let value_contains_null = value_contains_null.unwrap_or_else(super::default_true);
        Ok(MapType {
            type_name,
            key_type,
            value_type,
            value_contains_null,
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let type_name: String = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let key_type: DataType = seq
            .next_element_seed(SchemaSeed::<DataType>::new(self.diagnostic))?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        let value_type: DataType = seq
            .next_element_seed(SchemaSeed::<DataType>::new(self.diagnostic))?
            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
        let value_contains_null: bool = seq.next_element()?.unwrap_or_else(super::default_true);
        Ok(MapType {
            type_name,
            key_type,
            value_type,
            value_contains_null,
        })
    }
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, Vec<StructField>> {
    type Value = Vec<StructField>;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for SchemaSeed<'_, Vec<StructField>> {
    type Value = Vec<StructField>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of fields")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut fields = Vec::new();
        while let Some(field) =
            seq.next_element_seed(SchemaSeed::<StructField>::new(self.diagnostic))?
        {
            fields.push(field);
        }
        Ok(fields)
    }
}

impl<'de> DeserializeSeed<'de> for SchemaSeed<'_, DataType> {
    type Value = DataType;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<DataType, D::Error> {
        let value = Value::deserialize(deserializer)?;
        match &value {
            Value::String(name) if name == "variant" => Ok(DataType::unshredded_variant()),
            Value::String(name) if name == "file" => Ok(DataType::file_type()),
            Value::String(name) => {
                PrimitiveType::parse::<serde_json::Error>(name.clone(), |name| {
                    *self.diagnostic.borrow_mut() =
                        Some(SchemaDiagnostic::UnsupportedType(name.to_owned()));
                    super::unsupported_delta_type_error(name)
                })
                .map(DataType::Primitive)
                .map_err(D::Error::custom)
            }
            Value::Object(map) if map.get("type").is_some_and(Value::is_string) => {
                match map.get("type").and_then(Value::as_str).unwrap_or_default() {
                    "array" => SchemaSeed::<ArrayType>::new(self.diagnostic)
                        .deserialize(value)
                        .map(DataType::from)
                        .map_err(D::Error::custom),
                    "struct" => SchemaSeed::<StructType>::new(self.diagnostic)
                        .deserialize(value)
                        .map(DataType::from)
                        .map_err(D::Error::custom),
                    "map" => SchemaSeed::<MapType>::new(self.diagnostic)
                        .deserialize(value)
                        .map(DataType::from)
                        .map_err(D::Error::custom),
                    #[cfg(feature = "udt-in-dev")]
                    "udt" => self.deserialize_udt(value).map_err(D::Error::custom),
                    name => {
                        *self.diagnostic.borrow_mut() =
                            Some(SchemaDiagnostic::UnsupportedType(name.to_owned()));
                        Err(super::unsupported_delta_type_error(name))
                    }
                }
            }
            _ => Err(D::Error::custom(format!("Invalid data type: {value}"))),
        }
    }
}

#[cfg(feature = "udt-in-dev")]
impl SchemaSeed<'_, DataType> {
    fn deserialize_udt(self, value: Value) -> Result<DataType, serde_json::Error> {
        // UDT annotations are validated after the required fields. Retain the physical type's
        // diagnostic before validating annotations, including annotations that precede sqlType.
        let Value::Object(mut map) = value else {
            return Err(serde_json::Error::custom("Expected UDT object"));
        };
        let mut sql_type = None;
        let mut annotation = std::collections::HashMap::new();
        for (key, value) in &mut map {
            match key.as_str() {
                "type" => {
                    String::deserialize(value.take())?;
                }
                "sqlType" => {
                    sql_type = Some(
                        SchemaSeed::<DataType>::new(self.diagnostic).deserialize(value.take())?,
                    );
                }
                _ => {
                    annotation.insert(key.clone(), value.take());
                }
            }
        }
        let sql_type = sql_type.ok_or_else(|| serde_json::Error::missing_field("sqlType"))?;
        let annotation = annotation
            .into_iter()
            .map(|(key, value)| Option::<String>::deserialize(value).map(|value| (key, value)))
            .collect::<Result<_, _>>()?;
        super::UserDefinedType::try_new(sql_type, annotation)
            .map(DataType::UserDefined)
            .map_err(serde_json::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rstest::rstest;
    use serde::de::{DeserializeOwned, DeserializeSeed};
    use serde_json::json;

    use super::*;

    fn assert_serde_compatibility<T>(input: &str)
    where
        T: DeserializeOwned + Debug + PartialEq,
        for<'a, 'de> SchemaSeed<'a, T>: DeserializeSeed<'de, Value = T>,
    {
        let diagnostic = RefCell::new(None);
        let seeded = SchemaSeed::<T>::new(&diagnostic)
            .deserialize(&mut serde_json::Deserializer::from_str(input));
        let ordinary = serde_json::from_str::<T>(input);
        match (seeded, ordinary) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
            (Err(actual), Err(expected)) => {
                assert_eq!(actual.classify(), expected.classify());
                assert_eq!(
                    (actual.line(), actual.column()),
                    (expected.line(), expected.column())
                );
                assert_eq!(
                    actual.to_string().split(", expected").next(),
                    expected.to_string().split(", expected").next(),
                );
            }
            (actual, expected) => panic!("seeded: {actual:?}, ordinary: {expected:?}"),
        }
    }

    #[test]
    fn sequence_struct_duplicate_fields_retain_typed_diagnostic() {
        let (error, diagnostic) =
            decode_schema(r#"["struct",[["a","long",true,{}],["a","long",true,{}]]]"#).unwrap_err();
        assert!(matches!(
            diagnostic,
            Some(SchemaDiagnostic::DuplicateField(_))
        ));
        assert!(error.to_string().to_lowercase().contains("duplicate"));
    }

    #[rstest]
    #[case::structure("struct", r#"{"type":"struct","fields":[]}"#)]
    #[case::field("field", r#"{"name":"a","type":"long","nullable":true,"metadata":{}}"#)]
    #[case::array(
        "array",
        r#"{"type":"array","elementType":"long","containsNull":true}"#
    )]
    #[case::map(
        "map",
        r#"{"type":"map","keyType":"string","valueType":"long","valueContainsNull":false}"#
    )]
    fn seeded_decoding_preserves_missing_duplicate_and_unknown_field_behavior(
        #[case] kind: &str,
        #[case] input: &str,
    ) {
        let object: serde_json::Map<String, Value> = serde_json::from_str(input).unwrap();
        let check = |input: &str| match kind {
            "struct" => assert_serde_compatibility::<StructType>(input),
            "field" => assert_serde_compatibility::<StructField>(input),
            "array" => assert_serde_compatibility::<ArrayType>(input),
            "map" => assert_serde_compatibility::<MapType>(input),
            _ => unreachable!(),
        };
        check(input);
        check(&format!("{},true}}", &input[..input.len() - 1]));
        for (key, value) in &object {
            let mut wrong_type = object.clone();
            wrong_type.insert(key.clone(), Value::Null);
            check(&serde_json::to_string(&wrong_type).unwrap());
            let mut missing = object.clone();
            missing.remove(key);
            check(&serde_json::to_string(&missing).unwrap());
            let duplicate = format!(r#"{{"{key}":{value},{}"#, &input[1..]);
            check(&duplicate);
        }
        let mut extended = object;
        extended.insert("unknown".into(), json!({"nested": [1, null]}));
        check(&serde_json::to_string(&extended).unwrap());
        check("null");
    }

    #[rstest]
    #[case::structure("struct", r#"["struct",[]]"#, 2)]
    #[case::field("field", r#"["a","long",true,{}]"#, 4)]
    #[case::array("array", r#"["array","long",true]"#, 3)]
    #[case::map("map", r#"["map","string","long",false]"#, 4)]
    fn seeded_decoding_preserves_sequence_fields_and_truncation_errors(
        #[case] kind: &str,
        #[case] input: &str,
        #[case] length: usize,
    ) {
        let values: Vec<Value> = serde_json::from_str(input).unwrap();
        for index in 0..length {
            let mut wrong_type = values.clone();
            wrong_type[index] = Value::Null;
            let invalid = serde_json::to_string(&wrong_type).unwrap();
            match kind {
                "struct" => assert_serde_compatibility::<StructType>(&invalid),
                "field" => assert_serde_compatibility::<StructField>(&invalid),
                "array" => assert_serde_compatibility::<ArrayType>(&invalid),
                "map" => assert_serde_compatibility::<MapType>(&invalid),
                _ => unreachable!(),
            }
        }
        for end in 0..=length {
            let truncated = serde_json::to_string(&values[..end]).unwrap();
            match kind {
                "struct" => assert_serde_compatibility::<StructType>(&truncated),
                "field" => assert_serde_compatibility::<StructField>(&truncated),
                "array" => assert_serde_compatibility::<ArrayType>(&truncated),
                "map" => assert_serde_compatibility::<MapType>(&truncated),
                _ => unreachable!(),
            }
        }
    }
}
