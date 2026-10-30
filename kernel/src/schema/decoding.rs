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
    DuplicateField(String),
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
            Value::String(_) => PrimitiveType::deserialize(value)
                .map(DataType::Primitive)
                .map_err(D::Error::custom),
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
                    name => Err(super::unsupported_delta_type_error(name)),
                }
            }
            _ => Err(D::Error::custom(format!("Invalid data type: {value}"))),
        }
    }
}

#[cfg(feature = "udt-in-dev")]
impl SchemaSeed<'_, DataType> {
    fn deserialize_udt(self, value: Value) -> Result<DataType, serde_json::Error> {
        // The derived UDT decoder validates annotation fields in map order before constructing
        // the UDT. Decode its physical type with this seed to retain nested diagnostics.
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
