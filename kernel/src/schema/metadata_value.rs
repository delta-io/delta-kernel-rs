use std::fmt;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::MetadataValue;
use crate::memory::SharedString;

impl<'de> Deserialize<'de> for MetadataValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(MetadataVisitor)
    }
}

struct MetadataVisitor;

impl<'de> Visitor<'de> for MetadataVisitor {
    type Value = MetadataValue;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("column metadata")
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(MetadataValue::Boolean(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(MetadataValue::Number(value))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(match i64::try_from(value) {
            Ok(value) => MetadataValue::Number(value),
            Err(_) => MetadataValue::Other(value.into()),
        })
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(MetadataValue::Other(serde_json::Value::from(value)))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(MetadataValue::Other(serde_json::Value::Null))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let value = SharedString::try_copy(value).map_err(|error| {
            if let crate::KernelError::Memory(failure) = error.without_backtrace() {
                crate::memory::record_failure(*failure);
            }
            E::custom(error)
        })?;
        Ok(MetadataValue::String(value))
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.visit_str(&value)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, value: A) -> Result<Self::Value, A::Error> {
        serde_json::Value::deserialize(de::value::SeqAccessDeserializer::new(value))
            .map(MetadataValue::Other)
    }
    fn visit_map<A: MapAccess<'de>>(self, value: A) -> Result<Self::Value, A::Error> {
        serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(value))
            .map(MetadataValue::Other)
    }
}
