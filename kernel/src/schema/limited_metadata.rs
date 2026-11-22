use std::fmt;
use std::sync::Arc;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::MetadataValue;
use crate::memory::{LimitedString, MemoryBudget};

impl PartialEq for MetadataValue {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(a), Some(b)) = (self.as_string(), other.as_string()) {
            return a == b;
        }
        match (self, other) {
            (Self::Number(a), Self::Number(b)) => a == b,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Other(a), Self::Other(b)) => a == b,
            _ => false,
        }
    }
}

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
        if let Some(budget) = MemoryBudget::current() {
            let value = LimitedString::try_copy(&budget, value).map_err(|error| {
                crate::memory::record_failure(error);
                E::custom(error)
            })?;
            Ok(MetadataValue::LimitedString(Arc::new(value)))
        } else {
            Ok(MetadataValue::String(value.to_owned()))
        }
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        if MemoryBudget::current().is_some() {
            self.visit_str(&value)
        } else {
            Ok(MetadataValue::String(value))
        }
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
