use std::cell::Cell;
use std::fmt;

use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{MemoryError, MemoryResult, MetadataString, MetadataVec};

/// A fallibly materialized JSON tree for schema and log-action payloads.
///
/// All retained strings, object entries, and array elements use a shared budget. The serde_json
/// parser's private escape-decoding scratch buffer and error allocations remain unbudgeted. This
/// type proves fallible tree construction, not a cap on the complete parsing operation. It does
/// not validate Delta schema or action semantics.
#[derive(Debug)]
pub enum JsonValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON numeric scalar.
    Number(serde_json::Number),
    /// A string with budgeted owned UTF-8 storage.
    String(MetadataString),
    /// An array with budgeted element capacity.
    Array(MetadataVec<JsonValue>),
    /// Object entries with budgeted keys and values, in input order.
    Object(MetadataVec<(MetadataString, JsonValue)>),
}

impl Drop for JsonValue {
    fn drop(&mut self) {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match self {
            Self::Array(values) => values.clear(),
            Self::Object(entries) => entries.clear(),
            _ => {}
        });
    }
}

impl JsonValue {
    /// Borrow the last matching object member, or `None` for a missing key or non-object value.
    pub fn member(&self, name: &str) -> Option<&Self> {
        match self {
            Self::Object(entries) => entries
                .iter()
                .rev()
                .find(|(key, _)| key.as_str() == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }
}

impl Serialize for JsonValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Number(value) => value.serialize(serializer),
            Self::String(value) => serializer.serialize_str(value.as_str()),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values.iter() {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries.iter() {
                    map.serialize_entry(key.as_str(), value)?;
                }
                map.end()
            }
        }
    }
}

/// Parse JSON directly into budgeted tree storage, releasing partial state on failure.
///
/// Budget failure is propagated as `AllocationFailed`, independently of serde's error formatting.
/// Input bytes and parser-private scratch/error allocations are outside the retained-tree cap.
pub fn try_parse_json(input: &str) -> MemoryResult<JsonValue> {
    let context = Context {
        failure: Cell::new(None),
    };
    let mut deserializer = serde_json::Deserializer::from_str(input);
    let parsed = Seed { context: &context }
        .deserialize(&mut deserializer)
        .map_err(|_| context.failure.get().unwrap_or(MemoryError::InvalidJson))?;
    deserializer.end().map_err(|_| MemoryError::InvalidJson)?;
    Ok(parsed)
}

struct Context {
    failure: Cell<Option<MemoryError>>,
}

impl Context {
    fn propagate<T, E: de::Error>(&self, value: MemoryResult<T>) -> Result<T, E> {
        value.map_err(|error| {
            self.failure.set(Some(error));
            super::record_failure(error);
            E::custom("metadata allocation failed")
        })
    }
}

#[derive(Clone, Copy)]
struct Seed<'a> {
    context: &'a Context,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = JsonValue;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
            deserializer.deserialize_any(self)
        })
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonValue::Number(value.into()))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonValue::Number(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| E::custom("invalid JSON number"))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.context
            .propagate(MetadataString::try_copy(value))
            .map(JsonValue::String)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = MetadataVec::new();
        while let Some(value) = sequence.next_element_seed(self)? {
            self.context.propagate(values.try_push(value))?;
        }
        Ok(JsonValue::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = MetadataVec::new();
        while let Some(key) = map.next_key_seed(StringSeed(self.context))? {
            let value = map.next_value_seed(self)?;
            if let Some((_, previous)) = entries
                .as_mut_slice()
                .iter_mut()
                .find(|(name, _): &&mut (MetadataString, JsonValue)| name.as_str() == key.as_str())
            {
                *previous = value;
            } else {
                self.context.propagate(entries.try_push((key, value)))?;
            }
        }
        Ok(JsonValue::Object(entries))
    }
}

struct StringSeed<'a>(&'a Context);

impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = MetadataString;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for StringSeed<'_> {
    type Value = MetadataString;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.0.propagate(MetadataString::try_copy(value))
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let context = Context {
            failure: Cell::new(None),
        };
        Seed { context: &context }.deserialize(deserializer)
    }
}

impl<'de> IntoDeserializer<'de, de::value::Error> for &'de JsonValue {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> Deserializer<'de> for &'de JsonValue {
    type Error = de::value::Error;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            JsonValue::Null => visitor.visit_unit(),
            JsonValue::Bool(value) => visitor.visit_bool(*value),
            JsonValue::Number(value) => value.deserialize_any(visitor).map_err(de::Error::custom),
            JsonValue::String(value) => visitor.visit_borrowed_str(value.as_str()),
            JsonValue::Array(values) => {
                visitor.visit_seq(de::value::SeqDeserializer::new(values.iter()))
            }
            JsonValue::Object(entries) => {
                visitor.visit_map(de::value::MapDeserializer::new(entries.iter().map(
                    |(key, value)| (de::value::BorrowedStrDeserializer::new(key.as_str()), value),
                )))
            }
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            JsonValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct enum identifier
    }
}
