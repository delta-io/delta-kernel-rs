use std::cell::Cell;
use std::fmt;

use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{LimitedString, LimitedVec, MemoryBudget, MemoryError, MemoryResult};

/// A fallibly materialized JSON tree for schema and log-action payloads.
///
/// All retained strings, object entries, and array elements use a shared budget. The serde_json
/// parser's private escape-decoding scratch buffer and error allocations remain unbudgeted. This
/// type proves fallible tree construction, not a cap on the complete parsing operation. It does
/// not validate Delta schema or action semantics.
#[derive(Debug)]
pub enum LimitedJson {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON numeric scalar.
    Number(serde_json::Number),
    /// A string with budgeted owned UTF-8 storage.
    String(LimitedString),
    /// An array with budgeted element capacity.
    Array(LimitedVec<LimitedJson>),
    /// Object entries with budgeted keys and values, in input order.
    Object(LimitedVec<(LimitedString, LimitedJson)>),
}

impl LimitedJson {
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

impl Serialize for LimitedJson {
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
pub fn try_parse_json(budget: &MemoryBudget, input: &str) -> MemoryResult<LimitedJson> {
    let context = Context {
        budget,
        failure: Cell::new(None),
    };
    let mut deserializer = serde_json::Deserializer::from_str(input);
    let parsed = Seed { context: &context }
        .deserialize(&mut deserializer)
        .map_err(|_| context.failure.get().unwrap_or(MemoryError::InvalidJson))?;
    deserializer.end().map_err(|_| MemoryError::InvalidJson)?;
    Ok(parsed)
}

struct Context<'a> {
    budget: &'a MemoryBudget,
    failure: Cell<Option<MemoryError>>,
}

impl Context<'_> {
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
    context: &'a Context<'a>,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = LimitedJson;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = LimitedJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(LimitedJson::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(LimitedJson::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(LimitedJson::Number(value.into()))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(LimitedJson::Number(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(LimitedJson::Number)
            .ok_or_else(|| E::custom("invalid JSON number"))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.context
            .propagate(LimitedString::try_copy(self.context.budget, value))
            .map(LimitedJson::String)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = LimitedVec::new(self.context.budget);
        while let Some(value) = sequence.next_element_seed(self)? {
            self.context.propagate(values.try_push(value))?;
        }
        Ok(LimitedJson::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = LimitedVec::new(self.context.budget);
        while let Some(key) = map.next_key_seed(StringSeed(self.context))? {
            let value = map.next_value_seed(self)?;
            if let Some((_, previous)) = entries
                .as_mut_slice()
                .iter_mut()
                .find(|(name, _): &&mut (LimitedString, LimitedJson)| name.as_str() == key.as_str())
            {
                *previous = value;
            } else {
                self.context.propagate(entries.try_push((key, value)))?;
            }
        }
        Ok(LimitedJson::Object(entries))
    }
}

struct StringSeed<'a>(&'a Context<'a>);

impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = LimitedString;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for StringSeed<'_> {
    type Value = LimitedString;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.0
            .propagate(LimitedString::try_copy(self.0.budget, value))
    }
}

impl<'de> Deserialize<'de> for LimitedJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let budget = MemoryBudget::current().unwrap_or_else(|| MemoryBudget::new(usize::MAX));
        let context = Context {
            budget: &budget,
            failure: Cell::new(None),
        };
        Seed { context: &context }.deserialize(deserializer)
    }
}

impl<'de> IntoDeserializer<'de, de::value::Error> for &'de LimitedJson {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> Deserializer<'de> for &'de LimitedJson {
    type Error = de::value::Error;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            LimitedJson::Null => visitor.visit_unit(),
            LimitedJson::Bool(value) => visitor.visit_bool(*value),
            LimitedJson::Number(value) => value.deserialize_any(visitor).map_err(de::Error::custom),
            LimitedJson::String(value) => visitor.visit_borrowed_str(value.as_str()),
            LimitedJson::Array(values) => {
                visitor.visit_seq(de::value::SeqDeserializer::new(values.iter()))
            }
            LimitedJson::Object(entries) => {
                visitor.visit_map(de::value::MapDeserializer::new(entries.iter().map(
                    |(key, value)| (de::value::BorrowedStrDeserializer::new(key.as_str()), value),
                )))
            }
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            LimitedJson::Null => visitor.visit_none(),
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
