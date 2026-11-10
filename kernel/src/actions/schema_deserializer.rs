use std::cell::Cell;
use std::fmt;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::schema::StructType;
use crate::{KernelError, KernelResult};

/// Decodes a schema with a finite budget for visited JSON containers.
pub(super) fn parse(json: &str, recursion_limit: usize) -> KernelResult<StructType> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    deserializer.disable_recursion_limit();
    let exceeded = Cell::new(false);
    let budget = Budget {
        remaining: recursion_limit,
        exceeded: &exceeded,
    };
    StructType::deserialize(serde_stacker::Deserializer::new(
        budget.wrap(&mut deserializer),
    ))
    .and_then(|schema| deserializer.end().map(|()| schema))
    .map_err(|error| {
        if exceeded.get() {
            KernelError::schema(format!(
                "Table schema is too deeply nested: decoding metaData.schemaString \
                 reached the JSON recursion limit of {recursion_limit}"
            ))
            .with_backtrace()
        } else {
            super::schema_json_error(error)
        }
    })
}

#[derive(Clone, Copy)]
struct Budget<'a> {
    remaining: usize,
    exceeded: &'a Cell<bool>,
}

impl<'a> Budget<'a> {
    fn wrap<T>(self, inner: T) -> Limited<'a, T> {
        Limited {
            inner,
            budget: self,
        }
    }

    fn enter<E: de::Error>(self) -> KernelResult<Self, E> {
        if self.remaining <= 1 {
            self.exceeded.set(true);
            return Err(E::custom("recursion limit exceeded"));
        }
        Ok(Self {
            remaining: self.remaining - 1,
            ..self
        })
    }
}

struct Limited<'a, T> {
    inner: T,
    budget: Budget<'a>,
}

impl<'de, D: de::Deserializer<'de>> de::Deserializer<'de> for Limited<'_, D> {
    type Error = D::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> KernelResult<V::Value, Self::Error> {
        self.inner.deserialize_any(self.budget.wrap(visitor))
    }

    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> KernelResult<V::Value, Self::Error> {
        self.inner.deserialize_option(self.budget.wrap(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> KernelResult<V::Value, Self::Error> {
        self.inner
            .deserialize_newtype_struct(name, self.budget.wrap(visitor))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> KernelResult<V::Value, Self::Error> {
        // Preserve serde_json's iterative skip, which does not consume the recursion budget.
        self.inner.deserialize_ignored_any(visitor)
    }

    // shortcut: schema values are self-describing; add enum dispatch if schema decoding needs it.
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct enum identifier
    }
}

macro_rules! forward_value {
    ($($method:ident($value:ty)),* $(,)?) => {
        $(
            fn $method<E: de::Error>(self, value: $value) -> KernelResult<Self::Value, E> {
                self.inner.$method(value)
            }
        )*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Limited<'_, V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(formatter)
    }

    forward_value! {
        visit_bool(bool),
        visit_i64(i64),
        visit_u64(u64),
        visit_i128(i128),
        visit_u128(u128),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_borrowed_str(&'de str),
        visit_string(String),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>),
    }

    fn visit_unit<E: de::Error>(self) -> KernelResult<Self::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_none<E: de::Error>(self) -> KernelResult<Self::Value, E> {
        self.inner.visit_none()
    }

    fn visit_some<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> KernelResult<Self::Value, D::Error> {
        self.inner.visit_some(self.budget.wrap(deserializer))
    }

    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> KernelResult<Self::Value, D::Error> {
        self.inner
            .visit_newtype_struct(self.budget.wrap(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, access: A) -> KernelResult<Self::Value, A::Error> {
        self.inner.visit_seq(self.budget.enter()?.wrap(access))
    }

    fn visit_map<A: MapAccess<'de>>(self, access: A) -> KernelResult<Self::Value, A::Error> {
        self.inner.visit_map(self.budget.enter()?.wrap(access))
    }
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Limited<'_, S> {
    type Value = S::Value;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> KernelResult<Self::Value, D::Error> {
        self.inner.deserialize(self.budget.wrap(deserializer))
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Limited<'_, A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> KernelResult<Option<T::Value>, Self::Error> {
        self.inner.next_element_seed(self.budget.wrap(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for Limited<'_, A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> KernelResult<Option<K::Value>, Self::Error> {
        self.inner.next_key_seed(seed)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> KernelResult<V::Value, Self::Error> {
        self.inner.next_value_seed(self.budget.wrap(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}
