use std::cell::Cell;
use std::fmt;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::schema::StructType;
use crate::{KernelError, KernelResult, Result};

/// Decodes a schema with a finite budget for visited JSON containers.
pub(super) fn parse(json: &str, recursion_limit: usize) -> Result<StructType> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    deserializer.disable_recursion_limit();
    let exceeded = Cell::new(false);
    let budget = Budget {
        remaining: recursion_limit,
        exceeded: &exceeded,
    };
    // The depth budget bounds JSON nesting, not stack use across recursive Serde calls.
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
            .into()
        } else {
            super::schema_decode_error(error)
        }
    })
}

// Copying keeps sibling values at the same depth without restoring a shared counter on exit.
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use rstest::rstest;
    use serde::Deserialize;

    use super::{Budget, parse};
    use crate::KernelError;
    use crate::actions::SCHEMA_RECURSION_LIMIT;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[derive(Debug, Deserialize, PartialEq)]
    struct NestedArrays(Vec<Vec<u8>>);

    #[rstest]
    #[case::null_has_no_containers("null", 1, true)]
    #[case::wrappers_do_not_consume_depth("[[1],[2]]", 3, true)]
    #[case::wrappers_do_not_bypass_limit("[[1],[2]]", 2, false)]
    fn optional_newtype_preserves_container_budget(
        #[case] json: &str,
        #[case] limit: usize,
        #[case] succeeds: bool,
    ) {
        let expected = serde_json::from_str::<Option<NestedArrays>>(json).unwrap();
        let exceeded = Cell::new(false);
        let budget = Budget {
            remaining: limit,
            exceeded: &exceeded,
        };
        let mut deserializer = serde_json::Deserializer::from_str(json);
        let result = Option::<NestedArrays>::deserialize(budget.wrap(&mut deserializer));
        if succeeds {
            assert_eq!(result.unwrap(), expected);
            assert!(!exceeded.get());
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("recursion limit exceeded")
            );
            assert!(exceeded.get());
        }
    }

    #[rstest]
    #[case::formerly_rejected(160, true)]
    #[case::boundary(996, true)]
    #[case::exceeded(997, false)]
    fn parse_schema_collection_recursion_limit(
        #[values(false, true)] map: bool,
        #[case] depth: usize,
        #[case] succeeds: bool,
    ) {
        let data_type = (0..depth).fold(DataType::INTEGER, |nested, _| {
            if map {
                MapType::new(DataType::STRING, nested, true).into()
            } else {
                ArrayType::new(nested, true).into()
            }
        });
        let expected = StructType::try_new([StructField::nullable("nested", data_type)]).unwrap();
        let json = serde_json::to_string(&expected).unwrap();
        let result = parse(&json, SCHEMA_RECURSION_LIMIT);
        if succeeds {
            let parsed = result.unwrap();
            let cloned = parsed.clone();
            assert_eq!(cloned, expected);
            assert_eq!(serde_json::to_string(&cloned).unwrap(), json);
        } else {
            assert_eq!(
                result.unwrap_err().without_backtrace().to_string(),
                "Schema error: Table schema is too deeply nested: decoding \
                 metaData.schemaString reached the JSON recursion limit of 1000"
            );
        }
    }

    #[test]
    fn parse_schema_retains_unknown_field_handling() {
        let json = format!(
            r#"{{"type":"struct","fields":[],"ignored":{}0{}}}"#,
            "[".repeat(1100),
            "]".repeat(1100),
        );
        let expected = serde_json::from_str::<StructType>(&json).unwrap();
        assert_eq!(parse(&json, SCHEMA_RECURSION_LIMIT).unwrap(), expected);
    }

    #[rstest]
    #[case::root_unknown(r#"{"type":"struct","fields":[],"ignored":"#, "}")]
    #[case::field_unknown(
        concat!(r#"{"type":"struct","fields":[{"name":"x","type":"integer","#,
            r#""nullable":true,"metadata":{},"ignored":"#),
        "}]}"
    )]
    #[case::type_unknown(
        concat!(r#"{"type":"struct","fields":[{"name":"x","#,
            r#""type":{"type":"struct","fields":[],"ignored":"#),
        r#"},"nullable":true,"metadata":{}}]}"#
    )]
    #[case::metadata(
        concat!(r#"{"type":"struct","fields":[{"name":"x","type":"integer","#,
            r#""nullable":true,"metadata":{"nested":"#),
        "}}]}"
    )]
    fn schema_json_recursion_preserves_serde_handling(
        #[case] prefix: &str,
        #[case] suffix: &str,
        #[values(0, 1, 123, 124, 125, 127, 128, 129, 253, 300)] nesting: usize,
    ) {
        let json = format!(
            "{prefix}{}0{}{suffix}",
            "[".repeat(nesting),
            "]".repeat(nesting)
        );
        let default = serde_json::from_str::<StructType>(&json);
        let equivalent = parse(&json, 128);
        assert_eq!(default.is_ok(), equivalent.is_ok());
        let result = parse(&json, SCHEMA_RECURSION_LIMIT).unwrap();
        if let Ok(expected) = default {
            assert_eq!(result, expected);
        }
    }

    #[rstest]
    #[case::empty_schema_boundary(r#"{"type":"struct","fields":[]}"#, 3, true)]
    #[case::empty_schema_exceeded(r#"{"type":"struct","fields":[]}"#, 2, false)]
    #[case::field_boundary(
        concat!(r#"{"type":"struct","fields":[{"name":"a","type":"integer","#,
            r#""nullable":true,"metadata":{}}]}"#),
        5, true
    )]
    #[case::field_exceeded(
        concat!(r#"{"type":"struct","fields":[{"name":"a","type":"integer","#,
            r#""nullable":true,"metadata":{}}]}"#),
        4, false
    )]
    #[case::quoted_brackets(
        concat!(r#"{"type":"struct","fields":[{"name":"[\\\"{","type":"string","#,
            r#""nullable":true,"metadata":{"a":"\\\\}]"}}]}"#),
        5, true
    )]
    fn schema_json_depth_counts_decoded_containers(
        #[case] json: &str,
        #[case] limit: usize,
        #[case] succeeds: bool,
    ) {
        let expected = serde_json::from_str::<StructType>(json).unwrap();
        let result = parse(json, limit);
        if succeeds {
            assert_eq!(result.unwrap(), expected);
        } else {
            let error = result.unwrap_err();
            assert!(matches!(error.without_backtrace(), KernelError::Schema(_)));
            assert_eq!(
                error.without_backtrace().to_string(),
                format!(
                    "Schema error: Table schema is too deeply nested: decoding \
                     metaData.schemaString reached the JSON recursion limit of {limit}"
                )
            );
        }
    }

    #[rstest]
    #[case::schema(include_str!("../../tests/serde/schema.json"))]
    #[case::checkpoint_schema(include_str!("../../tests/serde/checkpoint_schema.json"))]
    fn parse_schema_preserves_schema_fixtures(#[case] json: &str) {
        let expected = serde_json::from_str::<StructType>(json).unwrap();
        assert_eq!(parse(json, SCHEMA_RECURSION_LIMIT).unwrap(), expected);
    }

    #[rstest]
    #[case::trailing_object(r#"{"type":"struct","fields":[]} {}"#)]
    #[case::trailing_text(r#"{"type":"struct","fields":[]} garbage"#)]
    #[case::unclosed_string(r#"{"type":"struct","fields":[],"x":"abc}"#)]
    #[case::unclosed_ignored_array(r#"{"type":"struct","fields":[],"x":[[0]}"#)]
    #[case::ignored_array_trailing_comma(r#"{"type":"struct","fields":[],"x":[0,]}"#)]
    fn parse_schema_rejects_malformed_json(#[case] json: &str) {
        let error = parse(json, SCHEMA_RECURSION_LIMIT).unwrap_err();
        assert!(matches!(
            error.without_backtrace(),
            KernelError::MalformedJson(_)
        ));
    }
}
