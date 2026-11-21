use std::borrow::{Borrow, Cow};
use std::fmt::{self, Display};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

use super::ColumnMetadataKey;

/// A metadata key with static storage for well-known annotations.
///
/// Known keys can be cloned without allocating. Unknown keys retain their owned spelling without
/// global interning. Equality, hashing, and serialization use the string contents in either case.
/// Implements [`Borrow<str>`] for string lookups in metadata maps.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct MetadataKey(Cow<'static, str>);

impl MetadataKey {
    /// Returns the key's string contents.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for MetadataKey {
    fn from(key: String) -> Self {
        Self(match known_key(&key) {
            Some(key) => Cow::Borrowed(key),
            None => Cow::Owned(key),
        })
    }
}

impl From<&str> for MetadataKey {
    fn from(key: &str) -> Self {
        Self(match known_key(key) {
            Some(key) => Cow::Borrowed(key),
            None => Cow::Owned(key.to_owned()),
        })
    }
}

impl From<&String> for MetadataKey {
    fn from(key: &String) -> Self {
        key.as_str().into()
    }
}

impl From<Cow<'_, str>> for MetadataKey {
    fn from(key: Cow<'_, str>) -> Self {
        match key {
            Cow::Borrowed(key) => key.into(),
            Cow::Owned(key) => key.into(),
        }
    }
}

impl From<Box<str>> for MetadataKey {
    fn from(key: Box<str>) -> Self {
        String::from(key).into()
    }
}

impl From<MetadataKey> for String {
    fn from(key: MetadataKey) -> Self {
        key.0.into_owned()
    }
}

impl<'de> Deserialize<'de> for MetadataKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> serde::de::Visitor<'de> for KeyVisitor {
            type Value = MetadataKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a metadata key")
            }

            fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
                Ok(key.into())
            }

            fn visit_string<E: serde::de::Error>(self, key: String) -> Result<Self::Value, E> {
                Ok(key.into())
            }

            fn visit_bytes<E: serde::de::Error>(self, key: &[u8]) -> Result<Self::Value, E> {
                let key = std::str::from_utf8(key).map_err(E::custom)?;
                Ok(key.into())
            }

            fn visit_byte_buf<E: serde::de::Error>(self, key: Vec<u8>) -> Result<Self::Value, E> {
                let key = String::from_utf8(key).map_err(E::custom)?;
                Ok(key.into())
            }
        }
        deserializer.deserialize_string(KeyVisitor)
    }
}

impl Borrow<str> for MetadataKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for MetadataKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for MetadataKey {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for MetadataKey {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Display for MetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Spark column-comment and existence-default keys supplement ColumnMetadataKey.
const EXTRA_KNOWN_KEYS: &[&str] = &["comment", "EXISTS_DEFAULT"];

fn known_key(key: &str) -> Option<&'static str> {
    EXTRA_KNOWN_KEYS
        .iter()
        .copied()
        .find(|known| *known == key)
        .or_else(|| ColumnMetadataKey::from_str(key).ok().map(Into::into))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;
    use serde::de::value::{BorrowedBytesDeserializer, BytesDeserializer, Error};
    use strum::IntoEnumIterator;

    use super::super::{DataType, StructField};
    use super::*;

    #[rstest]
    #[case::column_metadata(ColumnMetadataKey::iter().map(<&'static str>::from).collect())]
    #[case::other(EXTRA_KNOWN_KEYS.to_vec())]
    fn known_keys_share_storage_across_construction_and_clone(#[case] spellings: Vec<&str>) {
        for spelling in spellings {
            let borrowed = MetadataKey::from(spelling);
            let owned = MetadataKey::from(spelling.to_owned());
            let decoded: MetadataKey =
                serde_json::from_str(&serde_json::to_string(spelling).unwrap()).unwrap();
            for key in [borrowed.clone(), owned, decoded] {
                assert!(matches!(key.0, Cow::Borrowed(_)), "{spelling}");
                assert_eq!(key, borrowed);
                assert_eq!(key.as_str().as_ptr(), borrowed.as_str().as_ptr());
            }
        }
    }

    #[rstest]
    #[case("custom.key")]
    #[case("Comment")]
    #[case("ColumnMappingId")]
    #[case("delta.columnMapping.id.extra")]
    #[case("custom.\u{03bb}")]
    fn unknown_keys_keep_their_allocation_and_spelling(#[case] spelling: &str) {
        let original = spelling.to_owned();
        let pointer = original.as_ptr();
        let key = MetadataKey::from(original);
        assert!(matches!(key.0, Cow::Owned(_)));
        assert_eq!(key.as_str().as_ptr(), pointer);
        assert_eq!(key.as_str(), spelling);
        assert_eq!(key, MetadataKey::from(spelling));
        let cloned = key.clone();
        assert_ne!(key.as_str().as_ptr(), cloned.as_str().as_ptr());
    }

    #[rstest]
    #[case("comment")]
    #[case("custom.key")]
    fn string_key_types_work_with_field_metadata(#[case] spelling: &str) {
        let owned = spelling.to_owned();
        let field = StructField::nullable("a", DataType::INTEGER)
            .with_metadata([(&owned, 1)])
            .add_metadata([(Cow::Borrowed(spelling), 2)])
            .add_metadata([(Cow::Owned(owned.clone()), 3)])
            .add_metadata([(owned.clone().into_boxed_str(), 4)]);
        assert_eq!(field.metadata.len(), 1);
        assert_eq!(field.metadata.get(spelling), Some(&4.into()));
        let key = field.metadata.into_keys().next().unwrap();
        assert_eq!(key, spelling);
        assert_eq!(key, *spelling);
        assert_eq!(
            key.partial_cmp(&MetadataKey::from("z")),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(String::from(key), spelling);
    }

    #[test]
    fn moving_custom_keys_into_strings_preserves_the_allocation() {
        let owned = "custom.key".to_owned();
        let pointer = owned.as_ptr();
        let key = MetadataKey::from(Cow::Owned(owned));
        let owned = String::from(key);
        assert_eq!(owned.as_ptr(), pointer);
        let key = MetadataKey::from(owned.into_boxed_str());
        let owned = String::from(key);
        assert_eq!(owned.as_ptr(), pointer);
    }

    #[rstest]
    #[case(b"comment")]
    #[case(b"custom.key")]
    #[case(b"custom.\xce\xbb")]
    #[case(b"\xff")]
    fn byte_keys_accept_utf8_only(#[case] bytes: &[u8]) {
        let decoded = MetadataKey::deserialize(BytesDeserializer::<Error>::new(bytes));
        let borrowed = MetadataKey::deserialize(BorrowedBytesDeserializer::<Error>::new(bytes));
        match std::str::from_utf8(bytes) {
            Ok(spelling) => {
                assert_eq!(decoded.unwrap(), MetadataKey::from(spelling));
                assert_eq!(borrowed.unwrap(), MetadataKey::from(spelling));
            }
            Err(_) => {
                assert!(decoded.is_err());
                assert!(borrowed.is_err());
            }
        }
    }

    #[test]
    fn json_roundtrip_and_string_lookup_preserve_key_semantics() {
        let json = r#"{"comment":1,"custom.key":2,"delta.columnMapping.id":3}"#;
        let keys: HashMap<MetadataKey, i32> = serde_json::from_str(json).unwrap();
        assert_eq!(keys.get("comment"), Some(&1));
        assert_eq!(keys.get("custom.key"), Some(&2));
        let (known, _) = keys.get_key_value("comment").unwrap();
        let (custom, _) = keys.get_key_value("custom.key").unwrap();
        assert!(matches!(known.0, Cow::Borrowed(_)));
        assert!(matches!(custom.0, Cow::Owned(_)));
        assert_eq!(
            serde_json::to_value(&keys).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
    }

    #[test]
    fn escaped_json_keys_use_the_decoded_spelling() {
        let mut keys: HashMap<MetadataKey, i32> =
            serde_json::from_str(r#"{"comm\u0065nt":1,"comment":2,"custom\"key":3}"#).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys.remove("comment"), Some(2));
        assert_eq!(keys.remove("custom\"key"), Some(3));
    }
}
