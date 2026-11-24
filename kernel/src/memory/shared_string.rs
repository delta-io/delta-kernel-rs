use std::ops::Deref;
use std::sync::Arc;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::MetadataString;

/// Immutable tracked text, shared without copying its payload across clones and threads.
///
/// Both the `Arc` control block and text use Rust's global allocator. The text constructors
/// reserve fallibly; creating the `Arc` control block retains its infallible allocation behavior.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct SharedString(Arc<MetadataString>);

impl SharedString {
    /// Copy UTF-8 text through the shared Kernel allocator.
    pub fn try_copy(value: &str) -> crate::KernelResult<Self> {
        Ok(Self(Arc::new(MetadataString::try_copy(value)?)))
    }

    /// Serialize directly into tracked text storage.
    pub fn try_to_json<T: Serialize + ?Sized>(value: &T) -> crate::KernelResult<Self> {
        Ok(Self(Arc::new(MetadataString::try_serialize(value)?)))
    }

    /// Borrow the retained UTF-8 text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Return retained byte capacity.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn into_global(self) -> String {
        self.as_str().to_owned()
    }
}
impl From<&str> for SharedString {
    fn from(value: &str) -> Self {
        Self(Arc::new(MetadataString::from(value)))
    }
}
impl From<String> for SharedString {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}
impl From<&String> for SharedString {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}
impl Deref for SharedString {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl std::fmt::Debug for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}
impl std::fmt::Display for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl Serialize for SharedString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de> Deserialize<'de> for SharedString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Text;
        impl Visitor<'_> for Text {
            type Value = SharedString;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                SharedString::try_copy(value).map_err(|error| {
                    if let crate::KernelError::Memory(failure) = error.without_backtrace() {
                        super::record_failure(*failure);
                    }
                    E::custom(error)
                })
            }
        }
        deserializer.deserialize_str(Text)
    }
}

impl PartialEq<str> for SharedString {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}
