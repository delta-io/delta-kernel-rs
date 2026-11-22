use std::ops::Deref;
use std::sync::Arc;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{LimitedString, MemoryBudget};

/// Immutable shared text retaining its allocation owner across clones and threads.
///
/// Fallible constructors use the active or global budget. Converting an existing `String`
/// preserves its unbudgeted allocation; it does not retroactively enforce a limit.
#[derive(Clone)]
pub struct SharedString(Arc<Storage>);

impl std::fmt::Debug for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

#[derive(Debug)]
enum Storage {
    Global(String),
    Limited(LimitedString),
}

impl SharedString {
    /// Copy text into fallible storage when a budget is active.
    pub fn try_copy(value: &str) -> crate::KernelResult<Self> {
        Ok(match MemoryBudget::current() {
            Some(budget) => Self(Arc::new(Storage::Limited(LimitedString::try_copy(
                &budget, value,
            )?))),
            None => value.to_owned().into(),
        })
    }

    /// Serialize JSON directly into owned storage without first creating an unbounded string.
    pub fn try_to_json<T: Serialize + ?Sized>(value: &T) -> crate::KernelResult<Self> {
        Ok(match MemoryBudget::current() {
            Some(budget) => Self(Arc::new(Storage::Limited(LimitedString::try_serialize(
                &budget, value,
            )?))),
            None => serde_json::to_string(value)?.into(),
        })
    }

    /// Borrow the shared UTF-8 text.
    pub fn as_str(&self) -> &str {
        match self.0.as_ref() {
            Storage::Global(value) => value,
            Storage::Limited(value) => value.as_str(),
        }
    }

    /// Return the backing allocation's byte capacity.
    pub fn capacity(&self) -> usize {
        match self.0.as_ref() {
            Storage::Global(value) => value.capacity(),
            Storage::Limited(value) => value.capacity(),
        }
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    // Engine Scalar::String owns a standard String; this boundary still copies limited text
    // outside the budget. Keeping that limitation explicit prevents reporting it as controlled.
    pub(crate) fn into_global(self) -> String {
        match Arc::try_unwrap(self.0) {
            Ok(Storage::Global(value)) => value,
            Ok(Storage::Limited(value)) => value.as_str().to_owned(),
            Err(shared) => match shared.as_ref() {
                Storage::Global(value) => value.clone(),
                Storage::Limited(value) => value.as_str().to_owned(),
            },
        }
    }
}

impl Default for SharedString {
    fn default() -> Self {
        String::new().into()
    }
}

impl From<String> for SharedString {
    fn from(value: String) -> Self {
        Self(Arc::new(Storage::Global(value)))
    }
}

impl Deref for SharedString {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for SharedString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for SharedString {}

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
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                if MemoryBudget::current().is_some() {
                    self.visit_str(&value)
                } else {
                    Ok(value.into())
                }
            }
        }
        deserializer.deserialize_str(Text)
    }
}
