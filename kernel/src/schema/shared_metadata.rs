//! Copy-on-write storage for field metadata.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::MetadataValue;

/// Cloning shares the metadata map. Mutable map access copies it if it is shared.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SharedFieldMetadata(Arc<HashMap<String, MetadataValue>>);

impl std::fmt::Debug for SharedFieldMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Deref for SharedFieldMetadata {
    type Target = HashMap<String, MetadataValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SharedFieldMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl From<HashMap<String, MetadataValue>> for SharedFieldMetadata {
    fn from(value: HashMap<String, MetadataValue>) -> Self {
        Self(Arc::new(value))
    }
}

impl FromIterator<(String, MetadataValue)> for SharedFieldMetadata {
    fn from_iter<T: IntoIterator<Item = (String, MetadataValue)>>(iter: T) -> Self {
        HashMap::from_iter(iter).into()
    }
}

impl IntoIterator for SharedFieldMetadata {
    type Item = (String, MetadataValue);
    type IntoIter = std::collections::hash_map::IntoIter<String, MetadataValue>;

    fn into_iter(self) -> Self::IntoIter {
        Arc::unwrap_or_clone(self.0).into_iter()
    }
}

impl<'a> IntoIterator for &'a SharedFieldMetadata {
    type Item = (&'a String, &'a MetadataValue);
    type IntoIter = std::collections::hash_map::Iter<'a, String, MetadataValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl PartialEq<HashMap<String, MetadataValue>> for SharedFieldMetadata {
    fn eq(&self, other: &HashMap<String, MetadataValue>) -> bool {
        self.0.as_ref() == other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_shares_until_mutation_and_keeps_json_shape() {
        let original: SharedFieldMetadata = [(
            "custom".into(),
            MetadataValue::Other(serde_json::json!({"nested": [1, 2]})),
        )]
        .into_iter()
        .collect();
        let mut cloned = original.clone();
        assert!(Arc::ptr_eq(&original.0, &cloned.0));
        cloned.insert("added".into(), MetadataValue::Boolean(true));
        assert!(!Arc::ptr_eq(&original.0, &cloned.0));
        assert!(!original.contains_key("added"));
        let json = serde_json::to_value(&original).unwrap();
        assert_eq!(json, serde_json::json!({"custom": {"nested": [1, 2]}}));
        assert_eq!(
            serde_json::from_value::<SharedFieldMetadata>(json).unwrap(),
            original
        );
    }
}
