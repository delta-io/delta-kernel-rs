//! User-defined schema annotations and their physical representation.

use std::collections::BTreeMap;

use serde::de::Error as _;
use serde::ser::{Error as _, SerializeMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::DataType;
use crate::{DeltaResult, Error};

/// An engine-defined annotation over a physical Delta type.
///
/// Type equality compares only `sql_type`, so annotations do not affect read compatibility.
#[derive(Debug, Clone, Eq)]
pub struct UserDefinedType {
    /// Physical type. It must not contain another user-defined type.
    pub sql_type: Box<DataType>,
    /// Opaque engine members. `None` preserves an explicit JSON null; absent keys stay absent.
    /// The keys `type` and `sqlType` are reserved.
    pub annotation: BTreeMap<String, Option<String>>,
}

impl PartialEq for UserDefinedType {
    fn eq(&self, other: &Self) -> bool {
        self.sql_type == other.sql_type
    }
}

impl UserDefinedType {
    pub(crate) fn validate(&self) -> DeltaResult<()> {
        if contains_udt(&self.sql_type) {
            return Err(Error::schema("A UDT sqlType must not contain another UDT"));
        }
        if self.annotation.contains_key("type") || self.annotation.contains_key("sqlType") {
            return Err(Error::schema(
                "UDT annotation keys type and sqlType are reserved",
            ));
        }
        Ok(())
    }
}

impl Serialize for UserDefinedType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut map = serializer.serialize_map(Some(self.annotation.len() + 2))?;
        map.serialize_entry("type", "udt")?;
        map.serialize_entry("sqlType", &self.sql_type)?;
        for (key, value) in &self.annotation {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for UserDefinedType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(rename = "type")]
            type_name: String,
            #[serde(rename = "sqlType")]
            sql_type: Box<DataType>,
            #[serde(flatten)]
            annotation: BTreeMap<String, Option<String>>,
        }
        let repr = Repr::deserialize(deserializer)?;
        if repr.type_name != "udt" {
            return Err(D::Error::custom("Expected UDT type to be 'udt'"));
        }
        let udt = Self {
            sql_type: repr.sql_type,
            annotation: repr.annotation,
        };
        udt.validate().map_err(D::Error::custom)?;
        Ok(udt)
    }
}

fn contains_udt(data_type: &DataType) -> bool {
    match data_type {
        DataType::UserDefined(_) => true,
        DataType::Struct(s) | DataType::Variant(s) => {
            s.fields().any(|f| contains_udt(f.data_type()))
        }
        DataType::Array(a) => contains_udt(a.element_type()),
        DataType::Map(m) => contains_udt(m.key_type()) || contains_udt(m.value_type()),
        DataType::Primitive(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{json, Value};

    use super::*;
    use crate::schema::compare::SchemaComparison;
    use crate::transforms::{transform_output_type, SchemaTransform};

    #[rstest]
    #[case(json!({"type":"udt", "sqlType":"long"}))]
    #[case(json!({"type":"udt", "sqlType":"long", "class":"Id", "pyClass":null}))]
    #[case(json!({"type":"udt", "sqlType":"long", "pyClass":"Id", "serializedClass":"opaque"}))]
    #[case(json!({"type":"udt", "sqlType":{"type":"struct", "fields":[
        {"name":"values", "type":{"type":"array", "elementType":"double", "containsNull":false},
         "nullable":true, "metadata":{}}
    ]}, "class":"VectorUDT", "extension":null}))]
    fn round_trip(#[case] value: Value) {
        let data_type: DataType = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(data_type).unwrap(), value);
    }

    #[rstest]
    #[case(json!({"type":"udt"}), "sqlType")]
    #[case(json!({"type":"udt", "sqlType":"long", "class":123}), "string")]
    #[case(json!({"type":"udt", "sqlType":"long", "class":{}}), "string")]
    #[case(json!({"type":"udt", "sqlType":{"type":"udt", "sqlType":"long"}}), "another UDT")]
    #[case(json!({"type":"udt", "sqlType":{"type":"array", "elementType":
        {"type":"udt", "sqlType":"long"}, "containsNull":true}}), "another UDT")]
    #[case(json!({"type":"udt", "sqlType":{"type":"struct", "fields":[
        {"name":"inner", "type":{"type":"udt", "sqlType":"long"}, "nullable":true, "metadata":{}}
    ]}}), "another UDT")]
    #[case(json!({"type":"udt", "sqlType":{"type":"map", "keyType":"string",
        "valueType":{"type":"udt", "sqlType":"long"}, "valueContainsNull":true}}), "another UDT")]
    fn reject_invalid(#[case] value: Value, #[case] message: &str) {
        let error = serde_json::from_value::<DataType>(value).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn equality_ignores_annotation() {
        let a: DataType = serde_json::from_value(json!({
            "type":"udt", "sqlType":"long", "class":"First", "pyClass":null
        }))
        .unwrap();
        let b: DataType = serde_json::from_value(json!({
            "pyClass":"Second", "class":"Other", "sqlType":"long", "type":"udt"
        }))
        .unwrap();
        assert_eq!(a, b);
        assert!(a.can_read_as(&b).is_ok());
        assert_ne!(a, DataType::LONG);
    }

    #[rstest]
    #[case("type")]
    #[case("sqlType")]
    fn serialization_rejects_reserved_annotation_keys(#[case] key: &str) {
        let udt = UserDefinedType {
            sql_type: Box::new(DataType::LONG),
            annotation: [(key.to_owned(), None)].into(),
        };
        assert!(serde_json::to_value(udt)
            .unwrap_err()
            .to_string()
            .contains("reserved"));
    }

    #[test]
    fn schema_transform_treats_udt_as_leaf() {
        struct RejectPrimitives;
        impl<'a> SchemaTransform<'a> for RejectPrimitives {
            transform_output_type!(|'a, T| Result<(), ()>);
            fn transform_primitive(
                &mut self,
                _: &'a super::super::PrimitiveType,
            ) -> Result<(), ()> {
                Err(())
            }
        }
        let udt = UserDefinedType {
            sql_type: Box::new(DataType::LONG),
            annotation: BTreeMap::new(),
        };
        assert!(RejectPrimitives.transform(&DataType::from(udt)).is_ok());
        assert!(RejectPrimitives.transform(&DataType::LONG).is_err());
    }
}
