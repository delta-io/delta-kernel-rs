//! User-defined schema annotations and their physical representation.

use std::collections::BTreeMap;

use delta_kernel_derive::internal_api;
use serde::de::Error as _;
use serde::ser::{Error as _, SerializeMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::DataType;
use crate::{DeltaResult, Error};

/// An engine-defined annotation over a physical Delta type.
///
/// Use this in a logical schema to retain engine-specific type information. Physical readers use
/// `sql_type`; schema serialization preserves both the physical type and the opaque annotation.
///
/// Equality compares both the complete `sql_type` and the annotation, including explicit nulls.
/// Use equality to check logical schema identity or annotation preservation. Schema read
/// compatibility also requires exact UDT equality, even when physical types match.
///
/// # Example
///
/// A nullable UDT field in a table's `schemaString`:
///
/// ```rust
/// use delta_kernel::schema::{DataType, StructField, UserDefinedType};
/// use serde_json::json;
///
/// let udt = UserDefinedType {
///     sql_type: Box::new(DataType::LONG),
///     annotation: [
///         ("class".to_owned(), Some("example.Id".to_owned())),
///         ("pyClass".to_owned(), None),
///     ].into(),
/// };
/// let field = StructField::nullable("id", udt);
/// assert_eq!(serde_json::to_value(field)?, json!({
///     "name": "id",
///     "type": {"type": "udt", "sqlType": "long", "class": "example.Id", "pyClass": null},
///     "nullable": true,
///     "metadata": {}
/// }));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDefinedType {
    /// Physical schema used to read and write values. It must not contain another UDT.
    pub sql_type: Box<DataType>,
    /// Engine-specific type information retained when transporting or serializing the schema.
    /// `None` preserves an explicit JSON null; absent keys stay absent.
    /// The keys `type` and `sqlType` are reserved.
    pub annotation: BTreeMap<String, Option<String>>,
}

impl UserDefinedType {
    /// Returns an error if the physical type contains a UDT or the annotation uses a reserved key.
    #[internal_api]
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
        // The required `type` and `sqlType` members are separate from the engine annotation.
        const REQUIRED_UDT_FIELD_COUNT: usize = 2;

        self.validate().map_err(S::Error::custom)?;
        let mut map =
            serializer.serialize_map(Some(self.annotation.len() + REQUIRED_UDT_FIELD_COUNT))?;
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
    use crate::schema::compare::{Error as ComparisonError, SchemaComparison};
    use crate::schema::{schema, ArrayType, MapType, StructField};
    use crate::table_changes::CdfMode;
    use crate::transforms::{transform_output_type, SchemaTransform};
    #[cfg(feature = "declarative-plans")]
    use crate::PlanBuilder;

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

    /// Both CDF modes reject changed UDT annotations even when the physical type is unchanged.
    #[rstest]
    #[case::unannotated(json!({}), json!({}), true)]
    #[case::same(json!({"class":"Id"}), json!({"class":"Id"}), true)]
    #[case::different_class(json!({"class":"Id"}), json!({"class":"Other"}), false)]
    #[case::different_py_class(json!({"pyClass":"Id"}), json!({"pyClass":"Other"}), false)]
    #[case::different_serialized_class(
        json!({"pyClass":"Id", "serializedClass":"source"}),
        json!({"pyClass":"Id", "serializedClass":"target"}),
        false
    )]
    #[case::different_extension(json!({"extension":"source"}), json!({"extension":"target"}), false)]
    #[case::absent_vs_null(json!({}), json!({"pyClass":null}), false)]
    #[case::null_vs_string(json!({"pyClass":null}), json!({"pyClass":"Id"}), false)]
    #[case::key_order(
        json!({"class":"Id", "pyClass":null}),
        json!({"pyClass":null, "class":"Id"}),
        true
    )]
    fn udt_read_compatibility_and_cdf_require_equal_annotations(
        #[case] source_annotation: Value,
        #[case] target_annotation: Value,
        #[case] equal: bool,
        #[values(
            DataType::LONG,
            DataType::from(schema! { nullable "x": LONG }),
            DataType::from(ArrayType::new(DataType::LONG, true)),
            DataType::from(MapType::new(DataType::STRING, DataType::LONG, true))
        )]
        physical_type: DataType,
        #[values(
            |udt| udt,
            |udt| DataType::from(schema! { nullable "nested": (udt) }),
            |udt| DataType::from(ArrayType::new(udt, true)),
            |udt| DataType::from(MapType::new(udt, DataType::LONG, true)),
            |udt| DataType::from(MapType::new(DataType::STRING, udt, true))
        )]
        wrap: fn(DataType) -> DataType,
    ) {
        let with_annotation = |annotation| {
            DataType::from(UserDefinedType {
                sql_type: Box::new(physical_type.clone()),
                annotation: serde_json::from_value(annotation).unwrap(),
            })
        };
        let source = wrap(with_annotation(source_annotation));
        let target = wrap(with_annotation(target_annotation));
        assert_eq!(source == target, equal);
        for (source, target) in [(&source, &target), (&target, &source)] {
            assert_eq!(source.can_read_as(target).is_ok(), equal);
            assert_eq!(
                source.can_read_as_without_type_widening(target).is_ok(),
                equal
            );
        }

        let source = schema! { nullable "id": (source) };
        let target = schema! { nullable "id": (target) };
        assert_eq!(source == target, equal);
        assert_eq!(
            CdfMode::ChangeDataFeed.schemas_compatible(&source, &target),
            equal
        );
        assert_eq!(
            CdfMode::RowTracking.schemas_compatible(&source, &target),
            equal
        );
        let target = schema! {
            ..(target.fields()),
            nullable "added": STRING,
        };
        assert!(!CdfMode::ChangeDataFeed.schemas_compatible(&source, &target));
        assert_eq!(
            CdfMode::RowTracking.schemas_compatible(&source, &target),
            equal
        );
    }

    /// Keeps physical schema changes incompatible even when ordinary types could evolve or widen.
    #[rstest]
    #[case::primitive(DataType::INTEGER, DataType::LONG)]
    #[case::struct_field(
        DataType::from(schema! { nullable "x": LONG }),
        DataType::from(schema! { nullable "x": STRING })
    )]
    #[case::struct_widening(
        DataType::from(schema! { nullable "x": INTEGER }),
        DataType::from(schema! { nullable "x": LONG })
    )]
    #[case::struct_addition(
        DataType::from(schema! { nullable "x": LONG }),
        DataType::from(schema! { nullable "x": LONG, nullable "y": LONG })
    )]
    #[case::array_element(
        DataType::from(ArrayType::new(DataType::INTEGER, true)),
        DataType::from(ArrayType::new(DataType::LONG, true))
    )]
    #[case::map_key(
        DataType::from(MapType::new(DataType::INTEGER, DataType::LONG, true)),
        DataType::from(MapType::new(DataType::LONG, DataType::LONG, true))
    )]
    #[case::map_value(
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        DataType::from(MapType::new(DataType::STRING, DataType::LONG, true))
    )]
    #[case::struct_nullability(
        DataType::from(schema! { not_null "x": LONG }),
        DataType::from(schema! { nullable "x": LONG })
    )]
    #[case::array_nullability(
        DataType::from(ArrayType::new(DataType::LONG, false)),
        DataType::from(ArrayType::new(DataType::LONG, true))
    )]
    #[case::map_nullability(
        DataType::from(MapType::new(DataType::STRING, DataType::LONG, false)),
        DataType::from(MapType::new(DataType::STRING, DataType::LONG, true))
    )]
    fn different_physical_types_are_unequal_and_incompatible(
        #[case] source: DataType,
        #[case] target: DataType,
    ) {
        let wrap = |sql_type| {
            DataType::from(UserDefinedType {
                sql_type: Box::new(sql_type),
                annotation: [("class".to_owned(), Some("Id".to_owned()))].into(),
            })
        };
        let source = wrap(source);
        let target = wrap(target);
        assert_ne!(source, target);
        for (source, target) in [(&source, &target), (&target, &source)] {
            assert!(matches!(
                source.can_read_as(target),
                Err(ComparisonError::TypeMismatch)
            ));
            assert!(matches!(
                source.can_read_as_without_type_widening(target),
                Err(ComparisonError::TypeMismatch)
            ));
        }
        let source = schema! { nullable "id": (source) };
        let target = schema! { nullable "id": (target) };
        for mode in [CdfMode::ChangeDataFeed, CdfMode::RowTracking] {
            assert!(!mode.schemas_compatible(&source, &target));
        }
    }

    /// Prevents an unannotated UDT from being substituted with its bare physical type.
    #[rstest]
    fn udt_is_distinct_from_its_bare_physical_type(
        #[values(DataType::LONG, DataType::from(schema! { nullable "x": LONG }))]
        physical_type: DataType,
    ) {
        let udt = DataType::from(UserDefinedType {
            sql_type: Box::new(physical_type.clone()),
            annotation: BTreeMap::new(),
        });
        assert_ne!(udt, physical_type);
        for (source, target) in [(&udt, &physical_type), (&physical_type, &udt)] {
            assert!(matches!(
                source.can_read_as(target),
                Err(ComparisonError::TypeMismatch)
            ));
            assert!(matches!(
                source.can_read_as_without_type_widening(target),
                Err(ComparisonError::TypeMismatch)
            ));
        }
    }

    /// Allows outer nullability relaxation only when the UDT annotations are unchanged.
    #[rstest]
    #[case::relax(false, true, true)]
    #[case::tighten(true, false, false)]
    fn read_compatibility_checks_outer_field_nullability(
        #[case] source_nullable: bool,
        #[case] target_nullable: bool,
        #[case] compatible: bool,
        #[values(true, false)] same_annotation: bool,
    ) {
        let field = |nullable, class: &str| {
            StructField::new(
                "id",
                UserDefinedType {
                    sql_type: Box::new(DataType::LONG),
                    annotation: [("class".to_owned(), Some(class.to_owned()))].into(),
                },
                nullable,
            )
        };
        let source = field(source_nullable, "Source");
        let target = field(
            target_nullable,
            if same_annotation { "Source" } else { "Target" },
        );
        assert_eq!(
            source.can_read_as(&target).is_ok(),
            compatible && same_annotation
        );
        assert_eq!(
            source.can_read_as_without_type_widening(&target).is_ok(),
            compatible && same_annotation
        );
        let source = schema! { (source) };
        let target = schema! { (target) };
        assert!(!CdfMode::ChangeDataFeed.schemas_compatible(&source, &target));
        assert_eq!(
            CdfMode::RowTracking.schemas_compatible(&source, &target),
            compatible && same_annotation
        );
    }

    /// Ensures plan unions preserve UDT annotations as part of logical schema identity.
    #[cfg(feature = "declarative-plans")]
    #[rstest]
    #[case::same_annotation("Id", true)]
    #[case::different_annotation("Other", false)]
    fn union_all_requires_equal_udt_annotations(#[case] class: &str, #[case] equal: bool) {
        let input = |class: &str| {
            let udt = UserDefinedType {
                sql_type: Box::new(DataType::LONG),
                annotation: [("class".to_owned(), Some(class.to_owned()))].into(),
            };
            PlanBuilder::values(schema! { nullable "id": (udt) }, vec![]).unwrap()
        };
        let result = PlanBuilder::union_all([input("Id"), input(class)]);
        assert_eq!(result.is_ok(), equal);
        if let Err(error) = result {
            assert!(error.to_string().contains("differing from input 0"));
        }
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
