use super::schema::data_type::Kind as DataTypeKind;
use super::schema::metadata_value::Value as MetadataValueKind;
use super::schema::primitive_type::Kind as PrimitiveTypeKind;
#[cfg(feature = "geo-type-in-dev")]
use super::schema::EdgeInterpolationAlgorithm as EdgeAlgo;
use super::schema::SimplePrimitiveType as Simple;
use super::{schema as proto_schema, state as proto_state};
use crate::actions::{Metadata, Protocol};
use crate::schema::{
    ArrayType, DataType, DecimalType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};
#[cfg(feature = "geo-type-in-dev")]
use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};

impl From<&DataType> for proto_schema::DataType {
    fn from(data_type: &DataType) -> Self {
        let kind = match data_type {
            DataType::Primitive(primitive) => DataTypeKind::Primitive(primitive.into()),
            DataType::Array(array) => DataTypeKind::Array(Box::new(array.as_ref().into())),
            DataType::Struct(struct_type) => DataTypeKind::Struct(struct_type.as_ref().into()),
            DataType::Map(map) => DataTypeKind::Map(Box::new(map.as_ref().into())),
            DataType::Variant(_) => DataTypeKind::Variant(proto_schema::VariantType {}),
        };
        proto_schema::DataType { kind: Some(kind) }
    }
}

impl From<&PrimitiveType> for proto_schema::PrimitiveType {
    fn from(primitive: &PrimitiveType) -> Self {
        let kind = match primitive {
            PrimitiveType::String => PrimitiveTypeKind::Simple(Simple::String as i32),
            PrimitiveType::Long => PrimitiveTypeKind::Simple(Simple::Long as i32),
            PrimitiveType::Integer => PrimitiveTypeKind::Simple(Simple::Integer as i32),
            PrimitiveType::Short => PrimitiveTypeKind::Simple(Simple::Short as i32),
            PrimitiveType::Byte => PrimitiveTypeKind::Simple(Simple::Byte as i32),
            PrimitiveType::Float => PrimitiveTypeKind::Simple(Simple::Float as i32),
            PrimitiveType::Double => PrimitiveTypeKind::Simple(Simple::Double as i32),
            PrimitiveType::Boolean => PrimitiveTypeKind::Simple(Simple::Boolean as i32),
            PrimitiveType::Binary => PrimitiveTypeKind::Simple(Simple::Binary as i32),
            PrimitiveType::Date => PrimitiveTypeKind::Simple(Simple::Date as i32),
            PrimitiveType::Timestamp => PrimitiveTypeKind::Simple(Simple::Timestamp as i32),
            PrimitiveType::TimestampNtz => PrimitiveTypeKind::Simple(Simple::TimestampNtz as i32),
            PrimitiveType::Decimal(decimal) => PrimitiveTypeKind::Decimal((*decimal).into()),
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveType::Geometry(geometry) => {
                PrimitiveTypeKind::Geometry(geometry.as_ref().into())
            }
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveType::Geography(geography) => {
                PrimitiveTypeKind::Geography(geography.as_ref().into())
            }
            PrimitiveType::Void => PrimitiveTypeKind::Simple(Simple::Void as i32),
            PrimitiveType::IntervalYearMonth => {
                PrimitiveTypeKind::Simple(Simple::IntervalYearMonth as i32)
            }
            PrimitiveType::IntervalDayTime => {
                PrimitiveTypeKind::Simple(Simple::IntervalDayTime as i32)
            }
        };
        proto_schema::PrimitiveType { kind: Some(kind) }
    }
}

impl From<DecimalType> for proto_schema::DecimalType {
    fn from(decimal: DecimalType) -> Self {
        Self {
            precision: u32::from(decimal.precision()),
            scale: u32::from(decimal.scale()),
        }
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl From<&GeometryType> for proto_schema::GeometryType {
    fn from(geometry: &GeometryType) -> Self {
        Self {
            crs: geometry.crs().to_string(),
        }
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl From<&GeographyType> for proto_schema::GeographyType {
    fn from(geography: &GeographyType) -> Self {
        Self {
            crs: geography.crs().to_string(),
            algorithm: EdgeAlgo::from(geography.algorithm()) as i32,
        }
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl From<&EdgeInterpolationAlgorithm> for EdgeAlgo {
    fn from(algorithm: &EdgeInterpolationAlgorithm) -> Self {
        match algorithm {
            EdgeInterpolationAlgorithm::Spherical => EdgeAlgo::Spherical,
            EdgeInterpolationAlgorithm::Vincenty => EdgeAlgo::Vincenty,
            EdgeInterpolationAlgorithm::Thomas => EdgeAlgo::Thomas,
            EdgeInterpolationAlgorithm::Andoyer => EdgeAlgo::Andoyer,
            EdgeInterpolationAlgorithm::Karney => EdgeAlgo::Karney,
        }
    }
}

impl From<&ArrayType> for proto_schema::ArrayType {
    fn from(array: &ArrayType) -> Self {
        Self {
            element_type: Some(Box::new(array.element_type().into())),
            contains_null: array.contains_null(),
        }
    }
}

impl From<&MapType> for proto_schema::MapType {
    fn from(map: &MapType) -> Self {
        Self {
            key_type: Some(Box::new(map.key_type().into())),
            value_type: Some(Box::new(map.value_type().into())),
            value_contains_null: map.value_contains_null(),
        }
    }
}

impl From<&StructType> for proto_schema::StructType {
    fn from(struct_type: &StructType) -> Self {
        Self {
            fields: struct_type.fields().map(Into::into).collect(),
        }
    }
}

impl From<&StructField> for proto_schema::StructField {
    fn from(field: &StructField) -> Self {
        let metadata = field
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.into()))
            .collect();
        Self {
            name: field.name.clone(),
            data_type: Some((&field.data_type).into()),
            nullable: field.nullable,
            metadata,
        }
    }
}

impl From<&MetadataValue> for proto_schema::MetadataValue {
    fn from(metadata: &MetadataValue) -> Self {
        let value = match metadata {
            MetadataValue::Number(n) => MetadataValueKind::Number(*n),
            MetadataValue::String(s) => MetadataValueKind::String(s.clone()),
            MetadataValue::Boolean(b) => MetadataValueKind::Boolean(*b),
            MetadataValue::Other(json) => MetadataValueKind::OtherJson(json.to_string()),
        };
        Self { value: Some(value) }
    }
}

impl From<&Metadata> for proto_state::Metadata {
    fn from(metadata: &Metadata) -> Self {
        Self {
            id: metadata.id().to_owned(),
            name: metadata.name().map(str::to_owned),
            description: metadata.description().map(str::to_owned),
            format_provider: metadata.format_provider().to_owned(),
            format_options: metadata.format_options().clone(),
            schema_string: metadata.schema_string().clone(),
            partition_columns: metadata.partition_columns().to_vec(),
            created_time: metadata.created_time(),
            configuration: metadata.configuration().clone(),
        }
    }
}

impl From<&Protocol> for proto_state::Protocol {
    fn from(protocol: &Protocol) -> Self {
        let feature_list =
            |features: &[crate::table_features::TableFeature]| proto_state::FeatureList {
                features: features
                    .iter()
                    .map(|feature| feature.as_ref().to_owned())
                    .collect(),
            };
        Self {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: protocol.reader_features().map(feature_list),
            writer_features: protocol.writer_features().map(feature_list),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_preserves_absent_and_present_empty_feature_lists() {
        let legacy = Protocol::try_new_legacy(1, 2).unwrap();
        let legacy_proto = proto_state::Protocol::from(&legacy);
        assert!(legacy_proto.reader_features.is_none());
        assert!(legacy_proto.writer_features.is_none());

        let modern = Protocol::try_new_modern(Vec::<String>::new(), Vec::<String>::new()).unwrap();
        let modern_proto = proto_state::Protocol::from(&modern);
        assert_eq!(
            modern_proto.reader_features.unwrap().features,
            Vec::<String>::new()
        );
        assert_eq!(
            modern_proto.writer_features.unwrap().features,
            Vec::<String>::new()
        );
    }
}
