//! Definitions and functions to create and manipulate kernel schema

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

// re-export because many call sites that use schemas do not necessarily use expressions
pub(crate) use crate::expressions::{column_name, ColumnName};
use crate::utils::{require, CowExt as _};
use crate::{DeltaResult, Error};
use delta_kernel_derive::internal_api;

pub(crate) mod compare;

#[cfg(feature = "internal-api")]
pub mod derive_macro_utils;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod derive_macro_utils;
pub(crate) mod variant_utils;

pub type Schema = StructType;
pub type SchemaRef = Arc<StructType>;

/// Converts a type to a [`Schema`] that represents that type. Derivable for struct types using the
/// [`delta_kernel_derive::ToSchema`] derive macro.
#[internal_api]
pub(crate) trait ToSchema {
    fn to_schema() -> StructType;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged)]
pub enum MetadataValue {
    Number(i64),
    String(String),
    Boolean(bool),
    // The [PROTOCOL](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field) states
    // only that the metadata is "A JSON map containing information about this column.", so we can
    // actually have any valid json here. `Other` is therefore a catchall for things we don't need
    // to handle.
    Other(serde_json::Value),
}

impl Display for MetadataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataValue::Number(n) => write!(f, "{n}"),
            MetadataValue::String(s) => write!(f, "{s}"),
            MetadataValue::Boolean(b) => write!(f, "{b}"),
            MetadataValue::Other(v) => write!(f, "{v}"), // just write the json back
        }
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for MetadataValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for MetadataValue {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

#[derive(Debug)]
pub enum ColumnMetadataKey {
    ColumnMappingId,
    ColumnMappingPhysicalName,
    GenerationExpression,
    IdentityStart,
    IdentityStep,
    IdentityHighWaterMark,
    IdentityAllowExplicitInsert,
    Invariants,
}

impl AsRef<str> for ColumnMetadataKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ColumnMappingId => "delta.columnMapping.id",
            Self::ColumnMappingPhysicalName => "delta.columnMapping.physicalName",
            Self::GenerationExpression => "delta.generationExpression",
            Self::IdentityAllowExplicitInsert => "delta.identity.allowExplicitInsert",
            Self::IdentityHighWaterMark => "delta.identity.highWaterMark",
            Self::IdentityStart => "delta.identity.start",
            Self::IdentityStep => "delta.identity.step",
            Self::Invariants => "delta.invariants",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructField {
    /// Name of this (possibly nested) column
    pub name: String,
    /// The data type of this field
    #[serde(rename = "type")]
    pub data_type: DataType,
    /// Denotes whether this Field can be null
    pub nullable: bool,
    /// A JSON map containing information about this column
    pub metadata: HashMap<String, MetadataValue>,
}

impl StructField {
    /// Creates a new field
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new nullable field
    pub fn nullable(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, true)
    }

    /// Creates a new non-nullable field
    pub fn not_null(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, false)
    }

    /// Replaces `self.metadata` with the list of <key, value> pairs in `metadata`.
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Extends `self.metadata` to include the <key, value> pairs in `metadata`.
    pub fn add_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata
            .extend(metadata.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    pub fn get_config_value(&self, key: &ColumnMetadataKey) -> Option<&MetadataValue> {
        self.metadata.get(key.as_ref())
    }

    /// Get the physical name for this field as it should be read from parquet.
    ///
    /// NOTE: Caller affirms that the schema was already validated by
    /// [`crate::table_features::validate_schema_column_mapping`], to ensure that annotations are
    /// always and only present when column mapping mode is enabled.
    pub fn physical_name(&self) -> &str {
        match self
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
        {
            Some(MetadataValue::String(physical_name)) => physical_name,
            _ => &self.name,
        }
    }

    /// Change the name of a field. The field will preserve its data type and nullability. Note that
    /// this allocates a new field.
    pub fn with_name(&self, new_name: impl Into<String>) -> Self {
        StructField {
            name: new_name.into(),
            data_type: self.data_type().clone(),
            nullable: self.nullable,
            metadata: self.metadata.clone(),
        }
    }

    #[inline]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, MetadataValue> {
        &self.metadata
    }

    /// Convert our metadata into a HashMap<String, String>. Note this copies all the data so can be
    /// expensive for large metadata
    pub fn metadata_with_string_values(&self) -> HashMap<String, String> {
        self.metadata
            .iter()
            .map(|(key, val)| (key.clone(), val.to_string()))
            .collect()
    }

    /// Applies physical name mappings to this field
    ///
    /// NOTE: Caller affirms that the schema was already validated by
    /// [`crate::table_features::validate_schema_column_mapping`], to ensure that annotations are
    /// always and only present when column mapping mode is enabled.
    pub fn make_physical(&self) -> Self {
        struct MakePhysical;
        impl<'a> SchemaTransform<'a> for MakePhysical {
            fn transform_struct_field(
                &mut self,
                field: &'a StructField,
            ) -> Option<Cow<'a, StructField>> {
                let field = self.recurse_into_struct_field(field)?;
                Some(Cow::Owned(field.with_name(field.physical_name())))
            }
        }
        // NOTE: unwrap is safe because the transformer is incapable of returning None
        #[allow(clippy::unwrap_used)]
        MakePhysical
            .transform_struct_field(self)
            .unwrap()
            .into_owned()
    }

    fn has_invariants(&self) -> bool {
        self.metadata
            .contains_key(ColumnMetadataKey::Invariants.as_ref())
    }
}

/// A struct is used to represent both the top-level schema of the table
/// as well as struct columns that contain nested columns.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct StructType {
    pub type_name: String,
    /// The type of element stored in this array
    // We use indexmap to preserve the order of fields as they are defined in the schema
    // while also allowing for fast lookup by name. The alternative is to do a linear search
    // for each field by name would be potentially quite expensive for large schemas.
    pub fields: IndexMap<String, StructField>,
}

impl StructType {
    pub fn new(fields: impl IntoIterator<Item = StructField>) -> Self {
        Self {
            type_name: "struct".into(),
            fields: fields.into_iter().map(|f| (f.name.clone(), f)).collect(),
        }
    }

    pub fn try_new<E>(fields: impl IntoIterator<Item = Result<StructField, E>>) -> Result<Self, E> {
        let fields: Vec<_> = fields.into_iter().try_collect()?;
        Ok(Self::new(fields))
    }

    /// Get a [`StructType`] containing [`StructField`]s of the given names. The order of fields in
    /// the returned schema will match the order passed to this function, which can be different
    /// from this order in this schema. Returns an Err if a specified field doesn't exist.
    pub fn project_as_struct(&self, names: &[impl AsRef<str>]) -> DeltaResult<StructType> {
        let fields = names.iter().map(|name| {
            self.fields
                .get(name.as_ref())
                .cloned()
                .ok_or_else(|| Error::missing_column(name.as_ref()))
        });
        Self::try_new(fields)
    }

    /// Get a [`SchemaRef`] containing [`StructField`]s of the given names. The order of fields in
    /// the returned schema will match the order passed to this function, which can be different
    /// from this order in this schema. Returns an Err if a specified field doesn't exist.
    pub fn project(&self, names: &[impl AsRef<str>]) -> DeltaResult<SchemaRef> {
        let struct_type = self.project_as_struct(names)?;
        Ok(Arc::new(struct_type))
    }

    pub fn field(&self, name: impl AsRef<str>) -> Option<&StructField> {
        self.fields.get(name.as_ref())
    }

    pub fn index_of(&self, name: impl AsRef<str>) -> Option<usize> {
        self.fields.get_index_of(name.as_ref())
    }

    pub fn fields(&self) -> impl Iterator<Item = &StructField> {
        self.fields.values()
    }

    pub(crate) fn fields_len(&self) -> usize {
        // O(1) for indexmap
        self.fields.len()
    }

    // Checks if the `StructType` contains a field with the specified name.
    pub(crate) fn contains(&self, name: impl AsRef<str>) -> bool {
        self.fields.contains_key(name.as_ref())
    }

    /// Extracts the name and type of all leaf columns, in schema order. Caller should pass Some
    /// `own_name` if this schema is embedded in a larger struct (e.g. `add.*`) and None if the
    /// schema is a top-level result (e.g. `*`).
    ///
    /// NOTE: This method only traverses through `StructType` fields; `MapType` and `ArrayType`
    /// fields are considered leaves even if they contain `StructType` entries/elements.
    #[internal_api]
    pub(crate) fn leaves<'s>(&self, own_name: impl Into<Option<&'s str>>) -> ColumnNamesAndTypes {
        let mut get_leaves = GetSchemaLeaves::new(own_name.into());
        let _ = get_leaves.transform_struct(self);
        (get_leaves.names, get_leaves.types).into()
    }
}

#[derive(Debug, Default)]
pub(crate) struct InvariantChecker {
    has_invariants: bool,
}

impl<'a> SchemaTransform<'a> for InvariantChecker {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if field.has_invariants() {
            self.has_invariants = true;
        } else if !self.has_invariants {
            let _ = self.recurse_into_struct_field(field);
        }
        Some(Cow::Borrowed(field))
    }
}

impl InvariantChecker {
    /// Checks if any column in the schema (including nested columns) has invariants defined.
    ///
    /// This traverses the entire schema to check for the presence of the "delta.invariants"
    /// metadata key.
    pub(crate) fn has_invariants(schema: &Schema) -> bool {
        let mut checker = InvariantChecker::default();
        let _ = checker.transform_struct(schema);
        checker.has_invariants
    }
}

/// Helper for RowVisitor implementations
#[internal_api]
#[derive(Clone, Default)]
pub(crate) struct ColumnNamesAndTypes(Vec<ColumnName>, Vec<DataType>);
impl ColumnNamesAndTypes {
    #[internal_api]
    pub(crate) fn as_ref(&self) -> (&[ColumnName], &[DataType]) {
        (&self.0, &self.1)
    }

    pub(crate) fn extend(&mut self, other: ColumnNamesAndTypes) {
        self.0.extend(other.0);
        self.1.extend(other.1);
    }
}

impl From<(Vec<ColumnName>, Vec<DataType>)> for ColumnNamesAndTypes {
    fn from((names, fields): (Vec<ColumnName>, Vec<DataType>)) -> Self {
        ColumnNamesAndTypes(names, fields)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructTypeSerDeHelper {
    #[serde(rename = "type")]
    type_name: String,
    fields: Vec<StructField>,
}

impl Serialize for StructType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StructTypeSerDeHelper {
            type_name: self.type_name.clone(),
            fields: self.fields.values().cloned().collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
        Self: Sized,
    {
        let helper = StructTypeSerDeHelper::deserialize(deserializer)?;
        Ok(Self {
            type_name: helper.type_name,
            fields: helper
                .fields
                .into_iter()
                .map(|f| (f.name.clone(), f))
                .collect(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    #[serde(rename = "type")]
    pub type_name: String,
    /// The type of element stored in this array
    pub element_type: DataType,
    /// Denoting whether this array can contain one or more null values
    pub contains_null: bool,
}

impl ArrayType {
    pub fn new(element_type: DataType, contains_null: bool) -> Self {
        Self {
            type_name: "array".into(),
            element_type,
            contains_null,
        }
    }

    #[inline]
    pub const fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[inline]
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: String,
    /// The type of element used for the key of this map
    pub key_type: DataType,
    /// The type of element used for the value of this map
    pub value_type: DataType,
    /// Denoting whether this map can contain one or more null values
    #[serde(default = "default_true")]
    pub value_contains_null: bool,
}

impl MapType {
    pub fn new(
        key_type: impl Into<DataType>,
        value_type: impl Into<DataType>,
        value_contains_null: bool,
    ) -> Self {
        Self {
            type_name: "map".into(),
            key_type: key_type.into(),
            value_type: value_type.into(),
            value_contains_null,
        }
    }

    #[inline]
    pub const fn key_type(&self) -> &DataType {
        &self.key_type
    }

    #[inline]
    pub const fn value_type(&self) -> &DataType {
        &self.value_type
    }

    #[inline]
    pub const fn value_contains_null(&self) -> bool {
        self.value_contains_null
    }

    /// Create a schema assuming the map is stored as a struct with the specified key and value field names
    pub fn as_struct_schema(&self, key_name: String, val_name: String) -> Schema {
        StructType::new([
            StructField::not_null(key_name, self.key_type.clone()),
            StructField::new(val_name, self.value_type.clone(), self.value_contains_null),
        ])
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

impl DecimalType {
    /// Check if the given precision and scale are valid for a decimal type.
    pub fn try_new(precision: u8, scale: u8) -> DeltaResult<Self> {
        require!(
            0 < precision && precision <= 38,
            Error::invalid_decimal(format!(
                "precision must be in range 1..38 inclusive, found: {precision}."
            ))
        );
        require!(
            scale <= precision,
            Error::invalid_decimal(format!(
                "scale must be in range 0..{precision} inclusive, found: {scale}."
            ))
        );
        Ok(Self { precision, scale })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    /// UTF-8 encoded string of characters
    String,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
    Long,
    /// i32: 4-byte signed integer. Range: -2147483648 to 2147483647
    Integer,
    /// i16: 2-byte signed integer numbers. Range: -32768 to 32767
    Short,
    /// i8: 1-byte signed integer number. Range: -128 to 127
    Byte,
    /// f32: 4-byte single-precision floating-point numbers
    Float,
    /// f64: 8-byte double-precision floating-point numbers
    Double,
    /// bool: boolean values
    Boolean,
    Binary,
    Date,
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp,
    #[serde(rename = "timestamp_ntz")]
    TimestampNtz,
    #[serde(
        serialize_with = "serialize_decimal",
        deserialize_with = "deserialize_decimal",
        untagged
    )]
    Decimal(DecimalType),
}

impl PrimitiveType {
    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(DecimalType::try_new(precision, scale)?.into())
    }
}

fn serialize_decimal<S: serde::Serializer>(
    dtype: &DecimalType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("decimal({},{})", dtype.precision(), dtype.scale()))
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<DecimalType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let str_value = String::deserialize(deserializer)?;
    require!(
        str_value.starts_with("decimal(") && str_value.ends_with(')'),
        serde::de::Error::custom(format!("Invalid decimal: {str_value}"))
    );

    let mut parts = str_value[8..str_value.len() - 1].split(',');
    let precision = parts
        .next()
        .and_then(|part| part.trim().parse::<u8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid precision in decimal: {str_value}"))
        })?;
    let scale = parts
        .next()
        .and_then(|part| part.trim().parse::<u8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid scale in decimal: {str_value}"))
        })?;
    DecimalType::try_new(precision, scale).map_err(serde::de::Error::custom)
}

fn serialize_variant<S: serde::Serializer>(
    _: &StructType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("variant")
}

fn deserialize_variant<'de, D>(deserializer: D) -> Result<Box<StructType>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let str_value = String::deserialize(deserializer)?;
    require!(
        str_value == "variant",
        serde::de::Error::custom(format!("Invalid variant: {str_value}"))
    );
    match DataType::unshredded_variant() {
        DataType::Variant(st) => Ok(st),
        _ => Err(serde::de::Error::custom(
            "Issue in DataType::unshredded_variant(). Please raise an issue at ".to_string()
                + "delta-io/delta-kernel-rs.",
        )),
    }
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Integer => write!(f, "integer"),
            PrimitiveType::Short => write!(f, "short"),
            PrimitiveType::Byte => write!(f, "byte"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::TimestampNtz => write!(f, "timestamp_ntz"),
            PrimitiveType::Decimal(dtype) => {
                write!(f, "decimal({},{})", dtype.precision(), dtype.scale())
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
pub enum DataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
    /// A struct is used to represent both the top-level schema of the table as well
    /// as struct columns that contain nested columns.
    Struct(Box<StructType>),
    /// A map stores an arbitrary length collection of key-value pairs
    /// with a single keyType and a single valueType
    Map(Box<MapType>),
    /// The Variant data type. The physical representation can be flexible to support shredded
    /// reads. The unshredded schema is `Variant(StructType<metadata: BINARY, value: BINARY>)`.
    #[serde(
        serialize_with = "serialize_variant",
        deserialize_with = "deserialize_variant"
    )]
    Variant(Box<StructType>),
}

impl From<DecimalType> for PrimitiveType {
    fn from(dtype: DecimalType) -> Self {
        PrimitiveType::Decimal(dtype)
    }
}
impl From<DecimalType> for DataType {
    fn from(dtype: DecimalType) -> Self {
        PrimitiveType::from(dtype).into()
    }
}
impl From<PrimitiveType> for DataType {
    fn from(ptype: PrimitiveType) -> Self {
        DataType::Primitive(ptype)
    }
}
impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        DataType::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        DataType::Struct(Box::new(struct_type))
    }
}

impl From<ArrayType> for DataType {
    fn from(array_type: ArrayType) -> Self {
        DataType::Array(Box::new(array_type))
    }
}

impl From<SchemaRef> for DataType {
    fn from(schema: SchemaRef) -> Self {
        Arc::unwrap_or_clone(schema).into()
    }
}

/// cbindgen:ignore
impl DataType {
    pub const STRING: Self = DataType::Primitive(PrimitiveType::String);
    pub const LONG: Self = DataType::Primitive(PrimitiveType::Long);
    pub const INTEGER: Self = DataType::Primitive(PrimitiveType::Integer);
    pub const SHORT: Self = DataType::Primitive(PrimitiveType::Short);
    pub const BYTE: Self = DataType::Primitive(PrimitiveType::Byte);
    pub const FLOAT: Self = DataType::Primitive(PrimitiveType::Float);
    pub const DOUBLE: Self = DataType::Primitive(PrimitiveType::Double);
    pub const BOOLEAN: Self = DataType::Primitive(PrimitiveType::Boolean);
    pub const BINARY: Self = DataType::Primitive(PrimitiveType::Binary);
    pub const DATE: Self = DataType::Primitive(PrimitiveType::Date);
    pub const TIMESTAMP: Self = DataType::Primitive(PrimitiveType::Timestamp);
    pub const TIMESTAMP_NTZ: Self = DataType::Primitive(PrimitiveType::TimestampNtz);

    /// Create a new decimal type with the given precision and scale.
    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(PrimitiveType::decimal(precision, scale)?.into())
    }

    /// Create a new struct type with the given fields.
    pub fn struct_type(fields: impl IntoIterator<Item = StructField>) -> Self {
        StructType::new(fields).into()
    }

    /// Create a new struct type from a fallible iterator of fields.
    pub fn try_struct_type<E>(
        fields: impl IntoIterator<Item = Result<StructField, E>>,
    ) -> Result<Self, E> {
        Ok(StructType::try_new(fields)?.into())
    }

    /// Create a new unshredded [`DataType::Variant`]. This data type is a struct of two not-null
    /// binary fields: `metadata` and `value`.
    pub fn unshredded_variant() -> Self {
        DataType::variant_type([
            StructField::not_null("metadata", DataType::BINARY),
            StructField::not_null("value", DataType::BINARY),
        ])
    }

    /// Create a new [`DataType::Variant`] from the provided fields. For unshredded variants, you
    /// should prefer using [`DataType::unshredded_variant`].
    pub fn variant_type(fields: impl IntoIterator<Item = StructField>) -> Self {
        DataType::Variant(Box::new(StructType::new(fields)))
    }

    /// Attempt to convert this data type to a [`PrimitiveType`]. Returns `None` if this is a
    /// non-primitive type.
    pub fn as_primitive_opt(&self) -> Option<&PrimitiveType> {
        match self {
            DataType::Primitive(ptype) => Some(ptype),
            _ => None,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Primitive(p) => write!(f, "{p}"),
            DataType::Array(a) => write!(f, "array<{}>", a.element_type),
            DataType::Struct(s) => {
                write!(f, "struct<")?;
                for (i, field) in s.fields().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(m) => write!(f, "map<{}, {}>", m.key_type, m.value_type),
            DataType::Variant(_) => write!(f, "variant"),
        }
    }
}

/// Generic framework for describing recursive bottom-up schema transforms. Transformations return
/// `Option<Cow>` with the following semantics:
/// * `Some(Cow::Owned)` -- The schema element was transformed and should propagate to its parent.
/// * `Some(Cow::Borrowed)` -- The schema element was not transformed.
/// * `None` -- The schema element was filtered out and the parent should no longer reference it.
///
/// The transform can start from whatever schema element is available
/// (e.g. [`Self::transform_struct`] to start with [`StructType`]), or it can start from the generic
/// [`Self::transform`].
///
/// The provided `transform_xxx` methods all default to no-op, and implementations should
/// selectively override specific `transform_xxx` methods as needed for the task at hand.
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// child schema elements of each schema element. Implementations can call these as needed but will
/// generally not need to override them.
pub trait SchemaTransform<'a> {
    /// Called for each primitive encountered during the schema traversal.
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Borrowed(ptype))
    }

    /// Called for each struct encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_struct`] if they wish to recursively transform the struct's fields.
    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.recurse_into_struct(stype)
    }

    /// Called for each struct field encountered during the schema traversal. Implementations can
    /// call [`Self::recurse_into_struct_field`] if they wish to recursively transform the field's
    /// data type.
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.recurse_into_struct_field(field)
    }

    /// Called for each array encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_array`] if they wish to recursively transform the array's element type.
    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.recurse_into_array(atype)
    }

    /// Called for each array element encountered during the schema traversal. Implementations can
    /// call [`Self::transform`] if they wish to recursively transform the array element type.
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each map encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_map`] if they wish to recursively transform the map's key and/or value
    /// types.
    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.recurse_into_map(mtype)
    }

    /// Called for each map key encountered during the schema traversal. Implementations can call
    /// [`Self::transform`] if they wish to recursively transform the map key type.
    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each map value encountered during the schema traversal. Implementations can call
    /// [`Self::transform`] if they wish to recursively transform the map value type.
    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each variant values encountered. By default does nothing
    fn transform_variant(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        Some(Cow::Borrowed(etype))
    }

    /// General entry point for a recursive traversal over any data type. Also invoked internally to
    /// dispatch on nested data types encountered during the traversal.
    fn transform(&mut self, data_type: &'a DataType) -> Option<Cow<'a, DataType>> {
        use DataType::*;
        let result = match data_type {
            Primitive(ptype) => self
                .transform_primitive(ptype)?
                .map_owned_or_else(data_type, DataType::from),
            Array(atype) => self
                .transform_array(atype)?
                .map_owned_or_else(data_type, DataType::from),
            Struct(stype) => self
                .transform_struct(stype)?
                .map_owned_or_else(data_type, DataType::from),
            Map(mtype) => self
                .transform_map(mtype)?
                .map_owned_or_else(data_type, DataType::from),
            Variant(_) => self
                .transform_variant(data_type)?
                .map_owned_or_else(data_type, DataType::from),
        };
        Some(result)
    }

    /// Recursively transforms a struct field's data type. If the data type changes, update the
    /// field to reference it. Otherwise, no-op.
    fn recurse_into_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> Option<Cow<'a, StructField>> {
        let result = self.transform(&field.data_type)?;
        let f = |new_data_type| StructField {
            name: field.name.clone(),
            data_type: new_data_type,
            nullable: field.nullable,
            metadata: field.metadata.clone(),
        };
        Some(result.map_owned_or_else(field, f))
    }

    /// Recursively transforms a struct's fields. If one or more fields were changed or removed,
    /// update the struct to reference all surviving fields. Otherwise, no-op.
    fn recurse_into_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        use Cow::*;
        let mut num_borrowed = 0;
        let fields: Vec<_> = stype
            .fields()
            .filter_map(|field| self.transform_struct_field(field))
            .inspect(|field| {
                if let Borrowed(_) = field {
                    num_borrowed += 1;
                }
            })
            .collect();

        if fields.is_empty() {
            None
        } else if num_borrowed < stype.fields.len() {
            // At least one field was changed or filtered out, so make a new struct
            Some(Owned(StructType::new(
                fields.into_iter().map(|f| f.into_owned()),
            )))
        } else {
            Some(Borrowed(stype))
        }
    }

    /// Recursively transforms an array's element type. If the element type changes, update the
    /// array to reference it. Otherwise, no-op.
    fn recurse_into_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        let result = self.transform_array_element(&atype.element_type)?;
        let f = |element_type| ArrayType {
            type_name: atype.type_name.clone(),
            element_type,
            contains_null: atype.contains_null,
        };
        Some(result.map_owned_or_else(atype, f))
    }

    /// Recursively transforms a map's key and value types. If either one changes, update the map to
    /// reference them. If either one is removed, remove the map as well. Otherwise, no-op.
    fn recurse_into_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        let key_type = self.transform_map_key(&mtype.key_type)?;
        let value_type = self.transform_map_value(&mtype.value_type)?;
        let f = |(key_type, value_type)| MapType {
            type_name: mtype.type_name.clone(),
            key_type,
            value_type,
            value_contains_null: mtype.value_contains_null,
        };
        Some((key_type, value_type).map_owned_or_else(mtype, f))
    }
}

struct GetSchemaLeaves {
    path: Vec<String>,
    names: Vec<ColumnName>,
    types: Vec<DataType>,
}
impl GetSchemaLeaves {
    fn new(own_name: Option<&str>) -> Self {
        Self {
            path: own_name.into_iter().map(|s| s.to_string()).collect(),
            names: vec![],
            types: vec![],
        }
    }
}

impl<'a> SchemaTransform<'a> for GetSchemaLeaves {
    fn transform_struct_field(&mut self, field: &StructField) -> Option<Cow<'a, StructField>> {
        self.path.push(field.name.clone());
        if let DataType::Struct(_) = field.data_type {
            let _ = self.recurse_into_struct_field(field);
        } else {
            self.names.push(ColumnName::new(&self.path));
            self.types.push(field.data_type.clone());
        }
        self.path.pop();
        None
    }
}

/// A schema "transform" that doesn't actually change the schema at all. Instead, it measures the
/// maximum depth of a schema, with a depth limit to prevent stack overflow. Useful for verifying
/// that a schema has reasonable depth before attempting to work with it.
pub struct SchemaDepthChecker {
    depth_limit: usize,
    max_depth_seen: usize,
    current_depth: usize,
    call_count: usize,
}
impl SchemaDepthChecker {
    /// Depth-checks the given data type against a given depth limit. The return value is the
    /// largest depth seen, which is capped at one more than the depth limit (indicating the
    /// recursion was terminated).
    pub fn check(data_type: &DataType, depth_limit: usize) -> usize {
        Self::check_with_call_count(data_type, depth_limit).0
    }

    // Exposed for testing
    fn check_with_call_count(data_type: &DataType, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self {
            depth_limit,
            max_depth_seen: 0,
            current_depth: 0,
            call_count: 0,
        };
        checker.transform(data_type);
        (checker.max_depth_seen, checker.call_count)
    }

    // Triggers the requested recursion only doing so would not exceed the depth limit.
    fn depth_limited<'a, T: Clone + std::fmt::Debug>(
        &mut self,
        recurse: impl FnOnce(&mut Self, &'a T) -> Option<Cow<'a, T>>,
        arg: &'a T,
    ) -> Option<Cow<'a, T>> {
        self.call_count += 1;
        if self.max_depth_seen < self.current_depth {
            self.max_depth_seen = self.current_depth;
            if self.depth_limit < self.current_depth {
                tracing::warn!("Max schema depth {} exceeded by {arg:?}", self.depth_limit);
            }
        }
        if self.max_depth_seen <= self.depth_limit {
            self.current_depth += 1;
            let _ = recurse(self, arg);
            self.current_depth -= 1;
        }
        None
    }
}
impl<'a> SchemaTransform<'a> for SchemaDepthChecker {
    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.depth_limited(Self::recurse_into_struct, stype)
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.depth_limited(Self::recurse_into_struct_field, field)
    }
    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.depth_limited(Self::recurse_into_array, atype)
    }
    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.depth_limited(Self::recurse_into_map, mtype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_serde_data_types() {
        let data = r#"
        {
            "name": "a",
            "type": "integer",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::INTEGER));

        let data = r#"
        {
            "name": "c",
            "type": {
                "type": "array",
                "elementType": "integer",
                "containsNull": false
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));

        let data = r#"
        {
            "name": "e",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {}
                        }
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));
        match field.data_type {
            DataType::Array(array) => assert!(matches!(array.element_type, DataType::Struct(_))),
            _ => unreachable!(),
        }

        let data = r#"
        {
            "name": "f",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Map(_)));
    }

    #[test]
    fn test_roundtrip_decimal() {
        let data = r#"
        {
            "name": "a",
            "type": "decimal(10, 2)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert_eq!(field.data_type, DataType::decimal(10, 2).unwrap());

        let json_str = serde_json::to_string(&field).unwrap();
        assert_eq!(
            json_str,
            r#"{"name":"a","type":"decimal(10,2)","nullable":false,"metadata":{}}"#
        );
    }

    #[test]
    fn test_roundtrip_variant() {
        let data = r#"
        {
            "name": "v",
            "type": "variant",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert_eq!(field.data_type, DataType::unshredded_variant());

        let json_str = serde_json::to_string(&field).unwrap();
        assert_eq!(
            json_str,
            r#"{"name":"v","type":"variant","nullable":false,"metadata":{}}"#
        );
    }

    #[test]
    fn test_unshredded_variant() {
        let unshredded_variant_type = DataType::unshredded_variant();

        match &unshredded_variant_type {
            DataType::Variant(struct_type) => {
                let fields: Vec<_> = struct_type.fields().collect();
                assert_eq!(fields.len(), 2);

                assert_eq!(fields[0].name, "metadata");
                assert_eq!(fields[0].data_type, DataType::BINARY);
                assert!(!fields[0].nullable);

                assert_eq!(fields[1].name, "value");
                assert_eq!(fields[1].data_type, DataType::BINARY);
                assert!(!fields[1].nullable);
            }
            _ => panic!("Expected DataType::Variant, got {unshredded_variant_type:?}"),
        }
    }

    #[test]
    fn test_field_metadata() {
        let data = r#"
        {
            "name": "e",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {
                                "delta.columnMapping.id": 5,
                                "delta.columnMapping.physicalName": "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
                            }
                        }
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 4,
                "delta.columnMapping.physicalName": "col-5f422f40-de70-45b2-88ab-1d5c90e94db1",
                "delta.identity.start": 2147483648
            }
        }
        "#;

        let field: StructField = serde_json::from_str(data).unwrap();

        let col_id = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .unwrap();
        let id_start = field
            .get_config_value(&ColumnMetadataKey::IdentityStart)
            .unwrap();
        assert!(matches!(col_id, MetadataValue::Number(num) if *num == 4));
        assert!(matches!(id_start, MetadataValue::Number(num) if *num == 2147483648i64));
        assert_eq!(
            field.physical_name(),
            "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
        );
        let physical_field = field.make_physical();
        assert_eq!(
            physical_field.name,
            "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
        );
        let DataType::Array(atype) = physical_field.data_type else {
            panic!("Expected an Array");
        };
        let DataType::Struct(stype) = atype.element_type else {
            panic!("Expected a Struct");
        };
        assert_eq!(
            stype.fields.get_index(0).unwrap().1.name,
            "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
        );
    }

    #[test]
    fn test_read_schemas() {
        let file = std::fs::File::open("./tests/serde/schema.json").unwrap();
        let schema: Result<Schema, _> = serde_json::from_reader(file);
        assert!(schema.is_ok());

        let file = std::fs::File::open("./tests/serde/checkpoint_schema.json").unwrap();
        let schema: Result<Schema, _> = serde_json::from_reader(file);
        assert!(schema.is_ok())
    }

    #[test]
    fn test_invalid_decimal() {
        let data = r#"
        {
            "name": "a",
            "type": "decimal(39, 10)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        assert!(serde_json::from_str::<StructField>(data).is_err());

        let data = r#"
        {
            "name": "a",
            "type": "decimal(10, 39)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        assert!(serde_json::from_str::<StructField>(data).is_err());
    }

    #[test]
    fn test_depth_checker() {
        let schema = DataType::struct_type([
            StructField::nullable(
                "a",
                ArrayType::new(
                    DataType::struct_type([
                        StructField::nullable("w", DataType::LONG),
                        StructField::nullable("x", ArrayType::new(DataType::LONG, true)),
                        StructField::nullable(
                            "y",
                            MapType::new(DataType::LONG, DataType::STRING, true),
                        ),
                        StructField::nullable(
                            "z",
                            DataType::struct_type([
                                StructField::nullable("n", DataType::LONG),
                                StructField::nullable("m", DataType::STRING),
                            ]),
                        ),
                    ]),
                    true,
                ),
            ),
            StructField::nullable(
                "b",
                DataType::struct_type([
                    StructField::nullable("o", ArrayType::new(DataType::LONG, true)),
                    StructField::nullable(
                        "p",
                        MapType::new(DataType::LONG, DataType::STRING, true),
                    ),
                    StructField::nullable(
                        "q",
                        DataType::struct_type([
                            StructField::nullable(
                                "s",
                                DataType::struct_type([
                                    StructField::nullable("u", DataType::LONG),
                                    StructField::nullable("v", DataType::LONG),
                                ]),
                            ),
                            StructField::nullable("t", DataType::LONG),
                        ]),
                    ),
                    StructField::nullable("r", DataType::LONG),
                ]),
            ),
            StructField::nullable(
                "c",
                MapType::new(
                    DataType::LONG,
                    DataType::struct_type([
                        StructField::nullable("f", DataType::LONG),
                        StructField::nullable("g", DataType::STRING),
                    ]),
                    true,
                ),
            ),
        ]);

        // Similar to SchemaDepthChecker::check, but also returns call count
        let check_with_call_count =
            |depth_limit| SchemaDepthChecker::check_with_call_count(&schema, depth_limit);

        // Hit depth limit at "a" but still have to look at "b" "c" "d"
        assert_eq!(check_with_call_count(1), (2, 5));
        assert_eq!(check_with_call_count(2), (3, 6));

        // Hit depth limit at "w" but still have to look at "x" "y" "z"
        assert_eq!(check_with_call_count(3), (4, 10));
        assert_eq!(check_with_call_count(4), (5, 11));

        // Depth limit hit at "n" but still have to look at "m"
        assert_eq!(check_with_call_count(5), (6, 15));

        // Depth limit not hit until "u"
        assert_eq!(check_with_call_count(6), (7, 28));

        // Depth limit not hit (full traversal required)
        assert_eq!(check_with_call_count(7), (7, 32));
        assert_eq!(check_with_call_count(8), (7, 32));
    }

    #[test]
    fn test_metadata_value_to_string() {
        assert_eq!(MetadataValue::Number(0).to_string(), "0");
        assert_eq!(
            MetadataValue::String("hello".to_string()).to_string(),
            "hello"
        );
        assert_eq!(MetadataValue::Boolean(true).to_string(), "true");
        assert_eq!(MetadataValue::Boolean(false).to_string(), "false");
        let object_json = serde_json::json!({ "an": "object" });
        assert_eq!(
            MetadataValue::Other(object_json).to_string(),
            "{\"an\":\"object\"}"
        );
        let array_json = serde_json::json!(["an", "array"]);
        assert_eq!(
            MetadataValue::Other(array_json).to_string(),
            "[\"an\",\"array\"]"
        );
    }

    #[test]
    fn test_fields_len() {
        let schema = StructType::new([]);
        assert!(schema.fields_len() == 0);
        let schema = StructType::new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
            StructField::nullable("d", DataType::LONG),
        ]);
        assert_eq!(schema.fields_len(), 4);
        let schema = StructType::new([
            StructField::nullable("b", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
        ]);
        assert_eq!(schema.fields_len(), 2);
    }

    #[test]
    fn test_has_invariants() {
        // Schema with no invariants
        let schema = StructType::new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
        ]);
        assert!(!InvariantChecker::has_invariants(&schema));

        // Schema with top-level invariant
        let mut field = StructField::nullable("c", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::Invariants.as_ref().to_string(),
            MetadataValue::String("c > 0".to_string()),
        );

        let schema = StructType::new([StructField::nullable("a", DataType::STRING), field]);
        assert!(InvariantChecker::has_invariants(&schema));

        // Schema with nested invariant in a struct
        let nested_field = StructField::nullable(
            "nested_c",
            DataType::struct_type([{
                let mut field = StructField::nullable("d", DataType::INTEGER);
                field.metadata.insert(
                    ColumnMetadataKey::Invariants.as_ref().to_string(),
                    MetadataValue::String("d > 0".to_string()),
                );
                field
            }]),
        );

        let schema = StructType::new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            nested_field,
        ]);
        assert!(InvariantChecker::has_invariants(&schema));

        // Schema with nested invariant in an array of structs
        let array_field = StructField::nullable(
            "array_field",
            ArrayType::new(
                DataType::struct_type([{
                    let mut field = StructField::nullable("d", DataType::INTEGER);
                    field.metadata.insert(
                        ColumnMetadataKey::Invariants.as_ref().to_string(),
                        MetadataValue::String("d > 0".to_string()),
                    );
                    field
                }]),
                true,
            ),
        );

        let schema = StructType::new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            array_field,
        ]);
        assert!(InvariantChecker::has_invariants(&schema));

        // Schema with nested invariant in a map value that's a struct
        let map_field = StructField::nullable(
            "map_field",
            MapType::new(
                DataType::STRING,
                DataType::struct_type([{
                    let mut field = StructField::nullable("d", DataType::INTEGER);
                    field.metadata.insert(
                        ColumnMetadataKey::Invariants.as_ref().to_string(),
                        MetadataValue::String("d > 0".to_string()),
                    );
                    field
                }]),
                true,
            ),
        );

        let schema = StructType::new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            map_field,
        ]);
        assert!(InvariantChecker::has_invariants(&schema));
    }
}
