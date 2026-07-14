use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::{ArrayType, DataType, DecimalType, MapType, PrimitiveType, StructField};
use crate::utils::require;
use crate::{DeltaResult, Error};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecimalData {
    bits: i128,
    ty: DecimalType,
}

impl DecimalData {
    pub fn try_new(bits: impl Into<i128>, ty: DecimalType) -> DeltaResult<Self> {
        let bits = bits.into();
        require!(
            ty.precision() >= get_decimal_precision(bits),
            Error::invalid_decimal(format!(
                "Decimal value {} exceeds precision {}",
                bits,
                ty.precision()
            ))
        );
        Ok(Self { bits, ty })
    }

    pub fn bits(&self) -> i128 {
        self.bits
    }

    pub fn ty(&self) -> &DecimalType {
        &self.ty
    }

    pub fn precision(&self) -> u8 {
        self.ty.precision()
    }

    pub fn scale(&self) -> u8 {
        self.ty.scale()
    }
}

/// Computes the decimal precision of a 128-bit number. The largest possible magnitude is i128::MIN
/// = -2**127 with 39 decimal digits.
fn get_decimal_precision(value: i128) -> u8 {
    // Not sure why checked_ilog10 returns u32 when log10(2**127) = 38 fits easily in u8??
    value.unsigned_abs().checked_ilog10().map_or(0, |p| p + 1) as _
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArrayData {
    tpe: ArrayType,
    /// This exists currently for literal list comparisons, but should not be depended on see below
    elements: Vec<Scalar>,
}

impl ArrayData {
    pub fn try_new(
        tpe: ArrayType,
        elements: impl IntoIterator<Item = impl Into<Scalar>>,
    ) -> DeltaResult<Self> {
        let elements = elements
            .into_iter()
            .map(|v| {
                let v = v.into();
                // disallow nulls if the type is not allowed to contain nulls
                if !tpe.contains_null() && v.is_null() {
                    Err(Error::schema(
                        "Array element cannot be null for non-nullable array",
                    ))
                // check element types match
                } else if *tpe.element_type() != v.data_type() {
                    Err(Error::Schema(format!(
                        "Array scalar type mismatch: expected {}, got {}",
                        tpe.element_type(),
                        v.data_type()
                    )))
                } else {
                    Ok(v)
                }
            })
            .try_collect()?;
        Ok(Self { tpe, elements })
    }

    pub fn array_type(&self) -> &ArrayType {
        &self.tpe
    }

    pub fn array_elements(&self) -> &[Scalar] {
        &self.elements
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapData {
    data_type: MapType,
    pairs: Vec<(Scalar, Scalar)>,
}

impl MapData {
    pub fn try_new(
        data_type: MapType,
        values: impl IntoIterator<Item = (impl Into<Scalar>, impl Into<Scalar>)>,
    ) -> DeltaResult<Self> {
        let key_type = data_type.key_type();
        let val_type = data_type.value_type();
        let pairs = values
            .into_iter()
            .map(|(key, val)| {
                let (k, v) = (key.into(), val.into());
                // check key types match
                if k.data_type() != *key_type {
                    Err(Error::Schema(format!(
                        "Map scalar type mismatch: expected key type {}, got key type {}",
                        key_type,
                        k.data_type()
                    )))
                // keys can't be null
                } else if k.is_null() {
                    Err(Error::schema("Map key cannot be null"))
                // check val types match
                } else if v.data_type() != *val_type {
                    Err(Error::Schema(format!(
                        "Map scalar type mismatch: expected value type {}, got value type {}",
                        val_type,
                        v.data_type()
                    )))
                // vals can only be null if value_contains_null is true
                } else if v.is_null() && !data_type.value_contains_null {
                    Err(Error::schema(
                        "Null map value disallowed if map value_contains_null is false",
                    ))
                } else {
                    Ok((k, v))
                }
            })
            .try_collect()?;
        Ok(Self { data_type, pairs })
    }

    // TODO: array.elements is deprecated? do we want to expose this? How will FFI get pairs for
    // visiting?
    pub fn pairs(&self) -> &[(Scalar, Scalar)] {
        &self.pairs
    }

    pub fn map_type(&self) -> &MapType {
        &self.data_type
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructData {
    fields: Vec<StructField>,
    values: Vec<Scalar>,
}

impl StructData {
    /// Try to create a new struct data with the given fields and values.
    ///
    /// This will return an error:
    /// - if the number of fields and values do not match
    /// - if the data types of the values do not match the data types of the fields
    /// - if a null value is assigned to a non-nullable field
    pub fn try_new(fields: Vec<StructField>, values: Vec<Scalar>) -> DeltaResult<Self> {
        require!(
            fields.len() == values.len(),
            Error::invalid_struct_data(format!(
                "Incorrect number of values for Struct fields, expected {} got {}",
                fields.len(),
                values.len()
            ))
        );

        for (f, a) in fields.iter().zip(&values) {
            require!(
                f.data_type() == &a.data_type(),
                Error::invalid_struct_data(format!(
                    "Incorrect datatype for Struct field {:?}, expected {} got {}",
                    f.name(),
                    f.data_type(),
                    a.data_type()
                ))
            );

            require!(
                f.is_nullable() || !a.is_null(),
                Error::invalid_struct_data(format!(
                    "Value for non-nullable field {:?} cannot be null, got {}",
                    f.name(),
                    a
                ))
            );
        }

        Ok(Self { fields, values })
    }

    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }

    pub fn values(&self) -> &[Scalar] {
        &self.values
    }
}

/// A single value, which can be null. Used for representing literal values
/// in [Expressions][crate::expressions::Expression].
///
/// NOTE: `PartialEq` uses physical (structural) comparison semantics.
/// For SQL NULL semantics, use [`Scalar::logical_eq`] or [`Scalar::logical_partial_cmp`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Scalar {
    /// 32bit integer
    Integer(i32),
    /// 64bit integer
    Long(i64),
    /// 16bit integer
    Short(i16),
    /// 8bit integer
    Byte(i8),
    /// 32bit floating point
    Float(f32),
    /// 64bit floating point
    Double(f64),
    /// utf-8 encoded string.
    String(String),
    /// true or false value
    Boolean(bool),
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp(i64),
    /// Microsecond precision timestamp, with no timezone.
    TimestampNtz(i64),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date(i32),
    /// Binary data
    Binary(Vec<u8>),
    /// Decimal value with a given precision and scale.
    Decimal(DecimalData),
    /// Null value with a given data type.
    Null(DataType),
    /// Struct value
    Struct(StructData),
    /// Array Value
    Array(ArrayData),
    /// Map Value
    Map(MapData),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::INTEGER,
            Self::Long(_) => DataType::LONG,
            Self::Short(_) => DataType::SHORT,
            Self::Byte(_) => DataType::BYTE,
            Self::Float(_) => DataType::FLOAT,
            Self::Double(_) => DataType::DOUBLE,
            Self::String(_) => DataType::STRING,
            Self::Boolean(_) => DataType::BOOLEAN,
            Self::Timestamp(_) => DataType::TIMESTAMP,
            Self::TimestampNtz(_) => DataType::TIMESTAMP_NTZ,
            Self::Date(_) => DataType::DATE,
            Self::Binary(_) => DataType::BINARY,
            Self::Decimal(d) => DataType::from(*d.ty()),
            Self::Null(data_type) => data_type.clone(),
            Self::Struct(data) => DataType::struct_type_unchecked(data.fields.clone()),
            Self::Array(data) => data.tpe.clone().into(),
            Self::Map(data) => data.data_type.clone().into(),
        }
    }

    /// Returns true if this scalar is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    /// Constructs a null `Scalar` of the given type. Accepts anything convertible into a
    /// [`DataType`], so container types like [`StructType`], [`ArrayType`], and [`MapType`]
    /// can be passed directly without an explicit `DataType::from(...)` wrapper.
    ///
    /// [`StructType`]: crate::schema::StructType
    /// [`ArrayType`]: crate::schema::ArrayType
    /// [`MapType`]: crate::schema::MapType
    pub fn null(data_type: impl Into<DataType>) -> Self {
        Self::Null(data_type.into())
    }

    /// Constructs a Decimal value from raw parts
    pub fn decimal(bits: impl Into<i128>, precision: u8, scale: u8) -> DeltaResult<Self> {
        let dtype = DecimalType::try_new(precision, scale)?;
        let dval = DecimalData::try_new(bits, dtype)?;
        Ok(Self::Decimal(dval))
    }

    /// Constructs a Scalar timestamp (in UTC) from an `i64` millisecond since unix epoch
    pub(crate) fn timestamp_from_millis(millis: i64) -> DeltaResult<Self> {
        let Some(timestamp) = DateTime::from_timestamp_millis(millis) else {
            return Err(Error::generic(format!(
                "Failed to create millisecond timestamp from {millis}"
            )));
        };
        Ok(Self::Timestamp(timestamp.timestamp_micros()))
    }

    /// Attempts to add two scalars, returning None if they were incompatible.
    pub fn try_add(&self, other: &Scalar) -> Option<Scalar> {
        use Scalar::*;
        let result = match (self, other) {
            (Integer(a), Integer(b)) => Integer(a.checked_add(*b)?),
            (Long(a), Long(b)) => Long(a.checked_add(*b)?),
            (Short(a), Short(b)) => Short(a.checked_add(*b)?),
            (Byte(a), Byte(b)) => Byte(a.checked_add(*b)?),
            _ => return None,
        };
        Some(result)
    }

    /// Attempts to subtract two scalars, returning None if they were incompatible.
    pub fn try_sub(&self, other: &Scalar) -> Option<Scalar> {
        use Scalar::*;
        let result = match (self, other) {
            (Integer(a), Integer(b)) => Integer(a.checked_sub(*b)?),
            (Long(a), Long(b)) => Long(a.checked_sub(*b)?),
            (Short(a), Short(b)) => Short(a.checked_sub(*b)?),
            (Byte(a), Byte(b)) => Byte(a.checked_sub(*b)?),
            _ => return None,
        };
        Some(result)
    }

    /// Attempts to multiply two scalars, returning None if they were incompatible.
    pub fn try_mul(&self, other: &Scalar) -> Option<Scalar> {
        use Scalar::*;
        let result = match (self, other) {
            (Integer(a), Integer(b)) => Integer(a.checked_mul(*b)?),
            (Long(a), Long(b)) => Long(a.checked_mul(*b)?),
            (Short(a), Short(b)) => Short(a.checked_mul(*b)?),
            (Byte(a), Byte(b)) => Byte(a.checked_mul(*b)?),
            _ => return None,
        };
        Some(result)
    }

    /// Attempts to divide two scalars, returning None if they were incompatible.
    pub fn try_div(&self, other: &Scalar) -> Option<Scalar> {
        use Scalar::*;
        let result = match (self, other) {
            (Integer(a), Integer(b)) => Integer(a.checked_div(*b)?),
            (Long(a), Long(b)) => Long(a.checked_div(*b)?),
            (Short(a), Short(b)) => Short(a.checked_div(*b)?),
            (Byte(a), Byte(b)) => Byte(a.checked_div(*b)?),
            _ => return None,
        };
        Some(result)
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{i}"),
            Self::Long(i) => write!(f, "{i}"),
            Self::Short(i) => write!(f, "{i}"),
            Self::Byte(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl}"),
            Self::Double(fl) => write!(f, "{fl}"),
            Self::String(s) => write!(f, "'{s}'"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Timestamp(ts) => write!(f, "{ts}"),
            Self::TimestampNtz(ts) => write!(f, "{ts}"),
            Self::Date(d) => write!(f, "{d}"),
            Self::Binary(b) => write!(f, "{b:?}"),
            Self::Decimal(d) => match d.scale().cmp(&0) {
                Ordering::Equal => {
                    write!(f, "{}", d.bits())
                }
                Ordering::Greater => {
                    let scale = d.scale();
                    let scalar_multiple = 10_i128.pow(scale as u32);
                    let value = d.bits();
                    write!(f, "{}", value / scalar_multiple)?;
                    write!(f, ".")?;
                    write!(
                        f,
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = scale as usize
                    )
                }
                Ordering::Less => {
                    write!(f, "{}", d.bits())?;
                    for _ in 0..d.scale() {
                        write!(f, "0")?;
                    }
                    Ok(())
                }
            },
            Self::Null(_) => write!(f, "null"),
            Self::Struct(data) => {
                write!(f, "{{")?;
                let mut delim = "";
                for (value, field) in data.values.iter().zip(data.fields.iter()) {
                    write!(f, "{delim}{}: {value}", field.name)?;
                    delim = ", ";
                }
                write!(f, "}}")
            }
            Self::Array(data) => {
                write!(f, "(")?;
                let mut delim = "";
                for element in &data.elements {
                    write!(f, "{delim}{element}")?;
                    delim = ", ";
                }
                write!(f, ")")
            }
            Self::Map(data) => {
                write!(f, "{{")?;
                let mut delim = "";
                for (key, val) in &data.pairs {
                    write!(f, "{delim}{key}: {val}")?;
                    delim = ", ";
                }
                write!(f, "}}")
            }
        }
    }
}

impl Scalar {
    /// Logical (SQL semantics) equality comparison of two scalars.
    ///
    /// Returns `None` if the scalars cannot be compared (different types, NULL values, or
    /// unsupported types like Struct/Array/Map).
    ///
    /// Logical (SQL semantics) equality comparison of two scalars.
    ///
    /// Returns `true` if the scalars are logically equal, `false` otherwise.
    ///
    /// NOTE: This implements SQL NULL semantics where NULL is incomparable to everything,
    /// including itself, so `NULL != NULL` (returns `false`).
    pub fn logical_eq(&self, other: &Self) -> bool {
        self.logical_partial_cmp(other) == Some(Ordering::Equal)
    }

    /// Physical (structural) equality comparison of two scalars.
    ///
    /// Returns `true` if the scalars are structurally identical, `false` otherwise.
    ///
    /// Unlike logical comparison, this treats `Null(dt1) == Null(dt2)` as `true` when `dt1 == dt2`.
    /// This is used for query plan comparison, not SQL evaluation.
    pub fn physical_eq(&self, other: &Self) -> bool {
        self == other
    }

    /// Logical (SQL semantics) comparison of two scalars.
    ///
    /// Returns `None` if the scalars are incomparable (different types, NULL values, or
    /// unsupported types like Struct/Array/Map).
    ///
    /// NOTE: This implements SQL NULL semantics where NULL is incomparable to everything,
    /// including itself.
    pub fn logical_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Scalar::*;
        match (self, other) {
            // NOTE: We intentionally do two match arms for each variant to avoid a catch-all, so
            // that new variants trigger compilation failures instead of being silently ignored.
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Integer(_), _) => None,
            (Long(a), Long(b)) => a.partial_cmp(b),
            (Long(_), _) => None,
            (Short(a), Short(b)) => a.partial_cmp(b),
            (Short(_), _) => None,
            (Byte(a), Byte(b)) => a.partial_cmp(b),
            (Byte(_), _) => None,
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Float(_), _) => None,
            (Double(a), Double(b)) => a.partial_cmp(b),
            (Double(_), _) => None,
            (String(a), String(b)) => a.partial_cmp(b),
            (String(_), _) => None,
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Boolean(_), _) => None,
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (Timestamp(_), _) => None,
            (TimestampNtz(a), TimestampNtz(b)) => a.partial_cmp(b),
            (TimestampNtz(_), _) => None,
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Date(_), _) => None,
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            (Binary(_), _) => None,
            (Decimal(d1), Decimal(d2)) => (d1.ty() == d2.ty())
                .then(|| d1.bits().partial_cmp(&d2.bits()))
                .flatten(),
            (Decimal(_), _) => None,
            // NOTE: NULL values are incomparable by definition (SQL NULL semantics)
            (Null(_), _) => None,
            (Struct(_), _) => None, // TODO: Support Struct?
            (Array(_), _) => None,  // TODO: Support Array?
            (Map(_), _) => None,    // TODO: Support Map?
        }
    }
}

impl From<i8> for Scalar {
    fn from(i: i8) -> Self {
        Self::Byte(i)
    }
}

impl From<i16> for Scalar {
    fn from(i: i16) -> Self {
        Self::Short(i)
    }
}

impl From<i32> for Scalar {
    fn from(i: i32) -> Self {
        Self::Integer(i)
    }
}

impl From<i64> for Scalar {
    fn from(i: i64) -> Self {
        Self::Long(i)
    }
}

impl From<f32> for Scalar {
    fn from(i: f32) -> Self {
        Self::Float(i)
    }
}

impl From<f64> for Scalar {
    fn from(i: f64) -> Self {
        Self::Double(i)
    }
}

impl From<bool> for Scalar {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<DecimalData> for Scalar {
    fn from(d: DecimalData) -> Self {
        Self::Decimal(d)
    }
}

impl From<&str> for Scalar {
    fn from(s: &str) -> Self {
        Self::String(s.into())
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<T: Into<Scalar> + Copy> From<&T> for Scalar {
    fn from(t: &T) -> Self {
        (*t).into()
    }
}

impl From<&[u8]> for Scalar {
    fn from(b: &[u8]) -> Self {
        Self::Binary(b.into())
    }
}

impl From<bytes::Bytes> for Scalar {
    fn from(b: bytes::Bytes) -> Self {
        Self::Binary(b.into())
    }
}

impl<T> TryFrom<Vec<T>> for Scalar
where
    T: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(vec: Vec<T>) -> Result<Self, Self::Error> {
        let array_type = ArrayType::new(T::to_data_type(), false);
        let array_data = ArrayData::try_new(array_type, vec)?;
        Ok(array_data.into())
    }
}

impl<T> TryFrom<Vec<Option<T>>> for Scalar
where
    T: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(vec: Vec<Option<T>>) -> Result<Self, Self::Error> {
        let array_type = ArrayType::new(T::to_data_type(), true);
        let array_data = ArrayData::try_new(array_type, vec)?;
        Ok(array_data.into())
    }
}

impl<T> TryFrom<Option<Vec<T>>> for Scalar
where
    T: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(opt: Option<Vec<T>>) -> Result<Self, Self::Error> {
        match opt {
            Some(vec) => vec.try_into(),
            None => Ok(Self::Null(ArrayType::new(T::to_data_type(), false).into())),
        }
    }
}

impl<K, V> TryFrom<HashMap<K, V>> for Scalar
where
    K: Into<Scalar> + ToDataType,
    V: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(map: HashMap<K, V>) -> Result<Self, Self::Error> {
        let map_type = MapType::new(K::to_data_type(), V::to_data_type(), false);
        let map_data = MapData::try_new(map_type, map)?;
        Ok(map_data.into())
    }
}

impl<K, V> TryFrom<HashMap<K, Option<V>>> for Scalar
where
    K: Into<Scalar> + ToDataType,
    V: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(map: HashMap<K, Option<V>>) -> Result<Self, Self::Error> {
        let map_type = MapType::new(K::to_data_type(), V::to_data_type(), true);
        let map_data = MapData::try_new(map_type, map)?;
        Ok(map_data.into())
    }
}

impl<K, V> TryFrom<Option<HashMap<K, V>>> for Scalar
where
    K: Into<Scalar> + ToDataType,
    V: Into<Scalar> + ToDataType,
{
    type Error = Error;

    fn try_from(opt: Option<HashMap<K, V>>) -> Result<Self, Self::Error> {
        match opt {
            Some(map) => map.try_into(),
            None => Ok(Self::Null(
                MapType::new(K::to_data_type(), V::to_data_type(), false).into(),
            )),
        }
    }
}

// NOTE: We "cheat" and use the macro support trait `ToDataType`
impl<T: Into<Scalar> + ToDataType> From<Option<T>> for Scalar {
    fn from(t: Option<T>) -> Self {
        match t {
            Some(t) => t.into(),
            None => Self::Null(T::to_data_type()),
        }
    }
}

impl From<ArrayData> for Scalar {
    fn from(array_data: ArrayData) -> Self {
        Self::Array(array_data)
    }
}

impl From<MapData> for Scalar {
    fn from(map_data: MapData) -> Self {
        Self::Map(map_data)
    }
}

// TODO: add more From impls

impl PrimitiveType {
    fn data_type(&self) -> DataType {
        DataType::Primitive(self.clone())
    }

    /// Parses a serialized string into a [`Scalar`] of this primitive type, per the Delta
    /// protocol's [partition value serialization] rules. An empty string parses as
    /// [`Scalar::Null`].
    ///
    /// Note: the scan read path does not use this empty-string handling for partition values; it
    /// applies a type-dependent cast instead (empty string on a string or binary column surfaces as
    /// an empty value rather than null).
    ///
    /// Timestamp and TimestampNtz accept the space-separated form `{year}-{month}-{day}
    /// {hour}:{minute}:{second}[.{fraction}]`. Timestamp (only) also accepts ISO 8601 / RFC 3339
    /// strings such as `1970-01-01T00:00:00.123456Z`; an explicit offset is honored and the value
    /// is normalized to UTC. The spec stores timestamp partition values either without a zone
    /// (interpreted in the writer's local time zone) or adjusted to UTC with a `Z` suffix.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ParseError`] if `raw` is not a valid encoding of this type.
    ///
    /// [partition value serialization]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    pub fn parse_scalar(&self, raw: &str) -> Result<Scalar, Error> {
        use PrimitiveType::*;

        if raw.is_empty() {
            return Ok(Scalar::Null(self.data_type()));
        }

        match self {
            String => Ok(Scalar::String(raw.to_string())),
            Binary => Ok(Scalar::Binary(raw.to_string().into_bytes())),
            Byte => self.parse_str_as_scalar(raw, Scalar::Byte),
            Decimal(dtype) => Self::parse_decimal(raw, *dtype),
            Short => self.parse_str_as_scalar(raw, Scalar::Short),
            Integer => self.parse_str_as_scalar(raw, Scalar::Integer),
            Long => self.parse_str_as_scalar(raw, Scalar::Long),
            Float => self.parse_str_as_scalar(raw, Scalar::Float),
            Double => self.parse_str_as_scalar(raw, Scalar::Double),
            Void => Err(self.parse_error(raw)),
            Boolean => {
                if raw.eq_ignore_ascii_case("true") {
                    Ok(Scalar::Boolean(true))
                } else if raw.eq_ignore_ascii_case("false") {
                    Ok(Scalar::Boolean(false))
                } else {
                    Err(self.parse_error(raw))
                }
            }
            Date => {
                let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                    .map_err(|_| self.parse_error(raw))?
                    .and_hms_opt(0, 0, 0)
                    .ok_or(self.parse_error(raw))?;
                let date = Utc.from_utc_datetime(&date);
                let days = date.signed_duration_since(DateTime::UNIX_EPOCH).num_days() as i32;
                Ok(Scalar::Date(days))
            }
            // NOTE: Timestamp and TimestampNtz are both parsed into microseconds since unix
            // epoch. The difference arises mostly in how they are to be handled on the engine
            // side - i.e. timestampNTZ is not adjusted to UTC, this is just so we can
            // (de-)serialize it as a date string.
            TimestampNtz | Timestamp => {
                let mut timestamp = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f");

                if timestamp.is_err() && *self == Timestamp {
                    // `%+` is chrono's relaxed ISO 8601 / RFC 3339 parser: unlike the stricter
                    // DateTime::parse_from_rfc3339, it also accepts a space or lowercase `t`
                    // separator and colon-less offsets (e.g. `+0530`).
                    timestamp = DateTime::parse_from_str(raw, "%+").map(|dt| dt.naive_utc());
                }
                let timestamp = timestamp.map_err(|_| self.parse_error(raw))?;
                let timestamp = Utc.from_utc_datetime(&timestamp);
                let micros = timestamp
                    .signed_duration_since(DateTime::UNIX_EPOCH)
                    .num_microseconds()
                    .ok_or(self.parse_error(raw))?;
                match self {
                    Timestamp => Ok(Scalar::Timestamp(micros)),
                    TimestampNtz => Ok(Scalar::TimestampNtz(micros)),
                    _ => unreachable!(),
                }
            }
            IntervalYearMonth | IntervalDayTime => Err(Error::unsupported(
                "Interval types are not supported as scalar or partition values",
            )),
            // Kernel does not support parsing text into Geometry/Geography types yet.
            Geometry(_) | Geography(_) => Err(Error::Unsupported(format!(
                "parse_scalar is not supported for {self:?}"
            ))),
        }
    }

    /// Casts an empty partition-value string to its target [`Scalar`], or `None` for a null value.
    ///
    /// An empty string is a value for a string or binary column (the empty string / empty bytes)
    /// but has no representation for any other type, so it casts to null there. This aligns with
    /// Spark, which reads the partition-value map as a non-ANSI SQL cast, and is the empty-string
    /// rule the scan read path applies, in place of [`parse_scalar`]'s null-for-every-type
    /// handling.
    ///
    /// A literal empty string only reaches this path from a foreign writer, since kernel
    /// serializes its own empty and null partition values to JSON null on write.
    ///
    /// [`parse_scalar`]: PrimitiveType::parse_scalar
    pub(crate) fn empty_string_partition_cast(&self) -> Option<Scalar> {
        match self {
            PrimitiveType::String => Some(Scalar::String(String::new())),
            PrimitiveType::Binary => Some(Scalar::Binary(Vec::new())),
            _ => None,
        }
    }

    fn parse_error(&self, raw: &str) -> Error {
        Error::ParseError(raw.to_string(), self.data_type())
    }

    /// Parse a string as a scalar value, returning an error if the string is not parseable.
    ///
    /// The `f` function is used to convert the parsed value into a `Scalar`.
    /// The desired type that `FromStr::parse` should parse into is inferred from the parameter type
    /// of `f`.
    fn parse_str_as_scalar<T: std::str::FromStr>(
        &self,
        raw: &str,
        f: impl FnOnce(T) -> Scalar,
    ) -> Result<Scalar, Error> {
        match raw.parse() {
            Ok(val) => Ok(f(val)),
            Err(..) => Err(self.parse_error(raw)),
        }
    }

    fn parse_decimal(raw: &str, dtype: DecimalType) -> Result<Scalar, Error> {
        let (base, exp): (&str, i128) = match raw.find(['e', 'E']) {
            None => (raw, 0), // no 'e' or 'E', so there's no exponent
            Some(pos) => {
                let (base, exp) = raw.split_at(pos);
                // exp now has '[e/E][exponent]', strip the 'e/E' and parse it
                (base, exp[1..].parse()?)
            }
        };
        let parse_error = || PrimitiveType::from(dtype).parse_error(raw);
        require!(!base.is_empty(), parse_error());

        // now split on any '.' and parse
        let (int_part, frac_part, frac_digits) = match base.find('.') {
            None => {
                // no, '.', just base base as int_part
                (base, None, 0)
            }
            Some(pos) if pos == base.len() - 1 => {
                // '.' is at the end, just strip it
                (&base[..pos], None, 0)
            }
            Some(pos) => {
                let (int_part, frac_part) = (&base[..pos], &base[pos + 1..]);
                (int_part, Some(frac_part), frac_part.len() as i128)
            }
        };

        // we can assume this won't underflow since `frac_digits` is at minimum 0, and exp is at
        // most i128::MAX, and 0-i128::MAX doesn't underflow
        let scale = frac_digits - exp;
        let scale: u8 = scale.try_into().map_err(|_| parse_error())?;
        require!(scale == dtype.scale(), parse_error());
        let int: i128 = match frac_part {
            None => int_part.parse()?,
            Some(frac_part) => format!("{int_part}{frac_part}").parse()?,
        };
        Ok(Scalar::Decimal(DecimalData::try_new(int, dtype)?))
    }
}

#[cfg(test)]
mod tests {
    use std::f32::consts::PI;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{column_expr, BinaryPredicateOp};
    use crate::utils::test_utils::assert_result_error_with_message;
    use crate::{Expression as Expr, Predicate as Pred};

    #[test]
    fn test_void_parse_scalar() {
        // Empty string should produce Null (like all primitive types)
        let scalar = PrimitiveType::Void.parse_scalar("").unwrap();
        assert_eq!(scalar, Scalar::Null(DataType::VOID));

        // Non-empty string should fail
        PrimitiveType::Void.parse_scalar("anything").unwrap_err();
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Geometry(Box::new(
        crate::schema::GeometryType::try_new("EPSG:4326").unwrap()
    )))]
    #[case(PrimitiveType::Geography(Box::new(
        crate::schema::GeographyType::try_new(
            "EPSG:4326",
            crate::schema::EdgeInterpolationAlgorithm::Spherical,
        )
        .unwrap()
    )))]
    fn test_geo_parse_scalar_unsupported(#[case] ptype: PrimitiveType) {
        let err = ptype.parse_scalar("anything").unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "expected Unsupported, got: {err:?}"
        );
    }

    #[test]
    fn test_bad_decimal() {
        let dtype = DecimalType::try_new(3, 0).unwrap();
        DecimalData::try_new(123456789, dtype).expect_err("should have failed");
        PrimitiveType::parse_decimal("0.12345", dtype).expect_err("should have failed");
        PrimitiveType::parse_decimal("12345", dtype).expect_err("should have failed");
    }
    #[test]
    fn test_decimal_display() {
        let s = Scalar::decimal(123456789, 9, 2).unwrap();
        assert_eq!(s.to_string(), "1234567.89");

        let s = Scalar::decimal(123456789, 9, 0).unwrap();
        assert_eq!(s.to_string(), "123456789");

        let s = Scalar::decimal(123456789, 9, 9).unwrap();
        assert_eq!(s.to_string(), "0.123456789");
    }

    fn assert_decimal(
        raw: &str,
        expect_int: i128,
        expect_prec: u8,
        expect_scale: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let s = PrimitiveType::decimal(expect_prec, expect_scale)?;
        match s.parse_scalar(raw)? {
            Scalar::Decimal(val) => {
                assert_eq!(val.bits(), expect_int);
                assert_eq!(val.precision(), expect_prec);
                assert_eq!(val.scale(), expect_scale);
            }
            _ => panic!("Didn't parse as decimal"),
        };
        Ok(())
    }

    #[test]
    fn test_decimal_precision() {
        // Test around 0, 1, 2, and 4 digit boundaries
        assert_eq!(get_decimal_precision(0), 0);
        assert_eq!(get_decimal_precision(1), 1);
        assert_eq!(get_decimal_precision(9), 1);
        assert_eq!(get_decimal_precision(10), 2);
        assert_eq!(get_decimal_precision(99), 2);
        assert_eq!(get_decimal_precision(100), 3);
        assert_eq!(get_decimal_precision(999), 3);
        assert_eq!(get_decimal_precision(1000), 4);
        assert_eq!(get_decimal_precision(9999), 4);
        assert_eq!(get_decimal_precision(10000), 5);

        // Test around the 8 digit boundary
        assert_eq!(get_decimal_precision(999_9999), 7);
        assert_eq!(get_decimal_precision(1000_0000), 8);
        assert_eq!(get_decimal_precision(9999_9999), 8);
        assert_eq!(get_decimal_precision(1_0000_0000), 9);

        // Test around the 16 digit boundary
        assert_eq!(get_decimal_precision(999_9999_9999_9999), 15);
        assert_eq!(get_decimal_precision(1000_0000_0000_0000), 16);
        assert_eq!(get_decimal_precision(9999_9999_9999_9999), 16);
        assert_eq!(get_decimal_precision(1_0000_0000_0000_0000), 17);

        // Test around the 32 digit boundary
        assert_eq!(
            get_decimal_precision(999_9999_9999_9999_9999_9999_9999_9999),
            31
        );
        assert_eq!(
            get_decimal_precision(1000_0000_0000_0000_0000_0000_0000_0000),
            32
        );
        assert_eq!(
            get_decimal_precision(9999_9999_9999_9999_9999_9999_9999_9999),
            32
        );
        assert_eq!(
            get_decimal_precision(1_0000_0000_0000_0000_0000_0000_0000_0000),
            33
        );

        // Test around the 38 digit boundary
        assert_eq!(
            get_decimal_precision(9_9999_9999_9999_9999_9999_9999_9999_9999_9999),
            37
        );
        assert_eq!(
            get_decimal_precision(10_0000_0000_0000_0000_0000_0000_0000_0000_0000),
            38
        );
        assert_eq!(
            get_decimal_precision(99_9999_9999_9999_9999_9999_9999_9999_9999_9999),
            38
        );
        assert_eq!(
            get_decimal_precision(100_0000_0000_0000_0000_0000_0000_0000_0000_0000),
            39
        );
    }

    #[test]
    fn test_parse_decimal() -> Result<(), Box<dyn std::error::Error>> {
        assert_decimal("0.999", 999, 3, 3)?;
        assert_decimal("0", 0, 1, 0)?;
        assert_decimal("0.00", 0, 3, 2)?;
        assert_decimal("123", 123, 3, 0)?;
        assert_decimal("-123", -123, 3, 0)?;
        assert_decimal("-123.", -123, 3, 0)?;
        assert_decimal("123000", 123000, 6, 0)?;
        assert_decimal("12.0", 120, 3, 1)?;
        assert_decimal("12.3", 123, 3, 1)?;
        assert_decimal("0.00123", 123, 5, 5)?;
        assert_decimal("1234.5E-4", 12345, 5, 5)?;
        assert_decimal("-0", 0, 1, 0)?;
        assert_decimal("12.000000000000000000", 12000000000000000000, 38, 18)?;
        Ok(())
    }

    fn expect_fail_parse(raw: &str, prec: u8, scale: u8) {
        let s = PrimitiveType::decimal(prec, scale).unwrap();
        let res = s.parse_scalar(raw);
        assert!(res.is_err(), "Fail on {raw}");
    }

    #[test]
    fn test_parse_decimal_expect_fail() {
        expect_fail_parse("1.000", 3, 3);
        expect_fail_parse("iowjef", 1, 0);
        expect_fail_parse("123Ef", 1, 0);
        expect_fail_parse("1d2E3", 1, 0);
        expect_fail_parse("a", 1, 0);
        expect_fail_parse("2.a", 1, 1);
        expect_fail_parse("E45", 1, 0);
        expect_fail_parse("1.2.3", 1, 0);
        expect_fail_parse("1.2E1.3", 1, 0);
        expect_fail_parse("123.45", 5, 1);
        expect_fail_parse(".45", 5, 1);
        expect_fail_parse("+", 1, 0);
        expect_fail_parse("-", 1, 0);
        expect_fail_parse("0.-0", 2, 1);
        expect_fail_parse("--1.0", 1, 1);
        expect_fail_parse("+-1.0", 1, 1);
        expect_fail_parse("-+1.0", 1, 1);
        expect_fail_parse("++1.0", 1, 1);
        expect_fail_parse("1.0E1+", 1, 1);
        // overflow i8 for `scale`
        expect_fail_parse("0.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 1, 0);
        // scale will be too small to fit in i8
        expect_fail_parse("0.E170141183460469231731687303715884105727", 1, 0);
    }

    #[test]
    fn test_arrays() {
        let array = Scalar::Array(ArrayData {
            tpe: ArrayType::new(DataType::INTEGER, false),
            elements: vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
        });

        let column = column_expr!("item");
        let array_op = Pred::binary(BinaryPredicateOp::In, Expr::literal(10), array.clone());
        let array_not_op = Pred::not(Pred::binary(
            BinaryPredicateOp::In,
            Expr::literal(10),
            array,
        ));
        let column_op = Pred::binary(BinaryPredicateOp::In, Expr::literal(PI), column.clone());
        let column_not_op = Pred::not(Pred::binary(
            BinaryPredicateOp::In,
            Expr::literal("Cool"),
            column,
        ));
        assert_eq!(&format!("{array_op}"), "10 IN (1, 2, 3)");
        assert_eq!(&format!("{array_not_op}"), "NOT(10 IN (1, 2, 3))");
        assert_eq!(&format!("{column_op}"), "3.1415927 IN Column(item)");
        assert_eq!(&format!("{column_not_op}"), "NOT('Cool' IN Column(item))");
    }

    #[test]
    fn test_invalid_array() {
        assert_result_error_with_message(
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                [Scalar::Integer(1), Scalar::String("s".to_string())],
            ),
            "Schema error: Array scalar type mismatch: expected integer, got string",
        );

        assert_result_error_with_message(
            ArrayData::try_new(ArrayType::new(DataType::INTEGER, false), [1.into(), None]),
            "Schema error: Array element cannot be null for non-nullable array",
        );
    }

    #[test]
    fn test_invalid_map() {
        // incorrect schema
        assert_result_error_with_message(MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            [(Scalar::Integer(1), Scalar::String("s".to_string())),],
        ), "Schema error: Map scalar type mismatch: expected key type string, got key type integer");

        // key must be non-null
        assert_result_error_with_message(
            MapData::try_new(
                MapType::new(DataType::STRING, DataType::STRING, true),
                [(
                    Scalar::Null(DataType::STRING),  // key
                    Scalar::String("s".to_string()), // val
                )],
            ),
            "Schema error: Map key cannot be null",
        );

        // val must be non-null if we have value_contains_null = false
        assert_result_error_with_message(
            MapData::try_new(
                MapType::new(DataType::STRING, DataType::STRING, false),
                [(
                    Scalar::String("s".to_string()), // key
                    Scalar::Null(DataType::STRING),  // val
                )],
            ),
            "Schema error: Null map value disallowed if map value_contains_null is false",
        );
    }

    #[rstest]
    #[case::seconds("2011-01-11 13:06:07", 1294751167000000)]
    #[case::fractional_seconds("2011-01-11 13:06:07.123456", 1294751167123456)]
    #[case::epoch("1970-01-01 00:00:00", 0)]
    fn test_timestamp_space_form_parse(
        #[values(PrimitiveType::Timestamp, PrimitiveType::TimestampNtz)] p_type: PrimitiveType,
        #[case] raw: &str,
        #[case] micros: i64,
    ) {
        let expected = match p_type {
            PrimitiveType::Timestamp => Scalar::Timestamp(micros),
            PrimitiveType::TimestampNtz => Scalar::TimestampNtz(micros),
            _ => unreachable!(),
        };
        assert_eq!(p_type.parse_scalar(raw).unwrap(), expected);
    }

    #[rstest]
    #[case::z_fractional("1971-07-22T03:06:40.678910Z", 49000000678910)]
    #[case::z_seconds("1971-07-22T03:06:40Z", 49000000000000)]
    #[case::z("2024-06-15T14:30:00Z", 1718461800000000)]
    #[case::lowercase_t_z("2024-06-15t14:30:00z", 1718461800000000)]
    #[case::zero_offset("2024-06-15T14:30:00+00:00", 1718461800000000)]
    #[case::negative_zero_offset("2024-06-15T14:30:00-00:00", 1718461800000000)]
    #[case::positive_offset("2024-06-15T14:30:00+05:00", 1718443800000000)] // 09:30:00Z
    #[case::negative_offset("2024-06-15T14:30:00-05:00", 1718479800000000)] // 19:30:00Z
    #[case::half_hour_offset("2024-06-15T14:30:00+05:30", 1718442000000000)] // 09:00:00Z
    #[case::colonless_offset("2024-06-15T14:30:00+0530", 1718442000000000)] // 09:00:00Z
    #[case::fractional_with_offset("2024-06-15T14:30:00.456+05:00", 1718443800456000)]
    #[case::space_separator_with_offset("2024-06-15 14:30:00+05:00", 1718443800000000)]
    #[case::pre_epoch_after_normalization("1970-01-01T00:00:00+05:00", -18000000000)]
    fn test_timestamp_iso8601_parse(#[case] raw: &str, #[case] micros: i64) {
        let scalar = PrimitiveType::Timestamp.parse_scalar(raw).unwrap();
        assert_eq!(scalar, Scalar::Timestamp(micros));
    }

    #[rstest]
    // TimestampNtz has no ISO 8601 fallback: rejects Z, offsets, and date-only
    #[case::ntz_z_fractional(PrimitiveType::TimestampNtz, "1971-07-22T03:06:40.678910Z")]
    #[case::ntz_z(PrimitiveType::TimestampNtz, "1971-07-22T03:06:40Z")]
    #[case::ntz_offset(PrimitiveType::TimestampNtz, "2024-06-15T14:30:00+05:00")]
    #[case::ntz_space_offset(PrimitiveType::TimestampNtz, "2024-06-15 14:30:00+05:00")]
    #[case::ntz_date_only(PrimitiveType::TimestampNtz, "1971-07-22")]
    // Timestamp rejects date-only and the T-form without a trailing Z or offset
    #[case::date_only(PrimitiveType::Timestamp, "1971-07-22")]
    #[case::zoneless_t_form(PrimitiveType::Timestamp, "2024-06-15T14:30:00")]
    // out-of-range offsets and UTC normalizations that overflow chrono's datetime range
    // error cleanly rather than being misinterpreted
    #[case::offset_out_of_range(PrimitiveType::Timestamp, "2024-06-15T14:30:00+24:00")]
    #[case::normalization_overflow(PrimitiveType::Timestamp, "-262143-01-01T00:00:00+05:00")]
    fn test_timestamp_parse_fails(#[case] p_type: PrimitiveType, #[case] raw: &str) {
        assert!(p_type.parse_scalar(raw).is_err());
    }

    #[test]
    fn test_partial_cmp() {
        let a = Scalar::Integer(1);
        let b = Scalar::Integer(2);
        let c = Scalar::Null(DataType::INTEGER);

        assert_eq!(a.logical_partial_cmp(&b), Some(Ordering::Less));
        assert_eq!(b.logical_partial_cmp(&a), Some(Ordering::Greater));
        assert_eq!(a.logical_partial_cmp(&a), Some(Ordering::Equal));
        assert_eq!(b.logical_partial_cmp(&b), Some(Ordering::Equal));
        assert_eq!(a.logical_partial_cmp(&c), None);
        assert_eq!(c.logical_partial_cmp(&a), None);

        // assert that NULL values are incomparable
        let null = Scalar::Null(DataType::INTEGER);
        assert_eq!(null.logical_partial_cmp(&null), None);
    }

    #[test]
    fn test_partial_eq() {
        let a = Scalar::Integer(1);
        let b = Scalar::Integer(2);
        let c = Scalar::Null(DataType::INTEGER);
        assert!(!a.logical_eq(&b));
        assert!(a.logical_eq(&a));
        assert!(!a.logical_eq(&c));
        assert!(!c.logical_eq(&a));

        // assert that NULL values are incomparable
        let null = Scalar::Null(DataType::INTEGER);
        assert!(!null.logical_eq(&null));
    }

    #[test]
    fn test_hashmap_conversion() -> DeltaResult<()> {
        // Create a simple HashMap with string keys and integer values
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 42i32);
        map.insert("key2".to_string(), 100i32);

        // Convert HashMap to Scalar
        let scalar = Scalar::try_from(map)?;

        // Verify the scalar is of Map type
        assert!(matches!(scalar, Scalar::Map(_)));

        // Verify the data type
        let expected_map_type = MapType::new(DataType::STRING, DataType::INTEGER, false);
        assert_eq!(scalar.data_type(), DataType::from(expected_map_type));

        // Extract the MapData and verify contents
        let Scalar::Map(map_data) = scalar else {
            panic!("Expected Map scalar");
        };
        let pairs = map_data.pairs();
        assert_eq!(pairs.len(), 2);
        assert!(!map_data.map_type().value_contains_null());

        // Check that both expected pairs are present
        let entry1 = (Scalar::String("key1".to_string()), Scalar::Integer(42));
        let entry2 = (Scalar::String("key2".to_string()), Scalar::Integer(100));
        assert!(pairs.contains(&entry1), "Missing key1 -> 42 pair");
        assert!(pairs.contains(&entry2), "Missing key2 -> 100 pair");

        Ok(())
    }

    #[test]
    fn test_hashmap_conversion_with_nullable_values() -> DeltaResult<()> {
        // Create a HashMap with string keys and optional integer values
        let mut map = HashMap::new();
        map.insert("key1".to_string(), Some(42i32));
        map.insert("key2".to_string(), None);
        map.insert("key3".to_string(), Some(100i32));

        // Convert HashMap to Scalar
        let scalar = Scalar::try_from(map)?;

        // Verify the scalar is of Map type
        assert!(matches!(scalar, Scalar::Map(_)));

        // Verify the data type (should have value_contains_null = true)
        let expected_map_type = MapType::new(DataType::STRING, DataType::INTEGER, true);
        assert_eq!(scalar.data_type(), DataType::from(expected_map_type));

        // Extract the MapData and verify contents
        let Scalar::Map(map_data) = scalar else {
            panic!("Expected Map scalar");
        };
        let pairs = map_data.pairs();
        assert_eq!(pairs.len(), 3);
        assert!(map_data.map_type().value_contains_null());

        // Check that all expected pairs are present
        let entry1 = (Scalar::String("key1".to_string()), Scalar::Integer(42));
        let entry2 = (
            Scalar::String("key2".to_string()),
            Scalar::Null(DataType::INTEGER),
        );
        let entry3 = (Scalar::String("key3".to_string()), Scalar::Integer(100));
        assert!(pairs.contains(&entry1), "Missing key1 -> 42 pair");
        assert!(pairs.contains(&entry2), "Missing key2 -> null pair");
        assert!(pairs.contains(&entry3), "Missing key3 -> 100 pair");

        Ok(())
    }

    #[test]
    fn test_vec_conversion() -> DeltaResult<()> {
        // Create a simple Vec with integer values
        let vec = vec![42i32, 100i32, 200i32];

        // Convert Vec to Scalar
        let scalar = Scalar::try_from(vec)?;

        // Verify the scalar is of Array type
        assert!(matches!(scalar, Scalar::Array(_)));

        // Verify the data type
        let expected_array_type = ArrayType::new(DataType::INTEGER, false);
        assert_eq!(scalar.data_type(), DataType::from(expected_array_type));

        // Extract the ArrayData and verify contents
        let Scalar::Array(array_data) = scalar else {
            panic!("Expected Array scalar");
        };
        let elements = array_data.array_elements();
        assert_eq!(elements.len(), 3);
        assert!(!array_data.array_type().contains_null());

        // Check that all expected values are present
        assert_eq!(elements[0], Scalar::Integer(42));
        assert_eq!(elements[1], Scalar::Integer(100));
        assert_eq!(elements[2], Scalar::Integer(200));

        Ok(())
    }

    #[test]
    fn test_vec_conversion_with_nullable_values() -> DeltaResult<()> {
        // Create a Vec with optional integer values
        let vec = vec![Some(42i32), None, Some(100i32)];

        // Convert Vec to Scalar
        let scalar = Scalar::try_from(vec)?;

        // Verify the scalar is of Array type
        assert!(matches!(scalar, Scalar::Array(_)));

        // Verify the data type (should have contains_null = true)
        let expected_array_type = ArrayType::new(DataType::INTEGER, true);
        assert_eq!(scalar.data_type(), DataType::from(expected_array_type));

        // Extract the ArrayData and verify contents
        let Scalar::Array(array_data) = scalar else {
            panic!("Expected Array scalar");
        };

        let elements = array_data.array_elements();
        assert_eq!(elements.len(), 3);
        assert!(array_data.array_type().contains_null());

        // Check that all expected values are present
        assert_eq!(elements[0], Scalar::Integer(42));
        assert!(elements[1].is_null());
        assert_eq!(elements[2], Scalar::Integer(100));

        Ok(())
    }

    #[test]
    fn test_vec_conversion_different_types() -> DeltaResult<()> {
        // Test with string Vec
        let string_vec = vec!["hello".to_string(), "world".to_string()];
        let string_scalar = Scalar::try_from(string_vec)?;

        if let Scalar::Array(array_data) = string_scalar {
            let expected_array_type = ArrayType::new(DataType::STRING, false);
            assert_eq!(array_data.array_type(), &expected_array_type);
        } else {
            panic!("Expected Array scalar");
        }

        // Test with bool Vec
        let bool_vec = vec![true, false, true];
        let bool_scalar = Scalar::try_from(bool_vec)?;

        if let Scalar::Array(array_data) = bool_scalar {
            let expected_array_type = ArrayType::new(DataType::BOOLEAN, false);
            assert_eq!(array_data.array_type(), &expected_array_type);
        } else {
            panic!("Expected Array scalar");
        }

        Ok(())
    }

    #[test]
    fn test_bytes_conversion() {
        // Test with non-empty bytes
        let bytes = bytes::Bytes::from(vec![1, 2, 3, 4, 5]);
        let scalar: Scalar = bytes.into();

        // Verify the scalar is of Binary type
        assert!(matches!(scalar, Scalar::Binary(_)));

        // Verify the data type
        assert_eq!(scalar.data_type(), DataType::BINARY);

        // Extract the binary data and verify contents
        if let Scalar::Binary(data) = scalar {
            assert_eq!(data, vec![1, 2, 3, 4, 5]);
        } else {
            panic!("Expected Binary scalar");
        }

        // Test with empty bytes
        let empty_bytes = bytes::Bytes::new();
        let empty_scalar: Scalar = empty_bytes.into();

        assert!(matches!(empty_scalar, Scalar::Binary(_)));
        if let Scalar::Binary(data) = empty_scalar {
            assert!(data.is_empty());
        } else {
            panic!("Expected Binary scalar");
        }
    }

    // #[ignore]d checklist of core kernel interval dataflows, red until interval support lands
    const INTERVAL_YM_LITERAL: &str = "INTERVAL '1-0' YEAR TO MONTH";
    const INTERVAL_YM_LITERAL_LARGER: &str = "INTERVAL '2-0' YEAR TO MONTH";
    const INTERVAL_DT_LITERAL: &str = "INTERVAL '0 01:00:00.000000' DAY TO SECOND";
    const INTERVAL_DT_LITERAL_LARGER: &str = "INTERVAL '0 02:00:00.000000' DAY TO SECOND";

    #[ignore = "needs Scalar::Interval* variant + parse_scalar/data_type() support"]
    #[test]
    fn interval_parse_and_data_type() {
        let ym = PrimitiveType::IntervalYearMonth
            .parse_scalar(INTERVAL_YM_LITERAL)
            .unwrap();
        assert_eq!(ym.data_type(), DataType::INTERVAL_YEAR_MONTH);

        let dt = PrimitiveType::IntervalDayTime
            .parse_scalar(INTERVAL_DT_LITERAL)
            .unwrap();
        assert_eq!(dt.data_type(), DataType::INTERVAL_DAY_TIME);
    }

    #[ignore = "needs interval Display impl that round-trips with parse_scalar"]
    #[test]
    fn interval_display_round_trips() {
        for (ptype, lit) in [
            (PrimitiveType::IntervalYearMonth, INTERVAL_YM_LITERAL),
            (PrimitiveType::IntervalDayTime, INTERVAL_DT_LITERAL),
        ] {
            let scalar = ptype.parse_scalar(lit).unwrap();
            let reparsed = ptype.parse_scalar(&scalar.to_string()).unwrap();
            assert_eq!(
                scalar, reparsed,
                "Display is not the inverse of parse for {lit}"
            );
        }
    }

    #[ignore = "needs interval logical_partial_cmp (orders within a family, incomparable across)"]
    #[test]
    fn interval_logical_partial_cmp() {
        let ym = PrimitiveType::IntervalYearMonth
            .parse_scalar(INTERVAL_YM_LITERAL)
            .unwrap();
        let ym_larger = PrimitiveType::IntervalYearMonth
            .parse_scalar(INTERVAL_YM_LITERAL_LARGER)
            .unwrap();
        assert_eq!(ym.logical_partial_cmp(&ym_larger), Some(Ordering::Less));
        assert_eq!(ym_larger.logical_partial_cmp(&ym), Some(Ordering::Greater));
        assert_eq!(ym.logical_partial_cmp(&ym), Some(Ordering::Equal));

        let dt = PrimitiveType::IntervalDayTime
            .parse_scalar(INTERVAL_DT_LITERAL)
            .unwrap();
        let dt_larger = PrimitiveType::IntervalDayTime
            .parse_scalar(INTERVAL_DT_LITERAL_LARGER)
            .unwrap();
        assert_eq!(dt.logical_partial_cmp(&dt_larger), Some(Ordering::Less));

        // Comparison is defined only within a family; everything else is incomparable.
        assert_eq!(ym.logical_partial_cmp(&dt), None);
        assert_eq!(ym.logical_partial_cmp(&Scalar::Integer(0)), None);
    }

    #[ignore = "needs interval logical_eq (NULL-aware equality via logical_partial_cmp)"]
    #[test]
    fn interval_logical_eq() {
        let ym = PrimitiveType::IntervalYearMonth
            .parse_scalar(INTERVAL_YM_LITERAL)
            .unwrap();
        let ym_same = PrimitiveType::IntervalYearMonth
            .parse_scalar(INTERVAL_YM_LITERAL)
            .unwrap();
        assert!(ym.logical_eq(&ym_same));
        // SQL NULL semantics: NULL is not equal to anything, including a like-typed value.
        assert!(!ym.logical_eq(&Scalar::null(DataType::INTERVAL_YEAR_MONTH)));
    }
}
