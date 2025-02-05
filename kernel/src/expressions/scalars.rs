use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

use crate::schema::{ArrayType, DataType, PrimitiveType, StructField};
use crate::utils::require;
use crate::{DeltaResult, Error};

#[derive(Debug, Clone, PartialEq)]
pub struct StructData {
    fields: Vec<StructField>,
    values: Vec<Scalar>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArrayData {
    tpe: ArrayType,
    /// This exists currently for literal list comparisons, but should not be depended on see below
    elements: Vec<Scalar>,
}

impl ArrayData {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub fn new(tpe: ArrayType, elements: impl IntoIterator<Item = impl Into<Scalar>>) -> Self {
        let elements = elements.into_iter().map(Into::into).collect();
        Self { tpe, elements }
    }
    pub fn array_type(&self) -> &ArrayType {
        &self.tpe
    }

    #[deprecated(
        note = "These fields will be removed eventually and are unstable. See https://github.com/delta-io/delta-kernel-rs/issues/291"
    )]
    pub fn array_elements(&self) -> &[Scalar] {
        &self.elements
    }
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
#[derive(Debug, Clone)]
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
    Decimal(i128, u8, u8),
    /// Null value with a given data type.
    Null(DataType),
    /// Struct value
    Struct(StructData),
    /// Array Value
    Array(ArrayData),
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
            Self::Decimal(_, precision, scale) => DataType::decimal_unchecked(*precision, *scale),
            Self::Null(data_type) => data_type.clone(),
            Self::Struct(data) => DataType::struct_type(data.fields.clone()),
            Self::Array(data) => data.tpe.clone().into(),
        }
    }

    /// Returns true if this scalar is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    /// Constructs a Scalar timestamp with no timezone from an `i64` millisecond since unix epoch
    pub(crate) fn timestamp_ntz_from_millis(millis: i64) -> DeltaResult<Self> {
        let Some(timestamp) = DateTime::from_timestamp_millis(millis) else {
            return Err(Error::generic(format!(
                "Failed to create millisecond timestamp from {millis}"
            )));
        };
        Ok(Self::TimestampNtz(timestamp.timestamp_micros()))
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{}", i),
            Self::Long(i) => write!(f, "{}", i),
            Self::Short(i) => write!(f, "{}", i),
            Self::Byte(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Double(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Timestamp(ts) => write!(f, "{}", ts),
            Self::TimestampNtz(ts) => write!(f, "{}", ts),
            Self::Date(d) => write!(f, "{}", d),
            Self::Binary(b) => write!(f, "{:?}", b),
            Self::Decimal(value, _, scale) => match scale.cmp(&0) {
                Ordering::Equal => {
                    write!(f, "{}", value)
                }
                Ordering::Greater => {
                    let scalar_multiple = 10_i128.pow(*scale as u32);
                    write!(f, "{}", value / scalar_multiple)?;
                    write!(f, ".")?;
                    write!(
                        f,
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = *scale as usize
                    )
                }
                Ordering::Less => {
                    write!(f, "{}", value)?;
                    for _ in 0..*scale {
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
        }
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Scalar) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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
            (Decimal(v1, p1, s1), Decimal(v2, p2, s2)) => (s1.eq(s2) && p1.eq(p2))
                .then(|| v1.partial_cmp(v2))
                .flatten(),
            (Decimal(_, _, _), _) => None,
            (Null(_), _) => None, // NOTE: NULL values are incomparable by definition
            (Struct(_), _) => None, // TODO: Support Struct?
            (Array(_), _) => None, // TODO: Support Array?
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

// TODO: add more From impls

impl PrimitiveType {
    /// Check if the given precision and scale are valid for a decimal type.
    pub fn check_decimal(precision: u8, scale: u8) -> DeltaResult<()> {
        require!(
            0 < precision && precision <= 38,
            Error::invalid_decimal(format!(
                "precision must in range 1..38 inclusive, found: {}.",
                precision
            ))
        );
        require!(
            scale <= precision,
            Error::invalid_decimal(format!(
                "scale must be in range 0..precision inclusive, found: {scale}."
            ))
        );
        Ok(())
    }

    fn data_type(&self) -> DataType {
        DataType::Primitive(self.clone())
    }

    pub fn parse_scalar(&self, raw: &str) -> Result<Scalar, Error> {
        use PrimitiveType::*;

        if raw.is_empty() {
            return Ok(Scalar::Null(self.data_type()));
        }

        match self {
            String => Ok(Scalar::String(raw.to_string())),
            Binary => Ok(Scalar::Binary(raw.to_string().into_bytes())),
            Byte => self.parse_str_as_scalar(raw, Scalar::Byte),
            Decimal(precision, scale) => self.parse_decimal(raw, *precision, *scale),
            Short => self.parse_str_as_scalar(raw, Scalar::Short),
            Integer => self.parse_str_as_scalar(raw, Scalar::Integer),
            Long => self.parse_str_as_scalar(raw, Scalar::Long),
            Float => self.parse_str_as_scalar(raw, Scalar::Float),
            Double => self.parse_str_as_scalar(raw, Scalar::Double),
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
            // NOTE: Timestamp and TimestampNtz are both parsed into microsecond since unix epoch.
            // They may both have the format `{year}-{month}-{day} {hour}:{minute}:{second}`.
            // Timestamps may additionally be encoded as a ISO 8601 formatted string such as
            // `1970-01-01T00:00:00.123456Z`.
            //
            // The difference arises mostly in how they are to be handled on the engine side - i.e. timestampNTZ
            // is not adjusted to UTC, this is just so we can (de-)serialize it as a date sting.
            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
            TimestampNtz | Timestamp => {
                let mut timestamp = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f");

                if timestamp.is_err() && *self == Timestamp {
                    // Note: `%+` specifies the ISO 8601 / RFC 3339 format
                    timestamp = NaiveDateTime::parse_from_str(raw, "%+");
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
        }
    }

    fn parse_error(&self, raw: &str) -> Error {
        Error::ParseError(raw.to_string(), self.data_type())
    }

    /// Parse a string as a scalar value, returning an error if the string is not parseable.
    ///
    /// The `f` function is used to convert the parsed value into a `Scalar`.
    /// The desired type that `FromStr::parse` should parse into is inferred from the parameter type of `f`.
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

    fn parse_decimal(&self, raw: &str, precision: u8, expected_scale: u8) -> Result<Scalar, Error> {
        let (base, exp): (&str, i128) = match raw.find(['e', 'E']) {
            None => (raw, 0), // no 'e' or 'E', so there's no exponent
            Some(pos) => {
                let (base, exp) = raw.split_at(pos);
                // exp now has '[e/E][exponent]', strip the 'e/E' and parse it
                (base, exp[1..].parse()?)
            }
        };
        require!(!base.is_empty(), self.parse_error(raw));

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
        let scale: u8 = scale.try_into().map_err(|_| self.parse_error(raw))?;
        require!(scale == expected_scale, self.parse_error(raw));
        Self::check_decimal(precision, scale)?;

        let int: i128 = match frac_part {
            None => int_part.parse()?,
            Some(frac_part) => format!("{}{}", int_part, frac_part).parse()?,
        };
        Ok(Scalar::Decimal(int, precision, scale))
    }
}

#[cfg(test)]
mod tests {
    use std::f32::consts::PI;

    use crate::expressions::{column_expr, BinaryOperator};
    use crate::Expression;

    use super::*;

    #[test]
    fn test_decimal_display() {
        let s = Scalar::Decimal(123456789, 9, 2);
        assert_eq!(s.to_string(), "1234567.89");

        let s = Scalar::Decimal(123456789, 9, 0);
        assert_eq!(s.to_string(), "123456789");

        let s = Scalar::Decimal(123456789, 9, 9);
        assert_eq!(s.to_string(), "0.123456789");
    }

    fn assert_decimal(
        raw: &str,
        expect_int: i128,
        expect_prec: u8,
        expect_scale: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let s = PrimitiveType::Decimal(expect_prec, expect_scale);
        match s.parse_scalar(raw)? {
            Scalar::Decimal(int, prec, scale) => {
                assert_eq!(int, expect_int);
                assert_eq!(prec, expect_prec);
                assert_eq!(scale, expect_scale);
            }
            _ => panic!("Didn't parse as decimal"),
        };
        Ok(())
    }

    #[test]
    fn test_parse_decimal() -> Result<(), Box<dyn std::error::Error>> {
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
        let s = PrimitiveType::Decimal(prec, scale);
        let res = s.parse_scalar(raw);
        assert!(res.is_err(), "Fail on {raw}");
    }

    #[test]
    fn test_parse_decimal_expect_fail() {
        expect_fail_parse("iowjef", 0, 0);
        expect_fail_parse("123Ef", 0, 0);
        expect_fail_parse("1d2E3", 0, 0);
        expect_fail_parse("a", 0, 0);
        expect_fail_parse("2.a", 1, 1);
        expect_fail_parse("E45", 0, 0);
        expect_fail_parse("1.2.3", 0, 0);
        expect_fail_parse("1.2E1.3", 0, 0);
        expect_fail_parse("123.45", 5, 1);
        expect_fail_parse(".45", 5, 1);
        expect_fail_parse("+", 0, 0);
        expect_fail_parse("-", 0, 0);
        expect_fail_parse("0.-0", 2, 1);
        expect_fail_parse("--1.0", 1, 1);
        expect_fail_parse("+-1.0", 1, 1);
        expect_fail_parse("-+1.0", 1, 1);
        expect_fail_parse("++1.0", 1, 1);
        expect_fail_parse("1.0E1+", 1, 1);
        // overflow i8 for `scale`
        expect_fail_parse("0.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 0, 0);
        // scale will be too small to fit in i8
        expect_fail_parse("0.E170141183460469231731687303715884105727", 0, 0);
    }

    #[test]
    fn test_arrays() {
        #[allow(deprecated)]
        let array = Scalar::Array(ArrayData {
            tpe: ArrayType::new(DataType::INTEGER, false),
            elements: vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
        });

        let column = column_expr!("item");
        let array_op = Expression::binary(BinaryOperator::In, 10, array.clone());
        let array_not_op = Expression::binary(BinaryOperator::NotIn, 10, array);
        let column_op = Expression::binary(BinaryOperator::In, PI, column.clone());
        let column_not_op = Expression::binary(BinaryOperator::NotIn, "Cool", column);
        assert_eq!(&format!("{}", array_op), "10 IN (1, 2, 3)");
        assert_eq!(&format!("{}", array_not_op), "10 NOT IN (1, 2, 3)");
        assert_eq!(&format!("{}", column_op), "3.1415927 IN Column(item)");
        assert_eq!(&format!("{}", column_not_op), "'Cool' NOT IN Column(item)");
    }

    #[test]
    fn test_timestamp_parse() {
        let assert_timestamp_eq = |scalar_string, micros| {
            let scalar = PrimitiveType::Timestamp
                .parse_scalar(scalar_string)
                .unwrap();
            assert_eq!(scalar, Scalar::Timestamp(micros));
        };
        assert_timestamp_eq("1971-07-22T03:06:40.678910Z", 49000000678910);
        assert_timestamp_eq("1971-07-22T03:06:40Z", 49000000000000);
        assert_timestamp_eq("2011-01-11 13:06:07", 1294751167000000);
        assert_timestamp_eq("2011-01-11 13:06:07.123456", 1294751167123456);
        assert_timestamp_eq("1970-01-01 00:00:00", 0);
    }

    #[test]
    fn test_timestamp_ntz_parse() {
        let assert_timestamp_eq = |scalar_string, micros| {
            let scalar = PrimitiveType::TimestampNtz
                .parse_scalar(scalar_string)
                .unwrap();
            assert_eq!(scalar, Scalar::TimestampNtz(micros));
        };
        assert_timestamp_eq("2011-01-11 13:06:07", 1294751167000000);
        assert_timestamp_eq("2011-01-11 13:06:07.123456", 1294751167123456);
        assert_timestamp_eq("1970-01-01 00:00:00", 0);
    }

    #[test]
    fn test_timestamp_parse_fails() {
        let assert_timestamp_fails = |p_type: &PrimitiveType, scalar_string| {
            let res = p_type.parse_scalar(scalar_string);
            assert!(res.is_err());
        };

        let p_type = PrimitiveType::TimestampNtz;
        assert_timestamp_fails(&p_type, "1971-07-22T03:06:40.678910Z");
        assert_timestamp_fails(&p_type, "1971-07-22T03:06:40Z");
        assert_timestamp_fails(&p_type, "1971-07-22");

        let p_type = PrimitiveType::Timestamp;
        assert_timestamp_fails(&p_type, "1971-07-22");
    }

    #[test]
    fn test_partial_cmp() {
        let a = Scalar::Integer(1);
        let b = Scalar::Integer(2);
        let c = Scalar::Null(DataType::INTEGER);
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
        assert_eq!(b.partial_cmp(&a), Some(Ordering::Greater));
        assert_eq!(a.partial_cmp(&a), Some(Ordering::Equal));
        assert_eq!(b.partial_cmp(&b), Some(Ordering::Equal));
        assert_eq!(a.partial_cmp(&c), None);
        assert_eq!(c.partial_cmp(&a), None);

        // assert that NULL values are incomparable
        let null = Scalar::Null(DataType::INTEGER);
        assert_eq!(null.partial_cmp(&null), None);
    }

    #[test]
    fn test_partial_eq() {
        let a = Scalar::Integer(1);
        let b = Scalar::Integer(2);
        let c = Scalar::Null(DataType::INTEGER);
        assert!(!a.eq(&b));
        assert!(a.eq(&a));
        assert!(!a.eq(&c));
        assert!(!c.eq(&a));

        // assert that NULL values are incomparable
        let null = Scalar::Null(DataType::INTEGER);
        assert!(!null.eq(&null));
    }
}
