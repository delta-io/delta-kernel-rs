use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use crate::schema::{DataType, PrimitiveType};

/// A single value, which can be null. Used for representing literal values
/// in [Expressions][crate::expressions::Expression].
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Integer(i32),
    Long(i64),
    Float(f32),
    String(String),
    Boolean(bool),
    Timestamp(i64),
    Date(i32),
    Binary(Vec<u8>),
    Decimal(i128, i32, i32),
    Null(DataType),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::Primitive(PrimitiveType::Integer),
            Self::Long(_) => DataType::Primitive(PrimitiveType::Long),
            Self::Float(_) => DataType::Primitive(PrimitiveType::Float),
            Self::String(_) => DataType::Primitive(PrimitiveType::String),
            Self::Boolean(_) => DataType::Primitive(PrimitiveType::Boolean),
            Self::Timestamp(_) => DataType::Primitive(PrimitiveType::Timestamp),
            Self::Date(_) => DataType::Primitive(PrimitiveType::Date),
            Self::Binary(_) => DataType::Primitive(PrimitiveType::Binary),
            Self::Decimal(_, precision, scale) => {
                DataType::decimal(*precision as usize, *scale as usize)
            }
            Self::Null(data_type) => data_type.clone(),
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{}", i),
            Self::Long(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Timestamp(ts) => write!(f, "{}", ts),
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
                    for _ in 0..(scale.abs()) {
                        write!(f, "0")?;
                    }
                    Ok(())
                }
            },
            Self::Null(_) => write!(f, "null"),
        }
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

// TODO: add more From impls

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_display() {
        let s = Scalar::Decimal(123456789, 9, 2);
        assert_eq!(s.to_string(), "1234567.89");

        let s = Scalar::Decimal(123456789, 9, 0);
        assert_eq!(s.to_string(), "123456789");

        let s = Scalar::Decimal(123456789, 9, 9);
        assert_eq!(s.to_string(), "0.123456789");

        let s = Scalar::Decimal(123, 9, -3);
        assert_eq!(s.to_string(), "123000");
    }
}
