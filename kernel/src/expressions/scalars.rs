use std::fmt::{Display, Formatter};

use crate::schema::{DataType, PrimitiveType};

#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Integer(i32),
    Float(f32),
    String(String),
    Boolean(bool),
    Timestamp(i64),
    Date(i32),
    Binary(Vec<u8>),
    Null(DataType),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::Primitive(PrimitiveType::Integer),
            Self::Float(_) => DataType::Primitive(PrimitiveType::Float),
            Self::String(_) => DataType::Primitive(PrimitiveType::String),
            Self::Boolean(_) => DataType::Primitive(PrimitiveType::Boolean),
            Self::Timestamp(_) => DataType::Primitive(PrimitiveType::Timestamp),
            Self::Date(_) => DataType::Primitive(PrimitiveType::Date),
            Self::Binary(_) => DataType::Primitive(PrimitiveType::Binary),
            Self::Null(data_type) => data_type.clone(),
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "{}", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Timestamp(ts) => write!(f, "{}", ts),
            Self::Date(d) => write!(f, "{}", d),
            Self::Binary(b) => write!(f, "{:?}", b),
            Self::Null(_) => write!(f, "null"),
        }
    }
}

impl From<i32> for Scalar {
    fn from(i: i32) -> Self {
        Self::Integer(i)
    }
}

impl From<bool> for Scalar {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}
