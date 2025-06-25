pub(crate) const VERSION: u8 = 1;

pub(crate) const PRIMITIVE: u8 = 0;
pub(crate) const SHORT_STR: u8 = 1;
pub(crate) const OBJECT: u8 = 2;
pub(crate) const ARRAY: u8 = 3;

pub(crate) const NULL: u8 = 0;
pub(crate) const TRUE: u8 = 1;
pub(crate) const FALSE: u8 = 2;

pub(crate) const INT1: u8 = 3;
pub(crate) const INT2: u8 = 4;
pub(crate) const INT4: u8 = 5;
pub(crate) const INT8: u8 = 6;

pub(crate) const DOUBLE: u8 = 7;

pub(crate) const DECIMAL4: u8 = 8;
pub(crate) const DECIMAL8: u8 = 9;
pub(crate) const DECIMAL16: u8 = 10;

pub(crate) const LONG_STR: u8 = 16;

pub(crate) const MAX_SHORT_STR_SIZE: u8 = 0x3F;

pub(crate) const U8_SIZE: u8 = 1;
pub(crate) const U16_SIZE: u8 = 2;
pub(crate) const U24_SIZE: u8 = 3;
pub(crate) const U32_SIZE: u8 = 4;
pub(crate) const U64_SIZE: u8 = 8;

pub(crate) const U8_MAX: u8 = 0xFF;
pub(crate) const U16_MAX: u16 = 0xFFFF;

pub(crate) const BASIC_TYPE_BITS: u8 = 2;

pub(crate) const MAX_UNSCALED_DECIMAL_4: i32 = 999999999;
pub(crate) const MAX_PRECISION_DECIMAL_4: u8 = 9;
pub(crate) const MAX_UNSCALED_DECIMAL_8: i64 = 999999999999999999i64;
pub(crate) const MAX_PRECISION_DECIMAL_8: u8 = 18;
