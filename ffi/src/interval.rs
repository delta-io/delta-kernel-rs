use delta_kernel::schema::IntervalYearToMonthType;
use delta_kernel::{KernelError, KernelResult};

/// Exact year-month interval qualifier crossing the FFI boundary.
///
/// Kernel sends this `repr(C)` enum to engine callbacks. Engine-to-kernel constructors accept the
/// same numeric values as `u32`: `1` for `IntervalYear`, `2` for `IntervalMonth`, and `3` for
/// `IntervalYearToMonth`.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KernelIntervalYearToMonthType {
    IntervalYear = 1,
    IntervalMonth = 2,
    IntervalYearToMonth = 3,
}

impl From<IntervalYearToMonthType> for KernelIntervalYearToMonthType {
    fn from(value: IntervalYearToMonthType) -> Self {
        match value {
            IntervalYearToMonthType::IntervalYear => Self::IntervalYear,
            IntervalYearToMonthType::IntervalMonth => Self::IntervalMonth,
            IntervalYearToMonthType::IntervalYearToMonth => Self::IntervalYearToMonth,
        }
    }
}

impl From<KernelIntervalYearToMonthType> for IntervalYearToMonthType {
    fn from(value: KernelIntervalYearToMonthType) -> Self {
        match value {
            KernelIntervalYearToMonthType::IntervalYear => Self::IntervalYear,
            KernelIntervalYearToMonthType::IntervalMonth => Self::IntervalMonth,
            KernelIntervalYearToMonthType::IntervalYearToMonth => Self::IntervalYearToMonth,
        }
    }
}

pub(crate) fn decode_interval_year_month_kind(kind: u32) -> KernelResult<IntervalYearToMonthType> {
    match kind {
        1 => Ok(IntervalYearToMonthType::IntervalYear),
        2 => Ok(IntervalYearToMonthType::IntervalMonth),
        3 => Ok(IntervalYearToMonthType::IntervalYearToMonth),
        other => Err(KernelError::schema(format!(
            "invalid interval year-month kind: {other}"
        ))),
    }
}
