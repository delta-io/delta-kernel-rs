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
pub enum FfiIntervalYearToMonthType {
    IntervalYear = 1,
    IntervalMonth = 2,
    IntervalYearToMonth = 3,
}

impl From<IntervalYearToMonthType> for FfiIntervalYearToMonthType {
    fn from(value: IntervalYearToMonthType) -> Self {
        match value {
            IntervalYearToMonthType::IntervalYear => Self::IntervalYear,
            IntervalYearToMonthType::IntervalMonth => Self::IntervalMonth,
            IntervalYearToMonthType::IntervalYearToMonth => Self::IntervalYearToMonth,
        }
    }
}

impl From<FfiIntervalYearToMonthType> for IntervalYearToMonthType {
    fn from(value: FfiIntervalYearToMonthType) -> Self {
        match value {
            FfiIntervalYearToMonthType::IntervalYear => Self::IntervalYear,
            FfiIntervalYearToMonthType::IntervalMonth => Self::IntervalMonth,
            FfiIntervalYearToMonthType::IntervalYearToMonth => Self::IntervalYearToMonth,
        }
    }
}

impl TryFrom<u32> for FfiIntervalYearToMonthType {
    type Error = KernelError;

    fn try_from(kind: u32) -> KernelResult<Self> {
        match kind {
            1 => Ok(Self::IntervalYear),
            2 => Ok(Self::IntervalMonth),
            3 => Ok(Self::IntervalYearToMonth),
            other => Err(KernelError::schema(format!(
                "invalid interval year-month kind: {other}"
            ))),
        }
    }
}
