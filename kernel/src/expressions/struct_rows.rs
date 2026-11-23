//! Collects struct rows into typed Rust values without handwritten visitors or getters.
//!
//! [`TryFromStructRows`] accepts fallible [`StructData`] rows or engine batches and decodes each
//! row through `TryFrom<StructData>`, often derived with `TryFromStructData`. A required value
//! accepts exactly one row, `Option<T>` accepts at most one, and `Vec<T>` accepts any count.
//! Collection stops at the first source, decoding, or row count error.
//!
//! ```rust
//! # #![cfg(feature = "internal-api")]
//! use delta_kernel::delta_kernel_derive::{IntoStructData, ToSchema, TryFromStructData};
//! use delta_kernel::expressions::{StructData, TryFromStructRows};
//!
//! #[derive(Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData)]
//! struct Item {
//!     id: i32,
//! }
//!
//! let rows = vec![Item { id: 7 }, Item { id: 11 }]
//!     .into_iter()
//!     .map(|item| Ok(StructData::from(item)));
//! let items = Vec::<Item>::collect_into(rows).unwrap();
//! assert_eq!(items, vec![Item { id: 7 }, Item { id: 11 }]);
//! ```

use delta_kernel_derive::internal_api;

use super::StructData;
use crate::error::add_scalar_path_context;
use crate::schema::ToSchema;
use crate::{KernelError, KernelResult};

/// Supplies struct rows directly to a fallible consumer.
#[internal_api]
pub(crate) trait StructRowSource {
    /// Visits rows in order, stopping at the first source or callback error.
    fn try_for_each(self, consume: impl FnMut(StructData) -> KernelResult<()>) -> KernelResult<()>;
}

impl<I: IntoIterator<Item = KernelResult<StructData>>> StructRowSource for I {
    fn try_for_each(
        self,
        mut consume: impl FnMut(StructData) -> KernelResult<()>,
    ) -> KernelResult<()> {
        Iterator::try_for_each(&mut self.into_iter(), |row| consume(row?))
    }
}

/// Collects struct rows into a row, optional row, or vector.
#[internal_api]
pub(crate) trait TryFromStructRows: Sized {
    /// The input row type whose schema is used for decoding.
    type Row: ToSchema + TryFrom<StructData, Error = KernelError>;

    /// Collects rows, stopping at the first source, decoding, or cardinality error.
    fn collect_into(rows: impl StructRowSource) -> KernelResult<Self>;
}

macro_rules! impl_row_result {
    ($target:ty, |$rows:ident| $collect:expr) => {
        impl<T: ToSchema + TryFrom<StructData, Error = KernelError>> TryFromStructRows for $target {
            type Row = T;

            fn collect_into($rows: impl StructRowSource) -> KernelResult<Self> {
                $collect
            }
        }
    };
}

impl_row_result!(T, |rows| collect_one(rows, "exactly one row")?.ok_or_else(
    || KernelError::scalar_conversion("exactly one row", "0 rows")
));
impl_row_result!(Option<T>, |rows| collect_one(rows, "at most one row"));
impl_row_result!(Vec<T>, |rows| {
    let mut output = Vec::new();
    rows.try_for_each(|row| {
        output.push(decode_row(row, output.len())?);
        Ok(())
    })?;
    Ok(output)
});

#[cfg(feature = "declarative-plans")]
pub(crate) fn decode_rows<T: TryFrom<StructData, Error = KernelError>>(
    rows: impl IntoIterator<Item = KernelResult<StructData>>,
) -> impl Iterator<Item = KernelResult<T>> {
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| decode_row(row?, index))
}

fn collect_one<T: TryFrom<StructData, Error = KernelError>>(
    rows: impl StructRowSource,
    count_error: &str,
) -> KernelResult<Option<T>> {
    let mut output = None;
    rows.try_for_each(|row| {
        let value = decode_row(row, usize::from(output.is_some()))?;
        if output.replace(value).is_some() {
            return Err(KernelError::scalar_conversion(
                count_error,
                "at least 2 rows",
            ));
        }
        Ok(())
    })?;
    Ok(output)
}

fn decode_row<T: TryFrom<StructData, Error = KernelError>>(
    row: StructData,
    index: usize,
) -> KernelResult<T> {
    T::try_from(row).map_err(|error| add_scalar_path_context(error, format!("[{index}]")))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use delta_kernel_derive::{IntoStructData, ToSchema, TryFromStructData};
    use rstest::rstest;

    use super::*;

    #[derive(Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData)]
    struct Added {
        id: i32,
    }

    #[rstest]
    #[case::required(|rows| Added::collect_into(rows).map(|row| vec![row]), 1..=1)]
    #[case::optional(|rows| Option::<Added>::collect_into(rows).map(|row| row.into_iter().collect()), 0..=1)]
    #[case::vector(Vec::<Added>::collect_into, 0..=usize::MAX)]
    fn cardinality_applies_without_plan_results(
        #[case] collect: fn(Vec<KernelResult<StructData>>) -> KernelResult<Vec<Added>>,
        #[case] counts: RangeInclusive<usize>,
        #[values(0, 1, 2)] count: usize,
    ) {
        let expected: Vec<_> = (0..count).map(|id| Added { id: id as i32 }).collect();
        let result = collect(expected.iter().cloned().map(|row| Ok(row.into())).collect());
        if counts.contains(&count) {
            assert_eq!(result.unwrap(), expected);
        } else {
            assert!(result.unwrap_err().to_string().contains("row"));
        }
    }
}
