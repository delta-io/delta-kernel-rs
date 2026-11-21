//! Collection and cardinality rules for materialized struct rows.

use delta_kernel_derive::internal_api;

use super::StructData;
use crate::error::add_scalar_path_context;
use crate::schema::derive_macro_utils::validate_variant;
use crate::schema::{SchemaRef, ToSchema};
use crate::{KernelError, KernelResult};

/// Collects struct rows into a row, optional row, vector, or named payload collections.
#[internal_api]
pub(crate) trait TryFromStructRows: Sized {
    /// The input row type whose schema is used for materialization.
    type Row: ToSchema + TryFrom<StructData, Error = KernelError>;

    /// Collects fallible rows, stopping at the first decoding or cardinality error.
    fn collect_into(rows: impl IntoIterator<Item = KernelResult<StructData>>)
    -> KernelResult<Self>;
}

macro_rules! impl_row_result {
    ($target:ty, |$rows:ident| $finish:expr) => {
        impl<T: ToSchema + TryFrom<StructData, Error = KernelError>> TryFromStructRows for $target {
            type Row = T;

            fn collect_into(
                rows: impl IntoIterator<Item = KernelResult<StructData>>,
            ) -> KernelResult<Self> {
                let mut $rows = decode_rows::<T>(rows);
                $finish
            }
        }
    };
}

impl_row_result!(T, |rows| match rows.next().transpose()? {
    Some(value) if rows.next().transpose()?.is_none() => Ok(value),
    value => Err(KernelError::scalar_conversion(
        "exactly one row",
        if value.is_some() {
            "at least 2 rows"
        } else {
            "0 rows"
        },
    )),
});
impl_row_result!(Option<T>, |rows| match rows.next().transpose()? {
    Some(_) if rows.next().transpose()?.is_some() => Err(KernelError::scalar_conversion(
        "at most one row",
        "at least 2 rows",
    )),
    value => Ok(value),
});
impl_row_result!(Vec<T>, |rows| rows.by_ref().collect());

pub(crate) fn decode_rows<T: TryFrom<StructData, Error = KernelError>>(
    rows: impl IntoIterator<Item = KernelResult<StructData>>,
) -> impl Iterator<Item = KernelResult<T>> {
    rows.into_iter().enumerate().map(|(index, row)| {
        T::try_from(row?).map_err(|error| add_scalar_path_context(error, format!("[{index}]")))
    })
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
