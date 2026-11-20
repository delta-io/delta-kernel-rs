//! Collection and cardinality rules for materialized struct rows.

use delta_kernel_derive::internal_api;

use super::StructData;
use crate::error::add_scalar_path_context;
use crate::schema::{SchemaRef, ToSchema};
use crate::{KernelError, KernelResult};

/// Collects struct rows into a row, optional row, or vector.
#[internal_api]
pub(crate) trait TryFromStructRows: Sized {
    /// Returns the output schema.
    fn schema_info() -> KernelResult<SchemaRef>;

    /// Collects rows using the output's cardinality policy.
    fn collect_into(rows: Vec<StructData>) -> KernelResult<Self>;
}

macro_rules! impl_row_result {
    ($target:ty, $counts:expr, |$rows:ident| $finish:expr) => {
        impl<T: ToSchema + TryFrom<StructData, Error = KernelError>> TryFromStructRows for $target {
            fn schema_info() -> KernelResult<SchemaRef> {
                Ok(<T as ToSchema>::schema_ref().clone())
            }

            fn collect_into(rows: Vec<StructData>) -> KernelResult<Self> {
                if !($counts).contains(&rows.len()) {
                    return Err(KernelError::scalar_conversion(
                        if ($counts).contains(&0) {
                            "at most one row"
                        } else {
                            "exactly one row"
                        },
                        format!("{} rows", rows.len()),
                    ));
                }
                let mut $rows = decode_rows::<T>(rows);
                $finish
            }
        }
    };
}

impl_row_result!(T, 1..=1, |rows| rows.next().ok_or_else(|| {
    KernelError::internal_error("Missing checked result row")
})?);
impl_row_result!(Vec<T>, 0..=usize::MAX, |rows| rows.by_ref().collect());
impl_row_result!(Option<T>, 0..=1, |rows| rows.next().transpose());

pub(crate) fn decode_rows<T: TryFrom<StructData, Error = KernelError>>(
    rows: impl IntoIterator<Item = StructData>,
) -> impl Iterator<Item = KernelResult<T>> {
    rows.into_iter().enumerate().map(|(index, row)| {
        T::try_from(row).map_err(|error| add_scalar_path_context(error, format!("[{index}]")))
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
    #[case::required(|rows: Vec<StructData>| Added::collect_into(rows).map(|row| vec![row]), 1..=1)]
    #[case::optional(|rows: Vec<StructData>| Option::<Added>::collect_into(rows).map(|row| row.into_iter().collect()), 0..=1)]
    #[case::vector(|rows: Vec<StructData>| Vec::<Added>::collect_into(rows), 0..=usize::MAX)]
    fn cardinality_applies_without_plan_results(
        #[case] collect: fn(Vec<StructData>) -> KernelResult<Vec<Added>>,
        #[case] counts: RangeInclusive<usize>,
        #[values(0, 1, 2)] count: usize,
    ) {
        let expected: Vec<_> = (0..count).map(|id| Added { id: id as i32 }).collect();
        let result = collect(expected.iter().cloned().map(Into::into).collect());
        if counts.contains(&count) {
            assert_eq!(result.unwrap(), expected);
        } else {
            assert!(result.unwrap_err().to_string().contains("row"));
        }
    }
}
