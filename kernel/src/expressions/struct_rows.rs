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
#[cfg(feature = "declarative-plans")]
use crate::KernelResultIterator;
#[cfg(feature = "declarative-plans")]
use crate::engine_data::{EngineData, RowVisitor as _};
use crate::error::add_scalar_path_context;
use crate::schema::ToSchema;
use crate::{KernelError, KernelResult};

#[cfg(feature = "declarative-plans")]
pub(crate) mod engine_rows;

/// Collects struct rows into a row, optional row, vector, or named payload collections.
#[internal_api]
pub(crate) trait TryFromStructRows: Sized {
    /// The input row type whose schema is used for decoding.
    type Row: ToSchema + TryFrom<StructData, Error = KernelError>;

    /// Accumulates rows until the output can be constructed.
    type State;

    /// Creates an empty accumulator.
    fn new_state() -> Self::State;

    /// Consumes the next row. Discard the accumulator if this returns an error.
    fn push(state: &mut Self::State, row: StructData) -> KernelResult<()>;

    /// Constructs the output, checking final row counts.
    fn finish(state: Self::State) -> KernelResult<Self>;

    /// Collects fallible rows, stopping at the first decoding or cardinality error.
    fn collect_into(
        rows: impl IntoIterator<Item = KernelResult<StructData>>,
    ) -> KernelResult<Self> {
        let mut state = Self::new_state();
        rows.into_iter()
            .try_for_each(|row| Self::push(&mut state, row?))?;
        Self::finish(state)
    }

    /// Decodes batches in order, stopping at the first source, decoding, or cardinality error.
    /// Unsupported requested schemas are rejected even for empty streams.
    #[cfg(feature = "declarative-plans")]
    fn collect_batches(
        mut batches: impl KernelResultIterator<Box<dyn EngineData>>,
    ) -> KernelResult<Self> {
        Self::Row::schema_ref()
            .fields()
            .try_for_each(engine_rows::ensure_supported)?;
        let mut state = Self::new_state();
        batches.try_for_each(|batch| {
            let batch = batch?;
            engine_rows::StructVisitor::<Self> {
                batch: batch.as_ref(),
                state: &mut state,
            }
            .visit_rows_of(batch.as_ref())
        })?;
        Self::finish(state)
    }
}

macro_rules! impl_row_result {
    ($target:ty, $state_type:ty, |$state:ident, $row:ident| $push:expr, |$output:ident| $finish:expr) => {
        impl<T: ToSchema + TryFrom<StructData, Error = KernelError>> TryFromStructRows for $target {
            type Row = T;
            type State = $state_type;

            fn new_state() -> Self::State {
                Default::default()
            }

            fn push($state: &mut Self::State, $row: StructData) -> KernelResult<()> {
                $push
            }

            fn finish($output: Self::State) -> KernelResult<Self> {
                $finish
            }
        }
    };
}

impl_row_result!(
    T,
    Option<T>,
    |state, row| push_one(state, row, "exactly one row"),
    |state| state.ok_or_else(|| KernelError::scalar_conversion("exactly one row", "0 rows"))
);
impl_row_result!(
    Option<T>,
    Option<T>,
    |state, row| push_one(state, row, "at most one row"),
    |state| Ok(state)
);
impl_row_result!(
    Vec<T>,
    Vec<T>,
    |state, row| {
        state.push(decode_row(row, state.len())?);
        Ok(())
    },
    |state| Ok(state)
);

#[cfg(feature = "declarative-plans")]
pub(crate) fn decode_rows<T: TryFrom<StructData, Error = KernelError>>(
    rows: impl IntoIterator<Item = KernelResult<StructData>>,
) -> impl Iterator<Item = KernelResult<T>> {
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| decode_row(row?, index))
}

fn push_one<T: TryFrom<StructData, Error = KernelError>>(
    state: &mut Option<T>,
    row: StructData,
    count_error: &str,
) -> KernelResult<()> {
    let value = decode_row(row, usize::from(state.is_some()))?;
    if state.replace(value).is_some() {
        return Err(KernelError::scalar_conversion(
            count_error,
            "at least 2 rows",
        ));
    }
    Ok(())
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
    use std::sync::Arc;

    use delta_kernel_derive::{IntoStructData, ToSchema, TryFromStructData, TryFromStructRows};
    use rstest::rstest;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::unit_test_utils::assert_result_error_with_message;

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

    #[derive(Debug, PartialEq, TryFromStructRows)]
    struct Changes {
        added: Added,
        removed: Vec<Added>,
    }

    #[rstest]
    #[case::declared_order(false, None, false, false)]
    #[case::reordered(true, None, false, false)]
    #[case::wrong_null_type(false, None, true, false)]
    #[case::active_field_renamed(false, Some("other"), false, false)]
    #[case::non_struct_active_payload(false, None, false, true)]
    fn named_collections_resolve_struct_payloads_by_name(
        #[case] reverse: bool,
        #[case] renamed_active: Option<&str>,
        #[case] wrong_null_type: bool,
        #[case] non_struct_active: bool,
    ) -> KernelResult<()> {
        let active = if non_struct_active {
            crate::expressions::Scalar::Integer(7)
        } else {
            Added { id: 7 }.into()
        };
        let removed_type = if wrong_null_type {
            DataType::STRING
        } else {
            Added::schema_ref().clone().into()
        };
        let mut fields = vec![
            StructField::nullable(renamed_active.unwrap_or("added"), active.data_type()),
            StructField::nullable("removed", removed_type.clone()),
        ];
        let mut values = vec![active, crate::expressions::Scalar::Null(removed_type)];
        if reverse {
            fields.reverse();
            values.reverse();
        }
        let result = StructType::try_new(fields).and_then(|schema| {
            let row = StructData::try_new(Arc::new(schema), values)?;
            Changes::collect_into([Ok(row)])
        });
        if wrong_null_type {
            assert_result_error_with_message(result, "removed: expected struct, found string");
        } else if let Some(name) = renamed_active {
            assert_result_error_with_message(result, &format!("known variant field, found {name}"));
        } else if non_struct_active {
            assert_result_error_with_message(result, "added: expected StructData, found integer");
        } else {
            assert_eq!(
                result?,
                Changes {
                    added: Added { id: 7 },
                    removed: vec![]
                }
            );
        }
        Ok(())
    }
}
