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

/// Groups mixed rows by schema field for the `TryFromStructRows` derive.
#[internal_api]
pub(crate) fn group_struct_rows<const N: usize>(
    rows: impl IntoIterator<Item = KernelResult<StructData>>,
    schema: &SchemaRef,
) -> KernelResult<[Vec<StructData>; N]> {
    let mut groups: [Vec<StructData>; N] = std::array::from_fn(|_| Vec::new());
    for (row_index, row) in rows.into_iter().enumerate() {
        let (actual, values) = row?.into_parts();
        if actual != *schema || values.len() != schema.num_fields() {
            return Err(KernelError::invalid_struct_data(
                "Incompatible mixed-row payload schema",
            ));
        }
        validate_variant(&values)
            .map_err(|error| add_scalar_path_context(error, format!("[{row_index}]")))?;
        for (field, value) in actual
            .fields()
            .zip(values)
            .filter(|(_, value)| !value.is_null())
        {
            let group = schema
                .index_of(field.name())
                .and_then(|index| groups.get_mut(index))
                .ok_or_else(|| KernelError::internal_error("Missing payload group"))?;
            group.push(value.try_into()?);
        }
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use delta_kernel_derive::{IntoStructData, ToSchema, TryFromStructData, TryFromStructRows};
    use rstest::rstest;

    use super::*;
    use crate::schema::{StructField, StructType};

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
    #[case::declared_order(false, None)]
    #[case::reordered(true, None)]
    #[case::unknown(false, Some("other"))]
    fn named_collections_resolve_struct_payloads_by_name(
        #[case] reverse: bool,
        #[case] extra: Option<&str>,
    ) -> KernelResult<()> {
        let mut fields = vec![
            StructField::nullable("added", Added::schema_ref().clone()),
            StructField::nullable("removed", Added::schema_ref().clone()),
        ];
        let mut values = vec![
            Added { id: 7 }.into(),
            crate::expressions::Scalar::Null(Added::schema_ref().clone().into()),
        ];
        if reverse {
            fields.reverse();
            values.reverse();
        }
        if let Some(name) = extra {
            fields.push(StructField::nullable(name, Added::schema_ref().clone()));
            values.push(crate::expressions::Scalar::Null(
                Added::schema_ref().clone().into(),
            ));
        }
        let result = StructType::try_new(fields).and_then(|schema| {
            let row = StructData::try_new(Arc::new(schema), values)?;
            Changes::collect_into([Ok(row)])
        });
        if extra.is_some() {
            assert!(result.is_err());
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
