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

    /// Collects rows using the output's cardinality policy.
    fn collect_into(rows: Vec<StructData>) -> KernelResult<Self>;
}

macro_rules! impl_row_result {
    ($target:ty, $counts:expr, |$rows:ident| $finish:expr) => {
        impl<T: ToSchema + TryFrom<StructData, Error = KernelError>> TryFromStructRows for $target {
            type Row = T;

            fn collect_into(rows: Vec<StructData>) -> KernelResult<Self> {
                if T::VARIANT_ROWS {
                    for (index, row) in rows.iter().enumerate() {
                        validate_variant(row.values()).map_err(|error| {
                            add_scalar_path_context(error, format!("[{index}]"))
                        })?;
                    }
                }
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

/// Groups mixed rows by schema field for the `TryFromStructRows` derive.
#[internal_api]
pub(crate) fn group_struct_rows<const N: usize>(
    rows: Vec<StructData>,
    schema: &SchemaRef,
    names: [&str; N],
) -> KernelResult<[Vec<StructData>; N]> {
    if schema.num_fields() != N
        || schema
            .fields()
            .any(|field| !names.contains(&field.name().as_str()))
    {
        return Err(KernelError::invalid_struct_data(
            "Input row schema does not match collector fields",
        ));
    }
    let mut groups: [Vec<StructData>; N] = std::array::from_fn(|_| Vec::new());
    for (row_index, row) in rows.into_iter().enumerate() {
        let (actual, values) = row.into_parts();
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
            let group = names
                .iter()
                .position(|name| *name == field.name())
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

    #[derive(ToSchema, IntoStructData, TryFromStructData)]
    enum Change {
        Removed(Added),
        Added(Added),
    }

    #[derive(Debug, PartialEq, TryFromStructRows)]
    #[row_type(Change)]
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
            Changes::collect_into(vec![row])
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
