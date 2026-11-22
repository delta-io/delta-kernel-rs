use super::{StatsColumnFilter, StatsConfig, is_skipping_eligible_datatype};
use crate::KernelResult;
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS};
use crate::memory::Owned;
use crate::schema::{
    ArrayType, ColumnName, DataType, MapType, StructField, StructType, file_sub_field_has_min_max,
};

pub(super) fn expected(
    schema: &StructType,
    config: &StatsConfig<'_>,
    required: Option<&[ColumnName]>,
    requested: Option<&[ColumnName]>,
) -> KernelResult<StructType> {
    let mut filter = StatsColumnFilter::new(config, required, requested);
    let base = select(schema, &mut filter)?;
    // This outer schema has a fixed field count. Every nested struct and its retained field
    // storage is constructed fallibly; no infallible transform or deep clone is used.
    let mut fields = Vec::with_capacity(5);
    fields.push(StructField::nullable(NUM_RECORDS, DataType::LONG));
    if base.num_fields() != 0 {
        let nulls = derive(&base, Derivation::NullCount, config)?;
        fields.push(StructField::nullable(
            NULL_COUNT,
            DataType::try_from_struct(nulls)?,
        ));
        let bounds = derive(&base, Derivation::MinMax, config)?;
        if bounds.num_fields() != 0 {
            fields.push(StructField::nullable(
                MIN_VALUES,
                DataType::try_from_struct(bounds.try_clone()?)?,
            ));
            fields.push(StructField::nullable(
                MAX_VALUES,
                DataType::try_from_struct(bounds)?,
            ));
        }
    }
    fields.push(StructField::nullable(TIGHT_BOUNDS, DataType::BOOLEAN));
    StructType::try_new(fields)
}

fn select(schema: &StructType, filter: &mut StatsColumnFilter<'_>) -> KernelResult<StructType> {
    StructType::try_from_unchecked_results(schema.fields().filter_map(|field| {
        filter.enter_field(field.name());
        let selected = select_type(field.data_type(), filter);
        filter.exit_field();
        selected
            .transpose()
            .map(|selected| selected.map(|data_type| copy_field(field, data_type)))
    }))
}

fn select_type(
    value: &DataType,
    filter: &mut StatsColumnFilter<'_>,
) -> KernelResult<Option<DataType>> {
    stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || match value {
        DataType::Struct(schema) | DataType::File(schema) => {
            let selected = select(schema, filter)?;
            if selected.num_fields() == 0 {
                return Ok(None);
            }
            let selected = Owned::try_new(selected)?;
            Ok(Some(if matches!(value, DataType::File(_)) {
                DataType::File(selected)
            } else {
                DataType::Struct(selected)
            }))
        }
        #[cfg(feature = "udt-in-dev")]
        DataType::UserDefined(_) => Ok(Some(value.try_clone()?)),
        _ => {
            if !filter.should_include_for_table() {
                return Ok(None);
            }
            filter.record_included();
            if !filter.should_include_for_requested() {
                return Ok(None);
            }
            value.try_clone().map(Some)
        }
    })
}

#[derive(Clone, Copy)]
enum Derivation {
    NullCount,
    MinMax,
}

fn derive(
    schema: &StructType,
    kind: Derivation,
    config: &StatsConfig<'_>,
) -> KernelResult<StructType> {
    derive_fields(schema, kind, config, false)
}

fn derive_fields(
    schema: &StructType,
    kind: Derivation,
    config: &StatsConfig<'_>,
    file: bool,
) -> KernelResult<StructType> {
    StructType::try_from_unchecked_results(schema.fields().filter_map(|field| {
        if file && matches!(kind, Derivation::MinMax) && !file_sub_field_has_min_max(field.name()) {
            return None;
        }
        derive_type(field.data_type(), kind, config)
            .transpose()
            .map(|data_type| data_type.map(|data_type| copy_field(field, data_type)))
    }))
}

fn derive_type(
    value: &DataType,
    kind: Derivation,
    config: &StatsConfig<'_>,
) -> KernelResult<Option<DataType>> {
    stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || match value {
        DataType::Struct(schema) | DataType::File(schema) => {
            let schema = derive_fields(schema, kind, config, matches!(value, DataType::File(_)))?;
            if schema.num_fields() == 0 {
                return Ok(None);
            }
            DataType::try_from_struct(schema).map(Some)
        }
        _ if matches!(kind, Derivation::NullCount) => Ok(Some(DataType::LONG)),
        DataType::Array(_) | DataType::Map(_) => Ok(None),
        DataType::Variant(_) if !config.variant_min_max => Ok(None),
        #[cfg(feature = "geo-type-in-dev")]
        DataType::Primitive(crate::schema::PrimitiveType::Geometry(_))
            if config.geometry_min_max =>
        {
            value.try_clone().map(Some)
        }
        DataType::Primitive(primitive) if !is_skipping_eligible_datatype(primitive) => Ok(None),
        _ => value.try_clone().map(Some),
    })
}

fn copy_field(field: &StructField, data_type: DataType) -> StructField {
    StructField {
        name: field.name.clone(),
        data_type,
        nullable: true,
        metadata: field.metadata.clone(),
    }
}

pub(crate) fn strip_metadata(schema: &StructType) -> KernelResult<StructType> {
    StructType::try_from_unchecked_results(schema.fields().map(|field| {
        Ok(StructField {
            name: field.name.clone(),
            data_type: strip_type(&field.data_type)?,
            nullable: field.nullable,
            metadata: Default::default(),
        })
    }))
}

fn strip_type(value: &DataType) -> KernelResult<DataType> {
    stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || {
        Ok(match value {
            DataType::Struct(value) => DataType::Struct(Owned::try_new(strip_metadata(value)?)?),
            DataType::File(value) => DataType::File(Owned::try_new(strip_metadata(value)?)?),
            DataType::Variant(value) => DataType::Variant(Owned::try_new(strip_metadata(value)?)?),
            DataType::Array(value) => DataType::Array(Owned::try_new(ArrayType {
                type_name: value.type_name.clone(),
                element_type: strip_type(value.element_type())?,
                contains_null: value.contains_null(),
            })?),
            DataType::Map(value) => DataType::Map(Owned::try_new(MapType {
                type_name: value.type_name.clone(),
                key_type: strip_type(value.key_type())?,
                value_type: strip_type(value.value_type())?,
                value_contains_null: value.value_contains_null(),
            })?),
            _ => value.try_clone()?,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBudget;
    use crate::schema::schema;
    use crate::transforms::SchemaTransform;

    #[test]
    fn derived_stats_and_metadata_stripping_fail_without_retaining_partial_schemas() {
        let schema = schema! {
            (StructField::not_null("a", DataType::LONG).with_metadata([("comment", "large annotation")])),
            nullable "nested": { not_null "x": LONG, nullable "y": STRING },
            nullable "array": [ nullable LONG ],
        };
        let config = StatsConfig::default();
        let expected = super::super::StripFieldMetadataTransform
            .transform_struct(&schema)
            .into_owned();
        let unlimited = MemoryBudget::new(64 * 1024);
        let actual = unlimited.with(|| strip_metadata(&schema)).unwrap();
        assert_eq!(actual, expected);
        drop(actual);
        assert_eq!(unlimited.usage().reserved, 0);

        for cap in [0, 64, 512, 1024] {
            let budget = MemoryBudget::new(cap);
            let error = budget
                .with(|| super::expected(&schema, &config, None, None))
                .unwrap_err();
            assert!(matches!(
                error.without_backtrace(),
                crate::KernelError::Memory(_)
            ));
            assert_eq!(budget.usage().reserved, 0);
            assert!(budget.usage().peak <= cap);
        }
    }
}
