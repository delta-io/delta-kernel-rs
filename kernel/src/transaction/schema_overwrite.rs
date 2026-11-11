//! Complete target-schema validation for evolution and whole-table overwrite.

use super::schema_evolution::{SchemaOperation, evolve_table_config};
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::{ColumnMetadataKey, DataType, SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ColumnMappingMode, Operation, TableFeature};
use crate::transforms::{SchemaTransform, transform_output_type};
use crate::utils::require;
use crate::{KernelError, KernelResult};

pub(super) fn apply_target_schema(
    table_config: &TableConfiguration,
    target: SchemaRef,
    overwrite: bool,
) -> KernelResult<TableConfiguration> {
    if !overwrite {
        let mut operations = Vec::new();
        collect_evolution(
            &table_config.logical_schema(),
            &target,
            &[],
            &mut operations,
        )?;
        return evolve_table_config(table_config, operations);
    }

    require!(
        table_config.column_mapping_mode() == ColumnMappingMode::None,
        KernelError::unsupported("Schema overwrite with column mapping is not supported")
    );
    for feature in [
        TableFeature::ClusteredTable,
        TableFeature::RowTracking,
        TableFeature::ChangeDataFeed,
    ] {
        require!(
            !table_config.is_feature_enabled(&feature),
            KernelError::unsupported(format!(
                "Schema overwrite is not supported on tables with {feature} enabled"
            ))
        );
    }
    require!(
        !table_config
            .metadata()
            .configuration()
            .keys()
            .any(|key| key.starts_with("delta.constraints.")),
        KernelError::unsupported("Schema overwrite with CHECK constraints is not supported")
    );
    require!(
        target.fields().next().is_some(),
        KernelError::schema("Schema overwrite requires a nonempty target schema")
    );
    StructType::ensure_no_metadata_columns(&mut target.fields())?;
    OverwriteAnnotationValidator.transform_struct(&target)?;
    validate_schema(&target, ColumnMappingMode::None, false)?;
    let partitions = table_config.logical_partition_columns();
    if !partitions.is_empty() {
        require!(
            target.num_fields() > partitions.len(),
            KernelError::schema("Schema overwrite must retain at least one non-partition column")
        );
        for name in partitions {
            let field = target.field(name).ok_or_else(|| {
                KernelError::schema(format!(
                    "Schema overwrite must retain partition column '{name}'"
                ))
            })?;
            require!(
                matches!(field.data_type(), DataType::Primitive(_)),
                KernelError::schema(format!(
                    "Partition column '{name}' must have a primitive type"
                ))
            );
        }
    }

    let metadata = table_config
        .metadata()
        .clone()
        .with_schema(target.clone())?;
    let result = TableConfiguration::try_new_with_schema(table_config, metadata, target)?;
    result.ensure_operation_supported(Operation::Write)?;
    Ok(result)
}

fn collect_evolution(
    current: &StructType,
    target: &StructType,
    parent: &[String],
    operations: &mut Vec<SchemaOperation>,
) -> KernelResult<()> {
    require!(
        target.fields().len() >= current.fields().len(),
        evolution_error(parent)
    );
    for (old, new) in current.fields().zip(target.fields()) {
        let mut path = parent.to_vec();
        path.push(old.name().clone());
        require!(
            old.name() == new.name()
                && old.metadata() == new.metadata()
                && (!old.is_nullable() || new.is_nullable()),
            evolution_error(&path)
        );
        if !old.is_nullable() && new.is_nullable() {
            operations.push(SchemaOperation::SetNullable {
                column: ColumnName::new(&path),
            });
        }
        collect_type_evolution(old.data_type(), new.data_type(), &path, operations)?;
    }
    for field in target.fields().skip(current.fields().len()) {
        operations.push(SchemaOperation::add_column(
            Some(ColumnName::new(parent)),
            field.clone(),
        ));
    }
    Ok(())
}

fn collect_type_evolution(
    current: &DataType,
    target: &DataType,
    path: &[String],
    operations: &mut Vec<SchemaOperation>,
) -> KernelResult<()> {
    let nested_path = |segment: &str| {
        let mut nested = path.to_vec();
        nested.push(segment.to_string());
        nested
    };
    match (current, target) {
        (DataType::Struct(old), DataType::Struct(new)) => {
            collect_evolution(old, new, path, operations)
        }
        (DataType::Array(old), DataType::Array(new)) if old.contains_null == new.contains_null => {
            collect_type_evolution(
                &old.element_type,
                &new.element_type,
                &nested_path("element"),
                operations,
            )
        }
        (DataType::Map(old), DataType::Map(new))
            if old.value_contains_null == new.value_contains_null =>
        {
            collect_type_evolution(
                &old.key_type,
                &new.key_type,
                &nested_path("key"),
                operations,
            )?;
            collect_type_evolution(
                &old.value_type,
                &new.value_type,
                &nested_path("value"),
                operations,
            )
        }
        _ if current == target => Ok(()),
        _ => Err(evolution_error(path)),
    }
}

fn evolution_error(path: &[String]) -> KernelError {
    KernelError::schema(format!(
        "Unsupported schema evolution at '{}': existing fields must retain their names, order, \
         types, and metadata and cannot become non-nullable. Use whole-table schema overwrite \
         for an incompatible target schema",
        ColumnName::new(path)
    ))
}

struct OverwriteAnnotationValidator;

impl<'a> SchemaTransform<'a> for OverwriteAnnotationValidator {
    transform_output_type!(|'a, T| KernelResult<()>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> KernelResult<()> {
        if let Some(key) = field.metadata().keys().find(|key| {
            key.as_str().starts_with("delta.columnMapping.")
                || key.as_str().starts_with("delta.identity.")
                || key.as_str() == ColumnMetadataKey::GenerationExpression.as_ref()
                || key.as_str() == ColumnMetadataKey::CurrentDefault.as_ref()
                || key.as_str() == "EXISTS_DEFAULT"
                || key.as_str() == ColumnMetadataKey::TypeChanges.as_ref()
        }) {
            return Err(KernelError::unsupported(format!(
                "Schema overwrite does not support column annotation '{key}' on '{}'",
                field.name()
            )));
        }
        self.recurse_into_struct_field(field)
    }
}
