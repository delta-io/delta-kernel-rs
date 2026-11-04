//! Physical metadata compatibility and typed projections shared by action consumers.

use std::sync::Arc;

use super::{ActionOptions, ActionPlan, action_schema, merge_field, merge_schema};
use crate::action_reconciliation::file_metadata::{
    PARTITION_VALUES_FIELD, PARTITION_VALUES_PARSED_FIELD, STATS_FIELD, STATS_JSON_EXPR,
    stats_with_json_fallback,
};
use crate::actions::{ADD_FIELD, ADD_NAME, STATS_PARSED};
use crate::checkpoint::collation_stats::source_collation_stats;
use crate::expressions::{ColumnName, Expression as Expr, col, column_name, null_lit};
use crate::log_segment::LogSegment;
use crate::schema::{
    CheckpointFieldKind, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::table_configuration::TableConfiguration;
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, Snapshot};

/// Reads selected actions and compatible metadata, returning facts about the physical read.
pub(crate) fn read_source(
    table_config: Option<&TableConfiguration>,
    options: &ActionOptions,
    available: &StructType,
    source: impl FnOnce(SchemaRef) -> KernelResult<crate::PlanBuilder>,
) -> KernelResult<ActionPlan> {
    let read_schema = read_schema(table_config, options, available)?;
    let actual = read_options(options, &read_schema, available);
    Ok(ActionPlan {
        plan: source(read_schema)?,
        options: actual,
    })
}

/// Populates typed file metadata while retaining raw fields and compatible native extensions.
pub(crate) fn parse_file_metadata(
    snapshot: &Snapshot,
    mut input: ActionPlan,
    options: &ActionOptions,
    prefer_json_stats: bool,
) -> KernelResult<ActionPlan> {
    let (stats_schema, partition_schema) =
        metadata_schemas(Some(snapshot.table_configuration()), options)?;
    if !input
        .options
        .actions
        .iter()
        .any(|kind| matches!(kind, crate::actions::ActionKind::FileActions { .. }))
    {
        return Ok(input);
    }
    for (name, anchor, requested) in [
        (
            STATS_PARSED,
            STATS_FIELD,
            stats_schema
                .as_ref()
                .filter(|_| options.parsed_stats.is_some()),
        ),
        (
            PARTITION_VALUES_PARSED_FIELD,
            PARTITION_VALUES_FIELD,
            partition_schema.as_ref(),
        ),
    ] {
        let Some(requested) = requested else { continue };
        let original = input
            .plan
            .schema()
            .field_at(&ColumnName::new([ADD_NAME, name]))
            .ok()
            .cloned();
        let native = original.as_ref().and_then(|field| match field.data_type() {
            DataType::Struct(schema) => Some(Arc::new(schema.as_ref().clone())),
            _ => None,
        });
        let has_json = input.options.json_stats;
        let (target, native) = match native {
            Some(native) => match merge_schema(requested, &native) {
                Ok(target) => (target, Some(native)),
                Err(_) if name != STATS_PARSED || has_json => (requested.clone(), None),
                Err(error) => return Err(error),
            },
            None => (requested.clone(), None),
        };
        let fallback = if name == STATS_PARSED {
            if has_json {
                Expr::parse_json(col!(ADD_NAME, STATS_FIELD), target.clone())
            } else {
                null_lit(target.clone())
            }
        } else {
            Expr::map_to_struct(col!(ADD_NAME, PARTITION_VALUES_FIELD), Default::default())
        };
        let widened = native.as_ref().is_some_and(|native| native != &target);
        let helper = "_kernel_metadata_fallback";
        if widened {
            input.plan = input.plan.project_patch(|patch| {
                patch.append(
                    StructField::nullable(helper, target.clone()),
                    fallback.clone(),
                )
            })?;
        }
        let native_expr = match native.as_deref() {
            Some(native) if widened => fill_missing_fields(
                native,
                &target,
                ColumnName::new([ADD_NAME, name]),
                ColumnName::new([helper]),
            ),
            Some(_) => Expr::column([ADD_NAME, name]),
            None => fallback.clone(),
        };
        let expr = if name == STATS_PARSED && native.is_some() && has_json {
            let parsed = if widened {
                Expr::column([helper])
            } else {
                fallback
            };
            stats_with_json_fallback(native_expr, parsed, prefer_json_stats)
        } else if name == PARTITION_VALUES_PARSED_FIELD && native.is_some() && !widened {
            Expr::coalesce([native_expr, fallback])
        } else {
            native_expr
        };
        input.plan = input.plan.project_patch(|patch| {
            let field = StructField::nullable(name, target.clone());
            let patch = if original.is_some() {
                patch.replace_at([ADD_NAME], name, field, expr)
            } else {
                patch.insert_after_at([ADD_NAME], anchor, field, expr)
            };
            if widened { patch.drop(helper) } else { patch }
        })?;
        if name == STATS_PARSED {
            input.options.parsed_stats = Some(target);
        } else {
            input.options.parsed_partition_values = true;
        }
    }
    Ok(input)
}

/// Fills missing JSON from complete structured stats, preserving existing JSON.
/// Call before pruning structured fields to the requested output schema.
pub(crate) fn synthesize_json_stats(mut input: ActionPlan) -> KernelResult<ActionPlan> {
    if input.options.parsed_stats.is_none() {
        return Ok(input);
    }
    input.plan = input.plan.project_patch(|patch| {
        if input.options.json_stats {
            patch.replace_expr_at([ADD_NAME], STATS_FIELD, STATS_JSON_EXPR.clone())
        } else {
            patch.insert_after_at(
                [ADD_NAME],
                "dataChange",
                StructField::nullable(STATS_FIELD, DataType::STRING),
                Expr::unary(
                    crate::expressions::UnaryExpressionOp::ToJson,
                    col!(ADD_NAME, STATS_PARSED),
                ),
            )
        }
    })?;
    input.options.json_stats = true;
    Ok(input)
}

/// Records the representation actually read, including fallback JSON and physical remove rows.
pub(super) fn read_options(
    requested: &ActionOptions,
    read: &StructType,
    available: &StructType,
) -> ActionOptions {
    let mut actual = requested.clone();
    for kind in &mut actual.actions {
        if let crate::actions::ActionKind::FileActions { includes_removes } = kind {
            *includes_removes &= available.contains(crate::actions::REMOVE_NAME);
        }
    }
    actual.json_stats = read.field_at(&column_name!(ADD_NAME, STATS_FIELD)).is_ok();
    actual.parsed_stats = read
        .field_at(&column_name!(ADD_NAME, STATS_PARSED))
        .ok()
        .and_then(|field| match field.data_type() {
            DataType::Struct(schema) => Some(Arc::new(schema.as_ref().clone())),
            _ => None,
        });
    actual.parsed_partition_values = read
        .field_at(&column_name!(ADD_NAME, PARTITION_VALUES_PARSED_FIELD))
        .is_ok();
    actual
}

/// Keeps requested read types for fields physically present in the source, without null padding.
fn native_read_schema(actual: &StructType, requested: &StructType) -> KernelResult<SchemaRef> {
    let mut fields = Vec::new();
    for field in requested.fields() {
        let Some(actual) = actual.field(field.name()) else {
            continue;
        };
        fields.push(match (actual.data_type(), field.data_type()) {
            (DataType::Struct(actual), DataType::Struct(requested)) => {
                StructField::nullable(field.name(), native_read_schema(actual, requested)?)
            }
            _ => field.clone(),
        });
    }
    Ok(Arc::new(StructType::try_new(fields)?))
}

/// Projects a compatible union schema, filling schema-absent fields without replacing native NULLs.
fn fill_missing_fields(
    native: &StructType,
    target: &StructType,
    native_path: ColumnName,
    fallback_path: ColumnName,
) -> Expr {
    let values = target.fields().map(|field| {
        let child = |path: &ColumnName| path.join(&ColumnName::new([field.name()]));
        match (
            native.field(field.name()).map(StructField::data_type),
            field.data_type(),
        ) {
            (Some(DataType::Struct(actual)), DataType::Struct(target)) => {
                fill_missing_fields(actual, target, child(&native_path), child(&fallback_path))
            }
            (Some(_), _) => Expr::column(child(&native_path)),
            (None, _) => Expr::column(child(&fallback_path)),
        }
    });
    let values = Expr::struct_from(values);
    Expr::case_when(
        [(Expr::column(native_path).is_not_null(), values)],
        Expr::column(fallback_path),
    )
}

fn metadata_schemas(
    table_config: Option<&TableConfiguration>,
    options: &ActionOptions,
) -> KernelResult<(Option<SchemaRef>, Option<SchemaRef>)> {
    let partitions = if options.parsed_partition_values
        && options
            .actions
            .iter()
            .any(|kind| matches!(kind, crate::actions::ActionKind::FileActions { .. }))
    {
        table_config.and_then(TableConfiguration::build_partition_values_parsed_schema)
    } else {
        None
    };
    let stats_schema = match (options.json_stats, table_config) {
        // JSON must preserve statistics outside a scan's requested or indexed columns.
        (true, Some(config)) => {
            let columns = config
                .physical_data_schema_without_partition_columns()
                .leaves(None);
            let builder = config
                .stats_schema_builder()
                .with_required_physical_columns(Some(columns.as_ref().0))
                .with_variant_min_max(true);
            #[cfg(feature = "geo-type-in-dev")]
            let builder = builder.with_geometry_min_max(true);
            let complete = builder.build()?;
            Some(match options.parsed_stats.as_ref() {
                Some(requested) => merge_schema(requested, &complete)?,
                None => complete,
            })
        }
        _ => options.parsed_stats.clone(),
    };
    Ok((stats_schema, partitions))
}

fn read_schema(
    table_config: Option<&TableConfiguration>,
    options: &ActionOptions,
    available: &StructType,
) -> KernelResult<SchemaRef> {
    let actions = action_schema(&options.actions)?;
    let (stats_schema, partition_schema) = metadata_schemas(table_config, options)?;
    let include_collation_stats =
        options.json_stats && table_config.is_some_and(TableConfiguration::has_collations_feature);
    if !actions.contains(ADD_NAME) {
        return Ok(actions);
    }
    let add = available.field(ADD_NAME).unwrap_or(&ADD_FIELD);
    let DataType::Struct(native) = add.data_type() else {
        return Err(KernelError::schema(
            "Metadata source 'add' must be a struct",
        ));
    };
    let requested_add = actions.field_at(&column_name!(ADD_NAME))?;
    let DataType::Struct(requested_add) = requested_add.data_type() else {
        return Err(KernelError::schema("Requested 'add' must be a struct"));
    };
    let mut patch = SchemaStructPatchBuilder::new();
    if stats_schema.is_none() && !options.json_stats {
        patch = patch.drop(STATS_FIELD);
    }
    for (name, anchor, required, kind) in [
        (
            STATS_PARSED,
            STATS_FIELD,
            stats_schema.as_ref(),
            CheckpointFieldKind::Stats,
        ),
        (
            PARTITION_VALUES_PARSED_FIELD,
            PARTITION_VALUES_FIELD,
            partition_schema.as_ref(),
            CheckpointFieldKind::PartitionValues,
        ),
    ] {
        let Some(required) = required else { continue };
        let Some(DataType::Struct(actual)) = native.field(name).map(StructField::data_type) else {
            continue;
        };
        if !LogSegment::structs_have_compatible_types(actual, required, name, kind) {
            continue;
        }
        let required = required.clone().try_fold_with(
            (name == STATS_PARSED && include_collation_stats)
                .then(|| source_collation_stats(available))
                .flatten(),
            |schema, field| {
                let patch = match schema.field(field.name()) {
                    Some(requested) => SchemaStructPatchBuilder::new()
                        .replace(field.name(), merge_field(requested, field)?),
                    None => SchemaStructPatchBuilder::new().append(field.clone()),
                };
                patch.build(&schema).map(Arc::new)
            },
        )?;
        // Do not pad missing native fields here: conversion needs to distinguish an absent
        // field from a field whose value is NULL in an authoritative native struct.
        let read = native_read_schema(actual, &required)?;
        patch = patch.insert_after(anchor, StructField::nullable(name, read));
    }
    let read_schema = Arc::new(
        SchemaStructPatchBuilder::new()
            .replace(
                ADD_NAME,
                StructField::nullable(ADD_NAME, patch.build(requested_add)?),
            )
            .build(&actions)?,
    );
    Ok(read_schema)
}
