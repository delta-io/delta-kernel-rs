//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) lowering.

use std::sync::Arc;

use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::plans::ir::Plan;
use delta_kernel::schema::{DataType, MetadataColumnSpec, StructField, StructType};

use super::{compile_declarative_node, node_output_schema, CompileContext};
use crate::exec::KernelLoadSinkExec;

fn resolve_leaf_field(schema: &StructType, cn: &ColumnName) -> Result<StructField, DeltaError> {
    let parts = cn.path();
    let mut cur = schema;
    for (i, seg) in parts.iter().enumerate() {
        let field = cur.fields().find(|f| f.name() == seg).ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "upstream schema missing field `{}` while resolving `{}`",
                seg, cn
            ))
        })?;
        if i == parts.len() - 1 {
            return Ok(field.clone());
        }
        cur = match field.data_type() {
            DataType::Struct(inner) => inner.as_ref(),
            other => {
                return Err(crate::error::plan_compilation(format!(
                    "`{}` has non-struct type {other:?} while resolving `{}`",
                    seg, cn
                )));
            }
        };
    }
    Err(crate::error::plan_compilation(format!(
        "empty column path while resolving `{}`",
        cn
    )))
}

fn passthrough_output_field(
    upstream: &StructType,
    cn: &ColumnName,
) -> Result<StructField, DeltaError> {
    let leaf = resolve_leaf_field(upstream, cn)?;
    Ok(StructField::new(
        cn.to_string(),
        leaf.data_type().clone(),
        leaf.is_nullable(),
    ))
}

pub(super) fn expected_materialized_schema(
    load: &LoadSink,
    upstream: &StructType,
) -> Result<delta_kernel::schema::SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = load.file_schema.fields().cloned().collect();
    for cn in &load.passthrough_columns {
        fields.push(passthrough_output_field(upstream, cn)?);
    }
    StructType::try_new(fields).map(Arc::new).map_err(|e| {
        crate::error::plan_compilation(format!("invalid Load materialized schema: {e}"))
    })
}

pub(crate) fn physical_read_schema(
    load: &LoadSink,
) -> Result<delta_kernel::schema::SchemaRef, DeltaError> {
    Ok(load.file_schema.clone())
}

fn ensure_column(upstream: &StructType, cn: &ColumnName) -> Result<(), DeltaError> {
    resolve_leaf_field(upstream, cn).map(drop)
}

pub(super) fn validate_load_sink(load: &LoadSink, upstream: &StructType) -> Result<(), DeltaError> {
    if matches!(load.file_type, FileType::Json) && load.dv_ref.is_some() {
        return Err(crate::error::unsupported(
            "Load sink with Json file_type does not support dv_ref masking in the DataFusion engine",
        ));
    }
    if matches!(load.file_type, FileType::Json)
        && load
            .file_schema
            .contains_metadata_column(&MetadataColumnSpec::RowIndex)
    {
        return Err(crate::error::unsupported(
            "Load sink with Json file_type does not support RowIndex metadata column in file_schema in the DataFusion engine yet",
        ));
    }
    ensure_column(upstream, &load.file_meta.path)?;
    if let Some(ref s) = load.file_meta.size {
        ensure_column(upstream, s)?;
    }
    if let Some(ref rc) = load.file_meta.record_count {
        ensure_column(upstream, rc)?;
    }
    if let Some(ref dv) = load.dv_ref {
        ensure_column(upstream, &dv.column)?;
    }
    for cn in &load.passthrough_columns {
        ensure_column(upstream, cn)?;
    }

    let expected = expected_materialized_schema(load, upstream)?;
    if expected.as_ref() != load.output_relation.schema.as_ref() {
        return Err(crate::error::plan_compilation(format!(
            "LoadSink.output_relation.schema does not match expected materialization schema (expected {expected:?}, got {:?})",
            load.output_relation.schema
        )));
    }

    Ok(())
}

pub(super) fn compile_load_terminal(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let sink = match &plan.sink.sink_type {
        delta_kernel::plans::ir::nodes::SinkType::Load(s) => s,
        _ => unreachable!("compile_load_terminal requires Load sink"),
    };
    let upstream = node_output_schema(&plan.root)?;
    validate_load_sink(sink, upstream.as_ref())?;
    let inner = compile_declarative_node(&plan.root, ctx)?;
    Ok(Arc::new(KernelLoadSinkExec::try_new(
        inner,
        sink.clone(),
        Arc::clone(&ctx.engine),
        Arc::clone(&ctx.relation_registry),
        physical_read_schema(sink)?,
    )?))
}
