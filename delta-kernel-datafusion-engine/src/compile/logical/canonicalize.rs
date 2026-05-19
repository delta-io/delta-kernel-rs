//! Canonicalize a [`LogicalPlan`]'s output column names to match a kernel schema.

use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;

use crate::error::plan_compilation;

/// Project `plan`'s output columns so each name matches the corresponding field in
/// `kernel_schema`. The source plan must produce at least as many columns as `kernel_schema`
/// has fields; extra trailing columns are dropped.
pub(super) fn canonicalize_output_to_kernel_schema(
    plan: LogicalPlan,
    kernel_schema: &KernelSchemaRef,
) -> Result<LogicalPlan, DataFusionError> {
    let source_cols = plan.schema().columns();
    let target_len = kernel_schema.fields().count();
    if source_cols.len() < target_len {
        return Err(plan_compilation(format!(
            "canonicalization requires at least {target_len} source columns, found {}",
            source_cols.len()
        )));
    }
    LogicalPlanBuilder::from(plan)
        .project(
            kernel_schema
                .fields()
                .zip(source_cols)
                .map(|(field, source_col)| {
                    Expr::Column(source_col).alias(field.name().to_string())
                }),
        )?
        .build()
}
