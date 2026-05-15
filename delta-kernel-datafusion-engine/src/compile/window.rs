//! Compile [`WindowNode`] to a DataFusion physical operator.
//!
//! Lowers `WindowNode { row_number, partition_by, order_by }` to a native DataFusion physical
//! window — a [`SortExec`] over `(partition_by ASC NULLS LAST, order_by)` feeding a
//! [`WindowAggExec`] whose [`row_number_udwf`] window expression carries the same
//! `partition_by` and `order_by`. Empty `order_by` is rejected at compile time so ranking is
//! always deterministic.
//!
//! Only `row_number()` is supported as a window function today (matches kernel IR's
//! [`WindowNode`] semantics). Lowering to DataFusion `LogicalPlan::Window` is out of scope here
//! and will land alongside the broader operator-level LogicalPlan migration.

use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion_functions_window::row_number::row_number_udwf;
use datafusion_physical_expr::expressions::{CastExpr, Column as PhysColumn};
use datafusion_physical_expr::{create_physical_expr, LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::arrow::compute::SortOptions;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{OrderingSpec, WindowNode};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

use crate::compile::expr_translator;

pub(crate) fn window_output_kernel_schema(
    input_schema: &SchemaRef,
    node: &WindowNode,
) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = input_schema.fields().cloned().collect();
    for wf in &node.functions {
        fields.push(StructField::new(
            wf.output_col.clone(),
            DataType::LONG,
            false,
        ));
    }
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| crate::error::plan_compilation(format!("window output schema: {e}")))
}

pub(crate) fn compile_window_node(
    child: Arc<dyn ExecutionPlan>,
    input_schema: SchemaRef,
    node: &WindowNode,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    if node.functions.is_empty() {
        return Err(crate::error::plan_compilation(
            "Window node must declare at least one window function",
        ));
    }
    if node.order_by.is_empty() {
        return Err(crate::error::unsupported(
            "Window(row_number): empty order_by is not supported; add ORDER BY columns for \
             deterministic row_number (for example a version or tie-break column)",
        ));
    }
    compile_window_native(child, input_schema, node)
}

/// Lower a `WindowNode { row_number, partition_by, order_by != [] }` to native DataFusion physical
/// `WindowAggExec` over a `SortExec` of `(partition_by ASC NULLS LAST, order_by)`.
fn compile_window_native(
    child: Arc<dyn ExecutionPlan>,
    input_schema: SchemaRef,
    node: &WindowNode,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let arrow_schema = kernel_to_arrow_schema(&input_schema)?;
    let dfschema = DFSchema::try_from(arrow_schema.as_ref().clone())
        .map_err(|e| crate::error::internal_error(format!("Window: build DFSchema: {e}")))?;
    // ExecutionProps carries time-of-evaluation context (e.g. `now()`); empty default is
    // fine for the simple expressions we lower here (Column / Literal / arithmetic / etc.).
    let _config = SessionConfig::new();
    let props = ExecutionProps::new();

    let partition_by_phys = translate_partition_by(&node.partition_by, &dfschema, &props)?;
    let order_by_phys = translate_order_by(&node.order_by, &arrow_schema)?;

    let sorted = sort_for_window(child, &partition_by_phys, &order_by_phys)?;
    let single_partition = coalesce_for_window(sorted);
    let window = build_window_exec(
        single_partition,
        arrow_schema,
        node,
        partition_by_phys,
        order_by_phys,
    )?;
    cast_window_outputs_to_long(window, node)
}

fn translate_partition_by(
    partition_by: &[Arc<delta_kernel::expressions::Expression>],
    dfschema: &DFSchema,
    props: &ExecutionProps,
) -> Result<Vec<Arc<dyn PhysicalExpr>>, DeltaError> {
    partition_by
        .iter()
        .map(|kexpr| {
            let logical = expr_translator::kernel_expr_to_df(kexpr.as_ref())?;
            create_physical_expr(&logical, dfschema, props).map_err(|e| {
                crate::error::internal_error(format!(
                    "Window PARTITION BY physical translation: {e}"
                ))
            })
        })
        .collect()
}

fn translate_order_by(
    order_by: &[OrderingSpec],
    arrow_schema: &ArrowSchemaRef,
) -> Result<Vec<PhysicalSortExpr>, DeltaError> {
    order_by
        .iter()
        .map(|spec| order_spec_to_phys(spec, arrow_schema))
        .collect()
}

fn order_spec_to_phys(
    spec: &OrderingSpec,
    arrow_schema: &ArrowSchemaRef,
) -> Result<PhysicalSortExpr, DeltaError> {
    let path: Vec<&str> = spec.column.iter().map(String::as_str).collect();
    let [name] = path.as_slice() else {
        return Err(crate::error::unsupported(format!(
            "Window ORDER BY: nested column path {} is not yet supported",
            spec.column
        )));
    };
    let idx = arrow_schema.index_of(name).map_err(|e| {
        crate::error::plan_compilation(format!(
            "Window ORDER BY column `{name}` not in input schema: {e}"
        ))
    })?;
    Ok(PhysicalSortExpr::new(
        Arc::new(PhysColumn::new(name, idx)),
        SortOptions {
            descending: spec.descending,
            nulls_first: spec.nulls_first,
        },
    ))
}

/// Wrap `child` in a [`SortExec`] producing `(partition_by ASC NULLS LAST, order_by)`. The
/// partition keys come first because [`WindowAggExec`] requires its input to be sorted such that
/// each `partition_by` group is a contiguous run; within a group the rows are then ordered by
/// `order_by`.
fn sort_for_window(
    child: Arc<dyn ExecutionPlan>,
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let mut combined: Vec<PhysicalSortExpr> =
        Vec::with_capacity(partition_by.len() + order_by.len());
    for p in partition_by {
        combined.push(PhysicalSortExpr::new(
            Arc::clone(p),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ));
    }
    combined.extend(order_by.iter().cloned());
    let lex = LexOrdering::new(combined).ok_or_else(|| {
        crate::error::internal_error(
            "Window: empty LexOrdering despite non-empty order_by (invariant violation)",
        )
    })?;
    Ok(Arc::new(SortExec::new(lex, child)))
}

/// Globally serialize the input ahead of `WindowAggExec`. `row_number()` must observe a single
/// stream of rows for its sequence to be correct; if the input is already single-partition this
/// is a no-op.
fn coalesce_for_window(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if input.output_partitioning().partition_count() <= 1 {
        return input;
    }
    Arc::new(CoalescePartitionsExec::new(input))
}

fn build_window_exec(
    input: Arc<dyn ExecutionPlan>,
    arrow_schema: ArrowSchemaRef,
    node: &WindowNode,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    // Default ORDER-BY frame for `row_number`: ROWS UNBOUNDED PRECEDING TO CURRENT ROW.
    // Passing `Some(true)` makes WindowFrame produce exactly that strict-rows shape.
    let frame = Arc::new(WindowFrame::new(Some(true)));
    let udwf = row_number_udwf();
    let mut window_exprs = Vec::with_capacity(node.functions.len());
    for f in &node.functions {
        let we = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(Arc::clone(&udwf)),
            f.output_col.clone(),
            &[],
            &partition_by,
            &order_by,
            Arc::clone(&frame),
            Arc::clone(&arrow_schema),
            false,
            false,
            None,
        )
        .map_err(|e| {
            crate::error::internal_error(format!(
                "Window: create_window_expr({}): {e}",
                f.output_col
            ))
        })?;
        window_exprs.push(we);
    }
    let exec = WindowAggExec::try_new(window_exprs, input, false).map_err(|e| {
        crate::error::internal_error(format!("Window: WindowAggExec::try_new: {e}"))
    })?;
    Ok(Arc::new(exec))
}

/// DataFusion's `row_number()` UDWF emits `UInt64`, but the kernel IR declares
/// [`WindowFunction::output_col`] as `LONG` (`Int64`). Add a final [`ProjectionExec`] that
/// passes the original input columns through unchanged and casts each row-number column to
/// `Int64` so the produced `RecordBatch` schema matches [`window_output_kernel_schema`].
fn cast_window_outputs_to_long(
    window: Arc<dyn ExecutionPlan>,
    node: &WindowNode,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let schema = window.schema();
    let total_cols = schema.fields().len();
    let num_window = node.functions.len();
    debug_assert!(
        total_cols >= num_window,
        "WindowAggExec must emit at least one column per declared function"
    );
    let input_col_count = total_cols - num_window;

    let mut proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(total_cols);
    for (idx, field) in schema.fields().iter().take(input_col_count).enumerate() {
        let col = Arc::new(PhysColumn::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
        proj_exprs.push((col, field.name().to_string()));
    }
    for (offset, field) in schema.fields().iter().skip(input_col_count).enumerate() {
        let idx = input_col_count + offset;
        let col = Arc::new(PhysColumn::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
        let casted = Arc::new(CastExpr::new(col, ArrowDataType::Int64, None));
        proj_exprs.push((casted, field.name().to_string()));
    }
    let proj = ProjectionExec::try_new(proj_exprs, window).map_err(|e| {
        crate::error::internal_error(format!("Window: cast row_number outputs to Int64: {e}"))
    })?;
    Ok(Arc::new(proj))
}

fn kernel_to_arrow_schema(input_schema: &SchemaRef) -> Result<ArrowSchemaRef, DeltaError> {
    let arrow_schema = input_schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| crate::error::internal_error(format!("Window: arrow schema: {e}")))?;
    Ok(Arc::new(arrow_schema))
}

