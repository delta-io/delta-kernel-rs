//! Native scan lowering using DataFusion file sources.
//!
//! ## Parquet column matching (`PARQUET:field_id`)
//!
//! Kernel logical schemas encode parquet native field IDs on Arrow fields as `PARQUET:field_id`
//! during conversion from kernel `StructType`. The vendored [`ParquetSource`] / `ParquetOpener`
//! adapts decoded physical parquet columns **by ID first, then by name** at the Arrow-schema root
//! level—matching Delta Kernel parquet handlers for flat reads when writers kept stable IDs across
//! physical renames/reordering.
//!
//! When **no** root-level field IDs are present on the logical schema, behavior stays name-only
//! (upstream DataFusion).
//!
//! **Limits:** nested struct children are not independently matched by parquet field ID in this
//! path. Predicate pushdown inside the parquet decoder still keys off physical parquet statistics
//! paths; declarative scans prefer residual native filters after decode to avoid
//! over-pushdown.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{create_physical_expr, LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::object_store::path::Path as StorePath;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::FileMeta;
use parquet::arrow::RowNumber;

use crate::compile::expr_translator;
use crate::exec::NullabilityValidationExec;

/// Hidden ordinal column appended to each child of an ordered union; sorted on and then dropped.
const ORDINAL_COL: &str = "__dk_ord";

/// Build an ordered union over `children` using stock DataFusion operators.
///
/// Each child is projected to append a literal i64 [`ORDINAL_COL`] equal to its index in
/// `children`. The projections are unioned, coalesced to a single partition, sorted ASC by
/// ordinal, then re-projected to drop the ordinal. This preserves declaration order of children
/// without a custom executor.
///
/// Children must share an identical Arrow output schema (Union semantics already require this).
pub(crate) fn compile_ordered_union_via_ordinal(
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>, datafusion_common::DataFusionError> {
    let Some(first) = children.first() else {
        return Err(datafusion_common::DataFusionError::Plan(
            "ordered union requires at least one child".into(),
        ));
    };
    let child_schema = first.schema();

    let mut tagged: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());
    for (idx, child) in children.iter().enumerate() {
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(child.schema().fields().len() + 1);
        for (col_idx, field) in child.schema().fields().iter().enumerate() {
            let column = Arc::new(Column::new(field.name(), col_idx)) as Arc<dyn PhysicalExpr>;
            exprs.push((column, field.name().to_string()));
        }
        let ordinal_lit =
            Arc::new(Literal::new(ScalarValue::Int64(Some(idx as i64)))) as Arc<dyn PhysicalExpr>;
        exprs.push((ordinal_lit, ORDINAL_COL.to_string()));
        let projected = ProjectionExec::try_new(exprs, Arc::clone(child))?;
        tagged.push(Arc::new(projected));
    }

    let unioned: Arc<dyn ExecutionPlan> = UnionExec::try_new(tagged)?;
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(unioned));

    let ordinal_idx = coalesced.schema().index_of(ORDINAL_COL).map_err(|e| {
        datafusion_common::DataFusionError::Plan(format!(
            "ordered union: ordinal column `{ORDINAL_COL}` missing after union: {e}"
        ))
    })?;
    let ordinal_col = Arc::new(Column::new(ORDINAL_COL, ordinal_idx)) as Arc<dyn PhysicalExpr>;
    let sort_expr = PhysicalSortExpr::new_default(ordinal_col).asc();
    let lex = LexOrdering::new(std::iter::once(sort_expr)).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(
            "ordered union: failed to build LexOrdering for ordinal column".into(),
        )
    })?;
    let sorted: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(lex, coalesced as Arc<dyn ExecutionPlan>));

    // Drop the ordinal column by projecting back to the original child schema columns by index.
    let drop_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = child_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            (
                Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                field.name().to_string(),
            )
        })
        .collect();
    let final_proj = ProjectionExec::try_new(drop_exprs, sorted)?;
    Ok(Arc::new(final_proj))
}

struct PreparedScanFiles {
    file_group: FileGroup,
    object_store_url: ObjectStoreUrl,
}

fn file_meta_to_partitioned(file: &FileMeta) -> Result<PartitionedFile, DeltaError> {
    let location = if file.location.scheme() == "file" {
        let fs_path = file.location.to_file_path().map_err(|()| {
            crate::error::plan_compilation(format!("file URL is not local path: {}", file.location))
        })?;
        StorePath::from_absolute_path(&fs_path).map_err(|e| {
            crate::error::plan_compilation(format!(
                "object store path for {}: {e}",
                fs_path.display()
            ))
        })?
    } else {
        StorePath::from(file.location.path())
    };
    let mut pf = PartitionedFile::new(location, file.size);
    pf.object_meta.last_modified = Utc
        .timestamp_millis_opt(file.last_modified)
        .single()
        .unwrap_or_else(|| Utc.timestamp_nanos(0));
    Ok(pf)
}

fn prepare_scan_files(files: &[FileMeta]) -> Result<PreparedScanFiles, DeltaError> {
    let first = files
        .first()
        .ok_or_else(|| crate::error::plan_compilation("Scan node has no files"))?;

    let partitioned_files = files
        .iter()
        .map(file_meta_to_partitioned)
        .collect::<Result<Vec<_>, DeltaError>>()?;

    let base_url = format!(
        "{}://{}",
        first.location.scheme(),
        first.location.host_str().unwrap_or("")
    );
    let object_store_url =
        ObjectStoreUrl::parse(&base_url).map_err(crate::error::datafusion_err_to_delta)?;

    Ok(PreparedScanFiles {
        file_group: FileGroup::new(partitioned_files),
        object_store_url,
    })
}

fn parquet_scan_arrow_schema_and_virtual_columns(
    node: &ScanNode,
    kernel_arrow_schema: Arc<Schema>,
) -> Result<(Arc<Schema>, Vec<Arc<Field>>), DeltaError> {
    if node.file_type != FileType::Parquet {
        return Ok((kernel_arrow_schema, Vec::new()));
    }
    let Some(name) = node.row_index_column.as_ref() else {
        return Ok((kernel_arrow_schema, Vec::new()));
    };

    let row_field =
        Arc::new(Field::new(name.as_str(), DataType::Int64, false).with_extension_type(RowNumber));
    let mut fields = kernel_arrow_schema
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::clone(&row_field));
    Ok((Arc::new(Schema::new(fields)), vec![row_field]))
}

/// Schema used by [`NullabilityValidationExec`] on the scan output stream.
///
/// Parquet virtual row-index columns are not part of the kernel [`ScanNode::schema`], but the
/// decoded batch includes them. The validation layer zips `target_schema` fields to batch columns;
/// the target must therefore list the row-index field when native Parquet row numbers are enabled.
fn nullability_target_arrow_schema(
    node: &ScanNode,
    target_arrow_schema: Arc<Schema>,
) -> Arc<Schema> {
    if node.file_type != FileType::Parquet {
        return target_arrow_schema;
    }
    let Some(name) = node.row_index_column.as_ref() else {
        return target_arrow_schema;
    };
    let row_field =
        Arc::new(Field::new(name.as_str(), DataType::Int64, false).with_extension_type(RowNumber));
    let mut fields: Vec<_> = target_arrow_schema.fields().iter().cloned().collect();
    fields.push(row_field);
    Arc::new(Schema::new(fields))
}

/// DataFusion file sources can fail planning when decoded nested field nullability is wider than
/// the declared scan schema (for example nullable parquet/json children flowing into kernel
/// protocol NOT NULL children). Scan with a relaxed nested schema, then re-apply target
/// nullability in [`NullabilityValidationExec`].
pub(crate) fn relax_nested_nullability_for_scan(schema: &Schema) -> Arc<Schema> {
    fn relax_field(field: &Arc<Field>, force_nullable: bool) -> Arc<Field> {
        let relaxed_dt = relax_data_type(field.data_type());
        Arc::new(
            Field::new(
                field.name(),
                relaxed_dt,
                if force_nullable {
                    true
                } else {
                    field.is_nullable()
                },
            )
            .with_metadata(field.metadata().clone()),
        )
    }

    fn relax_data_type(dt: &DataType) -> DataType {
        match dt {
            DataType::Struct(fields) => {
                let relaxed: Vec<Arc<Field>> =
                    fields.iter().map(|f| relax_field(f, true)).collect();
                DataType::Struct(relaxed.into())
            }
            DataType::List(inner) => DataType::List(relax_field(inner, true)),
            DataType::LargeList(inner) => DataType::LargeList(relax_field(inner, true)),
            DataType::FixedSizeList(inner, n) => {
                DataType::FixedSizeList(relax_field(inner, true), *n)
            }
            DataType::Map(entries, sorted) => {
                // Keep map key nullability constraints intact; only relax values recursively.
                let relaxed_entries = match entries.data_type() {
                    DataType::Struct(entry_fields) if entry_fields.len() == 2 => {
                        let key = relax_field(&entry_fields[0], false);
                        let val = relax_field(&entry_fields[1], true);
                        Arc::new(
                            Field::new(
                                entries.name(),
                                DataType::Struct(vec![key, val].into()),
                                entries.is_nullable(),
                            )
                            .with_metadata(entries.metadata().clone()),
                        )
                    }
                    _ => relax_field(entries, true),
                };
                DataType::Map(relaxed_entries, *sorted)
            }
            other => other.clone(),
        }
    }

    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| relax_field(f, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_raw_scan(
    node: &ScanNode,
    prepared: PreparedScanFiles,
    arrow_schema: Arc<Schema>,
    parquet_virtual_columns: Vec<Arc<Field>>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let table_schema = TableSchema::new(arrow_schema, Vec::new());

    let scan: Arc<dyn ExecutionPlan> = match node.file_type {
        FileType::Parquet => {
            let mut source = ParquetSource::new(table_schema.clone());
            if !parquet_virtual_columns.is_empty() {
                source = source
                    .with_virtual_columns(parquet_virtual_columns)
                    .map_err(crate::error::datafusion_err_to_delta)?;
            }
            let source = Arc::new(source);
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
        FileType::Json => {
            let source = Arc::new(JsonSource::new(table_schema));
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
    };
    Ok(scan)
}

/// Applies nullability coercion, residual native filter when [`ScanNode::predicate`] is set, and
/// optional row index augmentation.
///
/// ## Predicate
///
/// The scan predicate is evaluated **only** as a residual native DataFusion [`FilterExec`] on
/// decoded record batches. Native Parquet / JSON readers are not given pushdown predicates here,
/// which avoids over-filtering relative to [`ScanNode`] semantics. NULL predicate rows are kept by
/// compiling `coalesce(predicate, true)`.
///
/// ## Row index
///
/// Parquet scans with [`ScanNode::row_index_column`] decode arrow-rs virtual [`RowNumber`] columns
/// in the file reader so indices are physical offsets **before** filtering; residual filtering
/// then masks rows without rewriting indices on survivors. JSON scans append contiguous indices
/// with [`RowIndexExec`] **after** decoding (and after any scan predicate filter).
///
/// Virtual Parquet row numbers are file-absolute in arrow-rs across batches from the same file.
fn wrap_scan_extensions(
    scan: Arc<dyn ExecutionPlan>,
    node: &ScanNode,
    arrow_schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let scan = Arc::new(NullabilityValidationExec::new(
        scan,
        Vec::new(),
        arrow_schema,
    ));

    let scan: Arc<dyn ExecutionPlan> = if let Some(pred) = &node.predicate {
        compile_residual_scan_filter_native(scan, &node.schema, pred.as_ref())?
    } else {
        scan
    };

    let native_parquet_row_index =
        node.file_type == FileType::Parquet && node.row_index_column.is_some();

    if node.row_index_column.is_some() && !native_parquet_row_index {
        return Err(crate::error::unsupported(
            "compile/scan: JSON+row-index combination is not supported by the physical scan \
             compile path; build the plan via the logical path which uses LogicalPlanBuilder \
             window_plan(row_number()) instead",
        ));
    }
    Ok(scan)
}

fn compile_scan_single_group(
    node: &ScanNode,
    files: &[FileMeta],
    source_arrow_schema: Arc<Schema>,
    target_arrow_schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let (scan_arrow_schema, virtual_cols) =
        parquet_scan_arrow_schema_and_virtual_columns(node, source_arrow_schema)?;
    let prepared = prepare_scan_files(files)?;
    let scan = build_raw_scan(node, prepared, scan_arrow_schema.clone(), virtual_cols)?;
    let validation_target = nullability_target_arrow_schema(node, target_arrow_schema);
    wrap_scan_extensions(scan, node, validation_target)
}

/// Convert kernel [`ScanNode`] into DataFusion physical scan.
///
/// ## Multi-file layout
///
/// When [`ScanNode::row_index_column`] is [`Some`], indices must restart at zero for each file.
/// That requires one native scan subtree per file (joined with
/// [`compile_ordered_union_via_ordinal`]), even if [`ScanNode::ordered`] is `false`. Parallel
/// multi-file grouping is therefore disabled whenever a row-index column is requested; file
/// emission order still follows [`ScanNode::files`].
///
/// When **both** `ordered == false` **and** `row_index_column.is_none()`, all files share one
/// native file group so DataFusion may parallelize partition scheduling across files (unordered
/// scan).
///
/// ## Predicate
///
/// [`ScanNode::predicate`] is applied exclusively via a residual native [`FilterExec`] after decode
/// (no reader pushdown). Engines may later add conservative pushdown optimizations while retaining
/// this filter as a residual guardrail.
///
/// **Why no pushdown:** kernel NULL semantics keep a row when a predicate references a NULL value
/// (vs SQL three-valued logic, which drops it). DataFusion's parquet filter pushdown applies SQL
/// semantics, so enabling it would silently change kernel behavior. Future contributors: pushdown
/// MUST stay disabled at the scan layer; opt-in pushdown belongs only behind kernel-side
/// null-aware filter rewriting.
///
/// ## Row-index implementation
///
/// Parquet uses arrow-rs [`RowNumber`] virtual columns decoded with each batch. JSON uses
/// [`RowIndexExec`].
pub fn compile_scan(node: &ScanNode) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let target_arrow_schema: Arc<Schema> = Arc::new(
        node.schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| crate::error::plan_compilation(format!("scan schema conversion: {e}")))?,
    );
    let source_arrow_schema = relax_nested_nullability_for_scan(target_arrow_schema.as_ref());

    let sequential_files = node.ordered || node.row_index_column.is_some();

    if node.files.len() <= 1 || !sequential_files {
        return compile_scan_single_group(
            node,
            &node.files,
            source_arrow_schema,
            target_arrow_schema,
        );
    }

    let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(node.files.len());
    for file in &node.files {
        let branch = compile_scan_single_group(
            node,
            std::slice::from_ref(file),
            source_arrow_schema.clone(),
            target_arrow_schema.clone(),
        )?;
        children.push(Arc::new(CoalescePartitionsExec::new(branch)));
    }

    compile_ordered_union_via_ordinal(children).map_err(crate::error::datafusion_err_to_delta)
}

fn compile_residual_scan_filter_native(
    child_plan: Arc<dyn ExecutionPlan>,
    input_schema: &delta_kernel::schema::SchemaRef,
    predicate: &delta_kernel::expressions::Expression,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let arrow_schema: delta_kernel::arrow::datatypes::Schema = input_schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| crate::error::plan_compilation(format!("scan filter schema to Arrow: {e}")))?;
    let df_schema = DFSchema::try_from(arrow_schema)
        .map_err(|e| crate::error::plan_compilation(format!("scan filter DFSchema: {e}")))?;
    let props = ExecutionProps::new();
    let logical = expr_translator::kernel_expr_to_df(predicate)?;
    let logical_keep_nulls = logical.clone().or(logical.is_null());
    let physical = create_physical_expr(&logical_keep_nulls, &df_schema, &props)
        .map_err(|e| crate::error::plan_compilation(format!("scan filter expression: {e}")))?;
    let native =
        FilterExec::try_new(physical, child_plan).map_err(crate::error::datafusion_err_to_delta)?;
    Ok(Arc::new(native))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
    use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
    use delta_kernel::expressions::{column_expr, Expression, Predicate};
    use delta_kernel::plans::ir::DeclarativePlanNode;
    use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};
    use delta_kernel::FileMeta;
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use super::compile_scan;
    use crate::DataFusionExecutor;

    fn kernel_schema_one_i64() -> Arc<StructType> {
        Arc::new(StructType::try_new([StructField::new("x", KernelDataType::LONG, false)]).unwrap())
    }

    fn write_i64_parquet(path: &std::path::Path, values: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = ArrowRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(
                values.iter().copied(),
            ))],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn file_meta(path: &std::path::Path) -> FileMeta {
        FileMeta::new(
            Url::from_file_path(path).unwrap(),
            0,
            std::fs::metadata(path).unwrap().len(),
        )
    }

    fn root_plan(scan: DeclarativePlanNode) -> delta_kernel::plans::ir::Plan {
        scan.into_results()
    }

    #[tokio::test]
    async fn row_index_resets_each_file() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("first.parquet");
        let p2 = dir.path().join("second.parquet");
        write_i64_parquet(&p1, &[10, 11]);
        write_i64_parquet(&p2, &[20]);

        let schema = kernel_schema_one_i64();
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p1), file_meta(&p2)],
                Arc::clone(&schema),
            )
            .with_row_index("rid")
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();
        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let xs: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(x_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let rids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(rid_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(xs, vec![10, 11, 20]);
        assert_eq!(rids, vec![0, 1, 0]);
    }

    #[tokio::test]
    async fn ordered_scan_emits_files_in_declaration_order() {
        let dir = tempfile::tempdir().unwrap();
        let p_low = dir.path().join("low.parquet");
        let p_high = dir.path().join("high.parquet");
        write_i64_parquet(&p_low, &[1]);
        write_i64_parquet(&p_high, &[2]);

        let schema = kernel_schema_one_i64();
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p_high), file_meta(&p_low)],
                Arc::clone(&schema),
            )
            .with_ordered()
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();

        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let xs: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(x_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(xs, vec![2, 1]);
    }

    #[tokio::test]
    async fn scan_predicate_filters_without_extra_rows() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.parquet");
        let p2 = dir.path().join("b.parquet");
        write_i64_parquet(&p1, &[5, 8]);
        write_i64_parquet(&p2, &[25, 30]);

        let schema = kernel_schema_one_i64();
        let pred = Arc::new(Expression::from_pred(Predicate::gt(
            column_expr!("x"),
            Expression::literal(delta_kernel::expressions::Scalar::Long(10)),
        )));
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p1), file_meta(&p2)],
                Arc::clone(&schema),
            )
            .with_predicate(pred)
            .unwrap()
            .with_row_index("rid")
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();
        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let xs: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(x_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let rids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(rid_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(xs, vec![25, 30]);
        assert_eq!(rids, vec![0, 1]);
    }

    #[tokio::test]
    async fn parquet_row_index_physical_offsets_survive_residual_filter() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rows.parquet");
        write_i64_parquet(&p, &[8, 25, 30]);

        let schema = kernel_schema_one_i64();
        let pred = Arc::new(Expression::from_pred(Predicate::gt(
            column_expr!("x"),
            Expression::literal(delta_kernel::expressions::Scalar::Long(10)),
        )));
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(vec![file_meta(&p)], Arc::clone(&schema))
                .with_predicate(pred)
                .unwrap()
                .with_row_index("rid")
                .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();
        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let xs: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(x_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let rids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(rid_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(xs, vec![25, 30]);
        assert_eq!(rids, vec![1, 2]);
    }

    fn plan_has_ordinal_sort(plan: &dyn datafusion_physical_plan::ExecutionPlan) -> bool {
        if plan.name() == "SortExec"
            && plan
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == super::ORDINAL_COL)
        {
            return true;
        }
        plan.children()
            .iter()
            .any(|c| plan_has_ordinal_sort(c.as_ref()))
    }

    #[test]
    fn parallel_multi_file_scan_has_no_ordered_union_without_row_index_or_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("x.parquet");
        let p2 = dir.path().join("y.parquet");
        write_i64_parquet(&p1, &[1]);
        write_i64_parquet(&p2, &[2]);

        let schema = kernel_schema_one_i64();
        let node = DeclarativePlanNode::scan_parquet(vec![file_meta(&p1), file_meta(&p2)], schema);
        let scan = match node {
            DeclarativePlanNode::Scan(s) => s,
            _ => unreachable!(),
        };
        let physical = compile_scan(&scan).unwrap();
        assert!(
            !plan_has_ordinal_sort(physical.as_ref()),
            "unordered scan without row index should stay a single native file group"
        );
    }

    #[test]
    fn row_index_multi_file_wraps_ordered_union() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("x.parquet");
        let p2 = dir.path().join("y.parquet");
        write_i64_parquet(&p1, &[1]);
        write_i64_parquet(&p2, &[2]);

        let schema = kernel_schema_one_i64();
        let node = DeclarativePlanNode::scan_parquet(vec![file_meta(&p1), file_meta(&p2)], schema)
            .with_row_index("rid")
            .unwrap();
        let scan = match node {
            DeclarativePlanNode::Scan(s) => s,
            _ => unreachable!(),
        };
        let physical = compile_scan(&scan).unwrap();
        assert!(plan_has_ordinal_sort(physical.as_ref()));
    }
}
