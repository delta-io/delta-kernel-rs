use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, AsArray as _, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::get_row_at_idx;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{
    Expr as DFExpr, LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, TableType,
};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::ColumnName as KernelColumnName;
use delta_kernel::plans::ir::nodes::{
    DynamicScan as KernelDynamicScan, FileType as KernelFileType,
};
use delta_kernel::schema::StructType as KernelStructType;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use itertools::Itertools;

use crate::expression::struct_null_when_not;
use crate::parquet_expr_adapter::KernelParquetExprAdapterFactory;
use crate::scan::{build_scan_table_layout, validate_scan_schema, ScanFormat};
use crate::utils::column_to_df_expr;

/// Lowers a kernel [`DynamicScan`](KernelDynamicScan) into a DataFusion
/// [`LogicalPlan::TableScan`](DFLogicalPlan::TableScan).
///
/// `input` produces one metadata row for every file discovered at execution time. The table scan
/// retains that logical plan in a [`TableProvider`]. During physical planning,
/// [`TableProvider::scan`] plans the metadata input and pushes the requested output projection
/// into each runtime-discovered Parquet scan. Each file scan uses
/// [`KernelParquetExprAdapterFactory`] to reconcile its physical schema with the schema requested
/// by kernel.
///
/// # Errors
/// Returns an error if the scan is not Parquet, its configured input columns are invalid, or its
/// output schema requires unsupported metadata columns or Parquet field-ID resolution.
pub(crate) fn lower_dynamic_scan(
    scan: &KernelDynamicScan,
    input: &Arc<DFLogicalPlan>,
) -> Result<DFLogicalPlan, DataFusionError> {
    if scan.file_type != KernelFileType::Parquet {
        return Err(DataFusionError::NotImplemented(
            "DynamicScan only supports Parquet files in the DataFusion executor".to_string(),
        ));
    }

    let input_schema: KernelStructType = input.schema().as_arrow().try_into_kernel()?;
    scan.validate_input(&Arc::new(input_schema))
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

    let output_schema: ArrowSchemaRef = Arc::new(scan.schema.as_ref().try_into_arrow()?);
    validate_scan_schema(ScanFormat::Parquet, scan.schema.as_ref(), &output_schema)?;
    let (input, input_layout) = project_dynamic_scan_input(scan, input.as_ref())?;
    let provider = DynamicScanProvider {
        input: Arc::new(input),
        input_layout,
        scan: Arc::new(scan.clone()),
        output_schema,
    };
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    LogicalPlanBuilder::scan("dynamic_scan", source, None)?.build()
}

#[derive(Debug, Clone)]
struct DynamicScanInputLayout {
    path_index: usize,
    size_index: usize,
    last_modified_index: usize,
    dv_index: usize,
    constant_indices: Range<usize>,
}

fn project_dynamic_scan_input(
    scan: &KernelDynamicScan,
    input: &DFLogicalPlan,
) -> Result<(DFLogicalPlan, DynamicScanInputLayout), DataFusionError> {
    let schema = input.schema().as_ref();
    let mut dv = column_to_df_expr(&scan.dv_column, schema)?;
    let mut dv_guard: Option<DFExpr> = None;
    for depth in 1..scan.dv_column.path().len() {
        let ancestor = KernelColumnName::new(&scan.dv_column.path()[..depth]);
        let present = column_to_df_expr(&ancestor, schema)?.is_not_null();
        dv_guard = Some(match dv_guard {
            Some(guard) => guard.and(present),
            None => present,
        });
    }
    if let Some(guard) = dv_guard {
        dv = struct_null_when_not(guard, dv);
    }

    let column_count = 4 + scan.file_constant_columns.len();
    let mut columns = Vec::with_capacity(column_count);
    let path = column_to_df_expr(&scan.path_column, schema)?;
    let path_index = push_aliased_column(&mut columns, path, "__dynamic_scan_path");
    let size = column_to_df_expr(&scan.file_size_column, schema)?;
    let size_index = push_aliased_column(&mut columns, size, "__dynamic_scan_size");
    let last_modified = column_to_df_expr(&scan.last_modified_column, schema)?;
    let last_modified_index =
        push_aliased_column(&mut columns, last_modified, "__dynamic_scan_last_modified");
    let dv_index = push_aliased_column(&mut columns, dv, "__dynamic_scan_dv");

    let constants_start = columns.len();
    for (index, name) in scan.file_constant_columns.iter().enumerate() {
        let column = KernelColumnName::new([name.as_str()]);
        let expression = column_to_df_expr(&column, schema)?;
        let alias = format!("__dynamic_scan_constant_{index}");
        columns.push(expression.alias(alias));
    }
    let constant_indices = constants_start..columns.len();

    let plan = LogicalPlanBuilder::from(input.clone())
        .project(columns)?
        .build()?;
    let layout = DynamicScanInputLayout {
        path_index,
        size_index,
        last_modified_index,
        dv_index,
        constant_indices,
    };
    Ok((plan, layout))
}

fn push_aliased_column(
    columns: &mut Vec<DFExpr>,
    expression: DFExpr,
    alias: impl Into<String>,
) -> usize {
    let index = columns.len();
    columns.push(expression.alias(alias));
    index
}

#[derive(Debug)]
struct DynamicScanProvider {
    input: Arc<DFLogicalPlan>,
    input_layout: DynamicScanInputLayout,
    scan: Arc<KernelDynamicScan>,
    output_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for DynamicScanProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        requested_output_indices: Option<&Vec<usize>>,
        _filters: &[DFExpr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = state.create_physical_plan(self.input.as_ref()).await?;
        let scan = DynamicScanExec::try_new(
            input,
            self.input_layout.clone(),
            Arc::clone(&self.scan),
            Arc::clone(&self.output_schema),
            requested_output_indices.cloned(),
        )?;
        Ok(Arc::new(scan))
    }
}

#[derive(Debug)]
struct DynamicScanExec {
    input: Arc<dyn ExecutionPlan>,
    input_layout: DynamicScanInputLayout,
    scan: Arc<KernelDynamicScan>,
    table_schema: TableSchema,
    table_schema_projection: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl DynamicScanExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        input_layout: DynamicScanInputLayout,
        scan: Arc<KernelDynamicScan>,
        declared_output_schema: ArrowSchemaRef,
        requested_output_indices: Option<Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        let (table_schema, output_to_table_indices) =
            build_scan_table_layout(&declared_output_schema, &scan.file_constant_columns);
        let (output_schema, table_schema_projection) = apply_projection_pushdown(
            declared_output_schema,
            output_to_table_indices,
            requested_output_indices.as_deref(),
        )?;
        let properties = Self::properties(&input, output_schema);
        let exec = Self {
            input,
            input_layout,
            scan,
            table_schema,
            table_schema_projection,
            properties,
        };
        Ok(exec)
    }

    fn properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: ArrowSchemaRef,
    ) -> Arc<PlanProperties> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            input.boundedness(),
        );
        Arc::new(properties)
    }
}

impl DisplayAs for DynamicScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DynamicScanExec: base_url={}", self.scan.base_url)
    }
}

impl ExecutionPlan for DynamicScanExec {
    fn name(&self) -> &str {
        "DynamicScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "DynamicScanExec expects one child, received {}",
                children.len()
            )));
        };
        let exec = Arc::new(Self {
            input: Arc::clone(input),
            input_layout: self.input_layout.clone(),
            scan: Arc::clone(&self.scan),
            table_schema: self.table_schema.clone(),
            table_schema_projection: self.table_schema_projection.clone(),
            properties: Self::properties(input, self.schema()),
        });
        Ok(exec)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let metadata_stream = self.input.execute(partition, Arc::clone(&context))?;
        let metadata_rows = metadata_stream
            .map_ok(|batch| {
                let row_count = batch.num_rows();
                let batch = Arc::new(batch);
                stream::iter(0..row_count)
                    .map(move |row| -> Result<_, DataFusionError> { Ok((Arc::clone(&batch), row)) })
            })
            .try_flatten();

        let scan = Arc::clone(&self.scan);
        let input_layout = self.input_layout.clone();
        let table_schema = self.table_schema.clone();
        let table_schema_projection = self.table_schema_projection.clone();
        let file_streams = metadata_rows.map(move |metadata_row| {
            let (batch, row) = metadata_row?;
            open_file(
                scan.as_ref(),
                &input_layout,
                &table_schema,
                &table_schema_projection,
                batch.as_ref(),
                row,
                Arc::clone(&context),
            )
        });
        let stream = RecordBatchStreamAdapter::new(self.schema(), file_streams.try_flatten());
        Ok(Box::pin(stream))
    }
}

fn apply_projection_pushdown(
    declared_output_schema: ArrowSchemaRef,
    output_to_table_indices: Vec<usize>,
    requested_output_indices: Option<&[usize]>,
) -> Result<(ArrowSchemaRef, Vec<usize>), DataFusionError> {
    let Some(requested_output_indices) = requested_output_indices else {
        return Ok((declared_output_schema, output_to_table_indices));
    };

    let output_columns: Vec<_> = declared_output_schema
        .fields()
        .iter()
        .cloned()
        .zip(output_to_table_indices)
        .collect();
    let get_requested_column = |&index: &usize| {
        output_columns.get(index).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "dynamic scan requested output index {index} is out of bounds for {} columns",
                output_columns.len()
            ))
        })
    };
    let requested_columns: Vec<_> = requested_output_indices
        .iter()
        .map(get_requested_column)
        .try_collect()?;
    let (fields, table_schema_projection): (Vec<_>, Vec<_>) = requested_columns.into_iter().unzip();
    let output_schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        declared_output_schema.metadata().clone(),
    ));
    Ok((output_schema, table_schema_projection))
}

fn open_file(
    scan: &KernelDynamicScan,
    input_layout: &DynamicScanInputLayout,
    table_schema: &TableSchema,
    table_schema_projection: &[usize],
    batch: &RecordBatch,
    row: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let path = projected_column(batch, input_layout.path_index)?;
    let size = projected_column(batch, input_layout.size_index)?;
    let last_modified = projected_column(batch, input_layout.last_modified_index)?;
    let dv = projected_column(batch, input_layout.dv_index)?;
    let Some(constants) = batch.columns().get(input_layout.constant_indices.clone()) else {
        return Err(DataFusionError::Execution(
            "dynamic scan projected input is missing file constants".to_string(),
        ));
    };

    // Input validation and Arrow batch construction guarantee these columns are non-null.
    let path = required_path(path, row)?;
    let size = required_long(size, row, "file size")?;
    let size = u64::try_from(size).map_err(|_| {
        DataFusionError::Execution(format!(
            "dynamic scan file size must be positive, found {size}"
        ))
    })?;
    if size == 0 {
        return Err(DataFusionError::Execution(
            "dynamic scan file size must be positive, found 0".to_string(),
        ));
    }
    let last_modified = required_long(last_modified, row, "last-modified time")?;
    let last_modified = DateTime::<Utc>::from_timestamp_millis(last_modified).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "dynamic scan last-modified time is outside the supported range: {last_modified}"
        ))
    })?;
    if !dv.is_null(row) {
        return Err(DataFusionError::NotImplemented(
            "DynamicScan with deletion vectors is not supported by the DataFusion executor"
                .to_string(),
        ));
    }

    let constants = get_row_at_idx(constants, row)?;
    let resolved = scan.base_url.join(path).map_err(|error| {
        DataFusionError::Execution(format!(
            "dynamic scan could not resolve path `{path}` against `{}`: {error}",
            scan.base_url
        ))
    })?;
    let location = ListingTableUrl::parse(resolved.as_str())?;
    let store_url = location.object_store();
    context.runtime_env().object_store(&store_url)?;

    let mut file = PartitionedFile::new("", size);
    file.object_meta.location = location.prefix().clone();
    file.object_meta.last_modified = last_modified;
    file.partition_values = constants;

    let source: Arc<dyn FileSource> = Arc::new(ParquetSource::new(table_schema.clone()));
    let config = FileScanConfigBuilder::new(store_url, source)
        .with_file_group(FileGroup::new(vec![file]))
        .with_projection_indices(Some(table_schema_projection.to_vec()))?
        .with_expr_adapter(Some(Arc::new(KernelParquetExprAdapterFactory)))
        .build();
    DataSourceExec::from_data_source(config).execute(0, context)
}

fn projected_column(batch: &RecordBatch, index: usize) -> Result<&dyn Array, DataFusionError> {
    let Some(column) = batch.columns().get(index) else {
        return Err(DataFusionError::Execution(format!(
            "dynamic scan projected input is missing column at index {index}"
        )));
    };
    Ok(column.as_ref())
}

fn required_path(array: &dyn Array, row: usize) -> Result<&str, DataFusionError> {
    if let Some(array) = array.as_string_opt::<i32>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_string_opt::<i64>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_string_view_opt() {
        return Ok(array.value(row));
    }

    Err(DataFusionError::Execution(format!(
        "dynamic scan path has unexpected type {}",
        array.data_type()
    )))
}

fn required_long(array: &dyn Array, row: usize, label: &str) -> Result<i64, DataFusionError> {
    if let Some(array) = array.as_primitive_opt::<Int64Type>() {
        return Ok(array.value(row));
    }

    Err(DataFusionError::Execution(format!(
        "dynamic scan {label} has unexpected type {}",
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
    use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::common::ScalarValue as DFScalarValue;
    use datafusion::logical_expr::col as df_col;
    use datafusion::prelude::SessionContext;
    use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
    use delta_kernel::expressions::column_name;
    use delta_kernel::schema::{schema_ref, ToSchema as _};
    use url::Url;

    use super::*;

    #[test]
    fn projection_pushdown_validates_and_maps_requested_indices() {
        let declared_output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("partition", ArrowDataType::Utf8, true),
            ArrowField::new("id", ArrowDataType::Int64, true),
            ArrowField::new("version", ArrowDataType::Int64, true),
        ]));

        let (output_schema, table_schema_projection) = apply_projection_pushdown(
            Arc::clone(&declared_output_schema),
            vec![2, 0, 1],
            Some(&[2, 0]),
        )
        .unwrap();
        let field_names: Vec<_> = output_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(field_names, ["version", "partition"]);
        assert_eq!(table_schema_projection, [1, 2]);

        let error = apply_projection_pushdown(declared_output_schema, vec![2, 0, 1], Some(&[3]))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requested output index 3 is out of bounds for 3 columns"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn input_projection_records_layout_and_preserves_dv_ancestor_nulls(
    ) -> Result<(), DataFusionError> {
        let input_schema = schema_ref! {
            not_null "file": {
                not_null "path": STRING,
                not_null "size": LONG,
                not_null "filemod": LONG,
            },
            nullable "metadata": {
                nullable "inner": {
                    nullable "dv": (DeletionVectorDescriptor::to_schema()),
                },
            },
            nullable "version": LONG,
            nullable "partition": STRING,
        };
        let output_schema = schema_ref! {
            nullable "version": LONG,
            nullable "id": LONG,
            nullable "partition": STRING,
        };
        let scan = KernelDynamicScan::try_new(
            &input_schema,
            output_schema,
            KernelFileType::Parquet,
            Url::parse("memory:///sidecars/").unwrap(),
            ["partition", "version"],
            column_name!("file.path"),
            column_name!("file.size"),
            column_name!("file.filemod"),
            column_name!("metadata.inner.dv"),
        )
        .unwrap();

        let arrow_schema: ArrowSchema = input_schema.as_ref().try_into_arrow().unwrap();
        let ArrowDataType::Struct(file_fields) = arrow_schema.field(0).data_type() else {
            panic!("expected file struct");
        };
        let ArrowDataType::Struct(metadata_fields) = arrow_schema.field(1).data_type() else {
            panic!("expected metadata struct");
        };
        let ArrowDataType::Struct(inner_fields) = metadata_fields[0].data_type() else {
            panic!("expected inner struct");
        };
        let ArrowDataType::Struct(dv_fields) = inner_fields[0].data_type() else {
            panic!("expected DV struct");
        };

        let file = StructArray::new(
            file_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("file.parquet"),
                    Some("other.parquet"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(123), Some(456)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(0), Some(1)])) as ArrayRef,
            ],
            None,
        );
        let dv = StructArray::new(
            dv_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("i"), Some("i")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("inline"), Some("inline")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![None::<i32>, None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(1)])) as ArrayRef,
            ],
            None,
        );
        let inner = StructArray::new(inner_fields.clone(), vec![Arc::new(dv)], None);
        let metadata = StructArray::new(
            metadata_fields.clone(),
            vec![Arc::new(inner)],
            Some(NullBuffer::new(BooleanBuffer::from(vec![false, true]))),
        );
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(file),
                Arc::new(metadata),
                Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
                Arc::new(StringArray::from(vec![Some("part"), Some("other")])),
            ],
        )?;

        let context = SessionContext::new();
        let input = LogicalPlanBuilder::from(context.read_batch(batch)?.into_unoptimized_plan())
            .project([
                df_col("file"),
                df_col("metadata"),
                df_col("version"),
                df_col("partition"),
            ])?
            .build()?;
        let (projected_input, layout) = project_dynamic_scan_input(&scan, &input)?;
        let fields = projected_input.schema().fields();
        let expected_fields = [
            (layout.path_index, "__dynamic_scan_path"),
            (layout.size_index, "__dynamic_scan_size"),
            (layout.last_modified_index, "__dynamic_scan_last_modified"),
            (layout.dv_index, "__dynamic_scan_dv"),
        ];
        for (index, expected_name) in expected_fields {
            assert_eq!(fields[index].name(), expected_name);
        }
        let constant_names: Vec<_> = layout
            .constant_indices
            .clone()
            .map(|index| fields[index].name().as_str())
            .collect();
        assert_eq!(
            constant_names,
            ["__dynamic_scan_constant_0", "__dynamic_scan_constant_1"]
        );
        let optimized = context.state().optimize(&projected_input)?;
        let DFLogicalPlan::Projection(projection) = &optimized else {
            panic!("expected input projection, got {optimized:?}");
        };
        assert!(
            !matches!(projection.input.as_ref(), DFLogicalPlan::Projection(_)),
            "adjacent projections were not combined: {optimized:?}"
        );

        let batches = context
            .execute_logical_plan(projected_input)
            .await?
            .collect()
            .await?;
        let [batch] = batches.as_slice() else {
            panic!("expected one projected batch, got {}", batches.len());
        };
        assert_eq!(
            DFScalarValue::try_from_array(batch.column(0).as_ref(), 0)?,
            DFScalarValue::Utf8(Some("file.parquet".to_string()))
        );
        assert_eq!(
            DFScalarValue::try_from_array(batch.column(1).as_ref(), 0)?,
            DFScalarValue::Int64(Some(123))
        );
        assert_eq!(
            DFScalarValue::try_from_array(batch.column(2).as_ref(), 0)?,
            DFScalarValue::Int64(Some(0))
        );
        assert!(batch.column(3).is_null(0));
        assert!(!batch.column(3).is_null(1));
        assert_eq!(
            DFScalarValue::try_from_array(batch.column(4).as_ref(), 0)?,
            DFScalarValue::Utf8(Some("part".to_string()))
        );
        assert_eq!(
            DFScalarValue::try_from_array(batch.column(5).as_ref(), 0)?,
            DFScalarValue::Int64(Some(7))
        );
        Ok(())
    }
}
