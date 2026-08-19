use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue as DFScalarValue};
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
use datafusion::physical_expr::EquivalenceProperties;
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
use futures::StreamExt as _;
use itertools::Itertools;
use url::Url;

use crate::scan::{build_scan_table_schema_and_projection, validate_scan_schema, ScanFormat};

/// Lowers a kernel [`DynamicScan`](KernelDynamicScan) into a DataFusion
/// [`LogicalPlan::TableScan`](DFLogicalPlan::TableScan).
///
/// `input` produces one metadata row for every file discovered at execution time. The table scan
/// retains that logical plan in a [`TableProvider`]. During physical planning,
/// [`TableProvider::scan`] plans the metadata input and pushes the requested output projection
/// into each runtime-discovered Parquet scan.
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
            "JSON DynamicScan is not supported by the DataFusion executor".to_string(),
        ));
    }

    let input_schema: KernelStructType = input.schema().as_arrow().try_into_kernel()?;
    scan.validate_input(&Arc::new(input_schema))
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

    let output_schema: ArrowSchemaRef = Arc::new(scan.schema.as_ref().try_into_arrow()?);
    validate_scan_schema(ScanFormat::Parquet, scan.schema.as_ref(), &output_schema)?;
    let provider = DynamicScanProvider {
        input: Arc::clone(input),
        output_schema,
        config: scan.into(),
    };
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    LogicalPlanBuilder::scan("dynamic_scan", source, None)?.build()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DynamicScanConfig {
    base_url: Url,
    file_constant_columns: Vec<String>,
    path_column: KernelColumnName,
    file_size_column: KernelColumnName,
    last_modified_column: KernelColumnName,
    dv_column: KernelColumnName,
}

impl From<&KernelDynamicScan> for DynamicScanConfig {
    fn from(scan: &KernelDynamicScan) -> Self {
        Self {
            base_url: scan.base_url.clone(),
            file_constant_columns: scan.file_constant_columns.clone(),
            path_column: scan.path_column.clone(),
            file_size_column: scan.file_size_column.clone(),
            last_modified_column: scan.last_modified_column.clone(),
            dv_column: scan.dv_column.clone(),
        }
    }
}

#[derive(Debug)]
struct DynamicScanProvider {
    input: Arc<DFLogicalPlan>,
    output_schema: ArrowSchemaRef,
    config: DynamicScanConfig,
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
        projection: Option<&Vec<usize>>,
        _filters: &[DFExpr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = state.create_physical_plan(self.input.as_ref()).await?;
        let scan = DynamicScanExec::try_new(
            input,
            Arc::clone(&self.output_schema),
            self.config.clone(),
            projection.cloned(),
        )?;
        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
struct DynamicFileScan {
    config: DynamicScanConfig,
    table_schema: TableSchema,
    projection: Vec<usize>,
}

impl DynamicFileScan {
    fn try_new(
        config: DynamicScanConfig,
        output_schema: &ArrowSchemaRef,
        output_projection: Option<&[usize]>,
    ) -> Result<Self, DataFusionError> {
        let (table_schema, full_projection) =
            build_scan_table_schema_and_projection(output_schema, &config.file_constant_columns);
        let projection: Vec<_> = match output_projection {
            Some(output_projection) => output_projection
                .iter()
                .map(|&index| {
                    let Some(&file_index) = full_projection.get(index) else {
                        return Err(DataFusionError::Plan(format!(
                            "dynamic scan projection index {index} is out of bounds for {} columns",
                            full_projection.len()
                        )));
                    };
                    Ok(file_index)
                })
                .try_collect()?,
            None => full_projection,
        };
        let file_scan = Self {
            config,
            table_schema,
            projection,
        };
        Ok(file_scan)
    }

    fn open(
        &self,
        batch: &RecordBatch,
        row: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let path = required_string(batch, row, &self.config.path_column, "path")?;
        let size = required_long(batch, row, &self.config.file_size_column, "file size")?;
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
        let last_modified = required_long(
            batch,
            row,
            &self.config.last_modified_column,
            "last-modified time",
        )?;
        let last_modified =
            DateTime::<Utc>::from_timestamp_millis(last_modified).ok_or_else(|| {
                DataFusionError::Execution(format!(
                "dynamic scan last-modified time is outside the supported range: {last_modified}"
            ))
            })?;
        if column_array(batch, row, &self.config.dv_column)?.is_some() {
            return Err(DataFusionError::NotImplemented(
                "DynamicScan with deletion vectors is not supported by the DataFusion executor"
                    .to_string(),
            ));
        }

        let constants = self
            .config
            .file_constant_columns
            .iter()
            .map(|name| file_constant(batch, row, name))
            .collect::<Result<Vec<_>, _>>()?;
        let resolved = self.config.base_url.join(&path).map_err(|error| {
            DataFusionError::Execution(format!(
                "dynamic scan could not resolve path `{path}` against `{}`: {error}",
                self.config.base_url
            ))
        })?;
        let location = ListingTableUrl::parse(resolved.as_str())?;
        let store_url = location.object_store();
        context.runtime_env().object_store(&store_url)?;

        let mut file = PartitionedFile::new("", size);
        file.object_meta.location = location.prefix().clone();
        file.object_meta.last_modified = last_modified;
        file.partition_values = constants;

        let source: Arc<dyn FileSource> = Arc::new(ParquetSource::new(self.table_schema.clone()));
        let config = FileScanConfigBuilder::new(store_url, source)
            .with_file_group(FileGroup::new(vec![file]))
            .with_projection_indices(Some(self.projection.clone()))?
            .with_partitioned_by_file_group(false)
            .build();
        DataSourceExec::from_data_source(config).execute(0, context)
    }
}

#[derive(Debug)]
struct DynamicScanExec {
    input: Arc<dyn ExecutionPlan>,
    full_output_schema: ArrowSchemaRef,
    output_projection: Option<Vec<usize>>,
    file_scan: DynamicFileScan,
    properties: Arc<PlanProperties>,
}

impl DynamicScanExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        full_output_schema: ArrowSchemaRef,
        config: DynamicScanConfig,
        output_projection: Option<Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        let output_schema = match output_projection.as_ref() {
            Some(projection) => Arc::new(full_output_schema.project(projection)?),
            None => Arc::clone(&full_output_schema),
        };
        let file_scan =
            DynamicFileScan::try_new(config, &full_output_schema, output_projection.as_deref())?;
        let partition_count = input.output_partitioning().partition_count();
        let boundedness = input.boundedness();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            boundedness,
        );
        let scan = Self {
            input,
            full_output_schema,
            output_projection,
            file_scan,
            properties: Arc::new(properties),
        };
        Ok(scan)
    }
}

impl DisplayAs for DynamicScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DynamicScanExec: base_url={}",
            self.file_scan.config.base_url
        )
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
        let scan = Self::try_new(
            Arc::clone(input),
            Arc::clone(&self.full_output_schema),
            self.file_scan.config.clone(),
            self.output_projection.clone(),
        )?;
        Ok(Arc::new(scan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let metadata_stream = self.input.execute(partition, Arc::clone(&context))?;
        let state = DynamicScanStreamState {
            metadata_stream,
            metadata_batch: None,
            next_row: 0,
            file_stream: None,
            file_scan: self.file_scan.clone(),
            context,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(file_stream) = state.file_stream.as_mut() {
                    match file_stream.next().await {
                        Some(Ok(batch)) => return Ok(Some((batch, state))),
                        Some(Err(error)) => return Err(error),
                        None => state.file_stream = None,
                    }
                }

                if let Some(batch) = state.metadata_batch.as_ref() {
                    if state.next_row < batch.num_rows() {
                        let row = state.next_row;
                        state.next_row += 1;
                        state.file_stream = Some(state.file_scan.open(
                            batch,
                            row,
                            Arc::clone(&state.context),
                        )?);
                        continue;
                    }
                    state.metadata_batch = None;
                }

                match state.metadata_stream.next().await {
                    Some(Ok(batch)) => {
                        state.metadata_batch = Some(batch);
                        state.next_row = 0;
                    }
                    Some(Err(error)) => return Err(error),
                    None => return Ok(None),
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

struct DynamicScanStreamState {
    metadata_stream: SendableRecordBatchStream,
    metadata_batch: Option<RecordBatch>,
    next_row: usize,
    file_stream: Option<SendableRecordBatchStream>,
    file_scan: DynamicFileScan,
    context: Arc<TaskContext>,
}

fn required_string(
    batch: &RecordBatch,
    row: usize,
    column: &KernelColumnName,
    label: &str,
) -> Result<String, DataFusionError> {
    match required_value(batch, row, column, label)? {
        DFScalarValue::Utf8(Some(value))
        | DFScalarValue::LargeUtf8(Some(value))
        | DFScalarValue::Utf8View(Some(value)) => Ok(value),
        value => Err(DataFusionError::Execution(format!(
            "dynamic scan {label} has unexpected value {value:?}"
        ))),
    }
}

fn required_long(
    batch: &RecordBatch,
    row: usize,
    column: &KernelColumnName,
    label: &str,
) -> Result<i64, DataFusionError> {
    match required_value(batch, row, column, label)? {
        DFScalarValue::Int64(Some(value)) => Ok(value),
        value => Err(DataFusionError::Execution(format!(
            "dynamic scan {label} has unexpected value {value:?}"
        ))),
    }
}

fn required_value(
    batch: &RecordBatch,
    row: usize,
    column: &KernelColumnName,
    label: &str,
) -> Result<DFScalarValue, DataFusionError> {
    let array = column_array(batch, row, column)?.ok_or_else(|| {
        DataFusionError::Execution(format!("dynamic scan {label} must not be null"))
    })?;
    DFScalarValue::try_from_array(array, row)
}

fn column_array<'a>(
    batch: &'a RecordBatch,
    row: usize,
    column: &KernelColumnName,
) -> Result<Option<&'a dyn Array>, DataFusionError> {
    let Some((top_level, nested)) = column.path().split_first() else {
        return Err(DataFusionError::Plan(
            "dynamic scan column path must not be empty".to_string(),
        ));
    };
    let index = batch.schema().index_of(top_level).map_err(|error| {
        DataFusionError::Execution(format!(
            "dynamic scan column `{column}` is missing at execution: {error}"
        ))
    })?;
    let mut array = batch.column(index).as_ref();
    if array.is_null(row) {
        return Ok(None);
    }

    for name in nested {
        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return Err(DataFusionError::Execution(format!(
                "dynamic scan column `{column}` traverses non-struct field `{name}`"
            )));
        };
        let Some(index) = struct_array
            .fields()
            .iter()
            .position(|field| field.name() == name)
        else {
            return Err(DataFusionError::Execution(format!(
                "dynamic scan column `{column}` is missing nested field `{name}`"
            )));
        };
        array = struct_array.column(index).as_ref();
        if array.is_null(row) {
            return Ok(None);
        }
    }
    Ok(Some(array))
}

fn file_constant(
    batch: &RecordBatch,
    row: usize,
    name: &str,
) -> Result<DFScalarValue, DataFusionError> {
    let index = batch.schema().index_of(name).map_err(|error| {
        DataFusionError::Execution(format!(
            "dynamic scan file constant `{name}` is missing at execution: {error}"
        ))
    })?;
    DFScalarValue::try_from_array(batch.column(index).as_ref(), row)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};

    use super::*;

    #[test]
    fn column_lookup_treats_null_struct_ancestor_as_null() {
        let nested_fields = Fields::from(vec![Arc::new(Field::new("dv", DataType::Int64, true))]);
        let nested = StructArray::new(
            nested_fields.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
            Some(NullBuffer::new(BooleanBuffer::from(vec![false]))),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(nested_fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(nested)]).unwrap();

        let value = column_array(&batch, 0, &KernelColumnName::new(["metadata", "dv"])).unwrap();

        assert!(value.is_none());
    }
}
