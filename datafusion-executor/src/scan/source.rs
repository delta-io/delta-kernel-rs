//! DataFusion file source backed by kernel's Arrow read reconciliation.

use std::io::{BufReader, Cursor};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::object_store::{ObjectStore, ObjectStoreExt as _};
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_utils::{
    build_json_reorder_indices, coerce_columns_to_schema, fixup_json_read, fixup_parquet_read,
    generate_mask, get_requested_indices, json_arrow_schema, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use delta_kernel::engine::reader_options;
use delta_kernel::schema::{SchemaRef as KernelSchemaRef, StructType as KernelStructType};
use futures::{FutureExt, StreamExt};

use super::ScanFormat;

/// Retains the full URL after DataFusion separates it into an object-store URL and object path.
#[derive(Debug)]
pub(super) struct ScanFileLocation(pub String);

/// A Parquet or JSON source that applies kernel's scan reconciliation to each file batch.
#[derive(Clone)]
pub(super) struct KernelFileSource {
    format: ScanFormat,
    table_schema: TableSchema,
    kernel_schema: KernelSchemaRef,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl KernelFileSource {
    /// Creates a source for `format` with DataFusion's table layout and kernel's file schema.
    pub(super) fn new(
        format: ScanFormat,
        table_schema: TableSchema,
        kernel_schema: KernelSchemaRef,
    ) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            format,
            table_schema,
            kernel_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
        }
    }

    fn projected_kernel_schema(&self) -> DataFusionResult<KernelSchemaRef> {
        let fields = self
            .projection
            .file_indices
            .iter()
            .map(|&index| {
                self.kernel_schema
                    .field_at_index(index)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "file projection contains invalid kernel field index {index}"
                        ))
                    })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        KernelStructType::try_new(fields)
            .map(Arc::new)
            .map_err(kernel_error)
    }
}

impl FileSource for KernelFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> DataFusionResult<Arc<dyn FileOpener>> {
        let batch_size = self.batch_size.ok_or_else(|| {
            DataFusionError::Internal("scan batch size was not initialized".to_string())
        })?;
        let schema = self.projected_kernel_schema()?;
        let output_schema = Arc::new(schema.as_ref().try_into_arrow()?);
        let opener: Arc<dyn FileOpener> = Arc::new(KernelFileOpener {
            format: self.format,
            object_store,
            schema,
            output_schema,
            batch_size,
        });
        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size: Some(batch_size),
            ..self.clone()
        })
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> DataFusionResult<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        match self.format {
            ScanFormat::Parquet => "parquet",
            ScanFormat::Json => "json",
        }
    }

    // Kernel's row-index contract is relative to the complete file. The opener therefore must not
    // receive byte-range fragments that would each restart reconciliation independently.
    fn supports_repartitioning(&self) -> bool {
        false
    }
}

struct KernelFileOpener {
    format: ScanFormat,
    object_store: Arc<dyn ObjectStore>,
    schema: KernelSchemaRef,
    output_schema: ArrowSchemaRef,
    batch_size: usize,
}

impl FileOpener for KernelFileOpener {
    fn open(&self, file: PartitionedFile) -> DataFusionResult<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::Internal(
                "kernel scan source received an unsupported file range".to_string(),
            ));
        }
        let file_location = file
            .extension::<ScanFileLocation>()
            .ok_or_else(|| {
                DataFusionError::Internal("scan file is missing its full location".to_string())
            })?
            .0
            .clone();
        let object_store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        let format = self.format;

        Ok(async move {
            match format {
                ScanFormat::Parquet => {
                    open_parquet(
                        object_store,
                        file,
                        schema,
                        output_schema,
                        batch_size,
                        file_location,
                    )
                    .await
                }
                ScanFormat::Json => {
                    open_json(
                        object_store,
                        file,
                        schema,
                        output_schema,
                        batch_size,
                        file_location,
                    )
                    .await
                }
            }
        }
        .boxed())
    }
}

async fn open_parquet(
    object_store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    schema: KernelSchemaRef,
    output_schema: ArrowSchemaRef,
    batch_size: usize,
    file_location: String,
) -> DataFusionResult<futures::stream::BoxStream<'static, DataFusionResult<RecordBatch>>> {
    let reader = ParquetObjectReader::new(object_store, file.object_meta.location)
        .with_file_size(file.object_meta.size);
    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_options()).await?;
    let (indices, requested_ordering) =
        get_requested_indices(&schema, builder.schema()).map_err(kernel_error)?;
    if let Some(mask) = generate_mask(builder.parquet_schema(), &indices) {
        builder = builder.with_projection(mask);
    }
    builder = builder.with_batch_size(batch_size);

    let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
        .then(|| RowIndexBuilder::new(builder.metadata().row_groups()))
        .map(RowIndexBuilder::build)
        .transpose()
        .map_err(kernel_error)?;
    let stream = builder.build()?.map(move |batch| {
        let data = fixup_parquet_read(
            batch?,
            &requested_ordering,
            row_indexes.as_mut(),
            Some(&file_location),
            Some(&schema),
        )
        .map_err(kernel_error)?;
        normalize_batch(RecordBatch::from(data), &output_schema)
    });
    Ok(stream.boxed())
}

async fn open_json(
    object_store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    schema: KernelSchemaRef,
    output_schema: ArrowSchemaRef,
    batch_size: usize,
    file_location: String,
) -> DataFusionResult<futures::stream::BoxStream<'static, DataFusionResult<RecordBatch>>> {
    let bytes = object_store
        .get(&file.object_meta.location)
        .await?
        .bytes()
        .await?;
    let json_schema = Arc::new(json_arrow_schema(&schema).map_err(kernel_error)?);
    let reorder_indices = build_json_reorder_indices(&schema).map_err(kernel_error)?;
    let reader = ReaderBuilder::new(json_schema)
        .with_batch_size(batch_size)
        .with_coerce_primitive(true)
        .build(BufReader::new(Cursor::new(bytes)))?;
    let stream = futures::stream::iter(reader.map(move |batch| {
        let data =
            fixup_json_read(batch?, &reorder_indices, &file_location).map_err(kernel_error)?;
        normalize_batch(RecordBatch::from(data), &output_schema)
    }));
    Ok(stream.boxed())
}

fn normalize_batch(
    batch: RecordBatch,
    output_schema: &ArrowSchemaRef,
) -> DataFusionResult<RecordBatch> {
    let row_count = batch.num_rows();
    let columns =
        coerce_columns_to_schema(batch.columns().to_vec(), output_schema).map_err(kernel_error)?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

fn kernel_error(error: delta_kernel::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
