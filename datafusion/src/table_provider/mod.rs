use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::{union::UnionExec, ExecutionPlan};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::error::Result;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::PhysicalExpr;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, ExpressionRef};
use futures::stream::{StreamExt, TryStreamExt};
use itertools::Itertools;

use self::scan_metadata::{DeltaTableSnapshot, ScanFileContext, TableSnapshot};
use crate::exec::{DeltaScanExec, FILE_ID_COLUMN};
use crate::expressions::{to_datafusion_expr, to_delta_predicate};

mod scan_metadata;

pub struct DeltaTableProvider {
    snapshot: Arc<dyn TableSnapshot>,
}

impl std::fmt::Debug for DeltaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableProvider")
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

impl DeltaTableProvider {
    pub fn try_new(snapshot: Arc<Snapshot>, engine: Arc<dyn Engine>) -> Result<Self> {
        let snapshot = DeltaTableSnapshot::try_new(snapshot, engine)?;
        Ok(Self {
            snapshot: Arc::new(snapshot),
        })
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(self.snapshot.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = to_delta_predicate(filters)?;
        let table_scan = self
            .snapshot
            .scan_metadata(state, projection, predicate)
            .await?;

        // Convert the delta expressions from the scan into a map of file id to datafusion physical expression
        // these will be applied to convert the raw data read from disk into the logical table schema
        let fs = table_scan.physical_schema.clone().try_into()?;
        let map_file = |f: &ScanFileContext| {
            f.transform
                .as_ref()
                .map(|t| Ok((f.file_id.clone(), to_physical(state, &fs, t)?)))
        };
        let transforms = table_scan
            .files
            .values()
            .flat_map(|files| files.iter().filter_map(map_file))
            .try_collect::<_, _, DataFusionError>()?;

        // Create DataSourceExec plans to read all files included in the scan.
        // A dedicated DataSourceExec plan needs to be created for each store (e.g. s3 bucket).
        // When data is distributed across multiple stores, a UnionExec will be used to combine the results.
        let metrics = ExecutionPlanMetricsSet::new();
        let source = Arc::new(ParquetSource::default());
        let mut plans = Vec::new();
        let file_id_field = Field::new(FILE_ID_COLUMN, DataType::Utf8, true);
        for (store_url, files) in table_scan.files.into_iter() {
            let store = state.runtime_env().object_store(&store_url)?;
            let reader_factory: Arc<dyn ParquetFileReaderFactory> =
                Arc::new(DefaultParquetFileReaderFactory::new(store));
            let file_group = compute_parquet_access_plans(&reader_factory, files, &metrics).await?;

            // TODO: convert passed predicate to an expression in terms of physical columns
            // and add it to the FileScanConfig
            let config = FileScanConfigBuilder::new(
                store_url,
                table_scan.physical_schema.clone(),
                source.clone(),
            )
            .with_file_group(file_group.into_iter().collect())
            .with_table_partition_cols(vec![file_id_field.clone()])
            .with_limit(limit)
            .build();
            let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
            plans.push(plan);
        }
        let plan = match plans.len() {
            1 => plans.remove(0),
            _ => Arc::new(UnionExec::new(plans)),
        };

        Ok(Arc::new(DeltaScanExec::new(
            table_scan.logical_schema,
            plan,
            Arc::new(transforms),
        )))
    }
}

// convert a delta expression to a datafusion physical expression
// we return a vector of expressions implicitly representing structs,
// as there is no top-level Struct expression type in datafusion
fn to_physical(
    state: &dyn Session,
    physical_schema_df: &DFSchema,
    transform: &ExpressionRef,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    to_datafusion_expr(transform)?
        .into_iter()
        .map(|expr: Expr| state.create_physical_expr(expr, physical_schema_df))
        .collect::<Result<Vec<_>>>()
}

async fn compute_parquet_access_plans(
    reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    files: Vec<ScanFileContext>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<Vec<PartitionedFile>> {
    futures::stream::iter(files)
        // NOTE: using filter_map here since 'map' somehow does not accept futures.
        .filter_map(|file| async {
            if let Some(sv) = file.selection_vector {
                Some(pq_access_plan(reader_factory, file.partitioned_file, sv, metrics).await)
            } else {
                Some(Ok(file.partitioned_file))
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

async fn pq_access_plan(
    reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: PartitionedFile,
    selection_vector: Vec<bool>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<PartitionedFile> {
    let mut parquet_file_reader = reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        metrics,
    )?;

    let parquet_metadata = parquet_file_reader.get_metadata(None).await?;
    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for row_group in parquet_metadata.row_groups().iter() {
        // If all rows in the row group are deleted, skip the row group
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    let plan = ParquetAccessPlan::new(row_groups);

    Ok(partitioned_file.with_extensions(Arc::new(plan)))
}

fn get_row_group_access(selection_vector: &[bool], start: usize, offset: usize) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if !selection_vector[start..start + offset].iter().any(|&x| x) {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[start..start + offset].iter().all(|&x| x) {
        return RowGroupAccess::Scan;
    }

    let mask = selection_vector[start..start + offset].to_vec();

    // If some rows are deleted, get a row selection that skips the deleted rows
    let row_selection = RowSelection::from_filters(&[mask.into()]);
    RowGroupAccess::Selection(row_selection)
}

fn get_full_selection_vector(selection_vector: &[bool], total_rows: usize) -> Vec<bool> {
    let mut new_selection_vector = vec![true; total_rows];
    let copy_len = std::cmp::min(selection_vector.len(), total_rows);
    new_selection_vector[..copy_len].copy_from_slice(&selection_vector[..copy_len]);
    new_selection_vector
}
