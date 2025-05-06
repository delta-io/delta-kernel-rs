use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::{union::UnionExec, ExecutionPlan};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::{DFSchema, HashMap, ScalarValue};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::object_store::{path::Path as ObjectStorePath, ObjectMeta};
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::schema::{Schema as DeltaSchema, SchemaRef as DeltaSchemaRef};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, ExpressionRef};
use url::Url;

use crate::error::to_df_err;
use crate::exec::{DeltaScanExec, FILE_ID_COLUMN};
use crate::expressions::{to_datafusion_expr, to_delta_predicate};

pub struct DeltaTableProvider {
    snapshot: Arc<Snapshot>,
    table_schema: ArrowSchemaRef,
    engine: Arc<dyn Engine>,
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
        let table_schema = snapshot.schema().as_ref().try_into()?;
        Ok(Self {
            snapshot,
            table_schema: Arc::new(table_schema),
            engine,
        })
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.table_schema)
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
        let projected_delta_schema =
            project_delta_schema(&self.table_schema, self.snapshot.schema(), projection);
        let predicate = to_delta_predicate(filters)?;

        let scan = self
            .snapshot
            .clone()
            .scan_builder()
            .with_schema(projected_delta_schema)
            .with_predicate(predicate)
            .build()
            .map_err(to_df_err)?;

        let scan_state = scan.global_scan_state();
        let table_root = Url::parse(&scan_state.table_root)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_schema: ArrowSchemaRef = Arc::new(scan_state.physical_schema.as_ref().try_into()?);

        let mut context = ScanContext::new(
            state,
            self.engine.clone(),
            table_root,
            file_schema.clone().try_into()?,
        );

        let meta = scan
            .scan_metadata(self.engine.as_ref())
            .map_err(to_df_err)?;
        for scan_meta in meta {
            let scan_meta = scan_meta.map_err(to_df_err)?;
            context = scan_meta
                .visit_scan_files(context, visit_scan_file)
                .map_err(to_df_err)?;
        }

        if let Some(err) = context.errs.into_iter().next() {
            return Err(err);
        }

        let source = Arc::new(ParquetSource::default());
        let mut plans = Vec::new();
        let mut transforms = HashMap::new();
        let file_id_field = Field::new(FILE_ID_COLUMN, DataType::Utf8, true);

        for (store_url, mut files) in context.files.into_iter() {
            let store = state.runtime_env().object_store(&store_url)?;
            let reader_factory = Arc::new(DefaultParquetFileReaderFactory::new(store))
                as Arc<dyn ParquetFileReaderFactory>;
            for file in files.iter_mut() {
                if let Some(selection_vector) = &file.selection_vector {
                    let access_plan = get_parquet_access_plan(
                        &reader_factory,
                        &file.partitioned_file,
                        selection_vector.clone(),
                    )
                    .await?;
                    file.partitioned_file = file
                        .partitioned_file
                        .clone()
                        .with_extensions(Arc::new(access_plan));
                    if let Some(transform) = &file.transform {
                        transforms.insert(file.file_id.clone(), transform.clone());
                    }
                }
            }
            // TODO: convert passed predicate to an expression in terms of physical columns
            // and add it to the FileScanConfig
            let config = FileScanConfigBuilder::new(store_url, file_schema.clone(), source.clone())
                .with_file_group(files.into_iter().map(|f| f.partitioned_file).collect())
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
            Arc::new(scan_state.logical_schema.as_ref().try_into()?),
            plan,
            Arc::new(transforms),
        )))
    }
}

fn project_delta_schema(
    arrow_schema: &ArrowSchemaRef,
    schema: DeltaSchemaRef,
    projections: Option<&Vec<usize>>,
) -> DeltaSchemaRef {
    if let Some(projections) = projections {
        let projected_fields = projections
            .iter()
            .filter_map(|i| schema.field(arrow_schema.field(*i).name()))
            .cloned();
        Arc::new(DeltaSchema::new(projected_fields))
    } else {
        schema
    }
}

struct ScanContext<'a> {
    /// Current datafusion session
    session: &'a dyn Session,
    /// Kernel engine for reading deletion vectors.
    engine: Arc<dyn Engine>,
    /// Table root URL
    table_root: Url,
    /// Datafusion schema for data as read from the data file.
    physical_schema_df: DFSchema,
    /// Files to be scanned.
    files: HashMap<ObjectStoreUrl, Vec<PartitionFileContext>>,
    /// Errors encountered during the scan.
    errs: Vec<DataFusionError>,
}

impl<'a> ScanContext<'a> {
    fn new(
        session: &'a dyn Session,
        engine: Arc<dyn Engine>,
        table_root: Url,
        physical_schema_df: DFSchema,
    ) -> Self {
        Self {
            session,
            engine,
            table_root,
            physical_schema_df,
            files: HashMap::new(),
            errs: Vec::new(),
        }
    }

    fn parse_path(&self, path: &str) -> Result<(ObjectStoreUrl, ObjectStorePath)> {
        Ok(match Url::parse(path) {
            Ok(mut url) => {
                // we have a fully qualified url
                let path = ObjectStorePath::from_url_path(url.path())?;
                url.set_path("/");
                let url = ObjectStoreUrl::parse(&url)?;
                (url, path)
            }
            Err(_) => {
                // we have a relative path
                let base_path = ObjectStorePath::from_url_path(self.table_root.path())?;
                let path = base_path.child(path);
                let mut obj_url = self.table_root.clone();
                obj_url.set_path("/");
                (ObjectStoreUrl::parse(&obj_url)?, path)
            }
        })
    }
}

struct PartitionFileContext {
    partitioned_file: PartitionedFile,
    selection_vector: Option<Vec<bool>>,
    transform: Option<PhysicalExprRef>,
    file_id: String,
}

fn visit_scan_file(
    context: &mut ScanContext,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    // NB: partition values are passed for backwards compatibility
    // all required transformations are now part of the transform field
    _: std::collections::HashMap<String, String>,
) {
    let (store_url, location) = match context.parse_path(path) {
        Ok(v) => v,
        Err(e) => {
            context.errs.push(e);
            return;
        }
    };

    let object_meta = ObjectMeta {
        location,
        last_modified: Utc.timestamp_nanos(0),
        size: size as u64,
        e_tag: None,
        version: None,
    };
    let mut partitioned_file = PartitionedFile::from(object_meta);
    partitioned_file.partition_values = vec![ScalarValue::Utf8(Some(path.to_string()))];

    // Get the selection vector (i.e. inverse deletion vector)
    let Ok(selection_vector) =
        dv_info.get_selection_vector(context.engine.as_ref(), &context.table_root)
    else {
        context.errs.push(DataFusionError::Execution(
            "Error getting selection vector".to_string(),
        ));
        return;
    };

    let Ok(transform) = transform.map(|t| to_datafusion_expr(&t)).transpose() else {
        context.errs.push(DataFusionError::Execution(
            "Error converting transform to Delta expression".to_string(),
        ));
        return;
    };

    let transform = transform
        .map(|t| {
            context
                .session
                .create_physical_expr(t, &context.physical_schema_df)
        })
        .transpose();

    let transform = match transform {
        Ok(transform) => transform,
        Err(e) => {
            context.errs.push(e);
            return;
        }
    };

    context
        .files
        .entry(store_url)
        .or_insert_with(Vec::new)
        .push(PartitionFileContext {
            partitioned_file,
            selection_vector,
            transform,
            file_id: path.to_string(),
        });
}

async fn get_parquet_access_plan(
    parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: &PartitionedFile,
    selection_vector: Vec<bool>,
) -> Result<ParquetAccessPlan> {
    let mut parquet_file_reader = parquet_file_reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        &ExecutionPlanMetricsSet::new(),
    )?;

    let parquet_metadata = parquet_file_reader
        .get_metadata(None)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Error getting parquet metadata: {e}")))?;

    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for (_idx, row_group) in parquet_metadata.row_groups().iter().enumerate() {
        // If all rows in the row group are deleted, skip the row group
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    Ok(ParquetAccessPlan::new(row_groups))
}

fn get_row_group_access(
    selection_vector: &[bool],
    row_group_row_start: usize,
    row_group_num_rows: usize,
) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| !x)
    {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| x)
    {
        return RowGroupAccess::Scan;
    }

    let mask =
        selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows].to_vec();

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
