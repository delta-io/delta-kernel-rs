use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_catalog::Session;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::{HashMap, ScalarValue};
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::object_store::{path::Path as ObjectStorePath, ObjectMeta};
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::schema::{Schema as DeltaSchema, SchemaRef as DeltaSchemaRef};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, Expression, ExpressionRef};
use url::Url;

use crate::error::to_df_err;

#[async_trait::async_trait]
pub trait TableSnapshot {
    /// The logical schema of the table.
    ///
    /// This is the fully resolved schema as it is presented to the end user.
    /// This includes any geneated or implicit columns, mapped names, etc.
    fn table_schema(&self) -> &ArrowSchemaRef;

    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Expression>,
    ) -> Result<TableScan>;
}

pub struct DeltaTableSnapshot {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    table_schema: ArrowSchemaRef,
}

impl std::fmt::Debug for DeltaTableSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableSnapshot")
            .field("snapshot", &self.snapshot)
            .field("table_schema", &self.table_schema)
            .finish()
    }
}

impl DeltaTableSnapshot {
    pub fn try_new(snapshot: Arc<Snapshot>, engine: Arc<dyn Engine>) -> Result<Self> {
        let table_schema = Arc::new(snapshot.schema().as_ref().try_into()?);
        Ok(Self {
            snapshot,
            engine,
            table_schema,
        })
    }
}

#[async_trait::async_trait]
impl TableSnapshot for DeltaTableSnapshot {
    fn table_schema(&self) -> &ArrowSchemaRef {
        &self.table_schema
    }

    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Expression>,
    ) -> Result<TableScan> {
        scan_metadata(
            state,
            &self.engine,
            &self.snapshot,
            projection,
            predicate,
            &self.table_schema,
        )
        .await
    }
}

pub struct TableScan {
    pub(crate) files: HashMap<ObjectStoreUrl, Vec<ScanFileContext>>,
    pub(crate) physical_schema: ArrowSchemaRef,
    pub(crate) logical_schema: ArrowSchemaRef,
}

async fn scan_metadata(
    _state: &dyn Session,
    engine: &Arc<dyn Engine>,
    snapshot: &Arc<Snapshot>,
    projection: Option<&Vec<usize>>,
    predicate: Arc<Expression>,
    table_schema: &ArrowSchemaRef,
) -> Result<TableScan> {
    let projected_delta_schema = project_delta_schema(table_schema, snapshot.schema(), projection);

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(projected_delta_schema)
        .with_predicate(predicate)
        .build()
        .map_err(to_df_err)?;

    let scan_state = scan.global_scan_state();
    let table_root =
        Url::parse(&scan_state.table_root).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let physical_schema: ArrowSchemaRef = Arc::new(scan_state.physical_schema.as_ref().try_into()?);

    let engine = engine.clone();

    let files = tokio::task::spawn_blocking(move || {
        let mut context = ScanContext::new(engine.clone(), table_root);

        let meta = scan.scan_metadata(engine.as_ref()).map_err(to_df_err)?;
        for scan_meta in meta {
            let scan_meta = scan_meta.map_err(to_df_err)?;
            context = scan_meta
                .visit_scan_files(context, visit_scan_file)
                .map_err(to_df_err)?;
        }

        if let Some(err) = context.errs.into_iter().next() {
            return Err(err);
        }

        Ok(context.files)
    })
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))??;

    Ok(TableScan {
        files,
        physical_schema,
        logical_schema: Arc::new(scan_state.logical_schema.as_ref().try_into()?),
    })
}

struct ScanContext {
    /// Kernel engine for reading deletion vectors.
    engine: Arc<dyn Engine>,
    /// Table root URL
    table_root: Url,
    /// Files to be scanned.
    ///
    /// The key is a store url which identifies the specific object store
    /// the file belongs to.
    files: HashMap<ObjectStoreUrl, Vec<ScanFileContext>>,
    /// Errors encountered during the scan.
    errs: Vec<DataFusionError>,
}

impl ScanContext {
    fn new(engine: Arc<dyn Engine>, table_root: Url) -> Self {
        Self {
            engine,
            table_root,
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
                let path = base_path
                    .parts()
                    .chain(ObjectStorePath::from_url_path(path)?.parts())
                    .collect();
                let mut obj_url = self.table_root.clone();
                obj_url.set_path("/");
                (ObjectStoreUrl::parse(&obj_url)?, path)
            }
        })
    }
}

pub(crate) struct ScanFileContext {
    pub(crate) partitioned_file: PartitionedFile,
    pub(crate) selection_vector: Option<Vec<bool>>,
    pub(crate) transform: Option<ExpressionRef>,
    pub(crate) file_id: String,
}

fn visit_scan_file(
    ctx: &mut ScanContext,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    // NB: partition values are passed for backwards compatibility
    // all required transformations are now part of the transform field
    _: std::collections::HashMap<String, String>,
) {
    let (store_url, location) = match ctx.parse_path(path) {
        Ok(v) => v,
        Err(e) => {
            ctx.errs.push(e);
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

    // Asssign file path as partition value to track the data source in downstream operations
    partitioned_file.partition_values = vec![ScalarValue::Utf8(Some(path.to_string()))];

    // Get the selection vector (i.e. inverse deletion vector)
    let Ok(selection_vector) = dv_info.get_selection_vector(ctx.engine.as_ref(), &ctx.table_root)
    else {
        ctx.errs.push(DataFusionError::Execution(
            "Error getting selection vector".to_string(),
        ));
        return;
    };

    // Convert the transform to datafusion physical expressions
    // These are later applied to the record batches we read from the file
    // let transform = match get_physical_transform(ctx, transform) {
    //     Some(value) => value,
    //     None => return,
    // };

    ctx.files
        .entry(store_url)
        .or_insert_with(Vec::new)
        .push(ScanFileContext {
            partitioned_file,
            selection_vector,
            transform,
            file_id: path.to_string(),
        });
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
