use std::sync::Arc;

use datafusion_catalog::Session;
use datafusion_common::{
    error::{DataFusionError, DataFusionErrorBuilder, Result},
    exec_datafusion_err as exec_err,
};
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::schema::{Schema as DeltaSchema, SchemaRef as DeltaSchemaRef};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, Expression, ExpressionRef};
use url::Url;

use crate::error::to_df_err;
use crate::session::KernelSessionExt;

pub struct ScanFileContext {
    pub selection_vector: Option<Vec<bool>>,
    pub transform: Option<ExpressionRef>,
    pub file_url: Url,
    pub stats: Option<Stats>,
    pub size: u64,
}

/// The metadata required to plan a table scan.
pub struct TableScan {
    /// Files included in the scan.
    ///
    /// These are filtered on a best effort basis to match the predicate passed to the scan.
    /// Files are grouped by the object store they are stored in.
    pub files: Vec<ScanFileContext>,
    /// The physical schema of the table.
    ///
    /// Data read from disk should be presented in this schema.
    /// While in most cases this corresponds to the data present in the file,
    /// the reader may be required to cast data to include columns not present
    /// in the file (i.e. the table may have undergone schema evolution),
    /// or type widening may have been applied.
    pub physical_schema: ArrowSchemaRef,
    /// The logical schema of the table.
    ///
    /// This is the schema as it is presented to the end user.
    /// This includes any geneated or implicit columns, mapped names, etc.
    pub logical_schema: ArrowSchemaRef,
}

#[async_trait::async_trait]
pub trait TableSnapshot: std::fmt::Debug + Send + Sync {
    /// The logical schema of the table.
    ///
    /// This is the fully resolved schema as it is presented to the end user.
    /// This includes any geneated or implicit columns, mapped names, etc.
    fn table_schema(&self) -> &ArrowSchemaRef;

    fn table_schema_delta(&self) -> DeltaSchemaRef;

    /// Produce the metadata required to plan a table scan.
    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Expression>,
    ) -> Result<TableScan>;
}

pub struct DeltaTableSnapshot {
    snapshot: Arc<Snapshot>,
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
    pub fn try_new(snapshot: Arc<Snapshot>) -> Result<Self> {
        let table_schema = Arc::new(snapshot.schema().as_ref().try_into()?);
        Ok(Self {
            snapshot,
            table_schema,
        })
    }
}

#[async_trait::async_trait]
impl TableSnapshot for DeltaTableSnapshot {
    fn table_schema(&self) -> &ArrowSchemaRef {
        &self.table_schema
    }

    fn table_schema_delta(&self) -> DeltaSchemaRef {
        self.snapshot.schema()
    }

    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Expression>,
    ) -> Result<TableScan> {
        scan_metadata(
            state.kernel_engine()?,
            &self.snapshot,
            projection,
            predicate,
            &self.table_schema,
        )
        .await
    }
}

async fn scan_metadata(
    engine: Arc<dyn Engine>,
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

    let scan_inner = move || {
        let mut context = ScanContext::new(engine.clone(), table_root);
        let meta = scan.scan_metadata(engine.as_ref()).map_err(to_df_err)?;
        for scan_meta in meta {
            let scan_meta = scan_meta.map_err(to_df_err)?;
            context = scan_meta
                .visit_scan_files(context, visit_scan_file)
                .map_err(to_df_err)?;
        }
        context.errs.error_or(context.files)
    };
    let files = tokio::task::spawn_blocking(scan_inner)
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
    files: Vec<ScanFileContext>,
    /// Errors encountered during the scan.
    errs: DataFusionErrorBuilder,
}

impl ScanContext {
    fn new(engine: Arc<dyn Engine>, table_root: Url) -> Self {
        Self {
            engine,
            table_root,
            files: Vec::new(),
            errs: DataFusionErrorBuilder::new(),
        }
    }

    fn parse_path(&self, path: &str) -> Result<Url> {
        Ok(match Url::parse(path) {
            Ok(url) => url,
            Err(_) => self
                .table_root
                .join(path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        })
    }
}

fn visit_scan_file(
    ctx: &mut ScanContext,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    // NB: partition values are passed for backwards compatibility
    // all required transformations are now part of the transform field
    _: std::collections::HashMap<String, String>,
) {
    let file_url = match ctx.parse_path(path) {
        Ok(v) => v,
        Err(e) => {
            ctx.errs.add_error(e);
            return;
        }
    };

    // Get the selection vector (i.e. inverse deletion vector)
    let Ok(selection_vector) = dv_info.get_selection_vector(ctx.engine.as_ref(), &ctx.table_root)
    else {
        ctx.errs
            .add_error(exec_err!("Failed to get selection vector"));
        return;
    };

    ctx.files.push(ScanFileContext {
        selection_vector,
        transform,
        stats,
        file_url,
        size: size as u64,
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
