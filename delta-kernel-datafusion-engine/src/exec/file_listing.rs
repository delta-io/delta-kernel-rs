use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use delta_kernel::object_store::{self, ObjectMeta, ObjectStore};
use futures::{Stream, StreamExt, TryStreamExt};

const BATCH_SIZE: usize = 1024;

fn metas_to_batch(
    metas: &[ObjectMeta],
    schema: &SchemaRef,
    base_url: &url::Url,
) -> DfResult<RecordBatch> {
    let paths = StringArray::from_iter_values(metas.iter().map(|m| {
        let mut full = base_url.clone();
        full.set_path(&format!("/{}", m.location.as_ref()));
        full.to_string()
    }));
    let sizes = Int64Array::from_iter_values(metas.iter().map(|m| m.size as i64));
    let mod_times =
        Int64Array::from_iter_values(metas.iter().map(|m| m.last_modified.timestamp_millis()));
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(paths), Arc::new(sizes), Arc::new(mod_times)],
    )
    .map_err(Into::into)
}

pub struct FileListingExec {
    path: url::Url,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FileListingExec {
    pub fn new(path: url::Url) -> Self {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modification_time", DataType::Int64, false),
        ]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            path,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for FileListingExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileListingExec")
            .field("path", &self.path)
            .finish()
    }
}

impl DisplayAs for FileListingExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileListingExec(path={})", self.path)
    }
}

impl ExecutionPlan for FileListingExec {
    fn name(&self) -> &str {
        "FileListingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "FileListingExec cannot have children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let object_store_url_str = format!(
            "{}://{}",
            self.path.scheme(),
            self.path.host_str().unwrap_or("")
        );
        let object_store_url = ObjectStoreUrl::parse(&object_store_url_str)?;
        let store = context.runtime_env().object_store(&object_store_url)?;
        let prefix = object_store::path::Path::from(self.path.path());
        let schema = self.schema.clone();
        let base_url = self.path.clone();
        // `ObjectStore::list` does not promise lexicographic order; local-fs backends are
        // particularly inconsistent (OS directory-walk order). Collect+sort there to keep
        // `LogStore` segmentation deterministic across hosts. Remote stores: passthrough.
        let stream: std::pin::Pin<Box<dyn Stream<Item = DfResult<RecordBatch>> + Send>> =
            if self.path.scheme() == "file" {
                Box::pin(sorted_local_listing(
                    store,
                    prefix,
                    schema.clone(),
                    base_url,
                ))
            } else {
                Box::pin(chunked_remote_listing(
                    store,
                    prefix,
                    schema.clone(),
                    base_url,
                ))
            };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

fn sorted_local_listing(
    store: Arc<dyn ObjectStore>,
    prefix: object_store::path::Path,
    schema: SchemaRef,
    base_url: url::Url,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    async_stream::try_stream! {
        let mut metas: Vec<ObjectMeta> = store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(crate::error::wrap_delta_err)?;
        metas.sort_by(|a, b| a.location.cmp(&b.location));
        for chunk in metas.chunks(BATCH_SIZE) {
            yield metas_to_batch(chunk, &schema, &base_url)?;
        }
    }
}

fn chunked_remote_listing(
    store: Arc<dyn ObjectStore>,
    prefix: object_store::path::Path,
    schema: SchemaRef,
    base_url: url::Url,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    store
        .list(Some(&prefix))
        .chunks(BATCH_SIZE)
        .map(move |chunk| {
            let metas: Vec<ObjectMeta> = chunk
                .into_iter()
                .collect::<Result<_, _>>()
                .map_err(crate::error::wrap_delta_err)?;
            metas_to_batch(&metas, &schema, &base_url)
        })
}
