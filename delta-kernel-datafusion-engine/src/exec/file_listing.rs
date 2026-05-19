use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use delta_kernel::object_store::{self, ObjectMeta};
use futures::{ready, stream, Stream, StreamExt};

const BATCH_SIZE: usize = 1024;

fn metas_to_batch(
    metas: &[ObjectMeta],
    schema: &delta_kernel::arrow::datatypes::SchemaRef,
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
    schema: delta_kernel::arrow::datatypes::SchemaRef,
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

    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
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
        let object_store = context.runtime_env().object_store(&object_store_url)?;
        let prefix = object_store::path::Path::from(self.path.path());
        let list_stream = object_store.list(Some(&prefix));
        // `object_store::ObjectStore::list` does not promise lexicographic order. Local-fs
        // backends are particularly inconsistent (directory-walk order from the OS), so we
        // collect+sort there to keep `LogStore` segmentation deterministic across hosts. For
        // remote stores DataFusion's downstream order-aware operators handle re-ordering.
        let state = if self.path.scheme() == "file" {
            ListingState::Collecting {
                inner: list_stream,
                collected: Vec::new(),
            }
        } else {
            ListingState::Chunked {
                inner: Box::pin(list_stream.chunks(BATCH_SIZE)),
            }
        };
        Ok(Box::pin(FileListingStream {
            state,
            schema: self.schema.clone(),
            base_url: self.path.clone(),
        }))
    }
}

type ObjectMetaChunkStream = dyn Stream<Item = Vec<Result<ObjectMeta, object_store::Error>>> + Send;

/// Streams [`ObjectMeta`] batches converted to [`RecordBatch`]es. Two modes:
///
/// * [`ListingState::Chunked`]: passthrough -- groups the underlying `list_stream` into
///   `BATCH_SIZE` chunks and emits one batch per chunk. The first error in a chunk surfaces
///   immediately and ends the stream.
/// * [`ListingState::Collecting`] -> [`ListingState::Emitting`]: collect-then-sort -- drains the
///   full listing into memory, sorts by `location`, then emits in `BATCH_SIZE` chunks.
struct FileListingStream {
    state: ListingState,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    base_url: url::Url,
}

enum ListingState {
    Chunked {
        inner: Pin<Box<ObjectMetaChunkStream>>,
    },
    Collecting {
        inner: Pin<Box<dyn Stream<Item = Result<ObjectMeta, object_store::Error>> + Send>>,
        collected: Vec<ObjectMeta>,
    },
    Emitting {
        inner: Pin<Box<dyn Stream<Item = Vec<ObjectMeta>> + Send>>,
    },
    Done,
}

impl Stream for FileListingStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                ListingState::Chunked { inner } => {
                    let next = ready!(inner.as_mut().poll_next(cx));
                    let Some(chunk) = next else {
                        return Poll::Ready(None);
                    };
                    let metas = chunk
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(crate::error::wrap_delta_err)?;
                    return Poll::Ready(Some(metas_to_batch(&metas, &self.schema, &self.base_url)));
                }
                ListingState::Collecting { inner, collected } => {
                    match ready!(inner.as_mut().poll_next(cx)) {
                        Some(Ok(meta)) => collected.push(meta),
                        Some(Err(e)) => {
                            self.state = ListingState::Done;
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(e))));
                        }
                        None => {
                            let mut sorted = std::mem::take(collected);
                            sorted.sort_by(|a, b| a.location.cmp(&b.location));
                            let chunks: Vec<Vec<ObjectMeta>> =
                                sorted.chunks(BATCH_SIZE).map(|c| c.to_vec()).collect();
                            self.state = ListingState::Emitting {
                                inner: Box::pin(stream::iter(chunks)),
                            };
                        }
                    }
                }
                ListingState::Emitting { inner } => match ready!(inner.as_mut().poll_next(cx)) {
                    Some(chunk) => {
                        return Poll::Ready(Some(metas_to_batch(
                            &chunk,
                            &self.schema,
                            &self.base_url,
                        )));
                    }
                    None => {
                        self.state = ListingState::Done;
                        return Poll::Ready(None);
                    }
                },
                ListingState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for FileListingStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
