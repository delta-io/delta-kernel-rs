use std::sync::{mpsc, Arc};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, JsonSource};
use datafusion::execution::SessionState;
use datafusion_catalog::memory::DataSourceExec;
use datafusion_common::HashMap;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_plan::execute_stream;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_utils::{parse_json as arrow_parse_json, to_json_bytes};
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::object_store;
use delta_kernel::object_store::{path::Path, PutMode};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, EngineData, Error as DeltaError, ExpressionRef, FileDataReadResultIterator,
    FileMeta, JsonHandler,
};
use futures::stream::{self, BoxStream, StreamExt};
use futures::TryStreamExt;
use itertools::Itertools;
use parking_lot::RwLock;
use tracing::warn;
use url::Url;

use crate::utils::{group_by_store, AsObjectStoreUrl};

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
pub struct DataFusionJsonHandler<E: TaskExecutor> {
    /// Shared session state for the session
    state: Arc<RwLock<SessionState>>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// size of the buffer (via our `sync_channel`).
    buffer_size: usize,
}

impl<E: TaskExecutor> DataFusionJsonHandler<E> {
    pub fn new(state: Arc<RwLock<SessionState>>, task_executor: Arc<E>) -> Self {
        Self {
            state,
            task_executor,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl<E: TaskExecutor> JsonHandler for DataFusionJsonHandler<E> {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<ExpressionRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let to_partitioned_files = |arg: (ObjectStoreUrl, Vec<&FileMeta>)| {
            let (url, files) = arg;
            let part_files = files
                .into_iter()
                .map(|f| {
                    let path = Path::from_url_path(f.location.path())?.to_string();
                    Ok::<_, DeltaError>(PartitionedFile::new(path, f.size))
                })
                .try_collect::<_, Vec<_>, _>()?;
            Ok::<_, DeltaError>((url, part_files))
        };
        let files_by_store = group_by_store(files)
            .into_iter()
            .map(to_partitioned_files)
            .try_collect::<_, HashMap<_, _>, _>()?;

        let source = Arc::new(JsonSource::default());
        let arrow_schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);

        let mut plans = Vec::new();

        for (store_url, files) in files_by_store.into_iter() {
            let config =
                FileScanConfigBuilder::new(store_url, arrow_schema.clone(), source.clone())
                    .with_file_group(files.into_iter().collect())
                    .build();
            // TODO: repartitition plan to read/parse from multiple threads
            let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
            plans.push(plan);
        }

        let plan = match plans.len() {
            1 => plans.remove(0),
            _ => Arc::new(UnionExec::new(plans)),
        };

        let task_ctx = self.state.read().task_ctx();
        let (tx, rx) = mpsc::sync_channel(self.buffer_size);

        self.task_executor.spawn(async move {
            let mut stream: BoxStream<'_, DeltaResult<Box<dyn EngineData>>> =
                match execute_stream(plan, task_ctx).map_err(DeltaError::generic_err) {
                    Ok(stream) => {
                        Box::pin(stream.map_err(DeltaError::generic_err).map_ok(|data| {
                            Box::new(ArrowEngineData::new(data)) as Box<dyn EngineData>
                        }))
                    }
                    Err(e) => {
                        warn!("failed to execute plan: {}", e);
                        Box::pin(stream::once(async move { Err(e) }))
                    }
                };

            // send each record batch over the channel
            while let Some(item) = stream.next().await {
                if tx.send(item).is_err() {
                    warn!("read_json receiver end of channel dropped before sending completed");
                }
            }
        });

        Ok(Box::new(rx.into_iter()))
    }

    // note: for now we just buffer all the data and write it out all at once
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let buffer = to_json_bytes(data)?;
        let put_mode = if overwrite {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };

        let store_url = path.as_object_store_url();
        let store = self
            .state
            .read()
            .runtime_env()
            .object_store(store_url)
            .map_err(DeltaError::generic_err)?;

        let path = Path::from_url_path(path.path())?;
        let path_str = path.to_string();
        self.task_executor
            .block_on(async move { store.put_opts(&path, buffer.into(), put_mode.into()).await })
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { .. } => {
                    DeltaError::FileAlreadyExists(path_str)
                }
                e => e.into(),
            })?;
        Ok(())
    }
}
