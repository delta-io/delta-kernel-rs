// Async trait for physical execution

use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::{future::Either, stream, Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use par_stream::{IndexStreamExt as _, ParParamsConfig, ParStreamExt as _};


use crate::{
    engine::{
        arrow_conversion::TryIntoArrow,
        arrow_data::ArrowEngineData,
        default::{
            executor::TaskExecutor,
            file_stream::{FileOpener, FileStream},
            json::JsonOpener,
            parquet::{ParquetOpener, PresignedUrlOpener},
            DefaultEngine,
        },
    },
    engine_data::FilteredEngineData,
    kernel_df::{
        FileType, FilterNode, FilterVisitor, FilteredEngineDataArc, LogicalPlanNode, ScanNode,
        SelectNode, UnionNode,
    },
    schema::SchemaRef,
    DeltaResult, Engine, EngineData, Error, Expression, FileMeta,
};

// Type alias for boxed streams
type BoxStream<T> = std::pin::Pin<Box<dyn Stream<Item = T> + Send>>;

#[async_trait]
trait AsyncEngine: Engine {
    async fn read_json_files_async(
        &self,
        json: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> DeltaResult<BoxStream<DeltaResult<Box<dyn EngineData>>>>;
    async fn read_parquet_files_async(
        &self,
        json: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> DeltaResult<BoxStream<DeltaResult<Box<dyn EngineData>>>>;
}

#[async_trait]
impl<E: TaskExecutor> AsyncEngine for DefaultEngine<E> {
    async fn read_json_files_async(
        &self,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> DeltaResult<BoxStream<DeltaResult<Box<dyn EngineData>>>> {
        if files.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
        let file_opener = Arc::new(JsonOpener::new(
            1024,
            arrow_schema,
            self.object_store.clone(),
        ));

        let file_futures = files.into_iter().map(move |file| {
            let opener = file_opener.clone();
            async move { opener.open(file, None).await }
        });

        let stream = stream::iter(file_futures)
            .buffered(1024)
            .try_flatten()
            .map_ok(|record_batch| -> Box<dyn EngineData> {
                Box::new(ArrowEngineData::new(record_batch))
            });

        Ok(Box::pin(stream) as BoxStream<_>)
    }
    async fn read_parquet_files_async(
        &self,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> DeltaResult<BoxStream<DeltaResult<Box<dyn EngineData>>>> {
        if files.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let file_opener = Box::new(ParquetOpener::new(
            1024,
            schema.clone(),
            None,
            self.object_store.clone(),
        ));

        self.create_file_stream(file_opener, &files, schema).await
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    async fn create_file_stream(
        &self,
        file_opener: Box<dyn FileOpener>,
        files: &[FileMeta],
        schema: SchemaRef,
    ) -> DeltaResult<BoxStream<DeltaResult<Box<dyn EngineData>>>> {
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
        let mut file_stream = FileStream::new(files.to_vec(), arrow_schema, file_opener)?;

        // Convert FileStream into our BoxStream
        let stream = async_stream::stream! {
            while let Some(result) = file_stream.next().await {
                yield result.map(|batch| {
                    Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
                });
            }
        };

        Ok(Box::pin(stream) as BoxStream<_>)
    }
}

/////////////////////////

// Async executor implementation
struct AsyncPlanExecutor {
    engine: Arc<dyn AsyncEngine>,
}

impl AsyncPlanExecutor {
    fn new(engine: Arc<dyn AsyncEngine>) -> Self {
        Self { engine }
    }
}

impl AsyncPlanExecutor {
    #[async_recursion]
    async fn execute(
        &self,
        plan: LogicalPlanNode,
    ) -> DeltaResult<BoxStream<DeltaResult<FilteredEngineDataArc>>> {
        match plan {
            LogicalPlanNode::Scan(node) => self.execute_scan(node).await,
            LogicalPlanNode::Filter(node) => self.execute_filter(node).await,
            LogicalPlanNode::Select(node) => self.execute_select(node).await,
            LogicalPlanNode::Union(node) => self.execute_union(node).await,
            _ => todo!(),
        }
    }

    #[async_recursion]
    async fn execute_scan(
        &self,
        node: ScanNode,
    ) -> DeltaResult<BoxStream<DeltaResult<FilteredEngineDataArc>>> {
        let stream = match node.file_type {
            FileType::Json => {
                self.engine
                    .read_json_files_async(node.files, node.schema)
                    .await?
            }
            FileType::Parquet => {
                self.engine
                    .read_parquet_files_async(node.files, node.schema)
                    .await?
            }
        };

        // Wrap engine data with full selection vector
        let wrapped = stream.map(|result| {
            result.map(|engine_data| FilteredEngineDataArc {
                selection_vector: vec![true; engine_data.len()],
                engine_data: engine_data.into(),
            })
        });

        Ok(Box::pin(wrapped) as BoxStream<_>)
    }

    #[async_recursion]
    async fn execute_filter(
        &self,
        node: FilterNode,
    ) -> DeltaResult<BoxStream<DeltaResult<FilteredEngineDataArc>>> {
        let FilterNode {
            child,
            filter,
            column_names,
            ordered,
        } = node;
        let child_stream = self.execute(*child).await?;
        // Apply filter asynchronously to each batch
        let then_fn = move |x| {
            println!("executing filtered iter");

            let filter_clone = filter.clone();
            async move {
                let FilteredEngineDataArc {
                    engine_data,
                    selection_vector,
                } = x?;
                let mut visitor = FilterVisitor {
                    filter: filter_clone,
                    selection_vector,
                };
                engine_data.visit_rows(node.column_names, &mut visitor)?;
                Ok(FilteredEngineDataArc {
                    engine_data,
                    selection_vector: visitor.selection_vector,
                })
            }
        };

        let filtered = if ordered {
            Either::Left(child_stream.then(then_fn))
        } else {
            let params = ParParamsConfig::FixedWorkers { num_workers: 16 }.to_params();
            Either::Right(child_stream.par_then_unordered(params, then_fn))
        };

        Ok(Box::pin(filtered) as BoxStream<_>)
    }

    #[async_recursion]
    async fn execute_select(
        &self,
        node: SelectNode,
    ) -> DeltaResult<BoxStream<DeltaResult<FilteredEngineDataArc>>> {
        let SelectNode {
            child,
            columns,
            input_schema,
            output_type,
        } = node;

        let child_stream = self.execute(*child).await?;
        let eval_handler = self.engine.evaluation_handler();
        let evaluator = eval_handler.new_expression_evaluator(
            input_schema,
            Expression::Struct(columns).into(),
            output_type.into(),
        );

        let selected = child_stream.then(move |result| {
            let evaluator_clone = evaluator.clone();
            async move {
                let FilteredEngineDataArc {
                    engine_data,
                    selection_vector,
                } = result?;
                let new_data = evaluator_clone.evaluate(engine_data.as_ref())?;

                Ok(FilteredEngineDataArc {
                    engine_data: new_data.into(),
                    selection_vector,
                })
            }
        });

        Ok(Box::pin(selected) as BoxStream<_>)
    }

    #[async_recursion]
    async fn execute_union(
        &self,
        node: UnionNode,
    ) -> DeltaResult<BoxStream<DeltaResult<FilteredEngineDataArc>>> {
        let stream_a = self.execute(*node.a).await?;
        let stream_b = self.execute(*node.b).await?;

        // NOTE: I buffer the right hand side for testing only
        // Buffer the right-hand side stream
        let buffer_size = 10; // Adjust based on your needs
        let buffered_stream_b = stream_b.ready_chunks(buffer_size);

        // Flatten the buffered chunks back into individual items
        let stream_b_flattened = buffered_stream_b.flat_map(|chunk| stream::iter(chunk));

        // Chain the streams
        Ok(Box::pin(stream_a.chain(stream_b_flattened)) as BoxStream<_>)
    }
}

#[cfg(test)]
mod async_tests {
    use std::{path::PathBuf, sync::Arc};

    use futures::StreamExt;
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use tokio::runtime::Runtime;

    use crate::arrow::compute::filter_record_batch;
    use crate::arrow::record_batch::RecordBatch;
    use crate::arrow::util::pretty::print_batches;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::{DeltaResult, Snapshot};
    use test_utils::{load_test_data, DefaultEngineExtension};

    use super::{AsyncPlanExecutor, FilteredEngineDataArc};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_scan_plan() -> DeltaResult<()> {
        let path = std::fs::canonicalize(PathBuf::from(
            "/Users/oussama/projects/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/"
        )).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        // Create async engine
        let task_executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::new(object_store, task_executor.into());

        let snapshot = Snapshot::builder(url).build(&engine)?;
        let plan = snapshot.get_scan_plan()?;

        // Create async executor
        let executor = AsyncPlanExecutor::new(Arc::new(engine));

        // Execute the plan and get a stream
        let mut stream = executor.execute(plan).await?;

        // Collect results from the stream
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result?;
            let FilteredEngineDataArc {
                engine_data,
                selection_vector,
            } = batch;

            let record_batch: RecordBatch = engine_data
                .as_any()
                .downcast_ref::<ArrowEngineData>()
                .unwrap()
                .record_batch()
                .clone();

            let filtered = filter_record_batch(&record_batch, &selection_vector.into())?;
            batches.push(filtered);
        }

        // Print results
        print_batches(&batches)?;

        Ok(())
    }

    // #[tokio::test]
    // async fn test_async_filter_pipeline() -> DeltaResult<()> {
    //     let path = std::fs::canonicalize(PathBuf::from(
    //         "./tests/data/table-with-dv-small/"
    //     )).unwrap();
    //     let url = url::Url::from_directory_path(path).unwrap();

    //     let engine = DefaultEngine::new_local();

    //     let snapshot = Snapshot::builder(url).build(&engine)?;
    //     let plan = snapshot.get_scan_plan()?;

    //     let executor = AsyncPlanExecutor::new(Arc::new(engine));
    //     let mut stream = executor.execute(plan).await?;

    //     // Count total rows across all batches
    //     let mut total_rows = 0;
    //     let mut total_selected = 0;

    //     while let Some(result) = stream.next().await {
    //         let batch = result?;
    //         total_rows += batch.engine_data.len();
    //         total_selected += batch.selection_vector.iter().filter(|&&x| x).count();
    //     }

    //     println!("Total rows: {}, Selected rows: {}", total_rows, total_selected);

    //     Ok(())
    // }

    // #[tokio::test]
    // async fn test_async_concurrent_execution() -> DeltaResult<()> {
    //     // Test that async execution properly handles concurrent file reading
    //     let path = std::fs::canonicalize(PathBuf::from(
    //         "./tests/data/table-with-dv-small/"
    //     )).unwrap();
    //     let url = url::Url::from_directory_path(path).unwrap();

    //     let engine = Arc::new(DefaultEngine::new(
    //         url.clone(),
    //         Arc::new(TokioBackgroundExecutor::new()),
    //     ));

    //     // Create multiple executors to test concurrent access
    //     let handles: Vec<_> = (0..3)
    //         .map(|i| {
    //             let engine = engine.clone();
    //             let url = url.clone();
    //             tokio::spawn(async move {
    //                 let snapshot = Snapshot::builder(url).build(engine.as_ref()).unwrap();
    //                 let plan = snapshot.get_scan_plan().unwrap();

    //                 let executor = AsyncPlanExecutor::new(engine);
    //                 let mut stream = executor.execute(plan).await.unwrap();

    //                 let mut count = 0;
    //                 while let Some(result) = stream.next().await {
    //                     result.unwrap();
    //                     count += 1;
    //                 }
    //                 println!("Task {} processed {} batches", i, count);
    //                 count
    //             })
    //         })
    //         .collect();

    //     // Wait for all tasks to complete
    //     let results = futures::future::join_all(handles).await;

    //     // Verify all tasks completed successfully
    //     for (i, result) in results.iter().enumerate() {
    //         assert!(result.is_ok(), "Task {} failed", i);
    //     }

    //     Ok(())
    // }
}
