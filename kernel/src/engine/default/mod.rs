//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::stream::{self, BoxStream, StreamExt as _, TryStreamExt as _};
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use self::scalar_from_arrow::extract_scalar;
use super::arrow_conversion::TryFromArrow as _;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowEvaluationHandler;
use crate::arrow::array::{ArrayRef, RecordBatch};
use crate::arrow::compute::{concat_batches, partition};
use crate::expressions::Scalar;
use crate::metrics::MetricsReporter;
use crate::object_store::DynObjectStore;
use crate::partition::serialization::serialize_partition_value;
use crate::schema::Schema;
use crate::transaction::{Transaction, WriteContext};
use crate::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandler, JsonHandler, ParquetHandler,
    StorageHandler,
};

/// Hashable key for grouping adjacent partition slices that share the same partition values.
/// Each element is the serialized partition value (`None` = null) for one partition column.
type PartitionGroupKey = Vec<Option<String>>;

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
pub mod scalar_from_arrow;
pub mod stats;
pub mod storage;

/// Converts a Stream-producing future to a synchronous iterator.
///
/// This method performs the initial blocking call to extract the stream from the future, and each
/// subsequent call to `next` on the iterator translates to a blocking `stream.next()` call, using
/// the provided `task_executor`. Buffered streams allow concurrency in the form of prefetching,
/// because that initial call will attempt to populate the N buffer slots; every call to
/// `stream.next()` leaves an empty slot (out of N buffer slots) that the stream immediately
/// attempts to fill by launching another future that can make progress in the background while we
/// block on and consume each of the N-1 entries that precede it.
///
/// This is an internal utility for bridging object_store's async API to
/// Delta Kernel's synchronous handler traits.
pub(crate) fn stream_future_to_iter<T: Send + 'static, E: executor::TaskExecutor>(
    task_executor: Arc<E>,
    stream_future: impl Future<Output = DeltaResult<BoxStream<'static, T>>> + Send + 'static,
) -> DeltaResult<Box<dyn Iterator<Item = T> + Send>> {
    Ok(Box::new(BlockingStreamIterator {
        stream: Some(task_executor.block_on(stream_future)?),
        task_executor,
    }))
}

struct BlockingStreamIterator<T: Send + 'static, E: executor::TaskExecutor> {
    stream: Option<BoxStream<'static, T>>,
    task_executor: Arc<E>,
}

impl<T: Send + 'static, E: executor::TaskExecutor> Iterator for BlockingStreamIterator<T, E> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Move the stream into the future so we can block on it.
        let mut stream = self.stream.take()?;
        let (item, stream) = self
            .task_executor
            .block_on(async move { (stream.next().await, stream) });

        // We must not poll an exhausted stream after it returned None.
        if item.is_some() {
            self.stream = Some(stream);
        }

        item
    }
}

const DEFAULT_BUFFER_SIZE: usize = 1000;
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Wraps a [`FileDataReadResultIterator`] to emit a [`MetricEvent`] exactly once when the iterator
/// is either exhausted or dropped. Used by JSON and Parquet handlers to report the number of files
/// and bytes requested per `read_*_files` call.
pub(super) struct ReadMetricsIterator {
    inner: crate::FileDataReadResultIterator,
    reporter: Arc<dyn crate::metrics::MetricsReporter>,
    num_files: u64,
    bytes_read: u64,
    emitted: bool,
    make_event: fn(u64, u64) -> crate::metrics::MetricEvent,
}

impl ReadMetricsIterator {
    pub(super) fn new(
        inner: crate::FileDataReadResultIterator,
        reporter: Arc<dyn crate::metrics::MetricsReporter>,
        num_files: u64,
        bytes_read: u64,
        make_event: fn(u64, u64) -> crate::metrics::MetricEvent,
    ) -> Self {
        Self {
            inner,
            reporter,
            num_files,
            bytes_read,
            emitted: false,
            make_event,
        }
    }

    fn emit_once(&mut self) {
        if !self.emitted {
            self.emitted = true;
            self.reporter
                .report((self.make_event)(self.num_files, self.bytes_read));
        }
    }
}

impl Iterator for ReadMetricsIterator {
    type Item = crate::DeltaResult<Box<dyn crate::EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if item.is_none() {
            self.emit_once();
        }
        item
    }
}

impl Drop for ReadMetricsIterator {
    fn drop(&mut self) {
        self.emit_once();
    }
}

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    storage: Arc<ObjectStoreStorageHandler<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

/// Builder for creating [`DefaultEngine`] instances.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel::engine::default::DefaultEngineBuilder;
/// # use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
/// # use delta_kernel::object_store::local::LocalFileSystem;
/// // Build a DefaultEngine with default executor
/// let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
///     .build();
///
/// // Build with a custom executor
/// let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
///     .with_task_executor(Arc::new(TokioBackgroundExecutor::new()))
///     .build();
/// ```
#[derive(Debug)]
pub struct DefaultEngineBuilder<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

impl DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`DefaultEngineBuilder`] instance with the default executor.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            task_executor: Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
            metrics_reporter: None,
        }
    }
}

impl<E: TaskExecutor> DefaultEngineBuilder<E> {
    /// Set the metrics reporter for the engine.
    pub fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }

    /// Set a custom task executor for the engine.
    ///
    /// See [`executor::TaskExecutor`] for more details.
    pub fn with_task_executor<F: TaskExecutor>(
        self,
        task_executor: Arc<F>,
    ) -> DefaultEngineBuilder<F> {
        DefaultEngineBuilder {
            object_store: self.object_store,
            task_executor,
            metrics_reporter: self.metrics_reporter,
        }
    }

    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<E> {
        DefaultEngine::new_with_opts(self.object_store, self.task_executor, self.metrics_reporter)
    }
}

impl DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a [`DefaultEngineBuilder`] for constructing a [`DefaultEngine`] with custom options.
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    pub fn builder(
        object_store: Arc<DynObjectStore>,
    ) -> DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(object_store)
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    fn new_with_opts(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        metrics_reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> Self {
        Self {
            storage: Arc::new(ObjectStoreStorageHandler::new(
                object_store.clone(),
                task_executor.clone(),
                metrics_reporter.clone(),
            )),
            json: Arc::new(
                DefaultJsonHandler::new(object_store.clone(), task_executor.clone())
                    .with_reporter(metrics_reporter.clone()),
            ),
            parquet: Arc::new(
                DefaultParquetHandler::new(object_store.clone(), task_executor.clone())
                    .with_reporter(metrics_reporter.clone()),
            ),
            object_store,
            task_executor,
            evaluation: Arc::new(ArrowEvaluationHandler {}),
            metrics_reporter,
        }
    }

    /// Enter the runtime context of the executor associated with this engine.
    ///
    /// # Panics
    ///
    /// When calling `enter` multiple times, the returned guards **must** be dropped in the reverse
    /// order that they were acquired.  Failure to do so will result in a panic and possible memory
    /// leaks.
    pub fn enter(&self) -> <E as TaskExecutor>::Guard<'_> {
        self.task_executor.enter()
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.object_store.clone())
    }

    /// Write `data` as a parquet file using the provided `write_context`.
    ///
    /// The `write_context` must be created by [`Transaction::partitioned_write_context`] or
    /// [`Transaction::unpartitioned_write_context`], which handle partition value validation,
    /// serialization, and logical-to-physical key translation.
    ///
    /// [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context
    /// [`Transaction::unpartitioned_write_context`]: crate::transaction::Transaction::unpartitioned_write_context
    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let transform = write_context.logical_to_physical();
        let input_schema = Schema::try_from_arrow(data.record_batch().schema())?;
        let output_schema = write_context.physical_schema();
        let logical_to_physical_expr = self.evaluation_handler().new_expression_evaluator(
            input_schema.into(),
            transform.clone(),
            output_schema.clone().into(),
        )?;
        let physical_data = logical_to_physical_expr.evaluate(data)?;
        // Random 2-char prefix for CM tables, Hive-style for partitioned, else table root.
        let write_dir = write_context.write_dir();
        self.parquet
            .write_parquet_file(
                &write_dir,
                physical_data,
                write_context.physical_partition_values(),
                Some(write_context.stats_columns()),
            )
            .await
    }

    /// Partitions a [`RecordBatch`] by partition column values and writes each partition to a
    /// separate parquet file, returning all add-file metadata as a single [`EngineData`] batch.
    ///
    /// The input `data` must contain ALL columns (data + partition) matching the table's logical
    /// schema. Partition columns are discovered from the transaction. For unpartitioned tables,
    /// writes the entire batch as a single file.
    ///
    /// Uses the partition-then-rejoin strategy: scans the batch once to find runs of adjacent
    /// identical partition values (zero-copy slices), groups them by partition key, merges
    /// non-adjacent runs via [`concat_batches`], then writes each distinct partition concurrently
    /// (up to 50 in-flight writes) using buffered streams.
    ///
    /// Returns a single [`EngineData`] batch containing all add-file metadata (one row per
    /// written file). Pass this directly to [`Transaction::add_files`].
    ///
    /// [`Transaction::add_files`]: crate::transaction::Transaction::add_files
    pub async fn write_partitioned_parquet<S>(
        &self,
        data: &ArrowEngineData,
        txn: &Transaction<S>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let batch = data.record_batch();
        let partition_cols = txn.logical_partition_columns();

        println!(
            "[write_partitioned_parquet] input: {} rows, {} cols, partition_cols={:?}",
            batch.num_rows(),
            batch.num_columns(),
            partition_cols
        );

        // Unpartitioned fast path: write the whole batch as a single file.
        if partition_cols.is_empty() {
            println!("[write_partitioned_parquet] unpartitioned table, writing single file");
            let wc = txn.unpartitioned_write_context()?;
            return self.write_parquet(data, &wc).await;
        }

        // === PHASE 1: Find partition column positions in the batch ===
        //
        // The input batch has columns in the table's logical schema order, e.g.:
        //
        //   batch schema: [id: INT, region: STRING, category: STRING, value: DOUBLE]
        //   partition_cols (from table metadata): ["region", "category"]
        //
        // We need the *index* of each partition column so we can pull out its Arrow
        // array later. `schema.index_of("region")` returns 1, `index_of("category")`
        // returns 2, so col_indices = [1, 2].
        let col_indices: Vec<usize> = partition_cols
            .iter()
            .map(|name| {
                // batch.schema() returns the Arrow Schema for this RecordBatch.
                // index_of(name) does a linear scan of its fields and returns the
                // 0-based position, or Err if the column doesn't exist.
                batch.schema().index_of(name).map_err(|_| {
                    Error::generic(format!(
                        "partition column '{}' not found in input batch schema",
                        name
                    ))
                })
            })
            // collect::<DeltaResult<Vec<usize>>>() short-circuits on the first Err,
            // so if any partition column is missing from the batch we fail immediately.
            .collect::<DeltaResult<_>>()?;

        // === PHASE 2: Find runs of identical partition values (the "partition" step) ===
        //
        // Goal: split the batch into groups where each group has the same partition
        // column values, so we can write one parquet file per distinct partition.
        //
        // Example input (6 rows, 2 partition columns):
        //
        //   row | id | region | category
        //   ----+----+--------+---------
        //    0  |  1 | "US"   | "A"
        //    1  |  2 | "US"   | "A"       <-- same as row 0
        //    2  |  3 | "EU"   | "B"       <-- different, new run starts
        //    3  |  4 | "US"   | "A"       <-- same as rows 0-1, but non-adjacent
        //    4  |  5 | "EU"   | "B"       <-- same as row 2, but non-adjacent
        //    5  |  6 | "EU"   | "B"       <-- same as row 4, continues run
        //
        // First, extract just the partition column arrays from the batch. Using
        // col_indices = [1, 2], we get:
        //   partition_arrays = [region_array, category_array]
        let partition_arrays: Vec<ArrayRef> = col_indices
            .iter()
            .map(|&idx| batch.column(idx).clone()) // clone() is cheap: Arc clone, not data copy
            .collect();

        // Arrow's `partition()` scans the arrays and finds ranges of *adjacent* rows
        // that have identical values across ALL partition columns. It does NOT sort or
        // reorder the data. It returns a `Partitions` struct whose `.ranges()` method
        // gives a Vec<Range<usize>>.
        //
        // For the example above:
        //   ranges = [0..2, 2..3, 3..4, 4..6]
        //
        //   0..2 = rows 0-1: ("US", "A")   <-- 2 adjacent rows with same values
        //   2..3 = row 2:    ("EU", "B")   <-- different from previous
        //   3..4 = row 3:    ("US", "A")   <-- same VALUES as 0..2, but non-adjacent
        //   4..6 = rows 4-5: ("EU", "B")   <-- same VALUES as 2..3, but non-adjacent
        //
        // Note: runs 0..2 and 3..4 have the same partition values ("US","A") but are
        // separate ranges because they are not adjacent in the input. We will merge
        // them in phase 3.
        let ranges = partition(&partition_arrays)
            .map_err(|e| Error::generic(format!("arrow partition failed: {e}")))?
            .ranges();

        println!(
            "[write_partitioned_parquet] phase 2: found {} adjacent runs from {} rows",
            ranges.len(),
            batch.num_rows()
        );

        // === PHASE 3: Group runs by partition key, merging non-adjacent runs ===
        //
        // We need to collect all rows for each distinct partition together, even if
        // they appeared in multiple non-adjacent runs. We use a HashMap keyed by the
        // serialized partition values (the "group key").
        //
        // The group key is a Vec<Option<String>> where each element is the serialized
        // partition value for one partition column (None = null). For example:
        //   ("US", "A") serializes to [Some("US"), Some("A")]
        //   ("EU", "B") serializes to [Some("EU"), Some("B")]
        //
        // Each map entry stores:
        //   (HashMap<String, Scalar>, Vec<RecordBatch>)
        //    ^                         ^
        //    |                         |
        //    typed partition values    all slices belonging to this partition
        //    (for write_context)       (will be concatenated if > 1)
        let mut groups: HashMap<PartitionGroupKey, (HashMap<String, Scalar>, Vec<RecordBatch>)> =
            HashMap::new();

        for range in &ranges {
            // batch.slice(offset, length) creates a zero-copy view: no data is copied,
            // it just records "start at row `offset`, include `length` rows" over the
            // same underlying Arrow buffers.
            //
            // For range 0..2: slice = rows 0-1 (length 2)
            // For range 2..3: slice = row 2 (length 1)
            // For range 3..4: slice = row 3 (length 1)
            // For range 4..6: slice = rows 4-5 (length 2)
            let slice = batch.slice(range.start, range.end - range.start);

            // For this slice, extract the typed Scalar value for each partition column
            // from row 0 of the slice (all rows in a run have identical partition values,
            // since that is exactly what `partition()` guarantees).
            //
            // For slice [rows 0-1], partition cols at indices [1,2]:
            //   extract_scalar(region_array, 0) => Scalar::String("US")
            //   extract_scalar(category_array, 0) => Scalar::String("A")
            //
            // `values` = {"region": Scalar::String("US"), "category": Scalar::String("A")}
            // `group_key` = [Some("US"), Some("A")]
            //
            // The group_key uses the *serialized* string form so it can be used as a
            // HashMap key (Scalar itself does not implement Hash because of floats).
            let mut values = HashMap::with_capacity(col_indices.len());
            let mut group_key = Vec::with_capacity(col_indices.len());
            for (name, &idx) in partition_cols.iter().zip(&col_indices) {
                let scalar = extract_scalar(slice.column(idx).as_ref(), 0)?;
                group_key.push(serialize_partition_value(&scalar)?);
                values.insert(name.clone(), scalar);
            }

            // Insert into the groups map. If this partition key already exists (because
            // we saw it in an earlier non-adjacent run), we append this slice to its
            // Vec<RecordBatch>. Otherwise, create a new entry.
            //
            // After processing all 4 ranges from our example:
            //   groups = {
            //     [Some("US"), Some("A")] => ({"region":"US","category":"A"}, [slice(0..2), slice(3..4)])
            //     [Some("EU"), Some("B")] => ({"region":"EU","category":"B"}, [slice(2..3), slice(4..6)])
            //   }
            //
            // The ("US","A") partition has 2 non-adjacent slices that will be merged next.
            println!(
                "[write_partitioned_parquet] phase 3: range {:?} -> key={:?} ({} rows)",
                range,
                group_key,
                range.end - range.start,
            );

            groups
                .entry(group_key)
                .or_insert_with(|| (values, Vec::new()))
                .1
                .push(slice);
        }

        println!(
            "[write_partitioned_parquet] phase 3 done: {} distinct partitions from {} runs",
            groups.len(),
            ranges.len()
        );

        // === PHASE 4: Rejoin non-adjacent runs into one batch per partition ===
        //
        // After phase 3, groups might look like:
        //   ("US","A") => [slice(0..2), slice(3..4)]   <-- 2 slices, need merging
        //   ("EU","B") => [slice(2..3), slice(4..6)]   <-- 2 slices, need merging
        //
        // If the input data happened to be pre-sorted by partition columns, each
        // partition would have exactly 1 slice (the common/fast case). In that case
        // we skip the concat and use the zero-copy slice directly.
        //
        // When there are multiple non-adjacent slices (as above), we use
        // `concat_batches` to physically copy and merge them into a single contiguous
        // RecordBatch. This is the only place data is actually copied.
        //
        // Result for our example:
        //   partitions = [
        //     ({"region":"US","category":"A"}, RecordBatch[rows 0,1,3]),  <-- 3 rows merged
        //     ({"region":"EU","category":"B"}, RecordBatch[rows 2,4,5]),  <-- 3 rows merged
        //   ]
        let partitions: Vec<(HashMap<String, Scalar>, RecordBatch)> = groups
            .into_values()
            .enumerate()
            .map(
                |(i, (values, slices)): (usize, (HashMap<String, Scalar>, Vec<RecordBatch>))| {
                    let total_rows: usize =
                        slices.iter().map(|s| s.num_rows()).sum();
                    println!(
                        "[write_partitioned_parquet] phase 4: partition {}: {} slices, {} total rows, values={:?}",
                        i, slices.len(), total_rows, values
                    );
                    if slices.len() == 1 {
                        // Fast path: single contiguous run, no copy needed.
                        let batch = slices.into_iter().next().ok_or_else(|| {
                            Error::generic("expected at least one slice in partition group")
                        })?;
                        Ok((values, batch))
                    } else {
                        // Slow path: multiple non-adjacent runs, must copy data to merge.
                        let merged = concat_batches(&slices[0].schema(), &slices)
                            .map_err(|e| Error::generic(format!("concat_batches failed: {e}")))?;
                        Ok((values, merged))
                    }
                },
            )
            .collect::<DeltaResult<_>>()?;

        // === PHASE 5: Write each partition to its own parquet file ===
        //
        // For each (partition_values, batch) pair:
        //   1. txn.partitioned_write_context(values) validates the partition values,
        //      serializes them, translates logical column names to physical names, and
        //      builds a WriteContext that knows the target directory and schema.
        //   2. self.write_parquet(batch, write_context) transforms the data from
        //      logical to physical schema (removing partition columns from the parquet
        //      file when applicable), writes the parquet file to object storage, and
        //      returns an EngineData batch containing the Add action metadata (file
        //      path, size, stats, partitionValues).
        //
        // `.buffered(50)` runs up to 50 writes concurrently. Each write is an async
        // operation (uploading to object storage), so parallelism helps throughput.
        //
        // For our example, this produces 2 parquet files:
        //   file1: region=US/category=A/<uuid>.parquet  (3 rows: ids 1,2,3)
        //   file2: region=EU/category=B/<uuid>.parquet  (3 rows: ids 2,4,5)
        // And 2 EngineData batches, each with one row of Add action metadata.
        let results: Vec<Box<dyn EngineData>> = stream::iter(partitions)
            .map(|(values, group_batch)| async move {
                let wc = txn.partitioned_write_context(values)?;
                self.write_parquet(&ArrowEngineData::new(group_batch), &wc)
                    .await
            })
            .buffered(MAX_CONCURRENT_PARTITION_WRITES)
            .try_collect()
            .await?;

        println!(
            "[write_partitioned_parquet] phase 5 done: wrote {} parquet files",
            results.len()
        );

        // === PHASE 6: Merge metadata into a single batch ===
        //
        // Each write_parquet call returned one EngineData batch with 1 row of Add
        // action metadata (path, size, partitionValues, stats). Merge them into a
        // single batch so the caller can do one txn.add_files(metadata) call.
        //
        // For our example: merges 2 single-row batches into 1 two-row batch.
        merge_add_file_metadata(results)
    }
}

/// Converts [`DataFileMetadata`] into Add action [`EngineData`] using the partition values
/// from the provided [`WriteContext`].
///
/// This is the public API for building Add action metadata from file write results. Custom
/// Arrow-based engines that write parquet files themselves (bypassing [`DefaultEngine::write_parquet`])
/// should call this to produce the Add action metadata for [`Transaction::add_files`].
///
/// [`DataFileMetadata`]: parquet::DataFileMetadata
/// [`Transaction::add_files`]: crate::transaction::Transaction::add_files
pub fn build_add_file_metadata(
    file_metadata: parquet::DataFileMetadata,
    write_context: &WriteContext,
) -> DeltaResult<Box<dyn EngineData>> {
    file_metadata.as_record_batch(write_context.physical_partition_values())
}

/// Maximum number of concurrent partition writes in [`DefaultEngine::write_partitioned_parquet`].
const MAX_CONCURRENT_PARTITION_WRITES: usize = 50;

/// Merges multiple add-file metadata batches into a single [`EngineData`] batch.
fn merge_add_file_metadata(batches: Vec<Box<dyn EngineData>>) -> DeltaResult<Box<dyn EngineData>> {
    if batches.len() == 1 {
        return batches
            .into_iter()
            .next()
            .ok_or_else(|| Error::generic("expected at least one metadata batch"));
    }
    let arrow_batches: Vec<Box<ArrowEngineData>> = batches
        .into_iter()
        .map(|b| {
            ArrowEngineData::try_from_engine_data(b)
                .map_err(|_| Error::generic("expected ArrowEngineData from write_parquet"))
        })
        .collect::<DeltaResult<_>>()?;
    let record_batches: Vec<&RecordBatch> =
        arrow_batches.iter().map(|d| d.record_batch()).collect();
    let schema = record_batches[0].schema();
    let merged = concat_batches(&schema, record_batches)
        .map_err(|e| Error::generic(format!("failed to merge add-file metadata: {e}")))?;
    Ok(Box::new(ArrowEngineData::new(merged)))
}

impl<E: TaskExecutor> Engine for DefaultEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    fn get_metrics_reporter(&self) -> Option<Arc<dyn MetricsReporter>> {
        self.metrics_reporter.clone()
    }
}

trait UrlExt {
    // Check if a given url is a presigned url and can be used
    // to access the object store via simple http requests
    fn is_presigned(&self) -> bool;
}

impl UrlExt for Url {
    fn is_presigned(&self) -> bool {
        matches!(self.scheme(), "http" | "https")
            && (
                // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
                // https://developers.cloudflare.com/r2/api/s3/presigned-urls/
                self
                .query_pairs()
                .any(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Signature")) ||
                // https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#version-2020-12-06-and-later
                // note signed permission (sp) must always be present
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("sp")) ||
                // https://cloud.google.com/storage/docs/authentication/signatures
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-Goog-Credential")) ||
                // https://www.alibabacloud.com/help/en/oss/user-guide/upload-files-using-presigned-urls
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-OSS-Credential"))
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::test_arrow_engine;
    use crate::metrics::MetricEvent;
    use crate::object_store::local::LocalFileSystem;

    #[derive(Debug)]
    struct TestMetricsReporter;

    impl MetricsReporter for TestMetricsReporter {
        fn report(&self, _event: MetricEvent) {}
    }

    #[test]
    fn test_default_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_new_and_build() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_with_metrics_reporter() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let engine = DefaultEngineBuilder::new(object_store)
            .with_metrics_reporter(reporter)
            .build();
        assert!(engine.get_metrics_reporter().is_some());
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_with_custom_executor() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(executor::tokio::TokioMultiThreadExecutor::new(
            rt.handle().clone(),
        ));
        let engine = DefaultEngineBuilder::new(object_store)
            .with_task_executor(executor)
            .build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_method() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::builder(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_all_options() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let executor = Arc::new(executor::tokio::TokioBackgroundExecutor::new());
        let engine = DefaultEngineBuilder::new(object_store)
            .with_metrics_reporter(reporter)
            .with_task_executor(executor)
            .build();
        assert!(engine.get_metrics_reporter().is_some());
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_pre_signed_url() {
        let url = Url::parse("https://example.com?X-Amz-Signature=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?sp=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?X-Goog-Credential=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?X-OSS-Credential=foo").unwrap();
        assert!(url.is_presigned());

        // assert that query keys are case insensitive
        let url = Url::parse("https://example.com?x-gooG-credenTIAL=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?x-oss-CREDENTIAL=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com").unwrap();
        assert!(!url.is_presigned());
    }
}
