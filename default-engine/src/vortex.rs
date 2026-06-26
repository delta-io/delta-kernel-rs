//! Vortex alternate file-format support (hackathon POC).
//!
//! Delta data files are normally Parquet. This module adds read+write support for files in the
//! [Vortex](https://docs.rs/vortex) columnar format, discriminated purely by a `.vortex` filename
//! extension (no protocol change). The scan path dispatches to `read_vortex_files` per file based
//! on that extension (see `parquet.rs`), which is what makes a *mixed* Parquet+Vortex table read
//! correctly: the discriminator round-trips through `add.path`.
//!
//! This is a prototype, not the production API. Keeping the dispatch inside the default engine and
//! reusing kernel's Arrow reconciliation helpers means zero kernel public-API change. The real API
//! (a `fileFormats` table feature + a per-file `fileFormat` field + a format->handler registry) is
//! a protocol RFC that is out of scope here.
//!
//! ## Reader contract
//!
//! Kernel runs no normalization over handler output, so the reader owns reconciliation. After
//! decoding a `.vortex` file to an Arrow `RecordBatch`, we reconcile it to the requested
//! `physical_schema` with the same helpers the Parquet path uses
//! ([`get_requested_indices`] + [`fixup_parquet_read`]). That gives us, for free: exact columns in
//! schema order, all-NULL backfill for missing nullable columns (error on missing non-nullable),
//! and `Cast` transforms that normalize logical types (notably timestamp unit -> microseconds).
//! Row order is preserved; each file is streamed one [`ArrowEngineData`] per Vortex scan split,
//! and batches are never merged across file boundaries.
//!
//! ## POC limitations
//!
//! Only flat (top-level primitive) schemas are supported on read. Kernel's projection mask uses
//! flattened *leaf* indices, which only coincide with top-level column indices when the schema has
//! no nested struct/list/map columns. Nested schemas are rejected rather than silently
//! mis-projected (see `ensure_flat_schema`). Column projection is pushed down into the Vortex scan
//! (only requested columns are decoded); predicate pushdown, deletion vectors, row indexes, and
//! column mapping are all punted -- safe given the locked POC table shape (column mapping None, no
//! deletion vectors, no row tracking).

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::arrow::array::{AsArray as _, RecordBatch};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_utils::{fixup_parquet_read, get_requested_indices};
use delta_kernel::expressions::ColumnName;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{DeltaResult, EngineData, Error, FileMeta};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;
use vortex::array::arrow::{ArrowSessionExt as _, FromArrowArray as _};
use vortex::array::{ArrayRef as VortexArrayRef, VortexSessionExecute as _};
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt as _, VortexFile, WriteOptionsSessionExt as _};
use vortex::session::VortexSession;
use vortex::VortexSessionDefault as _;

use crate::parquet::DataFileMetadata;
use crate::stats::collect_stats;

/// The filename extension that marks a Delta data file as Vortex-encoded.
pub(crate) const VORTEX_EXTENSION: &str = ".vortex";

/// Maps a Vortex error into a kernel [`Error`]. Vortex's error type does not implement
/// `Into<delta_kernel::Error>`, so we stringify at the boundary.
fn vortex_err(e: impl std::fmt::Display) -> Error {
    Error::generic(format!("Vortex error: {e}"))
}

/// Rejects schemas with nested (non-primitive) columns. See the module-level POC limitations.
fn ensure_flat_schema(schema: &SchemaRef) -> DeltaResult<()> {
    for field in schema.fields() {
        if !matches!(field.data_type(), DataType::Primitive(_)) {
            return Err(Error::unsupported(format!(
                "Vortex POC reader supports only flat primitive schemas; column '{}' has type {:?}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

/// Reads a set of `.vortex` files into a stream of [`EngineData`], each batch reconciled to
/// `physical_schema`.
///
/// Mirrors `read_parquet_files_impl`: it decodes each file independently and never merges batches
/// across file boundaries, so positional selection/deletion-vector alignment in `Scan::execute`
/// stays valid. Predicate pushdown is not applied (POC punt).
pub(crate) async fn read_vortex_files(
    store: Arc<DynObjectStore>,
    files: Vec<FileMeta>,
    physical_schema: SchemaRef,
) -> DeltaResult<BoxStream<'static, DeltaResult<Box<dyn EngineData>>>> {
    if files.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    ensure_flat_schema(&physical_schema)?;

    let file_futures = files.into_iter().map(move |file| {
        let store = store.clone();
        let physical_schema = physical_schema.clone();
        async move { decode_vortex_file(store, physical_schema, file).await }
    });
    let result_stream = stream::iter(file_futures)
        .buffered(super::DEFAULT_BUFFER_SIZE)
        .try_flatten()
        .map_ok(|data: ArrowEngineData| -> Box<dyn EngineData> { Box::new(data) });

    Ok(Box::pin(result_stream))
}

/// Decodes a single `.vortex` file into a stream of Arrow batches (one per Vortex scan split), each
/// reconciled to `physical_schema`.
///
/// Streaming chunk-by-chunk bounds peak memory and mirrors the Parquet reader's per-batch shape.
/// Chunks keep their in-file order and are never merged across files.
async fn decode_vortex_file(
    store: Arc<DynObjectStore>,
    physical_schema: SchemaRef,
    file: FileMeta,
) -> DeltaResult<BoxStream<'static, DeltaResult<ArrowEngineData>>> {
    let file_location = file.location.to_string();
    let path = Path::from_url_path(file.location.path())?;
    let bytes = store.get(&path).await?.bytes().await?;

    let session = VortexSession::default();
    // `open_buffer` is synchronous (no I/O runtime); the scan decode is async.
    let file = session
        .open_options()
        .open_buffer(bytes.to_vec())
        .map_err(vortex_err)?;

    // Projection pushdown: decode only the file columns the scan requests. We intersect the
    // requested columns with the file's actual fields (Vortex `select` errors on unknown names);
    // reconciliation still reorders, backfills missing nullable columns, and casts afterward, all
    // by name. When no requested column is present (e.g. all-missing -> all-NULL backfill) we skip
    // projection: an empty projection is invalid, and reconciliation still needs the file's rows.
    let projection = projection_columns(&file, &physical_schema);
    let mut scan = file.scan().map_err(vortex_err)?;
    if !projection.is_empty() {
        scan = scan.with_projection(select(projection, root()));
    }

    // `into_array_stream` yields one array per scan split (vs `read_all`, which collapses them into
    // a single array), so we decode and reconcile chunk-by-chunk to bound peak memory. The stream
    // is `'static`, so the opened file need not be held alive separately.
    let array_stream = scan.into_array_stream().map_err(vortex_err)?;

    let mut ctx = session.create_execution_ctx();
    let batches = array_stream.map(move |array| {
        // `execute_arrow` canonicalizes each chunk to one contiguous Arrow array (in-file row order
        // preserved); reconciliation then aligns it to `physical_schema`.
        let arrow_array = session
            .arrow()
            .execute_arrow(array.map_err(vortex_err)?, None, &mut ctx)
            .map_err(vortex_err)?;
        let batch = RecordBatch::from(arrow_array.as_struct());
        reconcile_to_schema(batch, &physical_schema, &file_location)
    });

    Ok(Box::pin(batches))
}

/// Returns the `physical_schema` column names that are present in `file`, in requested order, for
/// projection pushdown. Empty when `file` exposes no struct fields or none of the requested columns
/// exist -- the caller then skips pushdown and lets reconciliation backfill.
fn projection_columns<'a>(file: &VortexFile, physical_schema: &'a SchemaRef) -> Vec<&'a str> {
    let Some(file_fields) = file.dtype().as_struct_fields_opt() else {
        return Vec::new();
    };
    let present: HashSet<&str> = file_fields.names().iter().map(|n| n.as_ref()).collect();
    physical_schema
        .fields()
        .map(|f| f.name().as_str())
        .filter(|name| present.contains(name))
        .collect()
}

/// Reconciles a decoded Arrow batch (the file's physical columns, in file order) to the requested
/// `physical_schema`, reusing the Parquet path's reconciliation helpers.
fn reconcile_to_schema(
    batch: RecordBatch,
    physical_schema: &SchemaRef,
    file_location: &str,
) -> DeltaResult<ArrowEngineData> {
    let decoded_schema = batch.schema();
    // `mask_indices` are ascending leaf indices of the matched columns (in file order); for a flat
    // schema leaf index == top-level column index, so `project` reproduces the Parquet
    // `ProjectionMask`. `reorder_indices` carry any reordering and `Cast`/`Missing` transforms.
    let (mask_indices, reorder_indices) = get_requested_indices(physical_schema, &decoded_schema)?;
    let projected = batch.project(&mask_indices).map_err(Error::Arrow)?;
    fixup_parquet_read(
        projected,
        &reorder_indices,
        None,
        Some(file_location),
        Some(physical_schema),
    )
}

/// Writes `data` to a new `<uuid>.vortex` file under `write_dir` and returns its
/// [`DataFileMetadata`] (file metadata + collected statistics), ready for
/// [`build_add_file_metadata`].
///
/// Statistics are collected from the in-memory `RecordBatch` exactly as for Parquet, so data
/// skipping keeps working regardless of the on-disk format.
///
/// [`build_add_file_metadata`]: crate::build_add_file_metadata
pub(crate) async fn write_vortex(
    store: Arc<DynObjectStore>,
    write_dir: &url::Url,
    data: Box<dyn EngineData>,
    stats_columns: &[ColumnName],
) -> DeltaResult<DataFileMetadata> {
    let batch: Box<_> = ArrowEngineData::try_from_engine_data(data)?;
    let record_batch = batch.record_batch();

    // Collect statistics before encoding (format-independent, mirrors the Parquet writer).
    let stats = collect_stats(record_batch, stats_columns)?;

    let buffer = encode_vortex(record_batch).await?;
    let size: u64 = buffer
        .len()
        .try_into()
        .map_err(|_| Error::generic("unable to convert usize to u64"))?;

    let name = format!("{}{VORTEX_EXTENSION}", Uuid::new_v4());
    if !write_dir.path().ends_with('/') {
        return Err(Error::generic(format!(
            "Path must end with a trailing slash: {write_dir}"
        )));
    }
    let path = write_dir.join(&name)?;

    store
        .put(&Path::from_url_path(path.path())?, buffer.into())
        .await?;
    let metadata = store.head(&Path::from_url_path(path.path())?).await?;
    let modification_time = metadata.last_modified.timestamp_millis();
    if size != metadata.size {
        return Err(Error::generic(format!(
            "Size mismatch after writing vortex file: expected {size}, got {}",
            metadata.size
        )));
    }

    let file_meta = FileMeta::new(path, modification_time, size);
    Ok(DataFileMetadata::new(file_meta, stats))
}

/// Encodes an Arrow `RecordBatch` into an in-memory Vortex file (full file with footer).
async fn encode_vortex(record_batch: &RecordBatch) -> DeltaResult<Vec<u8>> {
    let session = VortexSession::default();
    // The top-level RecordBatch struct is never null, so `nullable = false` is safe; per-column
    // nullability is derived from the Arrow schema fields by `from_arrow`.
    let vortex_array = VortexArrayRef::from_arrow(record_batch, false).map_err(vortex_err)?;
    let mut buffer: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut buffer, vortex_array.to_array_stream())
        .await
        .map_err(vortex_err)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{
        Array, ArrayRef, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use delta_kernel::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit,
    };
    use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel_default_engine_test_utils::try_into_record_batch as into_record_batch;
    use vortex::file::WriteStrategyBuilder;

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![10_000_000i64, 20_000_000, 30_000_000])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap()
    }

    // Round-trips a RecordBatch through write_vortex + read_vortex_files using the full physical
    // schema. Asserts the read-back batch equals the written one (row order, values, and the
    // microsecond timestamp + timezone all preserved).
    #[tokio::test]
    async fn test_vortex_write_read_roundtrip() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let batch = sample_batch();
        let physical_schema: SchemaRef =
            Arc::new(batch.schema().as_ref().try_into_kernel().unwrap());

        let data = Box::new(ArrowEngineData::new(batch.clone()));
        let meta = write_vortex(
            store.clone(),
            &url::Url::parse("memory:///data/").unwrap(),
            data,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(meta.location().path().rsplit('.').next(), Some("vortex"));

        let read: Vec<RecordBatch> =
            read_vortex_files(store, vec![meta.file_meta().clone()], physical_schema)
                .await
                .unwrap()
                .map(into_record_batch)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(read.len(), 1);
        // Vortex decodes STRING columns as Utf8View, which kernel accepts as a valid physical
        // STRING representation (string-width normalization is an explicit reader-contract punt).
        // Cast back to the input's Utf8 encoding to compare logical content.
        let normalized = cast_to_schema(&read[0], &batch.schema());
        assert_eq!(normalized, batch);
    }

    /// Casts every column of `batch` to the types in `target` (rebuilding under `target`'s schema),
    /// so logical content can be compared across physical-encoding differences (e.g. Utf8View vs
    /// Utf8).
    fn cast_to_schema(
        batch: &RecordBatch,
        target: &Arc<delta_kernel::arrow::datatypes::Schema>,
    ) -> RecordBatch {
        let columns = batch
            .columns()
            .iter()
            .zip(target.fields())
            .map(|(col, field)| delta_kernel::arrow::compute::cast(col, field.data_type()).unwrap())
            .collect();
        RecordBatch::try_new(target.clone(), columns).unwrap()
    }

    // A subset+reorder projection: request only [ts, id] (dropping `name` and swapping order). The
    // reconciliation must select and reorder columns correctly while preserving values.
    #[tokio::test]
    async fn test_vortex_read_projects_and_reorders() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let batch = sample_batch();

        let data = Box::new(ArrowEngineData::new(batch.clone()));
        let meta = write_vortex(
            store.clone(),
            &url::Url::parse("memory:///data/").unwrap(),
            data,
            &[],
        )
        .await
        .unwrap();

        // Requested physical schema: ts, then id (reordered subset of the file).
        let requested: SchemaRef = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("ts", DataType::TIMESTAMP),
                StructField::not_null("id", DataType::INTEGER),
            ])
            .unwrap(),
        );

        let read: Vec<RecordBatch> =
            read_vortex_files(store, vec![meta.file_meta().clone()], requested)
                .await
                .unwrap()
                .map(into_record_batch)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(read.len(), 1);
        let out = &read[0];
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "ts");
        assert_eq!(out.schema().field(1).name(), "id");
        let ids = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    // Missing nullable column is backfilled with all-NULLs; the requested column absent from the
    // file must come back present and entirely null.
    #[tokio::test]
    async fn test_vortex_read_backfills_missing_nullable_column() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let batch = sample_batch();

        let data = Box::new(ArrowEngineData::new(batch.clone()));
        let meta = write_vortex(
            store.clone(),
            &url::Url::parse("memory:///data/").unwrap(),
            data,
            &[],
        )
        .await
        .unwrap();

        let requested: SchemaRef = Arc::new(
            StructType::try_new(vec![
                StructField::not_null("id", DataType::INTEGER),
                StructField::nullable("missing", DataType::STRING),
            ])
            .unwrap(),
        );

        let read: Vec<RecordBatch> =
            read_vortex_files(store, vec![meta.file_meta().clone()], requested)
                .await
                .unwrap()
                .map(into_record_batch)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(read.len(), 1);
        let out = &read[0];
        assert_eq!(out.num_columns(), 2);
        let missing = out.column(1);
        assert_eq!(missing.len(), 3);
        assert_eq!(missing.null_count(), 3);
    }

    // Nested schemas are explicitly rejected (POC limitation), never silently mis-projected.
    #[tokio::test]
    async fn test_vortex_read_rejects_nested_schema() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let nested: SchemaRef = Arc::new(
            StructType::try_new(vec![StructField::nullable(
                "s",
                StructType::try_new(vec![StructField::nullable("a", DataType::INTEGER)]).unwrap(),
            )])
            .unwrap(),
        );
        let files = vec![FileMeta::new(
            url::Url::parse("memory:///data/x.vortex").unwrap(),
            0,
            0,
        )];
        // `.err()` drops the Ok stream (which is not `Debug`) so we can inspect the error.
        let err = read_vortex_files(store, files, nested)
            .await
            .err()
            .expect("nested schema should be rejected");
        assert!(format!("{err}").contains("flat primitive schemas"));
    }

    /// Writes `batch` to a `.vortex` file under `dir` with an explicit `row_block_size` (forcing
    /// multiple layout chunks for small inputs) and returns its [`FileMeta`].
    async fn write_chunked_vortex_file(
        store: &Arc<DynObjectStore>,
        dir: &str,
        batch: &RecordBatch,
        row_block_size: usize,
    ) -> FileMeta {
        let session = VortexSession::default();
        let vortex_array = VortexArrayRef::from_arrow(batch, false).unwrap();
        let strategy = WriteStrategyBuilder::default()
            .with_row_block_size(row_block_size)
            .build();
        let mut buffer: Vec<u8> = Vec::new();
        session
            .write_options()
            .with_strategy(strategy)
            .write(&mut buffer, vortex_array.to_array_stream())
            .await
            .unwrap();
        let url = url::Url::parse(dir)
            .unwrap()
            .join("chunked.vortex")
            .unwrap();
        let size = buffer.len() as u64;
        store
            .put(&Path::from_url_path(url.path()).unwrap(), buffer.into())
            .await
            .unwrap();
        FileMeta::new(url, 0, size)
    }

    // A multi-chunk file decodes to more than one batch (one per scan split) and is never collapsed
    // into a single array; concatenating the batches reproduces the input rows in order.
    #[tokio::test]
    async fn test_vortex_read_streams_multiple_chunks_in_order() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        // High-entropy values (a multiplicative hash of the index) so the column does not compress
        // away to a single tiny block; with enough volume Vortex's layout splits into multiple
        // chunks that the reader must stream separately.
        let values: Vec<i32> = (0..600_000u64)
            .map(|i| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as i32)
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values.clone()))],
        )
        .unwrap();

        let file_meta = write_chunked_vortex_file(&store, "memory:///data/", &batch, 65_536).await;

        let physical_schema: SchemaRef = Arc::new(schema.as_ref().try_into_kernel().unwrap());
        let read: Vec<RecordBatch> = read_vortex_files(store, vec![file_meta], physical_schema)
            .await
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .await
            .unwrap();

        assert!(
            read.len() > 1,
            "expected multiple chunks from a chunked file, got {}",
            read.len()
        );
        let ids: Vec<i32> = read
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, values);
    }

    /// Builds an `n_cols`-column Int32 batch named `c0..c{n_cols}`; cell value is `col*100 + row`.
    fn wide_int_batch(n_cols: usize, n_rows: i32) -> RecordBatch {
        let fields: Vec<Field> = (0..n_cols)
            .map(|c| Field::new(format!("c{c}"), ArrowDataType::Int32, false))
            .collect();
        let columns: Vec<ArrayRef> = (0..n_cols)
            .map(|c| {
                let values: Vec<i32> = (0..n_rows).map(|r| c as i32 * 100 + r).collect();
                Arc::new(Int32Array::from(values)) as ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    /// Re-opens a just-written `.vortex` file from the store (so tests can inspect the scan/dtype
    /// directly, like the production reader does).
    async fn open_vortex(store: &Arc<DynObjectStore>, location: &url::Url) -> VortexFile {
        let bytes = store
            .get(&Path::from_url_path(location.path()).unwrap())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        VortexSession::default()
            .open_options()
            .open_buffer(bytes.to_vec())
            .unwrap()
    }

    // Projection pushdown decodes only the requested columns (verified via the post-projection scan
    // dtype), in requested order, and the end-to-end read returns just those columns with correct
    // values.
    #[tokio::test]
    async fn test_vortex_pushdown_decodes_only_requested_columns() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let batch = wide_int_batch(10, 3);

        let data = Box::new(ArrowEngineData::new(batch.clone()));
        let meta = write_vortex(
            store.clone(),
            &url::Url::parse("memory:///data/").unwrap(),
            data,
            &[],
        )
        .await
        .unwrap();

        // Request two columns, reordered: c7 then c3.
        let requested: SchemaRef = Arc::new(
            StructType::try_new(vec![
                StructField::not_null("c7", DataType::INTEGER),
                StructField::not_null("c3", DataType::INTEGER),
            ])
            .unwrap(),
        );

        // Pushdown selects exactly the requested columns present in the file, in requested order...
        let file = open_vortex(&store, meta.location()).await;
        assert_eq!(projection_columns(&file, &requested), vec!["c7", "c3"]);
        // ...and the scan decodes only those two columns (not all ten).
        let scan = file
            .scan()
            .unwrap()
            .with_projection(select(vec!["c7", "c3"], root()));
        let decoded = scan.dtype().unwrap();
        assert_eq!(decoded.as_struct_fields_opt().unwrap().names().len(), 2);

        let read: Vec<RecordBatch> =
            read_vortex_files(store, vec![meta.file_meta().clone()], requested)
                .await
                .unwrap()
                .map(into_record_batch)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(read.len(), 1);
        let out = &read[0];
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "c7");
        assert_eq!(out.schema().field(1).name(), "c3");
        let c7 = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let c3 = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c7.values(), &[700, 701, 702]);
        assert_eq!(c3.values(), &[300, 301, 302]);
    }

    // When no requested column exists in the file, pushdown is skipped (empty projection) and
    // reconciliation backfills the absent column as all-NULL with the file's row count.
    #[tokio::test]
    async fn test_vortex_pushdown_empty_projection_backfills_all_missing() {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let batch = sample_batch();

        let data = Box::new(ArrowEngineData::new(batch.clone()));
        let meta = write_vortex(
            store.clone(),
            &url::Url::parse("memory:///data/").unwrap(),
            data,
            &[],
        )
        .await
        .unwrap();

        let requested: SchemaRef = Arc::new(
            StructType::try_new(vec![StructField::nullable("absent", DataType::STRING)]).unwrap(),
        );

        let file = open_vortex(&store, meta.location()).await;
        assert!(projection_columns(&file, &requested).is_empty());
        drop(file);

        let read: Vec<RecordBatch> =
            read_vortex_files(store, vec![meta.file_meta().clone()], requested)
                .await
                .unwrap()
                .map(into_record_batch)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(read.len(), 1);
        let out = &read[0];
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.schema().field(0).name(), "absent");
        assert_eq!(out.column(0).len(), 3);
        assert_eq!(out.column(0).null_count(), 3);
    }
}
