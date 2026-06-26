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
//! Row order is preserved and batches are never merged across file boundaries (one
//! [`ArrowEngineData`] per file).
//!
//! ## POC limitations
//!
//! Only flat (top-level primitive) schemas are supported on read. Kernel's projection mask uses
//! flattened *leaf* indices, which only coincide with top-level column indices when the schema has
//! no nested struct/list/map columns. Nested schemas are rejected rather than silently
//! mis-projected (see `ensure_flat_schema`). Predicate/projection pushdown, deletion vectors, row
//! indexes, and column mapping are all punted -- safe given the locked POC table shape (column
//! mapping None, no deletion vectors, no row tracking).

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
use vortex::array::stream::ArrayStreamExt as _;
use vortex::array::{ArrayRef as VortexArrayRef, VortexSessionExecute as _};
use vortex::file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};
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
        async move {
            let batch = decode_vortex_file(store, physical_schema, file).await?;
            Ok::<_, Error>(stream::once(async { Ok(batch) }))
        }
    });
    let result_stream = stream::iter(file_futures)
        .buffered(super::DEFAULT_BUFFER_SIZE)
        .try_flatten()
        .map_ok(|data: ArrowEngineData| -> Box<dyn EngineData> { Box::new(data) });

    Ok(Box::pin(result_stream))
}

/// Decodes a single `.vortex` file to one Arrow batch and reconciles it to `physical_schema`.
async fn decode_vortex_file(
    store: Arc<DynObjectStore>,
    physical_schema: SchemaRef,
    file: FileMeta,
) -> DeltaResult<ArrowEngineData> {
    let file_location = file.location.to_string();
    let path = Path::from_url_path(file.location.path())?;
    let bytes = store.get(&path).await?.bytes().await?;

    let session = VortexSession::default();
    // `open_buffer` is synchronous (no I/O runtime); the scan stream decode is async. `read_all`
    // yields a single array (a ChunkedArray if the file has multiple chunks), and `execute_arrow`
    // canonicalizes it to one contiguous Arrow array -- in-file row order preserved.
    let array = session
        .open_options()
        .open_buffer(bytes.to_vec())
        .map_err(vortex_err)?
        .scan()
        .map_err(vortex_err)?
        .into_array_stream()
        .map_err(vortex_err)?
        .read_all()
        .await
        .map_err(vortex_err)?;

    let mut ctx = session.create_execution_ctx();
    let arrow_array = session
        .arrow()
        .execute_arrow(array, None, &mut ctx)
        .map_err(vortex_err)?;
    let batch = RecordBatch::from(arrow_array.as_struct());

    reconcile_to_schema(batch, &physical_schema, &file_location)
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
        Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use delta_kernel::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit,
    };
    use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel_default_engine_test_utils::try_into_record_batch as into_record_batch;

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
}
