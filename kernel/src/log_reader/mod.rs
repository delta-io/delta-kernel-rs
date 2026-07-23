pub(crate) mod checkpoint_manifest;
pub(crate) mod commit;

use crate::cancellation::{check_cancelled, CancellationTokenRef};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, FileDataReadResultIterator, FileMeta, PredicateRef};

// === Cancellation-aware log reads ===
//
// The default engine's file readers are *eager*: they `block_on` the stream-producing future when
// the iterator is constructed, not on first `next()`. So a cancellation-aware read is two parts
// that must always travel together: a `check_cancelled` fail-fast before construction (the only
// cancellation point when the engine's reader ignores the token) and threading the token into the
// read. These helpers bundle that pairing so a call site cannot read log files while forgetting
// the pre-read check.
//
// They deliberately do NOT wrap the result in `CancellableIterator`: kernel applies that per-batch
// poll once over the assembled action stream (see `Scan::scan_metadata_inner`). A caller that
// consumes batches locally rather than through that stream wraps the returned iterator itself.

/// Reads JSON log files, failing fast if `cancellation_token` is already cancelled before the
/// (possibly eager) read begins, then threading the token into the engine read.
pub(crate) fn read_json_files_cancellable(
    engine: &dyn Engine,
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    cancellation_token: Option<&CancellationTokenRef>,
) -> DeltaResult<FileDataReadResultIterator> {
    check_cancelled(cancellation_token)?;
    engine.json_handler().read_json_files_with_cancellation(
        files,
        schema,
        predicate,
        cancellation_token.cloned(),
    )
}

/// Reads Parquet log files, failing fast if `cancellation_token` is already cancelled before the
/// (possibly eager) read begins, then threading the token into the engine read.
pub(crate) fn read_parquet_files_cancellable(
    engine: &dyn Engine,
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    cancellation_token: Option<&CancellationTokenRef>,
) -> DeltaResult<FileDataReadResultIterator> {
    check_cancelled(cancellation_token)?;
    engine
        .parquet_handler()
        .read_parquet_files_with_cancellation(files, schema, predicate, cancellation_token.cloned())
}
