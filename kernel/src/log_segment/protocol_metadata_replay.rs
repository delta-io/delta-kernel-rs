//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

use crate::actions::{get_commit_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::crc::{CrcLoadResult, LazyCrc};
use crate::engine_data::{GetData, RowVisitor};
use crate::log_replay::ActionsBatch;
use crate::path::ParsedLogPath;
use crate::schema::{
    ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, StructField, StructType,
};
use crate::{DeltaResult, Engine, Expression, FileMeta, Predicate, PredicateRef, Version};

use tracing::{info, instrument, warn};
use url::Url;

use super::LogSegment;

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_unchecked`], used for
    /// fresh snapshot creation where both Protocol and Metadata must exist.
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Metadata, Protocol)> {
        match self.read_protocol_metadata_opt(engine, lazy_crc)? {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(crate::Error::MissingMetadata),
            (Some(_), None) => Err(crate::Error::MissingProtocol),
            (None, None) => Err(crate::Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns `None` for either if not found.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata_checked`], used for
    /// incremental snapshot updates where the caller can fall back to an existing snapshot's
    /// Protocol and Metadata.
    ///
    /// The `lazy_crc` parameter allows the CRC to be loaded at most once and shared for
    /// future use (domain metadata, in-commit timestamp, etc.).
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        let crc_version = lazy_crc.crc_version();

        // Case 1: If CRC at target version, use it directly and exit early.
        if crc_version == Some(self.end_version) {
            if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                info!("P&M from CRC at target version {}", self.end_version);
                return Ok((Some(crc.metadata.clone()), Some(crc.protocol.clone())));
            }
            warn!(
                "CRC at target version {} failed to load, falling back to log replay",
                self.end_version
            );
        }

        // Case 2: CRC exists at an earlier version. Use a single streaming pass over the full
        // log segment, leveraging per-batch FilePath column to detect the CRC version boundary
        // mid-stream:
        //
        //   (a) If P&M is complete once all commits after the CRC version are read, return it.
        //   (b) If P&M is incomplete, fall back to the CRC.
        //   (c) If the CRC also fails, continue streaming the remaining commits and checkpoint.
        //
        // This replaces the previous approach of pre-splitting the segment into two separate
        // LogSegment objects (one for "after CRC" and one for "through CRC").
        if let Some(crc_v) = crc_version.filter(|&v| v < self.end_version) {
            return self.replay_for_pm_with_crc_shortcircuit(engine, lazy_crc, crc_v);
        }

        // Case 3 / Case 4: Full P&M log replay (no CRC or CRC at target version failed).
        self.replay_for_pm(engine)
    }

    /// Single-pass P&M replay that short-circuits to the CRC at `crc_v` once all commits after
    /// that version have been read.
    ///
    /// Batches are streamed via `read_actions_with_projected_checkpoint_actions`. The CRC boundary
    /// is detected per-batch using the `FilePath` metadata column injected by `fixup_json_read`
    /// into commit batches. Checkpoint batches carry `is_log_batch = false`, which also signals
    /// that all commits have been exhausted.
    fn replay_for_pm_with_crc_shortcircuit(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
        crc_v: Version,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        // Commit schema includes FilePath so we can detect the CRC version boundary per-batch.
        // Checkpoint schema omits it — parquet checkpoints don't inject FilePath, and we use
        // is_log_batch=false as the boundary signal when we transition to checkpoint batches.
        let commit_schema = pm_schema_with_file_path()?;
        let checkpoint_schema = pm_schema()?;

        let mut metadata_opt = None;
        let mut protocol_opt = None;
        let mut crossed_crc_boundary = false;

        let actions = self
            .read_actions_with_projected_checkpoint_actions(
                engine,
                commit_schema,
                checkpoint_schema,
                META_PREDICATE.clone(),
                None,
            )?
            .actions;

        for actions_batch in actions {
            let ActionsBatch {
                actions,
                is_log_batch,
            } = actions_batch?;

            // Detect the CRC boundary: first commit batch at/before crc_v, or first checkpoint batch.
            if !crossed_crc_boundary {
                let past_boundary = if is_log_batch {
                    file_version_from_data(actions.as_ref())?.is_some_and(|v| v <= crc_v)
                } else {
                    true // all commits exhausted; now reading checkpoint
                };

                if past_boundary {
                    crossed_crc_boundary = true;
                    info!(
                        "Crossed CRC boundary at version {} while replaying P&M",
                        crc_v
                    );

                    // Case 2(a): P&M already complete from commits after CRC.
                    if metadata_opt.is_some() && protocol_opt.is_some() {
                        info!("Found P&M from commits after CRC version");
                        return Ok((metadata_opt, protocol_opt));
                    }

                    // Case 2(b): P&M incomplete — try the CRC.
                    if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                        info!("P&M fallback to CRC (no P&M changes after CRC version)");
                        return Ok((
                            metadata_opt.or_else(|| Some(crc.metadata.clone())),
                            protocol_opt.or_else(|| Some(crc.protocol.clone())),
                        ));
                    }

                    // Case 2(c): CRC failed — fall through and continue reading.
                    warn!(
                        "CRC at version {} failed to load, replaying remaining segment",
                        crc_v
                    );
                }
            }

            // Accumulate P&M from this batch.
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                break;
            }
        }

        Ok((metadata_opt, protocol_opt))
    }

    /// Replays the log segment for Protocol and Metadata. Stops early once both are found.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in self.read_pm_batches(engine)? {
            let actions = actions_batch?.actions;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                break;
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let schema = pm_schema()?;
        // read the same protocol and metadata schema for both commits and checkpoints
        self.read_actions(engine, schema, META_PREDICATE.clone())
    }
}

/// Projected schema for Protocol and Metadata replay.
fn pm_schema() -> DeltaResult<crate::schema::SchemaRef> {
    get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])
}

/// Projected schema for Protocol and Metadata replay, with a FilePath metadata column appended.
///
/// The FilePath column is injected by `fixup_json_read` for each commit batch and carries the
/// URL of the commit file, allowing us to extract the Delta version per batch.
fn pm_schema_with_file_path() -> DeltaResult<crate::schema::SchemaRef> {
    let base = pm_schema()?;
    let fields: Vec<StructField> = base
        .fields()
        .cloned()
        .chain(std::iter::once(StructField::create_metadata_column(
            MetadataColumnSpec::FilePath.text_value(),
            MetadataColumnSpec::FilePath,
        )))
        .collect();
    Ok(Arc::new(StructType::try_new(fields)?))
}

/// Extracts the Delta log version from the `FilePath` metadata column in `data`.
///
/// Returns `None` if the column is absent, empty, or its path cannot be parsed as a Delta log path.
fn file_version_from_data(data: &dyn crate::EngineData) -> DeltaResult<Option<Version>> {
    struct FilePathVisitor {
        path: Option<String>,
    }
    static FILE_PATH_COL: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
        ColumnNamesAndTypes::from((
            vec![ColumnName::new([MetadataColumnSpec::FilePath.text_value()])],
            vec![DataType::STRING],
        ))
    });
    impl RowVisitor for FilePathVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            FILE_PATH_COL.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            if row_count > 0 && self.path.is_none() {
                self.path = getters[0]
                    .get_str(0, MetadataColumnSpec::FilePath.text_value())?
                    .map(str::to_string);
            }
            Ok(())
        }
    }
    let mut visitor = FilePathVisitor { path: None };
    visitor.visit_rows_of(data)?;
    Ok(visitor.path.and_then(|p| {
        let url = Url::parse(&p).ok()?;
        ParsedLogPath::try_from(FileMeta::new(url, 0, 0))
            .ok()
            .flatten()
            .map(|parsed| parsed.version)
    }))
}

static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
    Some(Arc::new(Predicate::or(
        Expression::column([METADATA_NAME, "id"]).is_not_null(),
        Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
    )))
});

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use itertools::Itertools;
    use test_log::test;

    use crate::engine::sync::SyncEngine;
    use crate::schema::MetadataColumnSpec;
    use crate::Snapshot;

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let data: Vec<_> = snapshot
            .log_segment()
            .read_pm_batches(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
    }

    /// Verifies that `pm_schema_with_file_path` returns a schema with the standard P&M fields
    /// plus a `FilePath` metadata column.
    #[test]
    fn test_pm_schema_with_file_path() {
        use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
        let schema = super::pm_schema_with_file_path().unwrap();
        assert!(
            schema.field(PROTOCOL_NAME).is_some(),
            "missing protocol field"
        );
        assert!(
            schema.field(METADATA_NAME).is_some(),
            "missing metadata field"
        );
        let fp_field = schema
            .field(MetadataColumnSpec::FilePath.text_value())
            .expect("missing FilePath field");
        assert_eq!(
            fp_field.get_metadata_column_spec(),
            Some(MetadataColumnSpec::FilePath)
        );
    }

    /// Verifies that `file_version_from_data` correctly extracts the Delta version from the
    /// `FilePath` metadata column injected into commit batches by `fixup_json_read`.
    ///
    /// Uses the `app-txn-no-checkpoint` test table (commits v0 and v1). Reads with
    /// `pm_schema_with_file_path()` so the `_file` column is populated, then checks that each
    /// commit batch yields the expected version.
    #[test]
    fn test_file_version_from_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let commit_schema = super::pm_schema_with_file_path().unwrap();
        let checkpoint_schema = super::pm_schema().unwrap();

        // Collect only commit batches (is_log_batch=true); commits are read newest-first.
        let commit_versions: Vec<_> = snapshot
            .log_segment()
            .read_actions_with_projected_checkpoint_actions(
                &engine,
                commit_schema,
                checkpoint_schema,
                None,
                None,
            )
            .unwrap()
            .actions
            .filter_map(|b| {
                let b = b.unwrap();
                b.is_log_batch.then(|| {
                    super::file_version_from_data(b.actions.as_ref())
                        .unwrap()
                        .expect("commit batch must have a FilePath version")
                })
            })
            .collect();

        // app-txn-no-checkpoint has commits v0 and v1; newest first.
        assert_eq!(commit_versions, vec![1, 0]);
    }
}
