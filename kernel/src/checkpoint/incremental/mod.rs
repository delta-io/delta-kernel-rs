//! All-or-nothing V2 sidecar reuse.
//!
//! Root summaries establish base compatibility before a single streaming tail replay. Reuse
//! requires no inherited Removes, allowlisted operations and tail-local input identities.
//! An unsafe tail continues into inherited sidecars through the same reconciler, preserving
//! already-written tail output. Successful reuse never opens inherited sidecars.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use itertools::Itertools as _;
use url::Url;

use self::eligibility::TailEligibility;
use super::metrics::{CheckpointMetrics, CheckpointPhase};
use super::{CheckpointWriter, ReusedSidecars, WrittenCheckpointInfo};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::file_metadata::build_checkpoint_transform;
use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    CHECKPOINT_NUM_ADD_FILES_TAG, SIDECAR_FILE_SCHEMA_TAG, SIDECAR_NUM_ACTIONS_TAG,
    SIDECAR_SIZE_IN_BYTES_TAG, Sidecar,
};
use crate::checkpoint::sidecar::SidecarReference;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::expressions::ColumnName;
use crate::log_replay::ActionsBatch;
use crate::schema::{ColumnNamesAndTypes, DataType, StructType, schema_ref};
use crate::utils::current_time_ms;

mod eligibility;
use crate::{Engine, KernelError, KernelResult, KernelResultIterator, RowVisitor};

// Includes retained transaction and domain-metadata rows, as well as sidecar references.
const MAX_BUFFERED_ROOT_ROWS: usize = 10_000;
const SIDECAR_REUSE_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

impl CheckpointWriter {
    pub(crate) fn write_v2_checkpoint_reusing_all_sidecars(
        &self,
        engine: &dyn Engine,
        file_actions_per_sidecar_hint: usize,
        metrics: CheckpointMetrics,
    ) -> KernelResult<WrittenCheckpointInfo> {
        let now_ms = current_time_ms()?;
        let Some(ReusableBase {
            captured_root,
            inherited,
        }) = self.read_reusable_base(engine, &metrics, now_ms)?
        else {
            return self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics,
            );
        };
        let log_segment = self.snapshot.log_segment();

        // Tail output and inherited rows must use the same retention cutoffs.
        let file_retention = self.deleted_file_retention_timestamp()?;
        let txn_retention = self.get_transaction_expiration_timestamp()?;

        let inherited_files: Vec<_> = inherited
            .references
            .iter()
            .map(|reference| reference.file_meta.clone())
            .collect();
        let reuse_allowed = Arc::new(AtomicBool::new(true));
        let mut tail = TailEligibility::new(
            engine,
            self.read_schema.clone(),
            log_segment.listed.ascending_commit_files.iter(),
            self.snapshot.table_configuration().dv_normalization_root(),
        )?;
        let tail_actions = {
            let _timer = metrics.start(CheckpointPhase::LogTailInput);
            log_segment.read_commit_actions(engine, tail.read_schema().clone())?
        };
        let mut tail_actions = metrics.input_iterator(CheckpointPhase::LogTailInput, tail_actions);
        let tail_actions = std::iter::from_fn({
            let reuse_allowed = reuse_allowed.clone();
            let metrics = metrics.clone();
            move || {
                let Some(batch) = tail_actions.next() else {
                    reuse_allowed.store(tail.is_eligible(), Ordering::Relaxed);
                    return None;
                };
                Some(batch.and_then(|batch| {
                    let _timer = metrics.start(CheckpointPhase::Reconciliation);
                    tail.process_batch(batch)
                }))
            }
        })
        .fuse();

        // Tail exhaustion makes reuse_allowed final. Base actions are older than the tail, so
        // reconciled tail output stays valid whether or not the base is expanded.
        let base_actions = std::iter::once_with({
            let parquet = engine.parquet_handler();
            let read_schema = self.read_schema.clone();
            let metrics = metrics.clone();
            let reuse_allowed = reuse_allowed.clone();
            move || {
                let reuse = reuse_allowed.load(Ordering::Relaxed);
                let sidecars: Box<dyn KernelResultIterator<ActionsBatch>> = if reuse {
                    Box::new(std::iter::empty())
                } else {
                    let batches = {
                        let _timer = metrics.start(CheckpointPhase::CheckpointInput);
                        parquet.read_parquet_files(&inherited_files, read_schema, None)?
                    };
                    let batches =
                        batches.map(|batch| batch.map(|data| ActionsBatch::new(data, false)));
                    Box::new(metrics.input_iterator(CheckpointPhase::CheckpointInput, batches))
                };
                Ok::<_, KernelError>(captured_root.into_iter().map(Ok).chain(sidecars))
            }
        })
        .flatten_ok()
        .map(|batch| batch.and_then(std::convert::identity));
        let actions = tail_actions.chain(base_actions);
        let data = self.reconcile_checkpoint_actions_with_retention(
            engine,
            actions,
            file_retention,
            txn_retention,
            metrics.clone(),
            false,
        )?;
        self.write_sidecar_checkpoint(
            engine,
            file_actions_per_sidecar_hint,
            data,
            move || {
                if reuse_allowed.load(Ordering::Relaxed) {
                    inherited
                } else {
                    ReusedSidecars::default()
                }
            },
            metrics,
        )
    }

    fn read_reusable_base(
        &self,
        engine: &dyn Engine,
        metrics: &CheckpointMetrics,
        now_ms: i64,
    ) -> KernelResult<Option<ReusableBase>> {
        let log_segment = self.snapshot.log_segment();
        let [base] = log_segment.listed.checkpoint_parts.as_slice() else {
            return Ok(None);
        };
        // Collation statistics can require inherited-row sampling to choose the output schema.
        if base.version >= self.snapshot.version()
            || self.snapshot.table_configuration().has_collations_feature()
        {
            return Ok(None);
        }

        // Capture the root once. Re-reading a classic-named root after replay could see a
        // concurrent replacement with different references or retained non-file actions.
        let root_batches = {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            match base.extension.as_str() {
                "json" => engine.json_handler().read_json_files(
                    std::slice::from_ref(&base.location),
                    self.read_schema.clone(),
                    None,
                )?,
                "parquet" => engine.parquet_handler().read_parquet_files(
                    std::slice::from_ref(&base.location),
                    self.read_schema.clone(),
                    None,
                )?,
                _ => {
                    return Err(KernelError::invalid_checkpoint(
                        "Unsupported checkpoint extension",
                    ));
                }
            }
        };
        let mut references = SidecarReferenceVisitor {
            log_root: &log_segment.log_root,
            references: Vec::new(),
            has_missing_modification_time: false,
        };
        let mut root_summary = RootSummary::default();
        let mut captured_root = Vec::new();
        let mut buffered_rows = 0usize;
        {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            for batch in root_batches {
                let batch = batch?;
                metrics.record_checkpoint_rows(batch.len());
                references.visit_rows_of(batch.as_ref())?;
                root_summary.visit_rows_of(batch.as_ref())?;
                buffered_rows = buffered_rows.saturating_add(batch.len());
                if root_summary.has_file_actions || buffered_rows > MAX_BUFFERED_ROOT_ROWS {
                    return Ok(None);
                }
                captured_root.push(ActionsBatch::new(batch, false));
            }
        }
        if references.has_missing_modification_time
            || references.references.is_empty()
            || references
                .references
                .iter()
                .any(|reference| !sidecar_is_recent(reference.file_meta.last_modified, now_ms))
        {
            return Ok(None);
        }
        // Missing optional proof permits fallback; a malformed V2 root is an error.
        if root_summary.checkpoint_metadata_versions
            != [i64::try_from(base.version).map_err(|e| {
                KernelError::invalid_checkpoint(format!("checkpoint version overflow: {e}"))
            })?]
        {
            return Err(KernelError::invalid_checkpoint(
                "V2 base sidecar references require one matching checkpointMetadata action",
            ));
        }

        let Some(tags) = root_summary.checkpoint_metadata_tags else {
            return Ok(None);
        };
        let Some((num_adds, expected_sidecar_schema, sidecar_size)) = trusted_base_summary(&tags)
        else {
            return Ok(None);
        };
        let context = &self.schema_context;
        let (output_schema, _) = build_checkpoint_transform(
            &context.stats_config,
            &self.read_schema,
            &context.stats_schema,
            context.partition_schema.as_ref(),
        )?;
        if &expected_sidecar_schema != output_schema.project(&["add", "remove"])?.as_ref() {
            return Ok(None);
        }
        let mut seen_sidecar_locations = HashSet::new();
        let mut referenced_size = 0u64;
        for reference in &references.references {
            if !seen_sidecar_locations.insert(reference.file_meta.location.as_str()) {
                return Err(KernelError::invalid_checkpoint(
                    "V2 base references the same sidecar more than once",
                ));
            }
            referenced_size = referenced_size
                .checked_add(reference.file_meta.size)
                .ok_or_else(|| KernelError::invalid_checkpoint("sidecar sizes overflow"))?;
        }
        if referenced_size != sidecar_size {
            return Ok(None);
        }

        Ok(Some(ReusableBase {
            captured_root,
            inherited: ReusedSidecars {
                references: references.references,
                num_adds,
            },
        }))
    }
}

struct ReusableBase {
    captured_root: Vec<ActionsBatch>,
    inherited: ReusedSidecars,
}

fn sidecar_is_recent(modification_time: i64, now_ms: i64) -> bool {
    modification_time > 0
        && now_ms
            .checked_sub(modification_time)
            .is_some_and(|age| (0..SIDECAR_REUSE_WINDOW_MS).contains(&age))
}

#[derive(Default)]
struct RootSummary {
    has_file_actions: bool,
    checkpoint_metadata_versions: Vec<i64>,
    checkpoint_metadata_tags: Option<HashMap<String, String>>,
}

impl RowVisitor for RootSummary {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let schema = schema_ref! {
                nullable "add": { nullable "path": STRING },
                nullable "remove": { nullable "path": STRING },
                nullable "checkpointMetadata": {
                    nullable "version": LONG,
                    nullable "tags": { STRING => nullable STRING },
                },
            };
            schema.leaves(None)
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [add, remove, checkpoint, tags] = getters else {
            return Err(KernelError::internal_error("Expected root action getters"));
        };
        for row in 0..row_count {
            self.has_file_actions |= add.get_str(row, "add.path")?.is_some()
                || remove.get_str(row, "remove.path")?.is_some();
            if let Some(version) = checkpoint.get_long(row, "checkpointMetadata.version")? {
                self.checkpoint_metadata_versions.push(version);
                self.checkpoint_metadata_tags = tags
                    .get_map(row, "checkpointMetadata.tags")?
                    .map(|map| map.materialize());
            }
        }
        Ok(())
    }
}

fn trusted_base_summary(tags: &HashMap<String, String>) -> Option<(i64, StructType, u64)> {
    let num_actions: i64 = tags.get(SIDECAR_NUM_ACTIONS_TAG)?.parse().ok()?;
    let num_adds: i64 = tags.get(CHECKPOINT_NUM_ADD_FILES_TAG)?.parse().ok()?;
    let size_in_bytes: u64 = tags.get(SIDECAR_SIZE_IN_BYTES_TAG)?.parse().ok()?;
    let schema = serde_json::from_str(tags.get(SIDECAR_FILE_SCHEMA_TAG)?).ok()?;
    (num_actions >= 0 && num_actions == num_adds).then_some((num_adds, schema, size_in_bytes))
}

/// Collects base sidecar references, preserving null-valued tags that `Sidecar` drops.
struct SidecarReferenceVisitor<'a> {
    log_root: &'a Url,
    references: Vec<SidecarReference>,
    has_missing_modification_time: bool,
}

impl RowVisitor for SidecarReferenceVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        SidecarVisitor::default().selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [path, size_in_bytes, modification_time, tags] = getters else {
            return Err(KernelError::internal_error(
                "Expected sidecar reference getters",
            ));
        };
        for row in 0..row_count {
            if let Some(path) = path.get_opt(row, "sidecar.path")? {
                let modification_time =
                    modification_time.get_opt(row, "sidecar.modificationTime")?;
                self.has_missing_modification_time |= modification_time.is_none();
                let sidecar = Sidecar {
                    path,
                    size_in_bytes: size_in_bytes.get(row, "sidecar.sizeInBytes")?,
                    modification_time: modification_time.unwrap_or(0),
                    tags: None,
                };
                let tags: Option<HashMap<String, Option<String>>> =
                    tags.get_map(row, "sidecar.tags")?.map(|map| {
                        map.keys()
                            .map(|key| (key.to_owned(), map.get(key).map(str::to_owned)))
                            .collect()
                    });
                self.references.push(SidecarReference {
                    file_meta: sidecar.to_filemeta(self.log_root)?,
                    path: sidecar.path,
                    tags,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
