//! Experimental all-or-nothing V2 sidecar reuse.
//!
//! Preflight reads only the tail and the base root. Reuse requires base-root counts proving
//! the sidecars contain no Remove actions. The benchmark-only opt-in also assumes every
//! tail Add path is fresh; no inherited sidecar is opened on a successful reuse path.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use url::Url;

use super::metrics::{CheckpointMetrics, CheckpointPhase};
use super::{
    CheckpointWriter, NUM_OF_ADD_FILES_TAG, ReusedSidecars, SIDECAR_NUM_ACTIONS_TAG,
    SIDECAR_SIZE_IN_BYTES_TAG, WrittenCheckpointInfo,
};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::file_metadata::build_checkpoint_transform;
use crate::actions::visitors::SidecarVisitor;
use crate::actions::{SIDECAR_FILE_SCHEMA_TAG, Sidecar};
use crate::checkpoint::sidecar::SidecarReference;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::expressions::ColumnName;
use crate::log_replay::ActionsBatch;
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructType, lazy_schema_ref, schema_ref,
};
use crate::{Engine, KernelError, KernelResult, RowVisitor};

// Project only action-presence fields needed to reject file actions and schema changes.
static PREFLIGHT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "add": { nullable "path": STRING, nullable "dataChange": BOOLEAN },
    nullable "remove": { nullable "path": STRING },
    nullable "metaData": { nullable "id": STRING },
    nullable "protocol": { nullable "minReaderVersion": INTEGER },
};

const MAX_BUFFERED_ROOT_ROWS: usize = 10_000;

impl CheckpointWriter {
    pub(crate) fn write_v2_checkpoint_reusing_all_sidecars(
        &self,
        engine: &dyn Engine,
        file_actions_per_sidecar_hint: usize,
        metrics: CheckpointMetrics,
        assume_fresh_adds_for_benchmark: bool,
    ) -> KernelResult<WrittenCheckpointInfo> {
        let fallback = || {
            self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics.clone(),
            )
        };
        let log_segment = self.snapshot.log_segment();
        let [base] = log_segment.listed.checkpoint_parts.as_slice() else {
            return fallback();
        };
        if base.version >= self.snapshot.version()
            || self.snapshot.table_configuration().has_collations_feature()
        {
            return fallback();
        }

        // Hold the cutoff fixed across preflight and output replay.
        let file_retention = self.deleted_file_retention_timestamp()?;
        let txn_retention = self.get_transaction_expiration_timestamp()?;
        let mut tail = TailSummary::default();
        let preflight_actions = {
            let _timer = metrics.start(CheckpointPhase::LogTailInput);
            log_segment.read_commit_actions(engine, PREFLIGHT_SCHEMA.clone())?
        };
        for batch in metrics.input_iterator(CheckpointPhase::LogTailInput, preflight_actions) {
            tail.visit_rows_of(batch?.actions.as_ref())?;
            if tail.has_remove
                || tail.has_metadata_or_protocol
                || tail.has_unsafe_add
                || (tail.has_add && !assume_fresh_adds_for_benchmark)
            {
                return fallback();
            }
        }

        // Capture the root once. Re-reading a classic-named root after preflight could see a
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
        };
        let mut root_summary = RootSummary::default();
        let mut captured_root = Vec::new();
        let mut buffered_rows = 0usize;
        let mut reject_root = false;
        {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            for batch in root_batches {
                let batch = batch?;
                metrics.record_checkpoint_rows(batch.len());
                references.visit_rows_of(batch.as_ref())?;
                root_summary.visit_rows_of(batch.as_ref())?;
                buffered_rows = buffered_rows.saturating_add(batch.len());
                if root_summary.has_file_actions || buffered_rows > MAX_BUFFERED_ROOT_ROWS {
                    reject_root = true;
                    break;
                }
                captured_root.push(ActionsBatch::new(batch, false));
            }
        }
        if reject_root {
            return fallback();
        }
        if references.references.is_empty() {
            return fallback();
        }
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
            return fallback();
        };
        let Some((num_adds, expected_sidecar_schema, sidecar_size)) = trusted_base_summary(&tags)
        else {
            return fallback();
        };
        let context = &self.schema_context;
        let (output_schema, _) = build_checkpoint_transform(
            &context.stats_config,
            &self.read_schema,
            &context.stats_schema,
            context.partition_schema.as_ref(),
        )?;
        if &expected_sidecar_schema != output_schema.project(&["add", "remove"])?.as_ref() {
            return fallback();
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
            return fallback();
        }

        let tail_actions = {
            let _timer = metrics.start(CheckpointPhase::LogTailInput);
            log_segment.read_commit_actions(engine, self.read_schema.clone())?
        };
        let tail_actions = metrics.input_iterator(CheckpointPhase::LogTailInput, tail_actions);
        let actions = tail_actions.chain(captured_root.into_iter().map(Ok));
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
            ReusedSidecars {
                references: references.references,
                num_adds,
                num_removes: 0,
            },
            metrics,
        )
    }
}

#[derive(Default)]
struct TailSummary {
    has_add: bool,
    has_remove: bool,
    has_unsafe_add: bool,
    has_metadata_or_protocol: bool,
}

impl RowVisitor for TailSummary {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let schema = schema_ref! {
                nullable "add": { nullable "path": STRING, nullable "dataChange": BOOLEAN },
                nullable "remove": { nullable "path": STRING },
                nullable "metaData": { nullable "id": STRING },
                nullable "protocol": { nullable "minReaderVersion": INTEGER },
            };
            schema.leaves(None)
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [add, data_change, remove, metadata, protocol] = getters else {
            return Err(KernelError::internal_error("Expected tail action getters"));
        };
        for row in 0..row_count {
            if add.get_str(row, "add.path")?.is_some() {
                self.has_add = true;
                self.has_unsafe_add |= data_change.get_opt(row, "add.dataChange")? != Some(true);
            }
            self.has_remove |= remove.get_str(row, "remove.path")?.is_some();
            self.has_metadata_or_protocol |= metadata.get_str(row, "metaData.id")?.is_some()
                || protocol
                    .get_int(row, "protocol.minReaderVersion")?
                    .is_some();
        }
        Ok(())
    }
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
    let num_adds: i64 = tags.get(NUM_OF_ADD_FILES_TAG)?.parse().ok()?;
    let size_in_bytes: u64 = tags.get(SIDECAR_SIZE_IN_BYTES_TAG)?.parse().ok()?;
    let schema = serde_json::from_str(tags.get(SIDECAR_FILE_SCHEMA_TAG)?).ok()?;
    (num_actions >= 0 && num_actions == num_adds).then_some((num_adds, schema, size_in_bytes))
}

/// The shared `Sidecar` materialization discards null-valued map entries.
struct SidecarReferenceVisitor<'a> {
    log_root: &'a Url,
    references: Vec<SidecarReference>,
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
                let sidecar = Sidecar {
                    path,
                    size_in_bytes: size_in_bytes.get(row, "sidecar.sizeInBytes")?,
                    modification_time: modification_time.get(row, "sidecar.modificationTime")?,
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
