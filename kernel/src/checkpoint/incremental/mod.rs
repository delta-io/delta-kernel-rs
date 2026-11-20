//! Selective sidecar reuse. Preflight retains tail identities, not base file metadata.
//! A second tail replay produces output alongside affected sidecars, using the captured root and
//! retention cutoffs.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use url::Url;

use super::{CheckpointWriter, ReusedSidecars, WrittenCheckpointInfo, checked_sum_action_counts};
use crate::action_reconciliation::file_metadata::build_checkpoint_transform;
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::actions::Sidecar;
use crate::actions::visitors::SidecarVisitor;
use crate::checkpoint::metrics::{CheckpointMetrics, CheckpointPhase};
use crate::checkpoint::sidecar::SidecarReference;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::expressions::ColumnName;
use crate::log_replay::ActionsBatch;
use crate::schema::{
    ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef, StructField, StructType,
    lazy_schema_ref, schema_ref,
};
use crate::{Engine, FileMeta, KernelError, KernelResult, RowVisitor};

// The existing reconciliation visitor needs only these fields to check identities and retention.
// In particular, preflight does not decode stats, partition values, or other Add metadata.
static VALIDATION_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "add": {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "deletionVector": {
            nullable "storageType": STRING, nullable "pathOrInlineDv": STRING, nullable "offset": INTEGER,
        },
    },
    nullable "remove": {
        nullable "path": STRING,
        nullable "deletionTimestamp": LONG,
        nullable "deletionVector": {
            nullable "storageType": STRING, nullable "pathOrInlineDv": STRING, nullable "offset": INTEGER,
        },
    },
    nullable "metaData": { nullable "id": STRING },
    nullable "protocol": { nullable "minReaderVersion": INTEGER },
    nullable "txn": { nullable "appId": STRING, nullable "lastUpdated": LONG },
    nullable "domainMetadata": { nullable "domain": STRING, nullable "removed": BOOLEAN },
    nullable "checkpointMetadata": { nullable "version": LONG },
};

static SIDECAR_VALIDATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(
        VALIDATION_SCHEMA
            .fields()
            .cloned()
            .chain([StructField::create_metadata_column(
                "_file",
                MetadataColumnSpec::FilePath,
            )]),
    ))
});

impl CheckpointWriter {
    pub(crate) fn write_v2_checkpoint_reusing_eligible_sidecars(
        &self,
        engine: &dyn Engine,
        file_actions_per_sidecar_hint: usize,
        metrics: CheckpointMetrics,
    ) -> KernelResult<WrittenCheckpointInfo> {
        let log_segment = self.snapshot.log_segment();
        let [base_checkpoint] = log_segment.listed.checkpoint_parts.as_slice() else {
            return self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics,
            );
        };
        // Collation sampling can change the physical stats layout during output replay.
        if self.snapshot.table_configuration().has_collations_feature()
            || base_checkpoint.version >= self.snapshot.version()
        {
            return self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics,
            );
        }

        let mut eligibility_reconciler = ActionReconciliationProcessor::new(
            self.minimum_file_retention_timestamp,
            self.txn_expiration_timestamp,
            self.snapshot.table_configuration().dv_normalization_root(),
        );
        let mut tail_summary = CheckpointInputSummary::default();
        let tail_batches = {
            let _timer = metrics.start(CheckpointPhase::LogTailInput);
            log_segment.read_commit_actions(engine, VALIDATION_SCHEMA.clone())?
        };
        for batch in metrics.input_iterator(CheckpointPhase::LogTailInput, tail_batches) {
            let batch = batch?;
            tail_summary.visit_rows_of(batch.actions.as_ref())?;
            if tail_summary.has_metadata_or_protocol {
                break;
            }
            let _timer = metrics.start(CheckpointPhase::Reconciliation);
            // Record every original file key, including expired and superseded removes.
            // Looking only at surviving tail actions could miss an inherited conflict.
            eligibility_reconciler.process_actions_batch(batch)?;
        }
        if tail_summary.has_metadata_or_protocol {
            drop(eligibility_reconciler);
            drop(tail_summary);
            return self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics,
            );
        }

        let root_batches = {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            match base_checkpoint.extension.as_str() {
                "json" => engine.json_handler().read_json_files(
                    std::slice::from_ref(&base_checkpoint.location),
                    self.read_schema.clone(),
                    None,
                )?,
                "parquet" => engine.parquet_handler().read_parquet_files(
                    std::slice::from_ref(&base_checkpoint.location),
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
        let mut captured_root_batches = Vec::new();
        let mut sidecar_visitor = SidecarReferenceVisitor {
            log_root: &log_segment.log_root,
            references: Vec::new(),
        };
        let mut root_summary = CheckpointInputSummary::default();
        let root_batches = root_batches.map_ok(|batch| ActionsBatch::new(batch, false));
        for batch in metrics.input_iterator(CheckpointPhase::CheckpointInput, root_batches) {
            let batch = batch?;
            {
                let _timer = metrics.start(CheckpointPhase::CheckpointInput);
                sidecar_visitor.visit_rows_of(batch.actions.as_ref())?;
                root_summary.visit_rows_of(batch.actions.as_ref())?;
            }
            // An inline checkpoint can be table-sized. Stop before buffering its file payloads.
            if root_summary.has_file_actions {
                drop(eligibility_reconciler);
                drop(tail_summary);
                drop(captured_root_batches);
                drop(root_summary);
                return self.write_v2_checkpoint_with_sidecars(
                    engine,
                    file_actions_per_sidecar_hint,
                    metrics,
                );
            }
            captured_root_batches.push(Ok(batch));
        }
        if sidecar_visitor.references.is_empty()
            || root_summary.checkpoint_metadata_versions != [base_checkpoint.version as i64]
        {
            drop(eligibility_reconciler);
            drop(tail_summary);
            drop(captured_root_batches);
            return self.write_v2_checkpoint_with_sidecars(
                engine,
                file_actions_per_sidecar_hint,
                metrics,
            );
        }

        drop(tail_summary);
        let SidecarSelection {
            reused_sidecars,
            sidecars_to_rewrite,
        } = self.select_sidecars_for_reuse(
            engine,
            &metrics,
            sidecar_visitor.references,
            eligibility_reconciler,
        )?;
        let sidecar_batches_to_rewrite = if sidecars_to_rewrite.is_empty() {
            None
        } else {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            Some(engine.parquet_handler().read_parquet_files(
                &sidecars_to_rewrite,
                self.read_schema.clone(),
                None,
            )?)
        };
        // Reuse and fallback both consume the captured root. Never read a mutable classic root
        // again after selecting its descriptors: a replacement can have different txn retention.
        let tail_batches = {
            let _timer = metrics.start(CheckpointPhase::LogTailInput);
            log_segment.read_commit_actions(engine, self.read_schema.clone())?
        };
        let tail_batches = metrics.input_iterator(CheckpointPhase::LogTailInput, tail_batches);
        let sidecar_batches_to_rewrite = sidecar_batches_to_rewrite
            .into_iter()
            .flatten()
            .map_ok(|batch| ActionsBatch::new(batch, false));
        let sidecar_batches_to_rewrite =
            metrics.input_iterator(CheckpointPhase::CheckpointInput, sidecar_batches_to_rewrite);
        let replay_batches = tail_batches
            .chain(captured_root_batches)
            .chain(sidecar_batches_to_rewrite);
        let reconciled_data =
            self.reconcile_checkpoint_actions(engine, replay_batches, metrics.clone())?;
        self.write_sidecar_checkpoint(
            engine,
            file_actions_per_sidecar_hint,
            metrics,
            reconciled_data,
            reused_sidecars,
        )
    }

    /// Select whole sidecars for reuse, consuming proof state before output replay begins.
    fn select_sidecars_for_reuse(
        &self,
        engine: &dyn Engine,
        metrics: &CheckpointMetrics,
        sidecar_references: Vec<SidecarReference>,
        mut eligibility_reconciler: ActionReconciliationProcessor,
    ) -> KernelResult<SidecarSelection> {
        let schema_context = &self.schema_context;
        let (checkpoint_schema, _) = build_checkpoint_transform(
            &schema_context.stats_config,
            &self.read_schema,
            &schema_context.stats_schema,
            schema_context.partition_schema.as_ref(),
        )?;
        let expected_sidecar_schema = checkpoint_schema.project(&["add", "remove"])?;
        let mut reuse_state_by_url = HashMap::new();
        let mut sidecars_to_validate = Vec::new();
        for reference in &sidecar_references {
            let file_meta = &reference.file_meta;
            let schema_matches = {
                let _timer = metrics.start(CheckpointPhase::CheckpointInput);
                engine
                    .parquet_handler()
                    .read_parquet_footer(file_meta)?
                    .schema
                    .as_ref()
                    == expected_sidecar_schema.as_ref()
            };
            let reuse_state = if schema_matches {
                SidecarReuseState::Pending {
                    num_adds: 0,
                    num_removes: 0,
                }
            } else {
                SidecarReuseState::Rewrite
            };
            reuse_state_by_url.insert(file_meta.location.to_string(), reuse_state);
            if schema_matches {
                sidecars_to_validate.push(file_meta.clone());
            }
        }
        let validation_batches = if sidecars_to_validate.is_empty() {
            None
        } else {
            let _timer = metrics.start(CheckpointPhase::CheckpointInput);
            // _file preserves provenance across a multi-file read, allowing engine prefetching.
            Some(engine.parquet_handler().read_parquet_files(
                &sidecars_to_validate,
                SIDECAR_VALIDATION_SCHEMA.clone(),
                None,
            )?)
        };
        let validation_batches = validation_batches
            .into_iter()
            .flatten()
            .map_ok(|batch| ActionsBatch::new(batch, false));
        for batch in metrics.input_iterator(CheckpointPhase::CheckpointInput, validation_batches) {
            let batch = batch?;
            let row_count = batch.actions.len();
            if row_count == 0 {
                continue;
            }
            let mut source_visitor = SidecarSourceVisitor::default();
            source_visitor.visit_rows_of(batch.actions.as_ref())?;
            let Some(reuse_state) = source_visitor
                .source_file_path
                .as_ref()
                .and_then(|path| reuse_state_by_url.get_mut(path))
            else {
                // Readers may represent _file as a path instead of the input URL. Without an
                // exact association, rewrite every sidecar rather than guessing its identity.
                reuse_state_by_url.clear();
                break;
            };
            let SidecarReuseState::Pending {
                num_adds,
                num_removes,
            } = reuse_state
            else {
                continue;
            };
            // Valid DV replacements remove the old logical identity, so the tail key set also
            // catches changes between active Adds with the same path and different DVs.
            let _timer = metrics.start(CheckpointPhase::Reconciliation);
            let reconciled_batch = eligibility_reconciler.process_actions_batch(batch)?;
            if usize::try_from(checked_sum_action_counts(
                reconciled_batch.add_actions_count,
                reconciled_batch.remove_actions_count,
            )?) != Ok(row_count)
            {
                *reuse_state = SidecarReuseState::Rewrite;
                continue;
            }
            *num_adds = checked_sum_action_counts(*num_adds, reconciled_batch.add_actions_count)?;
            *num_removes =
                checked_sum_action_counts(*num_removes, reconciled_batch.remove_actions_count)?;
        }
        let mut reused_sidecars = ReusedSidecars::default();
        let mut sidecars_to_rewrite = Vec::new();
        for reference in sidecar_references {
            if let Some(SidecarReuseState::Pending {
                num_adds,
                num_removes,
            }) = reuse_state_by_url.remove(reference.file_meta.location.as_str())
            {
                reused_sidecars.num_adds =
                    checked_sum_action_counts(reused_sidecars.num_adds, num_adds)?;
                reused_sidecars.num_removes =
                    checked_sum_action_counts(reused_sidecars.num_removes, num_removes)?;
                reused_sidecars.references.push(reference);
            } else {
                sidecars_to_rewrite.push(reference.file_meta);
            }
        }
        drop(reuse_state_by_url);
        drop(sidecars_to_validate);
        // Release the preflight key set before the second replay builds its own set.
        drop(eligibility_reconciler);
        Ok(SidecarSelection {
            reused_sidecars,
            sidecars_to_rewrite,
        })
    }
}

/// Captures references without the shared `SidecarVisitor`'s lossy tag conversion.
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
                let tags = tags.get_map(row, "sidecar.tags")?.map(|map| {
                    map.entries()
                        .map(|(key, value)| (key.to_owned(), value.map(str::to_owned)))
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

struct SidecarSelection {
    reused_sidecars: ReusedSidecars,
    sidecars_to_rewrite: Vec<FileMeta>,
}

// Counts remain provisional until every batch from the sidecar has passed validation.
enum SidecarReuseState {
    Pending { num_adds: i64, num_removes: i64 },
    Rewrite,
}

/// Sidecar provenance for one validation batch.
#[derive(Default)]
struct SidecarSourceVisitor {
    source_file_path: Option<String>,
}

impl RowVisitor for SidecarSourceVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let schema: SchemaRef = schema_ref! {
                not_null "_file": STRING,
            };
            schema.leaves(None)
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [source_file] = getters else {
            return Err(KernelError::internal_error(
                "Expected sidecar validation getters",
            ));
        };
        // ParquetHandler guarantees batches never mix rows from different files.
        if row_count > 0 && self.source_file_path.is_none() {
            self.source_file_path = Some(source_file.get(0, "_file")?);
        }
        Ok(())
    }
}

/// Configuration and root-layout checks that logical-key reconciliation doesn't cover.
#[derive(Default)]
struct CheckpointInputSummary {
    has_file_actions: bool,
    has_metadata_or_protocol: bool,
    checkpoint_metadata_versions: Vec<i64>,
}

impl RowVisitor for CheckpointInputSummary {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let schema: SchemaRef = schema_ref! {
                nullable "add": { nullable "path": STRING },
                nullable "remove": { nullable "path": STRING },
                nullable "metaData": { nullable "id": STRING },
                nullable "protocol": { nullable "minReaderVersion": INTEGER },
                nullable "checkpointMetadata": { nullable "version": LONG },
            };
            schema.leaves(None)
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [add, remove, metadata, protocol, checkpoint] = getters else {
            return Err(KernelError::internal_error(
                "Expected checkpoint input getters",
            ));
        };
        for row in 0..row_count {
            self.has_file_actions |= add.get_str(row, "add.path")?.is_some();
            self.has_file_actions |= remove.get_str(row, "remove.path")?.is_some();
            self.has_metadata_or_protocol |= metadata.get_str(row, "metaData.id")?.is_some()
                || protocol
                    .get_int(row, "protocol.minReaderVersion")?
                    .is_some();
            if let Some(version) = checkpoint.get_long(row, "checkpointMetadata.version")? {
                self.checkpoint_metadata_versions.push(version);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
