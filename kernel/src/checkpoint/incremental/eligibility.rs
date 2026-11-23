//! Eligibility proof from original commit actions, before reconciliation or retention filtering.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::expressions::{Expression, Predicate, col};
use crate::log_replay::{ActionsBatch, FileActionKey};
use crate::path::ParsedLogPath;
use crate::schema::{
    ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef, StructField,
    schema_ref,
};
use crate::{Engine, ExpressionEvaluator, KernelError, KernelResult, RowVisitor, Version};

pub(super) struct TailEligibility {
    read_schema: SchemaRef,
    eligibility_projection: Arc<dyn ExpressionEvaluator>,
    action_projection: Arc<dyn ExpressionEvaluator>,
    summary: TailSummary,
}

impl TailEligibility {
    pub(super) fn new<'a>(
        engine: &dyn Engine,
        action_schema: SchemaRef,
        commits: impl Iterator<Item = &'a ParsedLogPath>,
        dv_normalization_root: Option<Url>,
    ) -> KernelResult<Self> {
        let read_schema = Arc::new(action_schema.add([
            StructField::nullable("commitInfo", schema_ref! { nullable "operation": STRING }),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        ])?);
        let evaluation = engine.evaluation_handler();
        let eligibility_projection = evaluation.new_expression_evaluator(
            read_schema.clone(),
            Expression::struct_from([
                col!("add.path"),
                col!("add.deletionVector.storageType"),
                col!("add.deletionVector.pathOrInlineDv"),
                col!("add.deletionVector.offset"),
                col!("remove.path"),
                col!("remove.deletionVector.storageType"),
                col!("remove.deletionVector.pathOrInlineDv"),
                col!("remove.deletionVector.offset"),
                col!("metaData.id"),
                col!("protocol.minReaderVersion"),
                col!("_file"),
                col!("commitInfo.operation"),
                Expression::from(Predicate::is_not_null(col!("commitInfo"))),
            ])
            .into(),
            ELIGIBILITY_SCHEMA.clone().into(),
        )?;
        // Drop private eligibility columns before the shared checkpoint transform sees the data.
        let action_projection = evaluation.new_expression_evaluator(
            read_schema.clone(),
            Expression::struct_from(
                action_schema
                    .fields()
                    .map(|field| Expression::column([field.name()])),
            )
            .into(),
            action_schema.into(),
        )?;
        let commits = commits
            .map(|commit| {
                (
                    commit.location.location.to_string(),
                    CommitSummary {
                        version: commit.version,
                        operations: 0,
                        operation_allowed: false,
                    },
                )
            })
            .collect();
        Ok(Self {
            read_schema,
            eligibility_projection,
            action_projection,
            summary: TailSummary {
                commits,
                pending_removes: HashMap::new(),
                previous_version: None,
                has_metadata_or_protocol: false,
                dv_normalization_root,
            },
        })
    }

    pub(super) fn read_schema(&self) -> &SchemaRef {
        &self.read_schema
    }

    pub(super) fn process_batch(&mut self, batch: ActionsBatch) -> KernelResult<ActionsBatch> {
        let projected = self
            .eligibility_projection
            .evaluate(batch.actions.as_ref())?;
        self.summary.visit_rows_of(projected.as_ref())?;
        Ok(ActionsBatch::new(
            self.action_projection.evaluate(batch.actions.as_ref())?,
            true,
        ))
    }

    pub(super) fn is_eligible(&self) -> bool {
        self.summary.is_eligible()
    }
}

static ELIGIBILITY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema_ref! {
        nullable "addPath": STRING,
        nullable "addDvType": STRING,
        nullable "addDvPath": STRING,
        nullable "addDvOffset": INTEGER,
        nullable "removePath": STRING,
        nullable "removeDvType": STRING,
        nullable "removeDvPath": STRING,
        nullable "removeDvOffset": INTEGER,
        nullable "metadataId": STRING,
        nullable "protocolVersion": INTEGER,
        not_null "_file": STRING,
        nullable "operation": STRING,
        not_null "hasCommitInfo": BOOLEAN,
    }
});

struct CommitSummary {
    version: Version,
    operations: usize,
    operation_allowed: bool,
}

struct TailSummary {
    commits: HashMap<String, CommitSummary>,
    pending_removes: HashMap<FileActionKey, Version>,
    previous_version: Option<Version>,
    has_metadata_or_protocol: bool,
    dv_normalization_root: Option<Url>,
}

impl TailSummary {
    fn is_eligible(&self) -> bool {
        // Classify only after all rows have been seen. commitInfo can follow actions, and a
        // listed empty JSON file produces no rows but still needs an operation to qualify.
        !self.has_metadata_or_protocol
            && self
                .commits
                .values()
                .all(|commit| commit.operations == 1 && commit.operation_allowed)
            && self.pending_removes.is_empty()
    }
}

impl RowVisitor for TailSummary {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| ELIGIBILITY_SCHEMA.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [
            add,
            add_dv_type,
            add_dv_path,
            add_dv_offset,
            remove,
            remove_dv_type,
            remove_dv_path,
            remove_dv_offset,
            metadata,
            protocol,
            source,
            operation,
            has_commit_info,
        ] = getters
        else {
            return Err(KernelError::internal_error(
                "Expected tail eligibility getters",
            ));
        };
        for row in 0..row_count {
            let source = source.get_str(row, "_file")?.ok_or_else(|| {
                KernelError::internal_error("Missing tail commit source provenance")
            })?;
            let commit = self.commits.get_mut(source).ok_or_else(|| {
                KernelError::internal_error(format!("Unlisted tail commit source: {source}"))
            })?;
            if has_commit_info.get(row, "hasCommitInfo")? {
                commit.operations = commit.operations.saturating_add(1);
                commit.operation_allowed = operation
                    .get_str(row, "operation")?
                    .is_some_and(operation_is_allowed);
            }
            let version = commit.version;
            if self
                .previous_version
                .is_some_and(|previous| version > previous)
            {
                return Err(KernelError::internal_error(
                    "Tail commit batches are not in newest-first order",
                ));
            }
            self.previous_version = Some(version);
            self.has_metadata_or_protocol |= metadata.get_str(row, "metadataId")?.is_some()
                || protocol.get_int(row, "protocolVersion")?.is_some();
            for (path, dv_type, dv_path, dv_offset, is_add) in [
                (add, add_dv_type, add_dv_path, add_dv_offset, true),
                (
                    remove,
                    remove_dv_type,
                    remove_dv_path,
                    remove_dv_offset,
                    false,
                ),
            ] {
                let Some(path) = path.get_str(row, "path")? else {
                    continue;
                };
                if is_add && self.pending_removes.is_empty() {
                    continue;
                }
                let dv_id = dv_type
                    .get_str(row, "deletionVector.storageType")?
                    .map(|storage_type| {
                        DeletionVectorDescriptor::unique_id_with_root(
                            storage_type,
                            dv_path.get(row, "deletionVector.pathOrInlineDv")?,
                            dv_offset.get_opt(row, "deletionVector.offset")?,
                            self.dv_normalization_root.as_ref(),
                        )
                    })
                    .transpose()?;
                let key = FileActionKey::new(path, dv_id);
                if is_add {
                    // Same-commit row order proves nothing. Only an earlier commit can establish
                    // that a removed identity was introduced after the base checkpoint.
                    if self
                        .pending_removes
                        .get(&key)
                        .is_some_and(|remove| version < *remove)
                    {
                        self.pending_removes.remove(&key);
                    }
                } else {
                    self.pending_removes
                        .entry(key)
                        .and_modify(|remove| *remove = (*remove).min(version))
                        .or_insert(version);
                }
            }
        }
        Ok(())
    }
}

fn operation_is_allowed(operation: &str) -> bool {
    // Like CRC's FileStatsPolicy, this trusts supported writers' operation semantics. These
    // operations create fresh outputs or pair replacement Adds with explicit input Removes.
    // CRC's IgnoreAdds/IgnoreRemoves policies must not be used for checkpoint state.
    matches!(
        operation,
        "WRITE" | "STREAMING UPDATE" | "OPTIMIZE" | "DELETE" | "UPDATE" | "MERGE"
    )
}
