mod precommit;
#[cfg(test)]
mod test_utils;

use precommit::{
    AddFileFieldsCheck, AppendOnlyCheck, CheckList, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};

use super::{CommitResult, Declarative, Transaction, UpdateTable};
#[cfg(doc)]
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::plans::{Operation, RelationRef, ScopedPlanExecutor};
#[cfg(doc)]
use crate::scan::scan_row_schema;
use crate::table_features::TableFeature;
use crate::{KernelError, KernelResult};

impl<S> Transaction<S, Declarative> {
    /// Consume the transaction and commit it to the table.
    ///
    /// See [`Transaction::commit`] for result semantics and validation requirements.
    /// The executor runs plans against staged relations, which must remain available in its scope.
    pub fn commit_declarative(
        self,
        _executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<CommitResult<S, Declarative>> {
        Err(KernelError::unsupported(
            "Declarative transaction commits are not implemented",
        ))
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple relations.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    /// The relation must remain available in the executor passed to
    /// [`commit_declarative`](Self::commit_declarative) until commit completes. Staging does not
    /// execute the relation or validate its rows.
    pub fn add_files_ref(&mut self, add_metadata: RelationRef) {
        self.staged_data_changes.add_files.push(add_metadata);
    }

    /// Performs validation against this transaction's staged file actions by executing a
    /// declarative plan (containing the needed set of validations). See `precommit` module for
    /// pla-based check logic.
    #[allow(dead_code)]
    fn validate_file_actions(&self, executor: &dyn ScopedPlanExecutor) -> KernelResult<()> {
        // TODO: DV updates is not yet supported
        if !self.staged_data_changes.dv_updates.is_empty() {
            return Err(KernelError::unsupported(
                "Declarative validation of staged DV updates",
            ));
        }

        let checks = self.file_action_checks()?;
        let plan = precommit::build_plan(
            &self.staged_data_changes.add_files,
            &self.staged_data_changes.remove_files,
            &checks,
        )?;
        let result = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?;
        precommit::validate_result(result, &checks)
    }

    /// Selects which pre-commit check should be run (based on the effective table configuration
    /// and transaction intent.)
    fn file_action_checks(&self) -> KernelResult<CheckList> {
        let mut checks: CheckList = vec![
            Box::new(AddFileFieldsCheck),
            Box::new(RemoveFileFieldsCheck),
        ];

        if self.data_change
            && self
                .effective_table_config
                .is_feature_enabled(&TableFeature::AppendOnly)
        {
            checks.push(Box::new(AppendOnlyCheck));
        }

        if self.effective_table_config.requires_stats_num_records() {
            checks.push(Box::new(NumRecordsCheck));
        }

        let physical_schema = self.effective_table_config.physical_schema();
        for column in self.physical_clustering_columns.iter().flatten() {
            checks.push(Box::new(ClusteringStatsCheck::new(
                column.clone(),
                physical_schema.field_at(column)?.data_type().clone(),
            )?));
        }
        Ok(checks)
    }
}

impl Transaction<UpdateTable, Declarative> {
    /// Remove files from the table in this transaction. This API generally enables the engine to
    /// delete data (at file-level granularity) from the table. Note that this API can be called
    /// multiple times to remove multiple relations.
    ///
    /// The expected schema for `remove_metadata` is given by
    /// [`scan_row_schema`]. The relation must contain only the rows selected for removal and remain
    /// available in the executor passed to [`commit_declarative`](Self::commit_declarative) until
    /// commit completes. Staging does not execute the relation or validate its rows.
    pub fn remove_files_ref(&mut self, remove_metadata: RelationRef) {
        self.staged_data_changes.remove_files.push(remove_metadata);
    }

    /// Update deletion vectors for files in the table.
    ///
    /// This method can be called multiple times to update deletion vectors for different sets of
    /// files. It stages relations to match by file path and generate remove/add actions at commit.
    /// Staging does not execute the relations or validate their rows.
    ///
    /// # Arguments
    ///
    /// * `new_dv_descriptors` - A relation with a string `path` column and a `deletionVector`
    ///   column containing the new [`DeletionVectorDescriptor`] for each file.
    /// * `existing_data_files` - Scan file metadata using [`scan_row_schema`], covering every path
    ///   in `new_dv_descriptors`. Every row in `existing_data_files` must have an accurate
    ///   `stats.numRecords` value.
    ///
    /// Both relations must remain available in the executor passed to
    /// [`commit_declarative`](Self::commit_declarative) until commit completes.
    pub fn update_deletion_vectors_ref(
        &mut self,
        new_dv_descriptors: RelationRef,
        existing_data_files: RelationRef,
    ) {
        self.staged_data_changes
            .dv_updates
            .push((new_dv_descriptors, existing_data_files));
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::Result;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::scan::scan_row_schema;
    use crate::schema::{ToSchema, schema_ref};
    use crate::transaction::UpdateTableOperation;
    use crate::transaction::create_table::create_table;
    use crate::transaction::data_layout::DataLayout;
    use crate::transaction::exec_mode::StagedDataChanges;
    use crate::unit_test_utils::load_test_table;

    // This test only validates which checks are selected. The check logic is validated in
    // precommit/tests.rs.
    #[rstest]
    #[case::plain(&[], false)]
    #[case::v2_supported(&[
        ("delta.feature.icebergCompatV2", "supported"),
        ("delta.columnMapping.mode", "name"),
    ], false)]
    #[case::v2_enabled(&[("delta.enableIcebergCompatV2", "true")], true)]
    #[case::v3_supported(&[
        ("delta.feature.icebergCompatV3", "supported"),
        ("delta.columnMapping.mode", "name"),
        ("delta.enableRowTracking", "true"),
    ], false)]
    #[case::v3_enabled(&[("delta.enableIcebergCompatV3", "true")], true)]
    fn selects_checks_based_on_table_props(
        #[case] properties: &[(&str, &str)],
        #[case] requires_num_records: bool,
        #[values(false, true)] append_only: bool,
        #[values(false, true)] data_change: bool,
    ) -> KernelResult<()> {
        let tempdir = tempfile::tempdir()?;
        let txn = create_table(
            tempdir.path().to_string_lossy(),
            schema_ref! { nullable "id": LONG },
            "test-engine",
            data_change,
        )
        .with_table_properties(properties.iter().copied())
        .with_table_properties([("delta.appendOnly", append_only.to_string())])
        .with_execution_mode::<Declarative>()
        .build(&SyncEngine::new(), Box::new(FileSystemCommitter::new()))?;
        let checks = txn.file_action_checks()?;
        let mut expected = vec!["add.fields", "remove.fields"];
        if append_only && data_change {
            expected.push("append_only.no_removes");
        }
        if requires_num_records {
            expected.push("add.num_records_present");
        }
        assert_eq!(
            checks.iter().map(|check| check.name()).collect::<Vec<_>>(),
            expected
        );
        Ok(())
    }

    // This test only validates which checks are selected. The check logic is validated in
    // precommit/tests.rs.
    #[rstest]
    #[case::no_clustering(&[])]
    #[case::one_column(&["id"])]
    #[case::multiple_columns(&["id", "tag"])]
    fn selects_checks_for_clustering_columns(
        #[case] columns: &[&str],
        #[values("none", "name")] column_mapping: &str,
    ) -> KernelResult<()> {
        let tempdir = tempfile::tempdir()?;
        let layout = if columns.is_empty() {
            DataLayout::None
        } else {
            DataLayout::clustered(columns)
        };
        let mut txn = create_table(
            tempdir.path().to_string_lossy(),
            schema_ref! { nullable "id": LONG, nullable "tag": STRING },
            "test-engine",
            true,
        )
        .with_table_properties([("delta.columnMapping.mode", column_mapping)])
        .with_data_layout(layout)
        .with_execution_mode::<Declarative>()
        .build(&SyncEngine::new(), Box::new(FileSystemCommitter::new()))?;
        let config = &txn.effective_table_config;
        let mut expected = vec!["add.fields".to_owned(), "remove.fields".to_owned()];
        for column in columns {
            let field = config.logical_schema_ref().field(column).unwrap();
            let physical_name = field.physical_name(config.column_mapping_mode());
            expected.push(format!(
                "add.clustering_stats:{}",
                serde_json::to_string(&[physical_name])?,
            ));
        }
        let checks = txn.file_action_checks()?;
        assert_eq!(
            checks.iter().map(|check| check.name()).collect::<Vec<_>>(),
            expected
        );
        let source_schema = schema_ref! {
            ..(txn.add_files_schema().fields().filter(|field| field.name() != "stats")),
            nullable "stats": (txn.stats_schema()?),
        };
        txn.add_files_ref(RelationRef::new("adds", source_schema));
        Ok(())
    }

    #[rstest]
    fn declarative_transaction_can_stage_relations(
        #[values(false, true)] add_files: bool,
        #[values(false, true)] remove_files: bool,
        #[values(false, true)] update_dvs: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_execution_mode::<Declarative>()
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        if add_files {
            txn.add_files_ref(RelationRef::new("adds", txn.add_files_schema().clone()));
        }
        if remove_files {
            txn.remove_files_ref(RelationRef::new("removes", scan_row_schema()));
        }
        if update_dvs {
            txn.update_deletion_vectors_ref(
                RelationRef::new(
                    "dvs",
                    schema_ref! {
                        not_null "path": STRING,
                        not_null "deletionVector": (DeletionVectorDescriptor::to_schema()),
                    },
                ),
                RelationRef::new("files", scan_row_schema()),
            );
        }

        assert_eq!(txn.staged_data_changes.has_adds(), add_files);
        assert_eq!(txn.staged_data_changes.has_removes(), remove_files);
        assert_eq!(txn.staged_data_changes.has_dv_inputs(), update_dvs);
        assert_eq!(txn.staged_data_changes.has_dv_updates(), update_dvs);
        Ok(())
    }

    #[test]
    fn commit_returns_unsupported_error() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_execution_mode::<Declarative>()
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        let executor = SyncPlanExecutor::default().get_scoped();
        let error = txn.commit_declarative(&executor).unwrap_err();
        assert!(matches!(&error, KernelError::Unsupported(_)));
        assert!(error.to_string().contains("not implemented"));
        Ok(())
    }
}
