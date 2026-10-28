use super::{CommitResult, Declarative, Transaction, UpdateTable};
#[cfg(doc)]
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::plans::{RelationRef, ScopedPlanExecutor};
#[cfg(doc)]
use crate::scan::scan_row_schema;
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
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::scan::scan_row_schema;
    use crate::schema::{ToSchema, schema_ref};
    use crate::transaction::UpdateTableOperation;
    use crate::transaction::exec_mode::StagedDataChanges;
    use crate::unit_test_utils::load_test_table;

    #[rstest]
    fn declarative_transaction_can_stage_relations(
        #[values(false, true)] add_files: bool,
        #[values(false, true)] remove_files: bool,
        #[values(false, true)] update_dvs: bool,
    ) -> KernelResult<()> {
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
    fn commit_returns_unsupported_error() -> KernelResult<()> {
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
