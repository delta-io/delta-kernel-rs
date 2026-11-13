//! Owning declarative transaction preparation and single-attempt commit execution.

use std::sync::{Arc, LazyLock};
use std::time::Instant;

use super::exec_mode::StagedDataChanges as _;
use super::imperative::{build_add_action_projection, build_remove_struct_patch};
use super::{CommittedTransaction, Declarative, Transaction, UpdateTable, WinningCommitHistory};
use crate::actions::{
    DomainMetadata, LOG_REMOVE_SCHEMA, Metadata, Protocol, SetTransaction, as_log_add_schema,
    get_commit_schema,
};
use crate::commit_range::CommitRange;
use crate::committer::CommitResponse;
use crate::crc::FileStatsDelta;
use crate::engine_data::TypedGetData as _;
use crate::expressions::{
    BinaryPredicateOp, ColumnName, Expression, Predicate, Scalar, UnaryExpressionOp, col,
    column_name, lit, null_lit,
};
use crate::plans::ir::nodes::Agg;
use crate::plans::{Operation, PlanBuilder, RelationRef, ScopedPlanExecutor};
use crate::schema::{DataType, StructType, schema_ref};
use crate::snapshot::SnapshotRef;
use crate::table_features::TableFeature;
use crate::utils::{current_time_ms, require};
use crate::{Engine, FileMeta, GetData, KernelError, KernelResult, RowVisitor, Version};

#[cfg(test)]
mod tests;

/// Prepared actions and context for a declarative commit attempt.
///
/// Obtain this object through [`Transaction::prepare`]. Its target version is the checked baseline
/// plus one. `commit` selects the attempt timestamp, validates the actions, and submits them. The
/// executor holding the relations must remain alive across preparation, inspection, and recovery.
/// The original transaction inputs remain available for validation and future regeneration.
#[derive(Debug)]
pub struct PreparedTransaction<STATE = UpdateTable> {
    transaction: Transaction<STATE, Declarative>,
    target_version: Version,
    file_actions: RelationRef,
    // Derived from the authoritative file relation and typed non-file actions. Regenerate this
    // cache if a future rebase changes either source.
    actions: RelationRef,
    non_file_actions: PreparedNonFileActions,
    submitted_attempt: Option<SubmittedAttempt>,
}

/// Result of submitting a prepared declarative transaction.
///
/// Retryable rejections preserve pending state for another attempt at the same version. Conflicts
/// preserve pending state and the winning history or discovery error returned by the committer.
/// Getters perform no I/O. Unknown outcomes
/// preserve the submitted attempt and must be resolved before another write; they are not evidence
/// that the write failed. Successful results contain no pending transaction.
#[derive(Debug)]
#[must_use]
pub enum PreparedTransactionCommitResult<STATE = UpdateTable> {
    /// The commit succeeded and its post-commit Snapshot was constructed.
    Committed(CommittedTransaction),
    /// Another commit occupies the proposed version.
    Conflicted {
        /// Pending work requiring conflict checking before another attempt.
        prepared_transaction: Option<Box<PreparedTransaction<STATE>>>,
        /// The rejected target version.
        conflict_version: Version,
        /// Saved winning history or the discovery error returned by the committer.
        winning_history: WinningCommitHistory,
    },
    /// A temporary failure prevented the attempt from committing.
    Retryable {
        /// Pending work that can attempt the same version without rebasing.
        prepared_transaction: Option<Box<PreparedTransaction<STATE>>>,
        /// The failure that must be addressed before another attempt.
        error: KernelError,
    },
    /// The committer did not establish whether the submitted attempt committed.
    Unknown {
        /// Pending work and the exact submitted attempt.
        prepared_transaction: Option<Box<PreparedTransaction<STATE>>>,
        /// The error that prevented establishing the outcome.
        error: KernelError,
    },
}

/// Immutable record of the actions and timestamps passed to a committer.
///
/// Recovery must establish whether this attempt committed before submitting new actions. Relations
/// are ordered, with CommitInfo first, and remain owned by the connector's scoped executor.
#[derive(Debug)]
pub struct SubmittedAttempt {
    version: Version,
    timestamp: i64,
    in_commit_timestamp: Option<i64>,
    actions: Vec<RelationRef>,
    file_meta: Option<FileMeta>,
}

impl<STATE> PreparedTransaction<STATE> {
    /// The proposed commit version. It changes only after an approved baseline change.
    pub fn target_version(&self) -> Version {
        self.target_version
    }

    /// The original snapshot, when writing an existing table.
    pub fn read_snapshot(&self) -> Option<&SnapshotRef> {
        self.transaction.read_snapshot_opt.as_ref()
    }

    /// Prepared file and non-file actions. CommitInfo is finalized separately during `commit`.
    pub fn get_delta_actions(&self) -> &RelationRef {
        &self.actions
    }

    /// The submitted attempt retained after an unknown outcome, otherwise `None`.
    pub fn submitted_attempt(&self) -> Option<&SubmittedAttempt> {
        self.submitted_attempt.as_ref()
    }

    /// Checks and resolves against winning commits without discarding pending state on error.
    ///
    /// Returns [`KernelError::Unsupported`]; conflict checking and resolution are not implemented.
    /// Neither the baseline nor prepared actions are changed by this call.
    pub fn rebase(&mut self, _winning_commits: &CommitRange) -> KernelResult<()> {
        Err(KernelError::unsupported(
            "Prepared transaction rebase is not implemented",
        ))
    }

    /// Validates and submits one attempt using engine metadata reads and executor plan execution.
    ///
    /// Returns conflict, retryable, or unknown outcomes with owning recovery state. Validation
    /// errors occur before calling the committer. A retained unknown attempt cannot be resubmitted
    /// until its outcome is resolved.
    ///
    /// Post-commit Snapshot construction errors propagate after the write has succeeded; they do
    /// not undo the commit or establish that resubmission is safe.
    pub fn commit(
        mut self,
        engine: &dyn Engine,
        executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<PreparedTransactionCommitResult<STATE>> {
        require!(
            self.submitted_attempt.is_none(),
            KernelError::invalid_transaction_state(
                "Resolve the previous unknown commit outcome before submitting another attempt"
            )
        );
        let started = Instant::now();
        self.transaction.validate_operation_compatibility()?;
        self.transaction.validate_commit()?;
        validate_inputs(&self.transaction, executor)?;
        let file_stats = summarize_files(&self.file_actions, executor)?;
        self.transaction.commit_timestamp = current_time_ms()?;
        let commit_info = self
            .transaction
            .prepare_nonfile_actions(engine)?
            .kernel_commit_info;
        let info = retain_small_actions(
            executor,
            "commit-info",
            vec![("commitInfo", commit_info.clone().into())],
        )?;
        let actions = vec![info, self.actions.clone()];
        let commit_metadata = self.transaction.create_commit_metadata(
            self.target_version,
            commit_info.in_commit_timestamp,
            self.non_file_actions.protocol.clone(),
            self.non_file_actions.metadata.clone(),
            self.non_file_actions.domain_metadata.clone(),
        )?;
        let crc_delta = self.transaction.build_crc_delta(
            file_stats.clone(),
            commit_info.clone(),
            self.non_file_actions.domain_metadata.clone(),
        )?;
        let prepare_duration = started.elapsed();
        let committer_started = Instant::now();
        let result = self.transaction.committer.commit_declarative(
            executor,
            Box::new(actions.clone().into_iter()),
            commit_metadata,
        );
        let committer_duration = committer_started.elapsed();
        match result {
            Ok(CommitResponse::Committed { file_meta }) => {
                self.transaction.record_commit_success_metrics(
                    &file_stats,
                    0,
                    prepare_duration,
                    committer_duration,
                );
                let committed = self.transaction.into_committed(
                    file_meta,
                    crc_delta,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    None,
                )?;
                Ok(PreparedTransactionCommitResult::Committed(committed))
            }
            Ok(CommitResponse::Conflict { version, commits }) => {
                require!(
                    version == self.target_version,
                    KernelError::internal_error(
                        "Committer returned a conflict for a different version"
                    )
                );
                let winning_history = WinningCommitHistory::new(
                    self.transaction.effective_table_config.table_root(),
                    version,
                    commits,
                );
                Ok(PreparedTransactionCommitResult::Conflicted {
                    prepared_transaction: Some(Box::new(self)),
                    conflict_version: version,
                    winning_history,
                })
            }
            Ok(CommitResponse::Retryable { error }) => {
                Ok(PreparedTransactionCommitResult::Retryable {
                    prepared_transaction: Some(Box::new(self)),
                    error,
                })
            }
            Err(error) => {
                self.submitted_attempt = Some(SubmittedAttempt {
                    version: self.target_version,
                    timestamp: self.transaction.commit_timestamp,
                    in_commit_timestamp: commit_info.in_commit_timestamp,
                    actions,
                    file_meta: None,
                });
                Ok(PreparedTransactionCommitResult::Unknown {
                    prepared_transaction: Some(Box::new(self)),
                    error,
                })
            }
        }
    }
}

impl<STATE> PreparedTransactionCommitResult<STATE> {
    /// Returns whether the write is known to have committed.
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }

    /// Borrows pending work after a conflict, retryable rejection, or unknown outcome.
    pub fn get_prepared_transaction(&self) -> Option<&PreparedTransaction<STATE>> {
        match self {
            Self::Committed(_) => None,
            Self::Conflicted {
                prepared_transaction,
                ..
            }
            | Self::Retryable {
                prepared_transaction,
                ..
            }
            | Self::Unknown {
                prepared_transaction,
                ..
            } => prepared_transaction.as_deref(),
        }
    }

    /// Transfers pending work without discarding the outcome or saved history.
    ///
    /// Returns `None` after success or if pending work was already taken. Take the transaction
    /// before borrowing history to pass to `rebase`.
    pub fn take_prepared_transaction(&mut self) -> Option<PreparedTransaction<STATE>> {
        match self {
            Self::Committed(_) => None,
            Self::Conflicted {
                prepared_transaction,
                ..
            }
            | Self::Retryable {
                prepared_transaction,
                ..
            }
            | Self::Unknown {
                prepared_transaction,
                ..
            } => prepared_transaction.take().map(|p| *p),
        }
    }

    /// Consumes the result and extracts pending work, including any unknown submitted attempt.
    pub fn into_prepared_transaction(mut self) -> Option<PreparedTransaction<STATE>> {
        self.take_prepared_transaction()
    }

    /// Returns saved winning history without performing I/O.
    ///
    /// Returns `None` for non-conflicts. If discovery failed, returns the saved error; restart
    /// from a fresh Snapshot and rebuild the transaction. Calls remain valid after pending work
    /// is taken or dropped and never retry discovery.
    pub fn get_winning_commits(&self) -> KernelResult<Option<&Arc<CommitRange>>> {
        match self {
            Self::Conflicted {
                winning_history, ..
            } => winning_history.get().map(Some),
            _ => Ok(None),
        }
    }

    /// Returns the saved history-load error without performing I/O.
    pub fn winning_commits_error(&self) -> Option<&KernelError> {
        match self {
            Self::Conflicted {
                winning_history, ..
            } => winning_history.load_error(),
            _ => None,
        }
    }

    /// Unwraps successful committed state, panicking for other outcomes. Intended for tests.
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::panic)]
    pub fn unwrap_committed(self) -> CommittedTransaction {
        match self {
            Self::Committed(committed) => committed,
            _ => panic!("Expected a committed prepared transaction"),
        }
    }
}

impl SubmittedAttempt {
    /// Version passed to the committer.
    pub fn version(&self) -> Version {
        self.version
    }
    /// Wall-clock timestamp recorded in CommitInfo.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
    /// Ordered in-commit timestamp when enabled.
    pub fn in_commit_timestamp(&self) -> Option<i64> {
        self.in_commit_timestamp
    }
    /// Exact ordered action relations submitted to the committer.
    pub fn actions(&self) -> &[RelationRef] {
        &self.actions
    }
    /// Written commit file when the committer returned its location during outcome recovery.
    pub fn file_meta(&self) -> Option<&FileMeta> {
        self.file_meta.as_ref()
    }
}

impl<STATE> Transaction<STATE, Declarative> {
    /// Ends input collection and transforms file metadata into retained Delta action rows.
    ///
    /// Metadata reads use `engine`; all file-row transforms and checks use `executor`. CommitInfo
    /// is created during the later commit attempt. Relations must remain available in this scope.
    ///
    /// Supports partitioned and unpartitioned adds/removes and metadata-only changes. Row tracking,
    /// clustered file writes, DV updates, and adaptive metadata return Unsupported. Validation is
    /// repeated by `PreparedTransaction::commit` before submission.
    pub fn prepare(
        self,
        engine: &dyn Engine,
        executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<PreparedTransaction<STATE>> {
        self.validate_operation_compatibility()?;
        self.validate_commit()?;
        validate_supported(&self)?;
        let target_version = match &self.read_snapshot_opt {
            Some(snapshot) => snapshot
                .version()
                .checked_add(1)
                .ok_or_else(|| KernelError::invalid_transaction_state("Commit version overflow"))?,
            None => 0,
        };
        let non_file_actions = PreparedNonFileActions {
            protocol: self
                .should_emit_protocol
                .then(|| self.effective_table_config.protocol().clone()),
            metadata: self
                .should_emit_metadata
                .then(|| self.effective_table_config.metadata().clone()),
            domain_metadata: self.generate_domain_metadata_actions(engine, None)?,
            set_transactions: self.set_transactions.clone(),
        };
        let mut plans = Vec::new();
        for input in &self.staged_data_changes.add_files {
            let (schema, expr) = build_add_action_projection(input.schema(), self.data_change)?;
            let source = PlanBuilder::relation_source(input.clone())
                .project(expr, as_log_add_schema(schema))?;
            plans.push(normalize_actions(source)?);
        }
        for input in &self.staged_data_changes.remove_files {
            let patch = build_remove_struct_patch(
                self.commit_timestamp,
                self.data_change,
                &[],
                false,
                false,
            )?;
            let source = PlanBuilder::relation_source(input.clone()).project(
                Expression::struct_from([Expression::struct_patch(patch)?]),
                LOG_REMOVE_SCHEMA.clone(),
            )?;
            plans.push(normalize_actions(source)?);
        }
        let file_plan = if plans.is_empty() {
            PlanBuilder::values(get_commit_schema().clone(), vec![])?
        } else {
            PlanBuilder::union_all(plans)?
        };
        let file_actions =
            executor.execute_and_retain("prepared-file-actions", file_plan.build()?)?;
        let small_actions = non_file_actions.rows();
        let small_ref = retain_small_actions(executor, "prepared-non-file-actions", small_actions)?;
        let actions = executor.execute_and_retain(
            "prepared-actions",
            PlanBuilder::union_all([
                PlanBuilder::relation_source(file_actions.clone()),
                PlanBuilder::relation_source(small_ref),
            ])?
            .build()?,
        )?;
        Ok(PreparedTransaction {
            transaction: self,
            target_version,
            file_actions,
            actions,
            non_file_actions,
            submitted_attempt: None,
        })
    }
}

#[derive(Debug)]
struct PreparedNonFileActions {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    domain_metadata: Vec<DomainMetadata>,
    set_transactions: Vec<SetTransaction>,
}

impl PreparedNonFileActions {
    fn rows(&self) -> Vec<(&'static str, Scalar)> {
        self.protocol
            .iter()
            .cloned()
            .map(|value| ("protocol", value.into()))
            .chain(
                self.metadata
                    .iter()
                    .cloned()
                    .map(|value| ("metaData", value.into())),
            )
            .chain(
                self.domain_metadata
                    .iter()
                    .cloned()
                    .map(|value| ("domainMetadata", value.into())),
            )
            .chain(
                self.set_transactions
                    .iter()
                    .cloned()
                    .map(|value| ("txn", value.into())),
            )
            .collect()
    }
}

fn retain_small_actions(
    executor: &dyn ScopedPlanExecutor,
    name: &str,
    actions: Vec<(&str, Scalar)>,
) -> KernelResult<RelationRef> {
    let schema = get_commit_schema().clone();
    let rows = actions
        .into_iter()
        .map(|(name, value)| {
            schema
                .fields()
                .map(|field| {
                    if field.name() == name {
                        value.clone()
                    } else {
                        Scalar::Null(field.data_type().clone())
                    }
                })
                .collect()
        })
        .collect();
    executor.execute_and_retain(name, PlanBuilder::values(schema, rows)?.build()?)
}

fn normalize_actions(source: PlanBuilder) -> KernelResult<PlanBuilder> {
    fn align(input: &StructType, output: &StructType, prefix: &[String]) -> Expression {
        Expression::struct_from(output.fields().map(|field| {
            let mut path = prefix.to_vec();
            path.push(field.name().to_string());
            match (input.field(field.name()), field.data_type()) {
                (None, _) => null_lit(field.data_type().clone()),
                (Some(input_field), DataType::Struct(target)) => match input_field.data_type() {
                    DataType::Struct(actual) => Expression::case_when(
                        [(
                            Expression::column(path.clone()).is_not_null(),
                            align(actual, target, &path),
                        )],
                        null_lit(field.data_type().clone()),
                    ),
                    _ => Expression::column(path),
                },
                (Some(_), _) => Expression::column(path),
            }
        }))
    }
    let expr = align(source.schema(), get_commit_schema(), &[]);
    source.project(expr, get_commit_schema().clone())
}

fn validate_supported<STATE>(txn: &Transaction<STATE, Declarative>) -> KernelResult<()> {
    require!(
        !txn.staged_data_changes.has_dv_inputs(),
        KernelError::unsupported("Declarative DV updates are not implemented")
    );
    if txn.has_data_file_actions() {
        require!(
            !txn.effective_table_config
                .is_feature_enabled(&TableFeature::RowTracking),
            KernelError::unsupported("Declarative row tracking is not implemented")
        );
    }
    if txn.staged_data_changes.has_adds() {
        require!(
            txn.physical_clustering_columns
                .as_ref()
                .is_none_or(Vec::is_empty),
            KernelError::unsupported("Declarative clustered file writes are not implemented")
        );
    }
    #[cfg(feature = "adaptive-metadata-in-dev")]
    require!(
        !txn.is_adaptive_metadata_enabled(),
        KernelError::unsupported("Declarative adaptive metadata writes are not implemented")
    );
    Ok(())
}

fn validate_inputs<STATE>(
    txn: &Transaction<STATE, Declarative>,
    executor: &dyn ScopedPlanExecutor,
) -> KernelResult<()> {
    validate_supported(txn)?;
    let partition_columns: Vec<_> = txn
        .effective_table_config
        .physical_partition_columns()
        .collect();
    let partition_count = i64::try_from(partition_columns.len())
        .map_err(|_| KernelError::internal_error("Partition column count exceeds LONG"))?;
    let mut paths = Vec::new();
    for input in &txn.staged_data_changes.add_files {
        let mut invalid = vec![
            Predicate::is_null(col!("path")),
            Predicate::eq(col!("path"), lit("")),
            Predicate::is_null(col!("size")),
            Predicate::lt(col!("size"), lit(0i64)),
            Predicate::is_null(col!("modificationTime")),
            Predicate::is_null(col!("partitionValues")),
            Predicate::ne(
                Expression::unary(UnaryExpressionOp::MapSize, col!("partitionValues")),
                lit(partition_count),
            ),
        ];
        if txn.effective_table_config.requires_stats_num_records() {
            invalid.push(Predicate::is_null(col!("stats.numRecords")));
            invalid.push(Predicate::lt(col!("stats.numRecords"), lit(0i64)));
        }
        if input.schema().contains("deletionVector") {
            invalid.push(Predicate::is_not_null(col!("deletionVector")));
        }
        if input.schema().contains("clusteringProvider") {
            invalid.push(Predicate::is_not_null(col!("clusteringProvider")));
        }
        let plan = PlanBuilder::relation_source(input.clone()).project(
            Expression::struct_from([
                Expression::from(Predicate::or_from(invalid)),
                Expression::unary(UnaryExpressionOp::MapKeys, col!("partitionValues")),
            ]),
            schema_ref! {
                nullable "invalid": BOOLEAN,
                nullable "partitionKeys": [not_null STRING],
            },
        )?;
        // Exact cardinality plus every expected key rejects extra, missing, and duplicate keys.
        // Inspect keys rather than values: a null value is a valid partition value.
        let invalid = Predicate::or_from(
            std::iter::once(Predicate::eq(col!("invalid"), lit(true))).chain(
                partition_columns.iter().map(|key| {
                    Predicate::not(Predicate::binary(
                        BinaryPredicateOp::In,
                        lit(key.clone()),
                        col!("partitionKeys"),
                    ))
                }),
            ),
        );
        ensure_no_rows(executor, plan.filter(invalid)?, "Invalid AddFile metadata")?;
        paths.push(PlanBuilder::relation_source(input.clone()).project(
            Expression::struct_from([col!("path")]),
            schema_ref! { nullable "path": STRING },
        )?);
    }
    for input in &txn.staged_data_changes.remove_files {
        let invalid = Predicate::or_from([
            Predicate::is_null(col!("path")),
            Predicate::eq(col!("path"), lit("")),
            Predicate::is_null(col!("size")),
            Predicate::lt(col!("size"), lit(0i64)),
            Predicate::is_not_null(col!("deletionVector")),
        ]);
        ensure_no_rows(
            executor,
            PlanBuilder::relation_source(input.clone()).filter(invalid)?,
            "Invalid RemoveFile metadata or unsupported deletion vector",
        )?;
        if txn.data_change
            && txn
                .effective_table_config
                .is_feature_enabled(&TableFeature::AppendOnly)
        {
            ensure_no_rows(
                executor,
                PlanBuilder::relation_source(input.clone()),
                "Append-only tables cannot remove data files",
            )?;
        }
        paths.push(PlanBuilder::relation_source(input.clone()).project(
            Expression::struct_from([col!("path")]),
            schema_ref! { nullable "path": STRING },
        )?);
    }
    if !txn.skip_dedup_validation && !paths.is_empty() {
        let duplicate_paths = PlanBuilder::union_all(paths)?
            .aggregate_by([column_name!("path")], |a| a.count_star())?
            .filter(Predicate::gt(col!("count"), lit(1i64)))?;
        ensure_no_rows(
            executor,
            duplicate_paths,
            "Transaction contains duplicate file action paths",
        )?;
    }
    Ok(())
}

fn ensure_no_rows(
    executor: &dyn ScopedPlanExecutor,
    plan: PlanBuilder,
    message: &str,
) -> KernelResult<()> {
    let count = read_summary(executor, plan.aggregate_ungrouped(|a| a.count_star())?)?.0;
    require!(count == 0, KernelError::invalid_transaction_state(message));
    Ok(())
}

fn summarize_files(
    actions: &RelationRef,
    executor: &dyn ScopedPlanExecutor,
) -> KernelResult<FileStatsDelta> {
    fn summary(
        actions: &RelationRef,
        name: &str,
        executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<(u64, u64)> {
        let plan = PlanBuilder::relation_source(actions.clone())
            .filter(Predicate::is_not_null(Expression::column([name])))?
            .project(
                Expression::struct_from([Expression::column([name, "size"])]),
                schema_ref! { nullable "size": LONG },
            )?
            .aggregate_ungrouped(|a| {
                a.count_star()
                    .aggregate_as(Agg::sum(column_name!("size")), "bytes")
            })?;
        read_summary(executor, plan)
    }
    let (gross_add_files, gross_add_bytes) = summary(actions, "add", executor)?;
    let (gross_remove_files, gross_remove_bytes) = summary(actions, "remove", executor)?;
    Ok(FileStatsDelta {
        gross_add_files,
        gross_remove_files,
        gross_add_bytes,
        gross_remove_bytes,
        net_histogram: None,
    })
}

fn read_summary(executor: &dyn ScopedPlanExecutor, plan: PlanBuilder) -> KernelResult<(u64, u64)> {
    let has_bytes = plan.schema().contains("bytes");
    let mut visitor = SummaryVisitor {
        value: None,
        has_bytes,
    };
    for batch in executor
        .execute_op(Operation::QueryPlan(plan.build()?))?
        .into_data()?
    {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    visitor
        .value
        .ok_or_else(|| KernelError::internal_error("Plan returned no summary row"))
}

struct SummaryVisitor {
    value: Option<(u64, u64)>,
    has_bytes: bool,
}
impl RowVisitor for SummaryVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 2]> =
            LazyLock::new(|| [column_name!("count"), column_name!("bytes")]);
        static TYPES: [DataType; 2] = [DataType::LONG, DataType::LONG];
        let len = if self.has_bytes { 2 } else { 1 };
        (&NAMES[..len], &TYPES[..len])
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..row_count {
            require!(
                self.value.is_none(),
                KernelError::internal_error("Plan returned multiple summary rows")
            );
            let count: i64 = getters[0].get(row, "count")?;
            let bytes: i64 = if self.has_bytes {
                getters[1].get_opt(row, "bytes")?.unwrap_or(0)
            } else {
                0
            };
            self.value = Some((
                u64::try_from(count)
                    .map_err(|_| KernelError::internal_error("Negative summary count"))?,
                u64::try_from(bytes)
                    .map_err(|_| KernelError::internal_error("Negative summary bytes"))?,
            ));
        }
        Ok(())
    }
}
