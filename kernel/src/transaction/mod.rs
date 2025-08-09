use std::collections::HashSet;
use std::iter;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actions::{
    get_log_add_schema, get_log_commit_info_schema, get_log_domain_metadata_schema,
    get_log_txn_schema,
};
use crate::actions::{CommitInfo, DomainMetadata, SetTransaction};
use crate::error::Error;
use crate::path::ParsedLogPath;
use crate::row_tracking::{
    RowTrackingDomainConfiguration, RowTrackingVisitor, ROW_TRACKING_DOMAIN,
};
use crate::schema::{DataType, MapType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, EngineData, Expression, IntoEngineData, RowVisitor, Version};

use url::Url;

/// This function specifies the schema for the add_files metadata (and soon remove_files metadata)
/// **as they are provided by the Parquet handler**. Concretely, it is the expected schema for engine data passed to [`add_files`].
/// Each row represents metadata about a file to be added to the table.
/// Kernel-rs takes this information and extends it to the full add_file metadata schema, adding
/// additional fields (e.g., baseRowID) as necessary.
///
/// [`add_files`]: crate::transaction::Transaction::add_files
pub(crate) static PARQUET_ADD_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::not_null("dataChange", DataType::BOOLEAN),
        StructField::nullable("stats", DataType::STRING),
    ]))
});

pub(crate) fn parquet_add_file_schema() -> &'static SchemaRef {
    &PARQUET_ADD_FILE_SCHEMA
}

/// A transaction represents an in-progress write to a table. After creating a transaction, changes
/// to the table may be staged via the transaction methods before calling `commit` to commit the
/// changes to the table.
///
/// # Examples
///
/// ```rust,ignore
/// // create a transaction
/// let mut txn = table.new_transaction(&engine)?;
/// // stage table changes (right now only commit info)
/// txn.commit_info(Box::new(ArrowEngineData::new(engine_commit_info)));
/// // commit! (consume the transaction)
/// txn.commit(&engine)?;
/// ```
pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    operation: Option<String>,
    engine_info: Option<String>,
    add_files_metadata: Vec<Box<dyn EngineData>>,
    // NB: hashmap would require either duplicating the appid or splitting SetTransaction
    // key/payload. HashSet requires Borrow<&str> with matching Eq, Ord, and Hash. Plus,
    // HashSet::insert drops the to-be-inserted value without returning the existing one, which
    // would make error messaging unnecessarily difficult. Thus, we keep Vec here and deduplicate in
    // the commit method.
    set_transactions: Vec<SetTransaction>,
    // commit-wide timestamp (in milliseconds since epoch) - used in ICT, `txn` action, etc. to
    // keep all timestamps within the same commit consistent.
    commit_timestamp: i64,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, engine_info: {} }}",
            self.read_snapshot.version(),
            self.engine_info.is_some()
        ))
    }
}

impl Transaction {
    /// Create a new transaction from a snapshot. The snapshot will be used to read the current
    /// state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Snapshot::transaction](crate::snapshot::Snapshot::transaction) to create a transaction from
    /// a snapshot.
    pub(crate) fn try_new(snapshot: impl Into<Arc<Snapshot>>) -> DeltaResult<Self> {
        let read_snapshot = snapshot.into();

        // important! before a read/write to the table we must check it is supported
        read_snapshot
            .table_configuration()
            .ensure_write_supported()?;

        // TODO: unify all these into a (safer) `fn current_time_ms()`
        let commit_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .ok_or_else(|| Error::generic("Failed to get current time for commit_timestamp"))?;

        Ok(Transaction {
            read_snapshot,
            operation: None,
            engine_info: None,
            add_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp,
        })
    }

    /// Consume the transaction and commit it to the table. The result is a [CommitResult] which
    /// will include the failed transaction in case of a conflict so the user can retry.
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        // Step 0: if there are txn(app_id, version) actions being committed, ensure that every
        // `app_id` is unique and create a row of `EngineData` for it.
        // TODO(zach): we currently do this in two passes - can we do it in one and still keep refs
        // in the HashSet?
        let mut app_ids = HashSet::new();
        if let Some(dup) = self
            .set_transactions
            .iter()
            .find(|t| !app_ids.insert(&t.app_id))
        {
            return Err(Error::generic(format!(
                "app_id {} already exists in transaction",
                dup.app_id
            )));
        }
        let set_transaction_actions = self
            .set_transactions
            .clone()
            .into_iter()
            .map(|txn| txn.into_engine_data(get_log_txn_schema().clone(), engine));

        // Step 1: construct commit info and initialize the action iterator
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            self.operation.clone(),
            self.engine_info.clone(),
        );
        let commit_info_action =
            commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine);

        let actions = iter::once(commit_info_action).chain(set_transaction_actions);

        // Step 2: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;

        // Step 3: add row tracking metadata if row tracking is supported
        // Step 4: construct the file actions we want to commit
        let actions = if self
            .read_snapshot
            .table_configuration()
            .is_row_tracking_supported()
        {
            // Read the current rowIdHighWaterMark from the snapshot
            // Note that there might be no row tracking domain metadata in the snapshot if the table hasn't been written to before
            let row_tracking_domain: RowTrackingDomainConfiguration = match self
                .read_snapshot
                .get_domain_metadata(ROW_TRACKING_DOMAIN, engine)?
            {
                Some(domain_metadata) => serde_json::from_str(&domain_metadata)?,
                None => RowTrackingDomainConfiguration {
                    row_id_high_water_mark: None,
                },
            };

            let mut row_tracking_visitor = RowTrackingVisitor::new(
                row_tracking_domain.row_id_high_water_mark,
                commit_version as i64,
            );

            self.add_files_metadata
                .iter()
                .try_for_each(|add_files_batch| {
                    row_tracking_visitor.visit_rows_of(add_files_batch.as_ref())
                })?;

            let add_actions: Vec<_> = row_tracking_visitor
                .adds
                .iter()
                .map(|add| {
                    add.clone()
                        // NOTE: This creates single-row EngineData - can we make this more efficient?
                        .into_engine_data(get_log_add_schema().clone(), engine)
                })
                .collect();

            // Generate a domain metadata action based on the new rowIdHighWaterMark and append it to the actions
            let domain_metadata = DomainMetadata::new(
                ROW_TRACKING_DOMAIN.to_string(),
                serde_json::to_string(&RowTrackingDomainConfiguration {
                    row_id_high_water_mark: row_tracking_visitor.row_id_high_water_mark,
                })?,
                false,
            );
            let domain_metadata_action =
                domain_metadata.into_engine_data(get_log_domain_metadata_schema().clone(), engine);

            Box::new(
                actions
                    .chain(add_actions)
                    .chain(iter::once(domain_metadata_action)),
            ) as Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>
        } else {
            let add_actions =
                self.generate_adds(engine, self.add_files_metadata.iter().map(|a| a.as_ref()));

            Box::new(actions.chain(add_actions))
                as Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>
        };
        // Step 5: commit the actions as a json file in the log
        let json_handler = engine.json_handler();
        match json_handler.write_json_file(&commit_path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResult::Committed {
                version: commit_version,
                post_commit_stats: PostCommitStats {
                    commits_since_checkpoint: self
                        .read_snapshot
                        .log_segment()
                        .commits_since_checkpoint()
                        + 1,
                    commits_since_log_compaction: self
                        .read_snapshot
                        .log_segment()
                        .commits_since_log_compaction_or_checkpoint()
                        + 1,
                },
            }),
            Err(Error::FileAlreadyExists(_)) => Ok(CommitResult::Conflict(self, commit_version)),
            Err(e) => Err(e),
        }
    }

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commit and visible to anyone who describes the table history.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Set the engine info field of this transaction's commit info action. This field is optional.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    /// Include a SetTransaction (app_id and version) action for this transaction (with an optional
    /// `last_updated` timestamp).
    /// Note that each app_id can only appear once per transaction. That is, multiple app_ids with
    /// different versions are disallowed in a single transaction. If a duplicate app_id is
    /// included, the `commit` will fail (that is, we don't eagerly check app_id validity here).
    pub fn with_transaction_id(mut self, app_id: String, version: i64) -> Self {
        let set_transaction = SetTransaction::new(app_id, version, Some(self.commit_timestamp));
        self.set_transactions.push(set_transaction);
        self
    }

    // Generate the logical-to-physical transform expression which must be evaluated on every data
    // chunk before writing. At the moment, this is a transaction-wide expression.
    fn generate_logical_to_physical(&self) -> Expression {
        // for now, we just pass through all the columns except partition columns.
        // note this is _incorrect_ if table config deems we need partition columns.
        let partition_columns = &self.read_snapshot.metadata().partition_columns;
        let schema = self.read_snapshot.schema();
        let fields = schema
            .fields()
            .filter(|f| !partition_columns.contains(f.name()))
            .map(|f| Expression::column([f.name()]));
        Expression::struct_from(fields)
    }

    /// Get the write context for this transaction. At the moment, this is constant for the whole
    /// transaction.
    // Note: after we introduce metadata updates (modify table schema, etc.), we need to make sure
    // that engines cannot call this method after a metadata change, since the write context could
    // have invalid metadata.
    pub fn get_write_context(&self) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        let logical_to_physical = self.generate_logical_to_physical();
        WriteContext::new(target_dir.clone(), snapshot_schema, logical_to_physical)
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_files` is given by [`parquet_add_file_schema`].
    pub fn add_files(&mut self, add_files: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_files);
    }

    // convert add_files_metadata into add actions using an expression to transform the data in a single
    // pass
    fn generate_adds<'a>(
        &'a self,
        engine: &dyn Engine,
        add_files_metadata: impl Iterator<Item = &'a dyn EngineData> + Send + 'a,
    ) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a {
        let evaluation_handler = engine.evaluation_handler();
        let log_schema = get_log_add_schema();

        add_files_metadata.map(move |add_files_batch| {
            let adds_expr = Expression::struct_from([Expression::struct_from(
                parquet_add_file_schema()
                    .fields()
                    .map(|f| Expression::column([f.name()])),
            )]);
            let adds_evaluator = evaluation_handler.new_expression_evaluator(
                parquet_add_file_schema().clone(),
                adds_expr,
                log_schema.clone().into(),
            );
            adds_evaluator.evaluate(add_files_batch)
        })
    }
}

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    target_dir: Url,
    snapshot_schema: SchemaRef,
    parquet_add_file_schema: SchemaRef,
    logical_to_physical: Expression,
}

impl WriteContext {
    fn new(target_dir: Url, snapshot_schema: SchemaRef, logical_to_physical: Expression) -> Self {
        WriteContext {
            target_dir,
            snapshot_schema,
            parquet_add_file_schema: PARQUET_ADD_FILE_SCHEMA.clone(),
            logical_to_physical,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    /// Get the logical schema of the read snapshot that this transaction is based on.
    pub fn snapshot_schema(&self) -> &SchemaRef {
        &self.snapshot_schema
    }

    /// Get the expected schema for engine data passed to [`add_files`].
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    pub fn parquet_add_file_schema(&self) -> &SchemaRef {
        &self.parquet_add_file_schema
    }

    pub fn logical_to_physical(&self) -> &Expression {
        &self.logical_to_physical
    }
}

/// Kernel exposes information about the state of the table that engines might want to use to
/// trigger actions like checkpointing or log compaction. This struct holds that information.
#[derive(Debug)]
pub struct PostCommitStats {
    /// The number of commits since this table has been checkpointed. Note that commit 0 is
    /// considered a checkpoint for the purposes of this computation.
    pub commits_since_checkpoint: u64,
    /// The number of commits since the log has been compacted on this table. Note that a checkpoint
    /// is considered a compaction for the purposes of this computation. Thus this is really the
    /// number of commits since a compaction OR a checkpoint.
    pub commits_since_log_compaction: u64,
}

/// Result of committing a transaction.
#[derive(Debug)]
pub enum CommitResult {
    /// The transaction was successfully committed.
    Committed {
        /// the version of the table that was just committed
        version: Version,
        /// The [`PostCommitStats`] for this transaction
        post_commit_stats: PostCommitStats,
    },
    /// This transaction conflicted with an existing version (at the version given). The transaction
    /// is returned so the caller can resolve the conflict (along with the version which
    /// conflicted).
    // TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
    // update the transaction to a new version etc.
    Conflict(Transaction, Version),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::schema::MapType;
    use std::path::PathBuf;

    // TODO: create a finer-grained unit tests for transactions (issue#1091)

    #[test]
    fn test_parquet_add_file_schema() -> DeltaResult<()> {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/"))?;
        let url = Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, None)?;
        let txn = Transaction::try_new(snapshot)?;
        let ctx = txn.get_write_context();
        let schema = ctx.parquet_add_file_schema();

        let expected = StructType::new(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::not_null("dataChange", DataType::BOOLEAN),
            StructField::nullable("stats", DataType::STRING),
            // StructField::nullable(
            //     "tags",
            //     MapType::new(DataType::STRING, DataType::STRING, false),
            // ),
            // StructField::nullable(
            //     "deletionVector",
            //     StructType::new(vec![
            //         StructField::not_null("storageType", DataType::STRING),
            //         StructField::not_null("pathOrInlineDv", DataType::STRING),
            //         StructField::nullable("offset", DataType::INTEGER),
            //         StructField::not_null("sizeInBytes", DataType::INTEGER),
            //         StructField::not_null("cardinality", DataType::LONG),
            //         // TODO: Should derived fields be present here?
            //         // StructField::not_null("uniqueID", DataType::STRING),
            //         // StructField::not_null("abslutePath", DataType::STRING),
            //     ]),
            // ),
            // StructField::nullable("baseRowId", DataType::LONG),
            // StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            // StructField::nullable("clusteringProvider", DataType::STRING),
        ]);
        assert_eq!(*schema, expected.into());
        Ok(())
    }
}
