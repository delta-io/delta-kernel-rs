use std::collections::HashSet;
use std::iter;
use std::sync::{Arc, LazyLock};

use crate::actions::{get_log_add_schema, get_log_commit_info_schema, get_log_txn_schema};
use crate::actions::{CommitInfo, SetTransaction};
use crate::error::Error;
use crate::expressions::UnaryExpressionOp;
use crate::path::ParsedLogPath;
use crate::schema::{MapType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::utils::current_time_ms;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, ExpressionRef, IntoEngineData, Version,
};

use url::Url;

/// The static instance referenced by [`add_files_schema`].
pub(crate) static ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::not_null("dataChange", DataType::BOOLEAN),
        StructField::nullable("numRecords", DataType::LONG),
    ]))
});

/// The schema that the [`Engine`]'s [`ParquetHandler`] is expected to use when reporting information about
/// a Parquet write operation back to Kernel.
///
/// Concretely, it is the expected schema for [`EngineData`] passed to [`add_files`], as it is the base
/// for constructing an add_file (and soon remove_file) action. Each row represents metadata about a
/// file to be added to the table. Kernel takes this information and extends it to the full add_file
/// action schema, adding additional fields (e.g., baseRowID) as necessary.
///
/// For now, Kernel only supports the number of records as a file statistic.
/// This will change in a future release.
///
/// [`add_files`]: crate::transaction::Transaction::add_files
/// [`ParquetHandler`]: crate::ParquetHandler
pub fn add_files_schema() -> &'static SchemaRef {
    &ADD_FILES_SCHEMA
}

pub(crate) static INTERMEDIATE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::not_null("dataChange", DataType::BOOLEAN),
        StructField::nullable(
            "stats",
            DataType::struct_type(vec![StructField::nullable("numRecords", DataType::LONG)]),
        ),
    ]))
});

fn intermediate_add_files_schema() -> &'static SchemaRef {
    &INTERMEDIATE_ADD_FILES_SCHEMA
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

        let commit_timestamp = current_time_ms()?;

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
        // step 0: if there are txn(app_id, version) actions being committed, ensure that every
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

        // step one: construct the iterator of commit info + file actions we want to commit
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            self.operation.clone(),
            self.engine_info.clone(),
        );

        let commit_info_schema = get_log_commit_info_schema().clone();

        let commit_info_action = commit_info.into_engine_data(commit_info_schema, engine);
        let add_actions = generate_adds(engine, self.add_files_metadata.iter().map(|a| a.as_ref()));

        let actions = iter::once(commit_info_action)
            .chain(add_actions)
            .chain(set_transaction_actions);

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;

        // step three: commit the actions as a json file in the log
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
        WriteContext::new(
            target_dir.clone(),
            snapshot_schema,
            Arc::new(logical_to_physical),
        )
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_metadata` is given by [`add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }
}

/// Convert file metadata provided by the engine into protocol-compliant add actions.
fn generate_adds<'a>(
    engine: &dyn Engine,
    add_files_metadata: impl Iterator<Item = &'a dyn EngineData> + Send + 'a,
) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a {
    let evaluation_handler = engine.evaluation_handler();
    let add_files_schema = add_files_schema();
    let intermediate_schema = intermediate_add_files_schema();
    let log_schema = get_log_add_schema();

    add_files_metadata.map(move |add_files_batch| {
        // Step 1: Nest numRecords inside a stats struct
        // TODO: Migrate this to a Transform expression once they support nested structs (see #1247)
        let intermediate_expr =
            Expression::struct_from(add_files_schema.fields().map(|f| match f.name().as_str() {
                "numRecords" => Expression::struct_from([Expression::column([f.name()])]),
                _ => Expression::column([f.name()]),
            }));
        let intermediate_evaluator = evaluation_handler.new_expression_evaluator(
            add_files_schema.clone(),
            Arc::new(intermediate_expr),
            intermediate_schema.clone().into(),
        );
        let add_files_batch = intermediate_evaluator.evaluate(add_files_batch)?;

        // Step 2: Convert stats to JSON string
        let field_expressions: Vec<Expression> = intermediate_schema
            .fields()
            .map(|f| match f.name().as_str() {
                "stats" => {
                    Expression::unary(UnaryExpressionOp::ToJson, Expression::column([f.name()]))
                }
                _ => Expression::column([f.name()]),
            })
            .collect();
        let adds_expr = Expression::struct_from([Expression::struct_from(field_expressions)]);
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            intermediate_schema.clone(),
            Arc::new(adds_expr),
            log_schema.clone().into(),
        );
        adds_evaluator.evaluate(add_files_batch.as_ref())
    })
}

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    target_dir: Url,
    schema: SchemaRef,
    logical_to_physical: ExpressionRef,
}

impl WriteContext {
    fn new(target_dir: Url, schema: SchemaRef, logical_to_physical: ExpressionRef) -> Self {
        WriteContext {
            target_dir,
            schema,
            logical_to_physical,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.logical_to_physical.clone()
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
    use crate::schema::MapType;

    // TODO: create a finer-grained unit tests for transactions (issue#1091)

    #[test]
    fn test_add_files_schema() {
        let schema = add_files_schema();
        let expected = StructType::new(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::not_null("dataChange", DataType::BOOLEAN),
            StructField::nullable("numRecords", DataType::LONG),
        ]);
        assert_eq!(*schema, expected.into());
    }
}
