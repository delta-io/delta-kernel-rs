use std::collections::{HashMap, HashSet};
use std::iter;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actions::SetTransaction;
use crate::actions::COMMIT_INFO_NAME;
use crate::actions::{get_log_add_schema, get_log_commit_info_schema, get_log_txn_schema};
use crate::error::Error;
use crate::expressions::{MapData, Scalar};
use crate::path::ParsedLogPath;
use crate::schema::{MapType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DataType, DeltaResult, Engine, EngineData, EvaluationHandlerExtension, Expression, IntoEngineData, Version};

use url::Url;

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
const UNKNOWN_OPERATION: &str = "UNKNOWN";

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
    ]))
});

/// This function specifies the schema for the add_files metadata (and soon remove_files metadata).
/// Concretely, it is the expected schema for engine data passed to [`add_files`].
///
/// Each row represents metadata about a file to be added to the table.
///
/// [`add_files`]: crate::transaction::Transaction::add_files
pub fn add_files_schema() -> &'static SchemaRef {
    &ADD_FILES_SCHEMA
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
    engine_commit_info: HashMap<String, String>,
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
            "Transaction {{ read_snapshot version: {}, commit_info: {:#?} }}",
            self.read_snapshot.version(),
            self.engine_commit_info
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
            engine_commit_info: HashMap::new(),
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
        if self.engine_commit_info.is_empty() {
            return Err(Error::MissingCommitInfo);
        }
        let commit_info_actions = generate_commit_info(
            engine,
            self.operation.as_deref(),
            self.commit_timestamp,
            &self.engine_commit_info,
        );
        let add_actions = generate_adds(engine, self.add_files_metadata.iter().map(|a| a.as_ref()));

        let actions = iter::once(commit_info_actions)
            .chain(add_actions)
            .chain(set_transaction_actions);

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;

        // step three: commit the actions as a json file in the log
        let json_handler = engine.json_handler();
        match json_handler.write_json_file(&commit_path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResult::Committed(commit_version)),
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

    /// Add commit info to this transaction and append the argument as its engine_commit_info.
    /// The engine is required to provide commit info before committing the transcation.
    pub fn with_engine_commit_info(mut self, engine_commit_info: (String, String)) -> Self {
        self.engine_commit_info.insert(engine_commit_info.0, engine_commit_info.1);
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
    /// The expected schema for `add_metadata` is given by [`add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }
}

// convert add_files_metadata into add actions using an expression to transform the data in a single
// pass
fn generate_adds<'a>(
    engine: &dyn Engine,
    add_files_metadata: impl Iterator<Item = &'a dyn EngineData> + Send + 'a,
) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a {
    let evaluation_handler = engine.evaluation_handler();
    let add_files_schema = add_files_schema();
    let log_schema = get_log_add_schema();

    add_files_metadata.map(move |add_files_batch| {
        let adds_expr = Expression::struct_from([Expression::struct_from(
            add_files_schema
                .fields()
                .map(|f| Expression::column([f.name()])),
        )]);
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            add_files_schema.clone(),
            adds_expr,
            log_schema.clone().into(),
        );
        adds_evaluator.evaluate(add_files_batch)
    })
}

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    target_dir: Url,
    schema: SchemaRef,
    logical_to_physical: Expression,
}

impl WriteContext {
    fn new(target_dir: Url, schema: SchemaRef, logical_to_physical: Expression) -> Self {
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

    pub fn logical_to_physical(&self) -> &Expression {
        &self.logical_to_physical
    }
}

/// Result after committing a transaction. If 'committed', the version is the new version written
/// to the log. If 'conflict', the transaction is returned so the caller can resolve the conflict
/// (along with the version which conflicted).
// TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
// update the transaction to a new version etc.
#[derive(Debug)]
pub enum CommitResult {
    /// The transaction was successfully committed at the version.
    Committed(Version),
    /// This transaction conflicted with an existing version (at the version given).
    Conflict(Transaction, Version),
}

// Helper function to convert HashMap to Scalar for commit_info
// TODO: should we implement Scalar::From<HashMap>?
fn hashmap_to_scalar(hm: Option<&HashMap<String, String>>) -> DeltaResult<Scalar> {
    let map_data = MapData::try_new(
        MapType::new(DataType::STRING, DataType::STRING, false),
        hm.into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.clone())),
    )?;
    Ok(Scalar::Map(map_data))
}

// given the engine's commit info we want to create commitInfo action to commit (and append more actions to)
fn generate_commit_info(
    engine: &dyn Engine,
    operation: Option<&str>,
    timestamp: i64,
    engine_commit_info: &HashMap<String, String>,
) -> DeltaResult<Box<dyn EngineData>> {
    let mut commit_info_schema = get_log_commit_info_schema().as_ref().clone();

    // Remove the inCommitTimestamp field from the schema
    let commit_info_field = commit_info_schema
        .fields
        .get_mut(COMMIT_INFO_NAME)
        .ok_or_else(|| Error::missing_column(COMMIT_INFO_NAME))?;

    let DataType::Struct(mut commit_info_struct) = commit_info_field.data_type().clone() else {
        return Err(Error::internal_error("commitInfo field should be a struct"));
    };
    commit_info_struct.fields.shift_remove("inCommitTimestamp");
    commit_info_field.data_type = DataType::Struct(commit_info_struct);

    let commit_info_values = vec![
        // timestamp
        Scalar::Long(timestamp),
        // operation
        Scalar::String(operation.unwrap_or(UNKNOWN_OPERATION).to_string()),
        // operation parameters
        hashmap_to_scalar(None)?,
        // kernel_version
        Scalar::String(format!("v{KERNEL_VERSION}")),
        // engine_commit_info
        hashmap_to_scalar(Some(engine_commit_info))?,
    ];

    engine
        .evaluation_handler()
        .create_one(Arc::new(commit_info_schema), &commit_info_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::schema::MapType;
    use crate::{EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

    use crate::arrow::json::writer::LineDelimitedWriter;
    use crate::arrow::record_batch::RecordBatch;

    struct ExprEngine(Arc<dyn EvaluationHandler>);

    impl ExprEngine {
        fn new() -> Self {
            ExprEngine(Arc::new(ArrowEvaluationHandler))
        }
    }

    impl Engine for ExprEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.0.clone()
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            unimplemented!()
        }
    }

    // convert it to JSON just for ease of comparison (and since we ultimately persist as JSON)
    fn as_json(data: Box<dyn EngineData>) -> serde_json::Value {
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let buf = Vec::new();
        let mut writer = LineDelimitedWriter::new(buf);
        writer.write_batches(&[&record_batch]).unwrap();
        writer.finish().unwrap();
        let buf = writer.into_inner();

        serde_json::from_slice(&buf).unwrap()
    }

    #[test]
    fn test_generate_commit_info() -> DeltaResult<()> {
        let engine = ExprEngine::new();
        
        let operation = Some("test operation");
        let mut engine_commit_info = HashMap::new();
        engine_commit_info.insert("engineInfo".to_string(), "default engine".to_string());

        let actions = generate_commit_info(&engine, operation, 123456789, &engine_commit_info)?;

        let expected = serde_json::json!({
            "commitInfo": {
                "timestamp": 123456789,
                "operation": "test operation",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineCommitInfo": {
                    "engineInfo": "default engine"
                }
            }
        });

        assert_eq!(actions.len(), 1);
        let result = as_json(actions);
        assert_eq!(result, expected);

        Ok(())
    }

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
        ]);
        assert_eq!(*schema, expected.into());
    }
}
