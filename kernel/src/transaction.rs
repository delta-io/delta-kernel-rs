use std::collections::{HashMap, HashSet};
use std::iter;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actions::{get_log_add_schema, get_log_commit_info_schema, get_log_txn_schema};
use crate::actions::{CommitInfo, SetTransaction};
use crate::error::Error;
use crate::expressions::{MapData, Scalar};
use crate::path::ParsedLogPath;
use crate::schema::{MapType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{
    DataType, DeltaResult, Engine, EngineData, EvaluationHandlerExtension, Expression,
    IntoEngineData, Version,
};

use url::Url;

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
const UNKNOWN_OPERATION: &str = "UNKNOWN";

pub(crate) static WRITE_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

/// Get the expected schema for engine data passed to [`add_write_metadata`].
///
/// [`add_write_metadata`]: crate::transaction::Transaction::add_write_metadata
pub fn get_write_metadata_schema() -> &'static SchemaRef {
    &WRITE_METADATA_SCHEMA
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
    commit_info: Option<CommitInfo>,
    write_metadata: Vec<Box<dyn EngineData>>,
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
            self.commit_info
        ))
    }
}

impl Transaction {
    /// Create a new transaction from a snapshot. The snapshot will be used to read the current
    /// state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Table::new_transaction](crate::table::Table::new_transaction) to create a transaction from
    /// a table automatically backed by the latest snapshot.
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
            commit_info: None,
            write_metadata: vec![],
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
        let commit_info = self
            .commit_info
            .clone()
            .ok_or_else(|| Error::MissingCommitInfo)?;

        let commit_info_actions = generate_commit_info(engine, &commit_info, self.commit_timestamp);

        let add_actions = generate_adds(engine, self.write_metadata.iter().map(|a| a.as_ref()));

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

    // Set the timestmap of this transaction.
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.commit_info.get_or_insert_default().timestamp = Some(timestamp);
        self
    }

    /// Set the in-commit timestamp for this transaction. This is used for In-Commit Timestamps (ICT) feature.
    /// When ICT is enabled, this timestamp must be monotonically increasing across commits.
    ///
    /// Note: For full ICT compliance, the caller should ensure this timestamp is greater than
    /// the previous commit's inCommitTimestamp.
    pub fn with_in_commit_timestamp(mut self, in_commit_timestamp: i64) -> Self {
        self.commit_info.get_or_insert_default().in_commit_timestamp = Some(in_commit_timestamp);
        self
    }

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commit and visible to anyone who describes the table history.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.commit_info.get_or_insert_default().operation = Some(operation);
        self
    }

    // Set the operation parameters for the operation that this transaction is performing.
    pub fn with_operation_parameters(
        mut self,
        operation_parameters: HashMap<String, String>,
    ) -> Self {
        self.commit_info
            .get_or_insert_default()
            .operation_parameters = Some(operation_parameters);
        self
    }

    // Set the version of the delta_kernel crate used to write this transaction.
    pub fn with_kernel_version(mut self, kernel_version: String) -> Self {
        self.commit_info.get_or_insert_default().kernel_version = Some(kernel_version);
        self
    }

    // Set the engine commit info of this transaction.
    //
    // Note: The engine data passed here must have exactly one row, and we
    /// only read one column: `engineCommitInfo` which must be a map<string, string> encoding the
    /// metadata.
    pub fn with_engine_commit_info(mut self, engine_commit_info: HashMap<String, String>) -> Self {
        self.commit_info.get_or_insert_default().engine_commit_info = Some(engine_commit_info);
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

    /// Add write metadata about files to include in the transaction. This API can be called
    /// multiple times to add multiple batches.
    ///
    /// The expected schema for `write_metadata` is given by [`get_write_metadata_schema`].
    pub fn add_write_metadata(&mut self, write_metadata: Box<dyn EngineData>) {
        self.write_metadata.push(write_metadata);
    }
}

// convert write_metadata into add actions using an expression to transform the data in a single
// pass
fn generate_adds<'a>(
    engine: &dyn Engine,
    write_metadata: impl Iterator<Item = &'a dyn EngineData> + Send + 'a,
) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a {
    let evaluation_handler = engine.evaluation_handler();
    let write_metadata_schema = get_write_metadata_schema();
    let log_schema = get_log_add_schema();

    write_metadata.map(move |write_metadata_batch| {
        let adds_expr = Expression::struct_from([Expression::struct_from(
            write_metadata_schema
                .fields()
                .map(|f| Expression::column([f.name()])),
        )]);
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            write_metadata_schema.clone(),
            adds_expr,
            log_schema.clone().into(),
        );
        adds_evaluator.evaluate(write_metadata_batch)
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

// given the CommitInfo struct we want to materialize it into a commitInfo action to commit (and append more actions to)
fn generate_commit_info(
    engine: &dyn Engine,
    commit_info: &CommitInfo,
    commit_timestamp: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    if let Some(engine_commit_info) = &commit_info.engine_commit_info {
        if engine_commit_info.len() != 1 {
            return Err(Error::InvalidCommitInfo(format!(
                "Engine commit info should have exactly one row, found {}",
                engine_commit_info.len()
            )));
        }
    }

    // Helper function to convert HashMap to Scalar for commit_info
    let hashmap_to_scalar = |hm: &Option<HashMap<String, String>>| -> DeltaResult<Scalar> {
        match hm {
            Some(map) => {
                let pairs = map.iter().map(|(k, v)| (k.clone(), v.clone()));
                let map_data = MapData::try_new(
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    pairs,
                )?;
                Ok(Scalar::Map(map_data))
            }
            None => {
                let map_data = MapData::try_new(
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    std::iter::empty::<(String, String)>(),
                )?;
                Ok(Scalar::Map(map_data))
            }
        }
    };

    let commit_info_schema = get_log_commit_info_schema();

    let commit_info_values = vec![
        // timestamp
        Scalar::Long(commit_info.timestamp.unwrap_or(commit_timestamp)),
        // in_commit_timestamp
        Scalar::Long(commit_info.in_commit_timestamp.unwrap_or(commit_timestamp)),
        //operation
        Scalar::String(
            commit_info
                .operation
                .clone()
                .unwrap_or(UNKNOWN_OPERATION.to_string()),
        ),
        // operation parameters
        hashmap_to_scalar(&commit_info.operation_parameters)?,
        // kernel_version
        Scalar::String(
            commit_info
                .kernel_version
                .clone()
                .unwrap_or(format!("v{}", KERNEL_VERSION)),
        ),
        // engine_commit_info
        hashmap_to_scalar(&commit_info.engine_commit_info)?,
    ];

    engine
        .evaluation_handler()
        .create_one(commit_info_schema.clone(), &commit_info_values)
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

        let commit_info = CommitInfo {
            timestamp: None,
            in_commit_timestamp: Some(123),
            operation: Some("test operation".to_string()),
            kernel_version: Some(format!("v{}", env!("CARGO_PKG_VERSION"))),
            operation_parameters: Some(HashMap::new()),
            engine_commit_info: Some(
                vec![("engineInfo".to_string(), "default engine".to_string())]
                    .into_iter()
                    .collect(),
            ),
        };

        let actions = generate_commit_info(&engine, &commit_info, 123456789)?;

        let expected = serde_json::json!({
            "commitInfo": {
                "timestamp": 123456789,
                "inCommitTimestamp": 123,
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
    fn test_generate_commit_info_invalid_engine_commit_info() -> DeltaResult<()> {
        let engine = ExprEngine::new();

        let mut commit_info = CommitInfo {
            timestamp: Some(123456789),
            in_commit_timestamp: Some(123),
            operation: Some("test operation".to_string()),
            kernel_version: Some(format!("v{}", env!("CARGO_PKG_VERSION"))),
            operation_parameters: Some(HashMap::new()),
            engine_commit_info: Some(HashMap::new()),
        };

        match generate_commit_info(&engine, &commit_info, 0) {
            Err(Error::InvalidCommitInfo(msg)) => {
                assert_eq!(
                    msg,
                    "Engine commit info should have exactly one row, found 0"
                );
            }
            _ => panic!("Expected InvalidCommitInfo error"),
        }

        commit_info
            .engine_commit_info
            .as_mut()
            .unwrap()
            .insert("row1".to_string(), "default engine".to_string());
        commit_info
            .engine_commit_info
            .as_mut()
            .unwrap()
            .insert("row2".to_string(), "default engine".to_string());

        match generate_commit_info(&engine, &commit_info, 0) {
            Err(Error::InvalidCommitInfo(msg)) => {
                assert_eq!(
                    msg,
                    "Engine commit info should have exactly one row, found 2"
                );
            }
            _ => panic!("Expected InvalidCommitInfo error"),
        }

        Ok(())
    }

    #[test]
    fn test_write_metadata_schema() {
        let schema = get_write_metadata_schema();
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
