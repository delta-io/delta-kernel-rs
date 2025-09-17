use std::collections::HashSet;
use std::iter;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use crate::actions::{as_log_add_schema, get_log_commit_info_schema, get_log_remove_schema, get_log_txn_schema, get_log_domain_metadata_schema};
use crate::actions::{CommitInfo, DomainMetadata, SetTransaction};
use crate::arrow::array::BooleanArray;
use crate::arrow::compute::filter_record_batch;
use crate::engine::arrow_data::{ArrowEngineData, extract_record_batch};
use crate::engine_data::FilteredEngineData;
use crate::error::Error;
use crate::path::ParsedLogPath;
use crate::scan::scan_row_schema;
use crate::schema::{ArrayType,MapType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, ExpressionRef, IntoEngineData, RowVisitor, Version
};

use url::Url;


use crate::expressions::{ArrayData, Transform, UnaryExpressionOp::ToJson};
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::utils::current_time_ms;

/// Type alias for an iterator of [`EngineData`] results.
type EngineDataResultIterator<'a> =
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a>;

/// The static instance referenced by [`add_files_schema`] that doesn't contain the dataChange column.
pub(crate) static MANDATORY_ADD_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
    ]))
});

///
/// Note this does not include "dataChange" which is a required field but
/// but should be set on the transactoin level. Getting the full schema
/// can be done with [`Transaction::add_files_schema`].
pub(crate) fn mandatory_add_file_schema() -> &'static SchemaRef {
    &MANDATORY_ADD_FILE_SCHEMA
}

/// The static instance referenced by [`add_files_schema`].
pub(crate) static BASE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let stats = StructField::nullable(
        "stats",
        DataType::struct_type(vec![StructField::nullable("numRecords", DataType::LONG)]),
    );

    Arc::new(StructType::new(
        mandatory_add_file_schema().fields().cloned().chain([stats]),
    ))
});

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

/// The static instance referenced by [`add_files_schema`] that contains the dataChange column.
static ADD_FILES_SCHEMA_WITH_DATA_CHANGE: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = BASE_ADD_FILES_SCHEMA.fields().collect::<Vec<_>>();
    let len = fields.len();
    let insert_position = fields
        .iter()
        .position(|f| f.name() == "modificationTime")
        .unwrap_or(len);
    fields.insert(insert_position + 1, &DATA_CHANGE_COLUMN);
    Arc::new(StructType::new(fields.into_iter().cloned()))
});

// NOTE: The following two methods are a workaround for the fact that we do not have a proper SchemaBuilder yet.
// See https://github.com/delta-io/delta-kernel-rs/issues/1284
/// Extend a schema with a statistics column and return a new SchemaRef.
///
/// The stats column is of type string as required by the spec.
///
/// Note that this method is only useful to extend an Add action schema.
pub(crate) fn with_stats_col(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .cloned()
        .chain([StructField::nullable("stats", DataType::STRING)]);
    Arc::new(StructType::new(fields))
}

/// Extend a schema with row tracking columns and return a new SchemaRef.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_row_tracking_cols(schema: &SchemaRef) -> SchemaRef {
    let fields = schema.fields().cloned().chain([
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
    ]);
    Arc::new(StructType::new(fields))
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
    remove_files_metadata: Vec<FilteredEngineData>,
    // NB: hashmap would require either duplicating the appid or splitting SetTransaction
    // key/payload. HashSet requires Borrow<&str> with matching Eq, Ord, and Hash. Plus,
    // HashSet::insert drops the to-be-inserted value without returning the existing one, which
    // would make error messaging unnecessarily difficult. Thus, we keep Vec here and deduplicate in
    // the commit method.
    set_transactions: Vec<SetTransaction>,
    // commit-wide timestamp (in milliseconds since epoch) - used in ICT, `txn` action, etc. to
    // keep all timestamps within the same commit consistent.
    commit_timestamp: i64,
    // Whether this transaction contains any logical data changes.
    data_change: Option<bool>,
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
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp,
            data_change: None,
        })
    }

    /// Consume the transaction and commit it to the table. The result is a [CommitResult] which
    /// will include the failed transaction in case of a conflict so the user can retry.
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        // Step 1: Check for duplicate app_ids and generate set transactions (`txn`)
        // Note: The commit info must always be the first action in the commit but we generate it in
        // step 2 to fail early on duplicate transaction appIds
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

        // Step 2: Construct commit info and initialize the action iterator
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            self.operation.clone(),
            self.engine_info.clone(),
        );
        let commit_info_action =
            commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine);

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let add_actions = if self
            .read_snapshot
            .table_configuration()
            .should_write_row_tracking()
        {
            self.generate_adds_with_row_tracking(engine, commit_version)?
        } else {
            self.generate_adds(
                engine,
                self.add_files_metadata.iter().map(|a| Ok(a.deref())),
                self.add_files_schema().clone(),
                with_stats_col(&ADD_FILES_SCHEMA_WITH_DATA_CHANGE.clone()),
            )
        };
        let remove_actions = generate_remove_actions(
            scan_row_schema().clone(),
            get_log_remove_schema().clone(),
            self.remove_files_metadata.iter(),
            engine,
        );

        // Step 4: Commit the actions as a JSON file to the Delta log
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;
        let actions = iter::once(commit_info_action)
            .chain(set_transaction_actions)
            .chain(add_actions)
            .chain(remove_actions);

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

    /// Set the data change flag.
    ///
    /// When set, writers should not provide a data_change column in EngineData (it will get replaced with the value set
    /// on this method).
    ///
    /// Engines/Connectors should prefer using this method over setting data_change to false in EngineData,
    /// as that route will eventually be deprecated.
    ///
    /// Data change is false in the following scenarios:
    /// 1. Operations that only change metadata (e.g. backfilling statistics)
    /// 2. Operations that make no logical changes to the contents of the table (i.e. rows are only moved
    ///    from old files to new ones.  OPTIMIZE commands is one example of this type of optimizaton).
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = Some(data_change);
        self
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
    /// Note: The returned schema can change depending on the options set on the transaction or
    /// in the future Table features. For example, calling [`Transaction::with_data_change`] will return a schema that
    /// does not contain the dataChange column.
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    /// [`ParquetHandler`]: crate::ParquetHandler
    pub fn add_files_schema(&self) -> &'static SchemaRef {
        // TODO: This won't scale as we have more columns set at the transaction level.
        // We should consider a builder pattern to construct the schema but
        // this would change the lifetime of SchemaRef, so we'll leave this to
        // a future PR.
        if self.data_change.is_some() {
            &BASE_ADD_FILES_SCHEMA
        } else {
            &ADD_FILES_SCHEMA_WITH_DATA_CHANGE
        }
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
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }

    /// Convert file metadata provided by the engine into protocol-compliant add actions.
    fn generate_adds<'a, I, T>(
        &'a self,
        engine: &dyn Engine,
        add_files_metadata: I,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> EngineDataResultIterator<'a>
    where
        I: Iterator<Item = DeltaResult<T>> + Send + 'a,
        T: Deref<Target = dyn EngineData> + Send + 'a,
    {
        let evaluation_handler = engine.evaluation_handler();

        Box::new(add_files_metadata.map(move |add_files_batch| {
            // Convert stats to a JSON string and nest the add action in a top-level struct
            let mut transform = Transform::new_top_level().with_replaced_field(
                "stats",
                Expression::unary(ToJson, Expression::column(["stats"])).into(),
            );
            if let Some(data_change) = self.data_change {
                transform = transform.with_inserted_field(
                    Some("modificationTime"),
                    Expression::literal(data_change).into(),
                );
            }
            let expr = Arc::new(Expression::struct_from([Expression::transform(transform)]));
            let update_evaluator = evaluation_handler.new_expression_evaluator(
                input_schema.clone(),
                expr,
                as_log_add_schema(output_schema.clone()).into(),
            );
            update_evaluator.evaluate(add_files_batch?.deref())
        }))
    }

    /// Extend file metadata provided by the engine with row tracking information and convert them into
    /// protocol-compliant add actions.
    fn generate_adds_with_row_tracking<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: u64,
    ) -> DeltaResult<EngineDataResultIterator<'a>> {
        // Return early if we have nothing to add
        if self.add_files_metadata.is_empty() {
            return Ok(Box::new(iter::empty()));
        }

        // Read the current rowIdHighWaterMark from the snapshot's row tracking domain metadata
        let row_id_high_water_mark =
            RowTrackingDomainMetadata::get_high_water_mark(&self.read_snapshot, engine)?;

        // Create a row tracking visitor and visit all files to collect row tracking information
        let mut row_tracking_visitor = RowTrackingVisitor::new(row_id_high_water_mark);
        let mut base_row_id_batches = Vec::with_capacity(self.add_files_metadata.len());

        // We visit all files with the row visitor before creating the add action iterator
        // because we need to know the final row ID high water mark to create the domain metadata action
        for add_files_batch in &self.add_files_metadata {
            row_tracking_visitor.visit_rows_of(add_files_batch.deref())?;
            base_row_id_batches.push(row_tracking_visitor.base_row_ids.clone());
        }

        // Generate a domain metadata action based on the final high water mark
        let domain_metadata = DomainMetadata::try_from(RowTrackingDomainMetadata::new(
            row_tracking_visitor.row_id_high_water_mark,
        ))?;
        let domain_metadata_action =
            domain_metadata.into_engine_data(get_log_domain_metadata_schema().clone(), engine);

        // Create an iterator that pairs each add action with its row tracking metadata
        let extended_add_files_metadata =
            self.add_files_metadata.iter().zip(base_row_id_batches).map(
                move |(add_files_batch, base_row_ids)| {
                    let commit_versions = vec![commit_version as i64; base_row_ids.len()];
                    let base_row_ids =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), base_row_ids)?;
                    let row_commit_versions =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), commit_versions)?;

                    add_files_batch.append_columns(
                        with_row_tracking_cols(&Arc::new(StructType::new(vec![]))),
                        vec![base_row_ids, row_commit_versions],
                    )
                },
            );

        // Generate add actions including row tracking metadata
        let add_actions = self.generate_adds(
            engine,
            extended_add_files_metadata,
            with_row_tracking_cols(self.add_files_schema()),
            with_row_tracking_cols(&with_stats_col(&ADD_FILES_SCHEMA_WITH_DATA_CHANGE.clone())),
        );

        // Return a chained iterator with add and domain metadata actions
        Ok(Box::new(
            add_actions.chain(iter::once(domain_metadata_action)),
        ))
    }

    /// Remove files from the table in this transaction. This API generally enables the engine to
    /// delete data (at file-level granularity) from the table. Note that this API can be called
    /// multiple times to remove multiple batches.
    ///
    /// the expected schema for `remove_metadata` is given by [`scan_row_schema`] it, is expected
    /// this will be the result of passing [`FilteredEngineData`] returned from a scan
    /// with rows modified for removal.
    pub fn remove_files(&mut self, remove_metadata: FilteredEngineData) {
        self.remove_files_metadata.push(remove_metadata);
    }
}


// Convert files_metadata (either add_files_metadata or remove_files_metadata) into add/remove file
// actions using an expression to transform the data (in a single pass) from [`input_schema`] into
// [`target_schema`].
fn generate_remove_actions<'a>(
    input_schema: SchemaRef,
    target_schema: SchemaRef,
    file_metadata: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
    engine: &dyn Engine,
) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a {
    let evaluation_handler = engine.evaluation_handler();
    file_metadata.map(move |file_metadata_batch| {
        let file_action_expr = Expression::struct_from([Expression::struct_from(
            input_schema
                .fields()
                .map(|f| Expression::column([f.name()])),
        )]);
        let file_action_eval = evaluation_handler.new_expression_evaluator(
            input_schema.clone(),
            Arc::new(file_action_expr),
            target_schema.clone().into(),
        );
        // Extract the underlying data and apply the selection vector to get only selected rows
        let batch = extract_record_batch(&*file_metadata_batch.data)?;
        let filtered_batch = filter_record_batch(batch, &BooleanArray::from(file_metadata_batch.selection_vector.clone()))
            .map_err(|e| Error::generic(format!("Failed to filter record batch: {e}")))?;
        let filtered_engine_data = Box::new(ArrowEngineData::from(filtered_batch)) as Box<dyn EngineData>;
        
        file_action_eval.evaluate(&*filtered_engine_data)
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
    use crate::engine::sync::SyncEngine;
    use crate::schema::MapType;
    use std::path::PathBuf;

    // TODO: create a finer-grained unit tests for transactions (issue#1091)

    #[test]
    fn test_add_files_schema() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Arc::new(Snapshot::builder(url).at_version(1).build(&engine).unwrap());
        let mut txn = snapshot.transaction()?.with_engine_info("default engine");

        let mut schema = txn.add_files_schema();
        let mut expected = StructType::new(vec![
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
        ]);
        assert_eq!(*schema, expected.into());

        txn = txn.with_data_change(true);
        schema = txn.add_files_schema();
        expected = StructType::new(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable(
                "stats",
                DataType::struct_type(vec![StructField::nullable("numRecords", DataType::LONG)]),
            ),
        ]);
        assert_eq!(*schema, expected.into());
        Ok(())
    }
}
