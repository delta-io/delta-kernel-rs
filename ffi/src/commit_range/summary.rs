//! Commit summaries of a commit range: build the summary plan, then validate its result.
//!
//! ```text
//! commit_range_get_commit_summary_plan(range, engine)      // -> SharedCommitSummaryPlan
//!     // or commit_range_get_commit_summary_plan_with_snapshot(range, engine, start_snapshot)
//!   -> commit_summary_plan_get_plan(plan)                  // -> proto bytes; engine executes them
//!   -> commit_summary_plan_validate(plan, engine, rows, n) // -> ExclusiveEngineData of summaries
//!   -> free_commit_summary_plan(plan)
//! ```

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use delta_kernel::commit_range::{CommitRange, CommitSummary, CommitSummaryPlan, SchemaChange};
use delta_kernel::expressions::{ArrayData, ColumnName, MapData, Scalar, StructData};
use delta_kernel::schema::{ArrayType, DataType, MapType, SchemaRef, StructField, lazy_schema_ref};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::{Engine, EngineData, KernelError, KernelResult, Operation, Result};
use delta_kernel_ffi_macros::handle_descriptor;

use super::SharedCommitRange;
use crate::handle::Handle;
use crate::{
    ExclusiveEngineData, ExternResult, IntoExternResult, KernelOwnedBytes, SharedExternEngine,
    SharedSnapshot,
};

/// An opaque, shared handle owning a [`CommitSummaryPlan`] produced by
/// [`commit_range_get_commit_summary_plan`]. The caller owns the handle and must release it with
/// [`free_commit_summary_plan`].
#[handle_descriptor(target=CommitSummaryPlan, mutable=false, sized=true)]
pub struct SharedCommitSummaryPlan;

/// What a schema change in the batch returned by [`commit_summary_plan_validate`] changed. The
/// `kind` field of each `schemaChanges` element holds the discriminant of one of these variants.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiSchemaChangeKind {
    /// A field was added to a struct.
    AddColumn = 0,
    /// A field was dropped.
    DropColumn = 1,
    /// A field was renamed.
    RenameColumn = 2,
    /// The type of a field, array element, map key, or map value changed.
    ChangeColumnType = 3,
    /// A field became nullable.
    SetNullable = 4,
    /// A field became not nullable.
    SetNotNull = 5,
    /// The table's column mapping mode changed.
    ChangeColumnMappingMode = 6,
}

/// Builds the plan that summarizes the commits of `commit_range`, without a start snapshot.
///
/// The returned handle does not borrow from `commit_range`. Execute the plan returned by
/// [`commit_summary_plan_get_plan`], then pass its rows to [`commit_summary_plan_validate`].
///
/// Returns a `CommitRangeError` if the range was built with commit range validation disabled.
///
/// # Safety
///
/// Caller is responsible for passing valid handles. Both are borrowed for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn commit_range_get_commit_summary_plan(
    commit_range: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedCommitSummaryPlan>> {
    let engine = unsafe { engine.as_ref() };
    let commit_range = unsafe { commit_range.as_ref() };
    let result = get_commit_summary_plan_impl(commit_range, None);
    unsafe { result.into_extern_result(&engine) }
}

/// Like [`commit_range_get_commit_summary_plan`], but validates the commits against the protocol
/// and metadata of `start_snapshot` and reports the table's schema for every commit.
///
/// `start_snapshot` must be the snapshot of the range's table at the range's start version,
/// whatever the range's ordering. Returns a `CommitRangeError` if it is not.
///
/// # Safety
///
/// Caller is responsible for passing valid handles. All are borrowed for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn commit_range_get_commit_summary_plan_with_snapshot(
    commit_range: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
    start_snapshot: Handle<SharedSnapshot>,
) -> ExternResult<Handle<SharedCommitSummaryPlan>> {
    let engine = unsafe { engine.as_ref() };
    let commit_range = unsafe { commit_range.as_ref() };
    let start_snapshot = unsafe { start_snapshot.clone_as_arc() };
    let result = get_commit_summary_plan_impl(commit_range, Some(start_snapshot));
    unsafe { result.into_extern_result(&engine) }
}

/// Returns the plan of `plan` as a proto-serialized `delta.kernel.operation.Operation` message
/// (a `QueryPlan`). The caller owns the buffer and must free it with
/// [`free_kernel_bytes`](crate::free_kernel_bytes).
///
/// # Safety
///
/// Caller is responsible for passing a valid handle, which is borrowed for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn commit_summary_plan_get_plan(
    plan: Handle<SharedCommitSummaryPlan>,
) -> KernelOwnedBytes {
    let plan = unsafe { plan.as_ref() };
    Operation::QueryPlan(plan.plan().clone())
        .to_proto_bytes()
        .into()
}

/// Validates the rows that the plan of `plan` produced and returns one summary row per commit of
/// the range, in the range's ordering, as a single batch of engine data.
///
/// `plan_result` points to `plan_result_len` batches that together hold every row of the plan, in
/// any order. This call consumes every batch, on success and on error: the caller must not use or
/// free them afterwards. `plan_result` may be null only if `plan_result_len` is zero; otherwise
/// the call fails without touching it. The array itself stays owned by the caller; the pointer
/// is `*mut` because the handles are moved out of it.
///
/// The returned batch has these columns. A null value means that the fact is unknown:
///
/// | Column | Type | Meaning |
/// | :---- | :---- | :---- |
/// | `version` | long, not null | The commit's version. |
/// | `timestamp` | long, not null | The commit's timestamp in milliseconds since the epoch. |
/// | `schema` | string | The table's logical schema as of the commit, as Delta schema JSON. |
/// | `partitionColumns` | array of string | The table's partition columns as of the commit. |
/// | `schemaChanges` | array of struct | The commit's schema changes; see below. |
/// | `hasDataChange` | boolean | Whether the commit changed the table's data. |
/// | `numAddedFiles` | long | The number of files the commit added. |
/// | `numRemovedFiles` | long | The number of files the commit removed. |
/// | `operation` | string | The commit's operation name as its writer recorded it. |
/// | `operationMetrics` | map of string to string | The operation's metrics as recorded. |
///
/// Each `schemaChanges` element has a not-null integer `kind`, the discriminant of an
/// [`FfiSchemaChangeKind`], and a not-null `column`: the path of the changed field in the
/// schema before the commit, or of the struct that received the field for `AddColumn` (empty
/// for the root, and for `ChangeColumnMappingMode`). The path segments `element`, `key`, and
/// `value` address the element of an array and the key and value of a map. `newName` is set for
/// `RenameColumn`, `oldDataType` and `newDataType` (Delta type JSON) for `ChangeColumnType`,
/// `field` (Delta field JSON) for `AddColumn`, and `oldColumnMappingMode` and
/// `newColumnMappingMode` (`none`, `id`, or `name`) for `ChangeColumnMappingMode`.
///
/// The caller owns the returned handle and must release it with
/// [`free_engine_data`](crate::free_engine_data) or consume it with
/// [`get_raw_arrow_data`](crate::engine_data::get_raw_arrow_data).
///
/// # Safety
///
/// Caller is responsible for passing valid `plan` and `engine` handles, which are borrowed for
/// this call, and a `plan_result` pointer that is null or addresses `plan_result_len` valid
/// engine data handles that no other code uses.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn commit_summary_plan_validate(
    plan: Handle<SharedCommitSummaryPlan>,
    engine: Handle<SharedExternEngine>,
    plan_result: *mut Handle<ExclusiveEngineData>,
    plan_result_len: usize,
) -> ExternResult<Handle<ExclusiveEngineData>> {
    // Take ownership of every batch before anything else can fail.
    let batches = unsafe { take_batches(plan_result, plan_result_len) };
    let extern_engine = unsafe { engine.as_ref() };
    let plan = unsafe { plan.as_ref() };
    let result =
        batches.and_then(|batches| validate_impl(plan, extern_engine.engine().as_ref(), batches));
    unsafe { result.into_extern_result(&extern_engine) }
}

/// Takes ownership of the `len` engine data handles at `ptr`.
///
/// # Errors
///
/// Returns an error when `len` is nonzero and `ptr` is null.
///
/// # Safety
///
/// For nonzero `len`, a `ptr` that is not null must address `len` valid engine data handles that
/// no other code uses.
unsafe fn take_batches(
    ptr: *mut Handle<ExclusiveEngineData>,
    len: usize,
) -> Result<Vec<Box<dyn EngineData>>> {
    if len > 0 && ptr.is_null() {
        return Err(
            KernelError::generic(format!("plan result pointer is null with length {len}")).into(),
        );
    }
    Ok((0..len)
        .map(|index| unsafe { ptr.add(index).read().into_inner() })
        .collect())
}

/// Releases a commit summary plan.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle, which must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_commit_summary_plan(plan: Handle<SharedCommitSummaryPlan>) {
    unsafe { plan.drop_handle() };
}

fn get_commit_summary_plan_impl(
    commit_range: &CommitRange,
    start_snapshot: Option<SnapshotRef>,
) -> Result<Handle<SharedCommitSummaryPlan>> {
    let plan = commit_range.get_commit_summary_plan(start_snapshot)?;
    Ok(Arc::new(plan).into())
}

fn validate_impl(
    plan: &CommitSummaryPlan,
    engine: &dyn Engine,
    batches: Vec<Box<dyn EngineData>>,
) -> Result<Handle<ExclusiveEngineData>> {
    let summaries = plan.validate(batches.into_iter().map(Ok))?;
    let mut schema_json = SchemaJson::default();
    let rows: Vec<_> = summaries
        .iter()
        .map(|summary| summary_row(summary, &mut schema_json))
        .collect::<KernelResult<_>>()?;
    let data = engine
        .evaluation_handler()
        .create_many(SUMMARY_BATCH_SCHEMA.clone(), rows)?;
    Ok(data.into())
}

/// The JSON of the schema that the previous summary reported. Commits that leave the schema as
/// it is report the same schema object, so its JSON is serialized once for all of them.
#[derive(Default)]
struct SchemaJson {
    last: Option<(SchemaRef, String)>,
}

impl SchemaJson {
    fn of(&mut self, schema: &SchemaRef) -> KernelResult<String> {
        match &self.last {
            Some((last, json)) if Arc::ptr_eq(last, schema) => Ok(json.clone()),
            _ => {
                let json = serde_json::to_string(schema).map_err(json_error)?;
                self.last = Some((schema.clone(), json.clone()));
                Ok(json)
            }
        }
    }
}

static STRING_ARRAY: LazyLock<ArrayType> =
    LazyLock::new(|| ArrayType::new(DataType::STRING, false));
static STRING_MAP: LazyLock<MapType> =
    LazyLock::new(|| MapType::new(DataType::STRING, DataType::STRING, false));
static SCHEMA_CHANGE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null "kind": INTEGER,
    (StructField::not_null("column", STRING_ARRAY.clone())),
    nullable "newName": STRING,
    nullable "oldDataType": STRING,
    nullable "newDataType": STRING,
    nullable "field": STRING,
    nullable "oldColumnMappingMode": STRING,
    nullable "newColumnMappingMode": STRING,
};
static SCHEMA_CHANGE_ARRAY: LazyLock<ArrayType> =
    LazyLock::new(|| ArrayType::new(SCHEMA_CHANGE_SCHEMA.clone(), false));

/// The schema of the batch returned by [`commit_summary_plan_validate`].
static SUMMARY_BATCH_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null "version": LONG,
    not_null "timestamp": LONG,
    nullable "schema": STRING,
    (StructField::nullable("partitionColumns", STRING_ARRAY.clone())),
    (StructField::nullable("schemaChanges", SCHEMA_CHANGE_ARRAY.clone())),
    nullable "hasDataChange": BOOLEAN,
    nullable "numAddedFiles": LONG,
    nullable "numRemovedFiles": LONG,
    nullable "operation": STRING,
    (StructField::nullable("operationMetrics", STRING_MAP.clone())),
};

/// Converts `summary` to a row of [`SUMMARY_BATCH_SCHEMA`].
fn summary_row(summary: &CommitSummary, schema_json: &mut SchemaJson) -> KernelResult<Vec<Scalar>> {
    let version = i64::try_from(summary.version()).map_err(|_| {
        KernelError::generic(format!("Commit version {} exceeds i64", summary.version()))
    })?;
    let schema = summary
        .schema()
        .map(|schema| schema_json.of(schema))
        .transpose()?;
    let partition_columns = summary
        .partition_columns()
        .map(|columns| ArrayData::try_new(STRING_ARRAY.clone(), columns.iter().cloned()))
        .transpose()?;
    let schema_changes = summary
        .schema_changes()
        .map(|changes| {
            let changes: Vec<_> = changes
                .iter()
                .map(schema_change_scalar)
                .collect::<KernelResult<_>>()?;
            ArrayData::try_new(SCHEMA_CHANGE_ARRAY.clone(), changes)
        })
        .transpose()?;
    Ok(vec![
        version.into(),
        summary.timestamp().into(),
        nullable(schema, &DataType::STRING),
        nullable(partition_columns, &*STRING_ARRAY),
        nullable(schema_changes, &*SCHEMA_CHANGE_ARRAY),
        nullable(summary.has_data_change(), &DataType::BOOLEAN),
        nullable(summary.num_added_files(), &DataType::LONG),
        nullable(summary.num_removed_files(), &DataType::LONG),
        nullable(summary.operation().map(str::to_string), &DataType::STRING),
        nullable(string_map(summary.operation_metrics())?, &*STRING_MAP),
    ])
}

/// The fields of a schema change row that only some kinds of change set.
#[derive(Default)]
struct ChangeDetails {
    new_name: Option<String>,
    old_type: Option<String>,
    new_type: Option<String>,
    field: Option<String>,
    old_mode: Option<String>,
    new_mode: Option<String>,
}

fn schema_change_scalar(change: &SchemaChange) -> KernelResult<Scalar> {
    let type_json = |data_type: &DataType| serde_json::to_string(data_type).map_err(json_error);
    let (kind, column, details) = match change {
        SchemaChange::AddColumn { parent, field } => (
            FfiSchemaChangeKind::AddColumn,
            parent.as_ref(),
            ChangeDetails {
                field: Some(serde_json::to_string(field).map_err(json_error)?),
                ..Default::default()
            },
        ),
        SchemaChange::DropColumn { column } => (
            FfiSchemaChangeKind::DropColumn,
            Some(column),
            ChangeDetails::default(),
        ),
        SchemaChange::RenameColumn { column, new_name } => (
            FfiSchemaChangeKind::RenameColumn,
            Some(column),
            ChangeDetails {
                new_name: Some(new_name.clone()),
                ..Default::default()
            },
        ),
        SchemaChange::ChangeColumnType {
            column,
            old_type,
            new_type,
        } => (
            FfiSchemaChangeKind::ChangeColumnType,
            Some(column),
            ChangeDetails {
                old_type: Some(type_json(old_type)?),
                new_type: Some(type_json(new_type)?),
                ..Default::default()
            },
        ),
        SchemaChange::SetNullable { column } => (
            FfiSchemaChangeKind::SetNullable,
            Some(column),
            ChangeDetails::default(),
        ),
        SchemaChange::SetNotNull { column } => (
            FfiSchemaChangeKind::SetNotNull,
            Some(column),
            ChangeDetails::default(),
        ),
        SchemaChange::ChangeColumnMappingMode { old_mode, new_mode } => (
            FfiSchemaChangeKind::ChangeColumnMappingMode,
            None,
            ChangeDetails {
                old_mode: Some(column_mapping_mode_name(*old_mode).to_string()),
                new_mode: Some(column_mapping_mode_name(*new_mode).to_string()),
                ..Default::default()
            },
        ),
        other => {
            return Err(KernelError::internal_error(format!(
                "Schema change {other:?} has no FFI representation"
            )));
        }
    };
    let segments = column.map_or(&[][..], ColumnName::path);
    let column = ArrayData::try_new(STRING_ARRAY.clone(), segments.iter().cloned())?;
    let values = vec![
        (kind as i32).into(),
        column.into(),
        nullable(details.new_name, &DataType::STRING),
        nullable(details.old_type, &DataType::STRING),
        nullable(details.new_type, &DataType::STRING),
        nullable(details.field, &DataType::STRING),
        nullable(details.old_mode, &DataType::STRING),
        nullable(details.new_mode, &DataType::STRING),
    ];
    Ok(StructData::try_new(SCHEMA_CHANGE_SCHEMA.clone(), values)?.into())
}

/// The name of a column mapping mode in the summary batch, which is the mode's name in the
/// `delta.columnMapping.mode` table property.
fn column_mapping_mode_name(mode: ColumnMappingMode) -> &'static str {
    match mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    }
}

fn string_map(map: Option<&HashMap<String, String>>) -> KernelResult<Option<MapData>> {
    map.map(|map| {
        let entries = map.iter().map(|(key, value)| (key.clone(), value.clone()));
        MapData::try_new(STRING_MAP.clone(), entries)
    })
    .transpose()
}

/// `value` as a scalar, or a null of `data_type`, which is copied only for a null.
fn nullable<T>(value: Option<impl Into<Scalar>>, data_type: &T) -> Scalar
where
    T: Clone + Into<DataType>,
{
    value.map_or_else(|| Scalar::Null(data_type.clone().into()), Into::into)
}

fn json_error(error: serde_json::Error) -> KernelError {
    KernelError::generic(format!("Cannot serialize commit summary: {error}"))
}

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{Array, AsArray, RecordBatch};
    use delta_kernel::arrow::datatypes::{Int32Type, Int64Type};
    use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
    use delta_kernel::engine::sync::SyncEngine;
    use delta_kernel::object_store::memory::InMemory;
    use test_utils::add_commit;

    use super::*;
    use crate::commit_range::{
        commit_range_builder_build, commit_range_builder_for,
        commit_range_builder_with_end_version, free_commit_range,
    };
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{
        TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains,
        assert_extern_result_error_with_message, build_snapshot, ok_or_panic,
    };
    use crate::{engine_to_handle, free_engine, free_snapshot, kernel_string_slice};

    const TABLE_ROOT: &str = "memory:///";
    const CREATE: &str = concat!(
        r#"{"commitInfo":{"timestamp":0,"operation":"CREATE TABLE"}}"#,
        "\n",
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        "\n",
        r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.appendOnly":"false"},"createdTime":1000}}"#,
    );
    const ADD_COLUMN: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1000}}"#;
    const WRITE: &str = concat!(
        r#"{"commitInfo":{"timestamp":0,"operation":"WRITE","operationMetrics":{"numOutputRows":"3"}}}"#,
        "\n",
        r#"{"add":{"path":"a","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
    );

    /// An engine with a plan executor over a table whose commits are `commits`, and the range of
    /// all of them.
    async fn setup(commits: &[&str]) -> (Handle<SharedExternEngine>, Handle<SharedCommitRange>) {
        let store = Arc::new(InMemory::new());
        for (version, commit) in commits.iter().enumerate() {
            add_commit(
                TABLE_ROOT,
                store.as_ref(),
                version as u64,
                commit.to_string(),
            )
            .await
            .unwrap();
        }
        let engine = engine_to_handle(
            Arc::new(SyncEngine::new_with_store(store)),
            TEST_ERROR_ALLOCATORS,
        );
        let range = unsafe {
            let builder = ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(TABLE_ROOT),
                0,
                engine.shallow_copy(),
            ));
            let builder = commit_range_builder_with_end_version(builder, commits.len() as u64 - 1);
            ok_or_panic(commit_range_builder_build(builder))
        };
        (engine, range)
    }

    /// Executes the plan of `plan` on `engine` and returns its batches as engine data handles.
    fn execute(
        plan: &Handle<SharedCommitSummaryPlan>,
        engine: &Handle<SharedExternEngine>,
    ) -> Vec<Handle<ExclusiveEngineData>> {
        let operation = Operation::QueryPlan(unsafe { plan.as_ref() }.plan().clone());
        let engine = unsafe { engine.as_ref() }.engine();
        let batches = engine
            .require_plan_executor()
            .unwrap()
            .execute_op(operation)
            .unwrap()
            .into_data()
            .unwrap();
        batches.map(|batch| batch.unwrap().into()).collect()
    }

    fn validate(
        plan: &Handle<SharedCommitSummaryPlan>,
        engine: &Handle<SharedExternEngine>,
        mut batches: Vec<Handle<ExclusiveEngineData>>,
    ) -> ExternResult<Handle<ExclusiveEngineData>> {
        unsafe {
            commit_summary_plan_validate(
                plan.shallow_copy(),
                engine.shallow_copy(),
                batches.as_mut_ptr(),
                batches.len(),
            )
        }
        // Dropping `batches` drops only the array: validation consumed the handles.
    }

    fn into_batch(data: Handle<ExclusiveEngineData>) -> RecordBatch {
        unsafe { data.into_inner() }
            .try_into_record_batch()
            .unwrap()
    }

    #[tokio::test]
    async fn validate_returns_one_summary_row_per_commit() {
        let (engine, range) = setup(&[CREATE, ADD_COLUMN, WRITE]).await;
        let plan = unsafe {
            ok_or_panic(commit_range_get_commit_summary_plan(
                range.shallow_copy(),
                engine.shallow_copy(),
            ))
        };

        let batches = execute(&plan, &engine);
        let summaries = into_batch(ok_or_panic(validate(&plan, &engine, batches)));

        assert_eq!(
            summaries.schema().fields().len(),
            SUMMARY_BATCH_SCHEMA.fields().len()
        );
        let column = |name: &str| summaries.column_by_name(name).unwrap().clone();
        assert_eq!(
            column("version").as_primitive::<Int64Type>().values(),
            &[0, 1, 2]
        );
        assert_eq!(
            column("numAddedFiles").as_primitive::<Int64Type>().values(),
            &[0, 0, 1]
        );
        let has_data_change = Vec::from_iter(column("hasDataChange").as_boolean().iter());
        assert_eq!(has_data_change, [Some(false), Some(false), Some(true)]);
        let operation = column("operation");
        let operation = Vec::from_iter(operation.as_string::<i32>().iter());
        assert_eq!(operation, [Some("CREATE TABLE"), None, Some("WRITE")]);
        assert!(
            column("schema")
                .as_string::<i32>()
                .value(1)
                .contains("\"value\"")
        );

        // The first commit sets the schema, the second adds a column, the third changes nothing.
        let changes = column("schemaChanges");
        let changes = changes.as_list::<i32>();
        assert!(changes.is_null(0));
        let added = changes.value(1);
        let added = added.as_struct();
        assert_eq!(
            added
                .column_by_name("kind")
                .unwrap()
                .as_primitive::<Int32Type>()
                .value(0),
            FfiSchemaChangeKind::AddColumn as i32
        );
        let field = added
            .column_by_name("field")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert!(field.contains("\"name\":\"value\""));
        assert_eq!(changes.value(2).len(), 0);

        // The metrics map holds what the commits recorded.
        let metrics = column("operationMetrics");
        assert!(metrics.is_null(0));
        assert_eq!(metrics.as_map().value(2).len(), 1);

        unsafe {
            free_commit_summary_plan(plan);
            free_commit_range(range);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn get_plan_returns_the_encoded_plan() {
        let (engine, range) = setup(&[CREATE, WRITE]).await;
        let plan = unsafe {
            ok_or_panic(commit_range_get_commit_summary_plan(
                range.shallow_copy(),
                engine.shallow_copy(),
            ))
        };

        let bytes = unsafe { commit_summary_plan_get_plan(plan.shallow_copy()).into_vec() };

        let operation = Operation::QueryPlan(unsafe { plan.as_ref() }.plan().clone());
        assert_eq!(bytes, operation.to_proto_bytes());

        unsafe {
            free_commit_summary_plan(plan);
            free_commit_range(range);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn plan_with_a_start_snapshot_validates_and_reports_the_schema() {
        let (engine, range) = setup(&[CREATE]).await;
        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(TABLE_ROOT), engine.shallow_copy()) };
        let plan = unsafe {
            ok_or_panic(commit_range_get_commit_summary_plan_with_snapshot(
                range.shallow_copy(),
                engine.shallow_copy(),
                snapshot.shallow_copy(),
            ))
        };

        let batches = execute(&plan, &engine);
        let summaries = into_batch(ok_or_panic(validate(&plan, &engine, batches)));

        assert_eq!(summaries.num_rows(), 1);
        assert!(!summaries.column_by_name("schema").unwrap().is_null(0));

        unsafe {
            free_commit_summary_plan(plan);
            free_snapshot(snapshot);
            free_commit_range(range);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn validate_reports_missing_and_duplicate_rows_and_a_null_result() {
        let (engine, range) = setup(&[CREATE, WRITE]).await;
        let plan = unsafe {
            ok_or_panic(commit_range_get_commit_summary_plan(
                range.shallow_copy(),
                engine.shallow_copy(),
            ))
        };

        let no_rows = unsafe {
            commit_summary_plan_validate(
                plan.shallow_copy(),
                engine.shallow_copy(),
                std::ptr::null_mut(),
                0,
            )
        };
        assert_extern_result_error_with_message(
            no_rows,
            FFIKernelError::CommitRangeError,
            Some("Commit range error: Commit summary result has no row for version 0"),
        );

        let mut batches = execute(&plan, &engine);
        batches.extend(execute(&plan, &engine));
        assert_extern_result_error_contains(
            validate(&plan, &engine, batches),
            FFIKernelError::CommitRangeError,
            "more than one row for version 0",
        );

        let null_result = unsafe {
            commit_summary_plan_validate(
                plan.shallow_copy(),
                engine.shallow_copy(),
                std::ptr::null_mut(),
                1,
            )
        };
        assert_extern_result_error_contains(
            null_result,
            FFIKernelError::GenericError,
            "plan result pointer is null with length 1",
        );

        unsafe {
            free_commit_summary_plan(plan);
            free_commit_range(range);
            free_engine(engine);
        }
    }

    /// The value of the field `name` of the schema change row `change`.
    fn field<'a>(change: &'a StructData, name: &str) -> &'a Scalar {
        let index = change.fields().position(|field| field.name() == name);
        &change.values()[index.unwrap()]
    }

    fn string(value: &str) -> Scalar {
        Scalar::String(value.to_string())
    }

    #[test]
    fn type_change_reports_its_path_and_the_old_and_new_type() {
        let change = SchemaChange::ChangeColumnType {
            column: ColumnName::new(["list", "element"]),
            old_type: DataType::INTEGER,
            new_type: DataType::LONG,
        };
        let Scalar::Struct(change) = schema_change_scalar(&change).unwrap() else {
            panic!("a schema change is a struct");
        };

        let kind = Scalar::Integer(FfiSchemaChangeKind::ChangeColumnType as i32);
        assert_eq!(field(&change, "kind"), &kind);
        assert_eq!(field(&change, "oldDataType"), &string("\"integer\""));
        assert_eq!(field(&change, "newDataType"), &string("\"long\""));
        assert_eq!(field(&change, "newName"), &Scalar::Null(DataType::STRING));
        assert_eq!(field(&change, "field"), &Scalar::Null(DataType::STRING));
        let Scalar::Array(column) = field(&change, "column") else {
            panic!("a column path is an array");
        };
        assert_eq!(column.array_elements(), [string("list"), string("element")]);
    }

    #[test]
    fn column_mapping_mode_change_reports_the_old_and_new_mode() {
        let change = SchemaChange::ChangeColumnMappingMode {
            old_mode: ColumnMappingMode::Name,
            new_mode: ColumnMappingMode::None,
        };
        let Scalar::Struct(change) = schema_change_scalar(&change).unwrap() else {
            panic!("a schema change is a struct");
        };

        let kind = Scalar::Integer(FfiSchemaChangeKind::ChangeColumnMappingMode as i32);
        assert_eq!(field(&change, "kind"), &kind);
        assert_eq!(field(&change, "oldColumnMappingMode"), &string("name"));
        assert_eq!(field(&change, "newColumnMappingMode"), &string("none"));
        let Scalar::Array(column) = field(&change, "column") else {
            panic!("a column path is an array");
        };
        assert!(column.array_elements().is_empty());
    }
}
