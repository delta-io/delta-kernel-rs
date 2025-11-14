use crate::arrow::array::StringArray;
use crate::scan::state_info::StateInfo;
use crate::schema::StructType;
use crate::utils::test_utils::string_array_to_engine_data;
use itertools::Itertools;
use std::sync::Arc;

use crate::log_replay::ActionsBatch;
use crate::{
    actions::get_commit_schema,
    engine::{
        arrow_data::ArrowEngineData,
        sync::{json::SyncJsonHandler, SyncEngine},
    },
    scan::log_replay::scan_action_iter,
    schema::SchemaRef,
    JsonHandler,
};

use super::state::ScanCallback;
use super::PhysicalPredicate;
use crate::table_features::ColumnMappingMode;
use crate::transforms::TransformSpec;

// Generates a batch of sidecar actions with the given paths.
// The schema is provided as null columns affect equality checks.
pub(crate) fn sidecar_batch_with_given_paths(
    paths: Vec<&str>,
    output_schema: SchemaRef,
) -> Box<ArrowEngineData> {
    let handler = SyncJsonHandler {};

    let mut json_strings: Vec<String> = paths
        .iter()
        .map(|path| {
            format!(
                r#"{{"sidecar":{{"path":"{path}","sizeInBytes":9268,"modificationTime":1714496113961,"tags":{{"tag_foo":"tag_bar"}}}}}}"#
            )
        })
        .collect();
    json_strings.push(r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#.to_string());

    let json_strings_array: StringArray =
        json_strings.iter().map(|s| s.as_str()).collect_vec().into();

    let parsed = handler
        .parse_json(
            string_array_to_engine_data(json_strings_array),
            output_schema,
        )
        .unwrap();

    ArrowEngineData::try_from_engine_data(parsed).unwrap()
}

// Generates a batch with an add action.
// The schema is provided as null columns affect equality checks.
pub(crate) fn add_batch_simple(output_schema: SchemaRef) -> Box<ArrowEngineData> {
    let handler = SyncJsonHandler {};
    let json_strings: StringArray = vec![
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
    ]
        .into();
    let parsed = handler
        .parse_json(string_array_to_engine_data(json_strings), output_schema)
        .unwrap();
    ArrowEngineData::try_from_engine_data(parsed).unwrap()
}

// Generates a batch with an add action.
// The schema is provided as null columns affect equality checks.
pub(crate) fn add_batch_for_row_id(output_schema: SchemaRef) -> Box<ArrowEngineData> {
    let handler = SyncJsonHandler {};
    let json_strings: StringArray = vec![
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","baseRowId": 42, "tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none", "delta.enableRowTracking": "true", "delta.rowTracking.materializedRowIdColumnName":"row_id_col"},"createdTime":1677811175819}}"#,
    ]
        .into();
    let parsed = handler
        .parse_json(string_array_to_engine_data(json_strings), output_schema)
        .unwrap();
    ArrowEngineData::try_from_engine_data(parsed).unwrap()
}

// An add batch with a removed file parsed with the schema provided
pub(crate) fn add_batch_with_remove(output_schema: SchemaRef) -> Box<ArrowEngineData> {
    let handler = SyncJsonHandler {};
    let json_strings: StringArray = vec![
        r#"{"remove":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","deletionTimestamp":1677811194426,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":635,"tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
    ]
        .into();
    let parsed = handler
        .parse_json(string_array_to_engine_data(json_strings), output_schema)
        .unwrap();
    ArrowEngineData::try_from_engine_data(parsed).unwrap()
}

// add batch with a `date` partition col
pub(crate) fn add_batch_with_partition_col() -> Box<ArrowEngineData> {
    let handler = SyncJsonHandler {};
    let json_strings: StringArray = vec![
        r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","partitionValues": {"date": "2017-12-11"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
    ]
        .into();
    let output_schema = get_commit_schema().clone();
    let parsed = handler
        .parse_json(string_array_to_engine_data(json_strings), output_schema)
        .unwrap();
    ArrowEngineData::try_from_engine_data(parsed).unwrap()
}

/// Create a scan action iter and validate what's called back. If you pass `None` as
/// `logical_schema`, `transform` should also be `None`
#[allow(clippy::vec_box)]
pub(crate) fn run_with_validate_callback<T: Clone>(
    batch: Vec<Box<ArrowEngineData>>,
    logical_schema: Option<SchemaRef>,
    transform_spec: Option<Arc<TransformSpec>>,
    expected_sel_vec: &[bool],
    context: T,
    validate_callback: ScanCallback<T>,
) {
    let logical_schema =
        logical_schema.unwrap_or_else(|| Arc::new(StructType::new_unchecked(vec![])));
    let state_info = Arc::new(StateInfo {
        logical_schema: logical_schema.clone(),
        physical_schema: logical_schema,
        physical_predicate: PhysicalPredicate::None,
        transform_spec,
        column_mapping_mode: ColumnMappingMode::None,
    });
    let iter = scan_action_iter(
        &SyncEngine::new(),
        batch
            .into_iter()
            .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
        state_info,
    )
    .unwrap();
    let mut batch_count = 0;
    for res in iter {
        let scan_metadata = res.unwrap();
        assert_eq!(
            scan_metadata.scan_files.selection_vector(),
            expected_sel_vec
        );
        scan_metadata
            .visit_scan_files(context.clone(), validate_callback)
            .unwrap();
        batch_count += 1;
    }
    assert_eq!(batch_count, 1);
}
