use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use deltakernel::client::DefaultTableClient;
use deltakernel::executor::tokio::TokioBackgroundExecutor;
use deltakernel::expressions::{BinaryOperator, Expression};
use deltakernel::scan::ScanBuilder;
use deltakernel::simple_client::data::SimpleData;
use deltakernel::Table;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use url::Url;

const PARQUET_FILE1: &str = "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";
const PARQUET_FILE2: &str = "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet";

const METADATA: &str = r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

enum TestAction {
    Add(String),
    Remove(String),
    Metadata,
}

fn generate_commit(actions: Vec<TestAction>) -> String {
    actions
            .into_iter()
            .map(|test_action| match test_action {
                TestAction::Add(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\": 1}},\"maxValues\":{{\"id\":3}}}}"}}}}"#, action = "add", path = path),
                TestAction::Remove(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true}}}}"#, action = "remove", path = path),
                TestAction::Metadata => METADATA.into(),
            })
            .fold(String::new(), |a, b| a + &b + "\n")
}

fn load_parquet(batch: &RecordBatch) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props)).unwrap();
    writer.write(batch).expect("Writing batch");
    // writer must be closed to write footer
    writer.close().unwrap();
    data
}

fn generate_simple_batch() -> Result<RecordBatch, ArrowError> {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let vals = StringArray::from(vec!["a", "b", "c"]);
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("val", Arc::new(vals) as ArrayRef),
    ])
}

async fn add_commit(
    store: &dyn ObjectStore,
    version: u64,
    data: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let filename = format!("_delta_log/{:0>20}.json", version);
    store.put(&Path::from(filename), data.into()).await?;
    Ok(())
}

#[tokio::test]
async fn single_commit_two_add_files() -> Result<(), Box<dyn std::error::Error>> {
    let batch = generate_simple_batch()?;
    let storage = Arc::new(InMemory::new());
    add_commit(
        storage.as_ref(),
        0,
        generate_commit(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
            TestAction::Add(PARQUET_FILE2.to_string()),
        ]),
    )
    .await?;
    storage
        .put(&Path::from(PARQUET_FILE1), load_parquet(&batch).into())
        .await?;
    storage
        .put(&Path::from(PARQUET_FILE2), load_parquet(&batch).into())
        .await?;

    let location = Url::parse("memory:///")?;
    let engine_client = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(&engine_client, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let mut files = 0;
    let stream = scan.execute(&engine_client)?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        let engine_data = data.raw_data?;
        let raw = Box::into_raw(engine_data) as *mut SimpleData;
        let simple_data = unsafe { Box::from_raw(raw) };
        files += 1;
        assert_eq!(simple_data.into_record_batch(), expected);
    }
    assert_eq!(2, files, "Expected to have scanned two files");
    Ok(())
}

#[tokio::test]
async fn two_commits() -> Result<(), Box<dyn std::error::Error>> {
    let batch = generate_simple_batch()?;
    let storage = Arc::new(InMemory::new());
    add_commit(
        storage.as_ref(),
        0,
        generate_commit(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
        ]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        1,
        generate_commit(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;
    storage
        .put(&Path::from(PARQUET_FILE1), load_parquet(&batch).into())
        .await?;
    storage
        .put(&Path::from(PARQUET_FILE2), load_parquet(&batch).into())
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine_client = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(&engine_client, None).unwrap();
    let scan = ScanBuilder::new(snapshot).build();

    let mut files = 0;
    let stream = scan.execute(&engine_client)?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        let engine_data = data.raw_data?;
        let raw = Box::into_raw(engine_data) as *mut SimpleData;
        let simple_data = unsafe { Box::from_raw(raw) };
        files += 1;
        assert_eq!(simple_data.into_record_batch(), expected);
    }
    assert_eq!(2, files, "Expected to have scanned two files");

    Ok(())
}

#[tokio::test]
async fn remove_action() -> Result<(), Box<dyn std::error::Error>> {
    let batch = generate_simple_batch()?;
    let storage = Arc::new(InMemory::new());
    add_commit(
        storage.as_ref(),
        0,
        generate_commit(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
        ]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        1,
        generate_commit(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        2,
        generate_commit(vec![TestAction::Remove(PARQUET_FILE2.to_string())]),
    )
    .await?;
    storage
        .put(&Path::from(PARQUET_FILE1), load_parquet(&batch).into())
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine_client = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch];

    let snapshot = table.snapshot(&engine_client, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let stream = scan.execute(&engine_client)?.into_iter().zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        let engine_data = data.raw_data?;
        let raw = Box::into_raw(engine_data) as *mut SimpleData;
        let simple_data = unsafe { Box::from_raw(raw) };
        files += 1;
        assert_eq!(simple_data.into_record_batch(), expected);
    }
    assert_eq!(1, files, "Expected to have scanned one file");
    Ok(())
}

#[tokio::test]
async fn stats() -> Result<(), Box<dyn std::error::Error>> {
    fn generate_commit2(actions: Vec<TestAction>) -> String {
        actions
            .into_iter()
            .map(|test_action| match test_action {
                TestAction::Add(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\": 5}},\"maxValues\":{{\"id\":7}}}}"}}}}"#, action = "add", path = path),
                TestAction::Remove(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true}}}}"#, action = "remove", path = path),
                TestAction::Metadata => METADATA.into(),
            })
            .fold(String::new(), |a, b| a + &b + "\n")
    }
    fn generate_simple_batch2() -> Result<RecordBatch, ArrowError> {
        let ids = Int32Array::from(vec![5, 7]);
        let vals = StringArray::from(vec!["e", "g"]);
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as ArrayRef),
            ("val", Arc::new(vals) as ArrayRef),
        ])
    }

    let batch1 = generate_simple_batch()?;
    let batch2 = generate_simple_batch2()?;
    let storage = Arc::new(InMemory::new());
    // valid commit with min/max (0, 2)
    add_commit(
        storage.as_ref(),
        0,
        generate_commit(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
        ]),
    )
    .await?;
    // storage.add_commit(1, &format!("{}\n", r#"{{"add":{{"path":"doesnotexist","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\": 0}},\"maxValues\":{{\"id\":2}}}}"}}}}"#));
    add_commit(
        storage.as_ref(),
        1,
        generate_commit2(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;

    storage
        .put(&Path::from(PARQUET_FILE1), load_parquet(&batch1).into())
        .await?;

    storage
        .put(&Path::from(PARQUET_FILE2), load_parquet(&batch2).into())
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine_client = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let snapshot = table.snapshot(&engine_client, None)?;

    // The first file has id between 1 and 3; the second has id between 5 and 7. For each operator,
    // we validate the boundary values where we expect the set of matched files to change.
    //
    // NOTE: For cases that match both batch1 and batch2, we list batch2 first because log replay
    // returns most recently added files first.
    use BinaryOperator::{Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual};
    let test_cases: Vec<(_, i64, _)> = vec![
        (Equal, 0, vec![]),
        (Equal, 1, vec![&batch1]),
        (Equal, 3, vec![&batch1]),
        (Equal, 4, vec![]),
        (Equal, 5, vec![&batch2]),
        (Equal, 7, vec![&batch2]),
        (Equal, 8, vec![]),
        (LessThan, 1, vec![]),
        (LessThan, 2, vec![&batch1]),
        (LessThan, 5, vec![&batch1]),
        (LessThan, 6, vec![&batch2, &batch1]),
        (LessThanOrEqual, 0, vec![]),
        (LessThanOrEqual, 1, vec![&batch1]),
        (LessThanOrEqual, 4, vec![&batch1]),
        (LessThanOrEqual, 5, vec![&batch2, &batch1]),
        (GreaterThan, 2, vec![&batch2, &batch1]),
        (GreaterThan, 3, vec![&batch2]),
        (GreaterThan, 6, vec![&batch2]),
        (GreaterThan, 7, vec![]),
        (GreaterThanOrEqual, 3, vec![&batch2, &batch1]),
        (GreaterThanOrEqual, 4, vec![&batch2]),
        (GreaterThanOrEqual, 7, vec![&batch2]),
        (GreaterThanOrEqual, 8, vec![]),
    ];
    for (op, value, expected_batches) in test_cases {
        let predicate = Expression::BinaryOperation {
            op,
            left: Box::new(Expression::column("id")),
            right: Box::new(Expression::literal(value)),
        };
        let scan = ScanBuilder::new(snapshot.clone())
            .with_predicate(predicate)
            .build();

        let expected_files = expected_batches.len();
        let mut files_scanned = 0;
        let stream = scan
            .execute(&engine_client)?
            .into_iter()
            .zip(expected_batches);

        for (batch, expected) in stream {
            let engine_data = batch.raw_data?;
            let raw = Box::into_raw(engine_data) as *mut SimpleData;
            let simple_data = unsafe { Box::from_raw(raw) };
            files_scanned += 1;
            assert_eq!(&simple_data.into_record_batch(), expected);
        }
        assert_eq!(expected_files, files_scanned);
    }
    Ok(())
}
