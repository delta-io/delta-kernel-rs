use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use deltakernel::client::DefaultTableClient;
use deltakernel::executor::tokio::TokioBackgroundExecutor;
use deltakernel::expressions::Expression;
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
                TestAction::Add(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"ids\":0}},\"minValues\":{{\"ids\": 0}},\"maxValues\":{{\"ids\":2}}}}"}}}}"#, action = "add", path = path),
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
    let table_client = Arc::new(DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location, table_client);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(None)?;
    let scan = snapshot.scan()?.build();

    let mut files = 0;
    let stream = scan.execute()?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
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
    let table_client = Arc::new(DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location, table_client);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(None).unwrap();
    let scan = snapshot.scan()?.build();

    let mut files = 0;
    let stream = scan.execute()?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
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
    let table_client = Arc::new(DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location, table_client);
    let expected_data = vec![batch];

    let snapshot = table.snapshot(None)?;
    let scan = snapshot.scan()?.build();

    let stream = scan.execute()?.into_iter().zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
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
                TestAction::Add(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"ids\":0}},\"minValues\":{{\"ids\": 3}},\"maxValues\":{{\"ids\":5}}}}"}}}}"#, action = "add", path = path),
                TestAction::Remove(path) => format!(r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true}}}}"#, action = "remove", path = path),
                TestAction::Metadata => METADATA.into(),
            })
            .fold(String::new(), |a, b| a + &b + "\n")
    }
    let batch = generate_simple_batch()?;
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
    // storage.add_commit(1, &format!("{}\n", r#"{{"add":{{"path":"doesnotexist","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"ids\":0}},\"minValues\":{{\"ids\": 0}},\"maxValues\":{{\"ids\":2}}}}"}}}}"#));
    add_commit(
        storage.as_ref(),
        1,
        generate_commit2(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;

    storage
        .put(&Path::from(PARQUET_FILE1), load_parquet(&batch).into())
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let table_client = Arc::new(DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location, table_client);
    let expected_data = vec![batch];

    let snapshot = table.snapshot(None)?;

    let predicate = Expression::LessThan(
        Box::new(Expression::Column(String::from("ids"))),
        Box::new(Expression::Literal(2)),
    );
    let scan = snapshot.scan()?.with_predicate(predicate).build();

    let mut files = 0;
    let stream = scan.execute()?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
    }
    assert_eq!(1, files, "Expected to have scanned one file");
    Ok(())
}
