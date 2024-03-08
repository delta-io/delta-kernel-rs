use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_select::concat::concat_batches;
use deltakernel::client::DefaultTableClient;
use deltakernel::executor::tokio::TokioBackgroundExecutor;
use deltakernel::expressions::{BinaryOperator, Expression};
use deltakernel::scan::ScanBuilder;
use deltakernel::simple_client::data::SimpleData;
use deltakernel::{EngineData, Table};
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

fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
    SimpleData::try_from_engine_data(engine_data)
        .unwrap()
        .into()
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
    let engine_interface = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(&engine_interface, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let mut files = 0;
    let stream = scan
        .execute(&engine_interface)?
        .into_iter()
        .zip(expected_data);

    for (data, expected) in stream {
        let raw_data = data.raw_data?;
        files += 1;
        assert_eq!(into_record_batch(raw_data), expected);
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
    let engine_interface = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(&engine_interface, None).unwrap();
    let scan = ScanBuilder::new(snapshot).build();

    let mut files = 0;
    let stream = scan
        .execute(&engine_interface)?
        .into_iter()
        .zip(expected_data);

    for (data, expected) in stream {
        let raw_data = data.raw_data?;
        files += 1;
        assert_eq!(into_record_batch(raw_data), expected);
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
    let engine_interface = DefaultTableClient::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch];

    let snapshot = table.snapshot(&engine_interface, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let stream = scan
        .execute(&engine_interface)?
        .into_iter()
        .zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        let raw_data = data.raw_data?;
        files += 1;
        assert_eq!(into_record_batch(raw_data), expected);
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
    let engine_interface = DefaultTableClient::new(
        storage.clone(),
        Path::from(""),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let snapshot = table.snapshot(&engine_interface, None)?;

    // The first file has id between 1 and 3; the second has id between 5 and 7. For each operator,
    // we validate the boundary values where we expect the set of matched files to change.
    //
    // NOTE: For cases that match both batch1 and batch2, we list batch2 first because log replay
    // returns most recently added files first.
    use BinaryOperator::{
        Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
    };
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
        (NotEqual, 0, vec![&batch2, &batch1]),
        (NotEqual, 1, vec![&batch2]),
        (NotEqual, 3, vec![&batch2]),
        (NotEqual, 4, vec![&batch2, &batch1]),
        (NotEqual, 5, vec![&batch1]),
        (NotEqual, 7, vec![&batch1]),
        (NotEqual, 8, vec![&batch2, &batch1]),
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
            .execute(&engine_interface)?
            .into_iter()
            .zip(expected_batches);

        for (batch, expected) in stream {
            let raw_data = batch.raw_data?;
            files_scanned += 1;
            assert_eq!(into_record_batch(raw_data), expected.clone());
        }
        assert_eq!(expected_files, files_scanned);
    }
    Ok(())
}

macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

fn read_table_data(path: &str, expected: Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from(path))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let table_client = DefaultTableClient::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table = Table::new(url);
    let snapshot = table.snapshot(&table_client, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let scan_results = scan.execute(&table_client)?;
    let batches: Vec<RecordBatch> = scan_results
        .into_iter()
        .map(|sr| {
            let data = sr.raw_data.unwrap();
            data.into_any().downcast::<SimpleData>().unwrap().into()
        })
        .collect();
    let schema = batches[0].schema();
    let batch = concat_batches(&schema, &batches)?;

    assert_batches_sorted_eq!(&expected, &[batch]);
    Ok(())
}

#[test]
fn data() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+--------+--------+---------+",
        "| letter | number | a_float |",
        "+--------+--------+---------+",
        "|        | 6      | 6.6     |",
        "| a      | 1      | 1.1     |",
        "| a      | 4      | 4.4     |",
        "| b      | 2      | 2.2     |",
        "| c      | 3      | 3.3     |",
        "| e      | 5      | 5.5     |",
        "+--------+--------+---------+",
    ];
    read_table_data("./tests/data/basic_partitioned", expected)?;

    Ok(())
}
