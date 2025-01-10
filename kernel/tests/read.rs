use std::collections::HashMap;
use std::ops::Not;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::concat::concat_batches;
use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{column_expr, BinaryOperator, Expression, ExpressionRef};
use delta_kernel::scan::state::{transform_to_logical, visit_scan_files, DvInfo, Stats};
use delta_kernel::scan::Scan;
use delta_kernel::schema::{DataType, Schema};
use delta_kernel::{Engine, FileMeta, Table};
use object_store::{memory::InMemory, path::Path, ObjectStore};
use test_utils::{
    actions_to_string, add_commit, generate_batch, generate_simple_batch, into_record_batch,
    record_batch_to_bytes, IntoArray, TestAction, METADATA,
};
use url::Url;

mod common;
use common::{read_scan, to_arrow};

const PARQUET_FILE1: &str = "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";
const PARQUET_FILE2: &str = "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet";

#[tokio::test]
async fn single_commit_two_add_files() -> Result<(), Box<dyn std::error::Error>> {
    let batch = generate_simple_batch()?;
    let storage = Arc::new(InMemory::new());
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
            TestAction::Add(PARQUET_FILE2.to_string()),
        ]),
    )
    .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE2),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;

    let location = Url::parse("memory:///")?;
    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let mut files = 0;
    let stream = scan.execute(engine)?.zip(expected_data);

    for (data, expected) in stream {
        let raw_data = data?.raw_data?;
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
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
        ]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE2),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine = DefaultEngine::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch.clone(), batch];

    let snapshot = table.snapshot(&engine, None).unwrap();
    let scan = snapshot.into_scan_builder().build()?;

    let mut files = 0;
    let stream = scan.execute(Arc::new(engine))?.zip(expected_data);

    for (data, expected) in stream {
        let raw_data = data?.raw_data?;
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
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
        ]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add(PARQUET_FILE2.to_string())]),
    )
    .await?;
    add_commit(
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Remove(PARQUET_FILE2.to_string())]),
    )
    .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine = DefaultEngine::new(
        storage.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table = Table::new(location);
    let expected_data = vec![batch];

    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let stream = scan.execute(Arc::new(engine))?.zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        let raw_data = data?.raw_data?;
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

    let batch1 = generate_simple_batch()?;
    let batch2 = generate_batch(vec![
        ("id", vec![5, 7].into_array()),
        ("val", vec!["e", "g"].into_array()),
    ])?;
    let storage = Arc::new(InMemory::new());
    // valid commit with min/max (0, 2)
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string(vec![
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
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch1).into(),
        )
        .await?;

    storage
        .put(
            &Path::from(PARQUET_FILE2),
            record_batch_to_bytes(&batch2).into(),
        )
        .await?;

    let location = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Path::from(""),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let table = Table::new(location);
    let snapshot = Arc::new(table.snapshot(engine.as_ref(), None)?);

    // The first file has id between 1 and 3; the second has id between 5 and 7. For each operator,
    // we validate the boundary values where we expect the set of matched files to change.
    //
    // NOTE: For cases that match both batch1 and batch2, we list batch2 first because log replay
    // returns most recently added files first.
    use BinaryOperator::{
        Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
    };
    let test_cases: Vec<(_, i32, _)> = vec![
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
        (NotEqual, 1, vec![&batch2, &batch1]),
        (NotEqual, 3, vec![&batch2, &batch1]),
        (NotEqual, 4, vec![&batch2, &batch1]),
        (NotEqual, 5, vec![&batch2, &batch1]),
        (NotEqual, 7, vec![&batch2, &batch1]),
        (NotEqual, 8, vec![&batch2, &batch1]),
    ];
    for (op, value, expected_batches) in test_cases {
        let predicate = Expression::binary(op, column_expr!("id"), value);
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_predicate(Arc::new(predicate.clone()))
            .build()?;

        let expected_files = expected_batches.len();
        let mut files_scanned = 0;
        let stream = scan.execute(engine.clone())?.zip(expected_batches);

        for (batch, expected) in stream {
            let raw_data = batch?.raw_data?;
            files_scanned += 1;
            assert_eq!(into_record_batch(raw_data), expected.clone());
        }
        assert_eq!(expected_files, files_scanned, "{predicate:?}");
    }
    Ok(())
}

fn read_with_execute(
    engine: Arc<dyn Engine>,
    scan: &Scan,
    expected: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let result_schema: ArrowSchemaRef = Arc::new(scan.schema().as_ref().try_into()?);
    let batches = read_scan(scan, engine)?;

    if expected.is_empty() {
        assert_eq!(batches.len(), 0);
    } else {
        let batch = concat_batches(&result_schema, &batches)?;
        assert_batches_sorted_eq!(expected, &[batch]);
    }
    Ok(())
}

struct ScanFile {
    path: String,
    size: i64,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
}

fn scan_data_callback(
    batches: &mut Vec<ScanFile>,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    _: HashMap<String, String>,
) {
    batches.push(ScanFile {
        path: path.to_string(),
        size,
        dv_info,
        transform,
    });
}

fn read_with_scan_data(
    location: &Url,
    engine: &dyn Engine,
    scan: &Scan,
    expected: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let global_state = scan.global_scan_state();
    let result_schema: ArrowSchemaRef = Arc::new(scan.schema().as_ref().try_into()?);
    let scan_data = scan.scan_data(engine)?;
    let mut scan_files = vec![];
    for data in scan_data {
        let (data, vec, transforms) = data?;
        scan_files = visit_scan_files(
            data.as_ref(),
            &vec,
            &transforms,
            scan_files,
            scan_data_callback,
        )?;
    }

    let mut batches = vec![];
    for scan_file in scan_files.into_iter() {
        let file_path = location.join(&scan_file.path)?;
        let mut selection_vector = scan_file
            .dv_info
            .get_selection_vector(engine, location)
            .unwrap();
        let meta = FileMeta {
            last_modified: 0,
            size: scan_file.size as usize,
            location: file_path,
        };
        let read_results = engine
            .get_parquet_handler()
            .read_parquet_files(
                &[meta],
                global_state.physical_schema.clone(),
                scan.physical_predicate().clone(),
            )
            .unwrap();

        for read_result in read_results {
            let read_result = read_result.unwrap();
            let len = read_result.len();
            // to transform the physical data into the correct logical form
            let logical = transform_to_logical(
                engine,
                read_result,
                &global_state.physical_schema,
                &global_state.logical_schema,
                &scan_file.transform,
            )
            .unwrap();
            let record_batch = to_arrow(logical).unwrap();
            let rest = split_vector(selection_vector.as_mut(), len, Some(true));
            let batch = if let Some(mask) = selection_vector.clone() {
                // apply the selection vector
                filter_record_batch(&record_batch, &mask.into()).unwrap()
            } else {
                record_batch
            };
            selection_vector = rest;
            batches.push(batch);
        }
    }

    if expected.is_empty() {
        assert_eq!(batches.len(), 0);
    } else {
        let batch = concat_batches(&result_schema, &batches)?;
        assert_batches_sorted_eq!(expected, &[batch]);
    }
    Ok(())
}

fn read_table_data(
    path: &str,
    select_cols: Option<&[&str]>,
    predicate: Option<Expression>,
    mut expected: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from(path))?;
    let predicate = predicate.map(Arc::new);
    let url = url::Url::from_directory_path(path).unwrap();
    let default_engine = DefaultEngine::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;
    let sync_engine = delta_kernel::engine::sync::SyncEngine::new();

    let engines: Vec<Arc<dyn Engine>> = vec![Arc::new(sync_engine), Arc::new(default_engine)];
    for engine in engines {
        let table = Table::new(url.clone());
        let snapshot = table.snapshot(engine.as_ref(), None)?;

        let read_schema = select_cols.map(|select_cols| {
            let table_schema = snapshot.schema();
            let selected_fields = select_cols
                .iter()
                .map(|col| table_schema.field(col).cloned().unwrap());
            Arc::new(Schema::new(selected_fields))
        });
        println!("Read {url:?} with schema {read_schema:#?} and predicate {predicate:#?}");
        let scan = snapshot
            .into_scan_builder()
            .with_schema_opt(read_schema)
            .with_predicate(predicate.clone())
            .build()?;

        sort_lines!(expected);
        read_with_scan_data(table.location(), engine.as_ref(), &scan, &expected)?;
        read_with_execute(engine, &scan, &expected)?;
    }
    Ok(())
}

// util to take a Vec<&str> and call read_table_data with Vec<String>
fn read_table_data_str(
    path: &str,
    select_cols: Option<&[&str]>,
    predicate: Option<Expression>,
    expected: Vec<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    read_table_data(
        path,
        select_cols,
        predicate,
        expected.into_iter().map(String::from).collect(),
    )
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
    read_table_data_str("./tests/data/basic_partitioned", None, None, expected)?;

    Ok(())
}

#[test]
fn column_ordering() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+---------+--------+--------+",
        "| a_float | letter | number |",
        "+---------+--------+--------+",
        "| 6.6     |        | 6      |",
        "| 4.4     | a      | 4      |",
        "| 5.5     | e      | 5      |",
        "| 1.1     | a      | 1      |",
        "| 2.2     | b      | 2      |",
        "| 3.3     | c      | 3      |",
        "+---------+--------+--------+",
    ];
    read_table_data_str(
        "./tests/data/basic_partitioned",
        Some(&["a_float", "letter", "number"]),
        None,
        expected,
    )?;

    Ok(())
}

#[test]
fn column_ordering_and_projection() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+---------+--------+",
        "| a_float | number |",
        "+---------+--------+",
        "| 6.6     | 6      |",
        "| 4.4     | 4      |",
        "| 5.5     | 5      |",
        "| 1.1     | 1      |",
        "| 2.2     | 2      |",
        "| 3.3     | 3      |",
        "+---------+--------+",
    ];
    read_table_data_str(
        "./tests/data/basic_partitioned",
        Some(&["a_float", "number"]),
        None,
        expected,
    )?;

    Ok(())
}

// get the basic_partitioned table for a set of expected numbers
fn table_for_numbers(nums: Vec<u32>) -> Vec<String> {
    let mut res: Vec<String> = vec![
        "+---------+--------+",
        "| a_float | number |",
        "+---------+--------+",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    for num in nums.iter() {
        res.push(format!("| {num}.{num}     | {num}      |"));
    }
    res.push("+---------+--------+".to_string());
    res
}

#[test]
fn predicate_on_number() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            column_expr!("number").lt(4i64),
            table_for_numbers(vec![1, 2, 3]),
        ),
        (
            column_expr!("number").le(4i64),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            column_expr!("number").gt(4i64),
            table_for_numbers(vec![5, 6]),
        ),
        (
            column_expr!("number").ge(4i64),
            table_for_numbers(vec![4, 5, 6]),
        ),
        (column_expr!("number").eq(4i64), table_for_numbers(vec![4])),
        (
            column_expr!("number").ne(4i64),
            table_for_numbers(vec![1, 2, 3, 5, 6]),
        ),
    ];

    for (expr, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(expr),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn predicate_on_number_not() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            Expression::not(column_expr!("number").lt(4i64)),
            table_for_numbers(vec![4, 5, 6]),
        ),
        (
            Expression::not(column_expr!("number").le(4i64)),
            table_for_numbers(vec![5, 6]),
        ),
        (
            Expression::not(column_expr!("number").gt(4i64)),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            Expression::not(column_expr!("number").ge(4i64)),
            table_for_numbers(vec![1, 2, 3]),
        ),
        (
            Expression::not(column_expr!("number").eq(4i64)),
            table_for_numbers(vec![1, 2, 3, 5, 6]),
        ),
        (
            Expression::not(column_expr!("number").ne(4i64)),
            table_for_numbers(vec![4]),
        ),
    ];
    for (expr, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(expr),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn predicate_on_number_with_not_null() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+---------+--------+",
        "| a_float | number |",
        "+---------+--------+",
        "| 1.1     | 1      |",
        "| 2.2     | 2      |",
        "+---------+--------+",
    ];
    read_table_data_str(
        "./tests/data/basic_partitioned",
        Some(&["a_float", "number"]),
        Some(Expression::and(
            column_expr!("number").is_not_null(),
            column_expr!("number").lt(Expression::literal(3i64)),
        )),
        expected,
    )?;
    Ok(())
}

#[test]
fn predicate_null() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![]; // number is never null
    read_table_data_str(
        "./tests/data/basic_partitioned",
        Some(&["a_float", "number"]),
        Some(column_expr!("number").is_null()),
        expected,
    )?;
    Ok(())
}

#[test]
fn mixed_null() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+------+--------------+",
        "| part | n            |",
        "+------+--------------+",
        "| 0    |              |",
        "| 0    |              |",
        "| 0    |              |",
        "| 0    |              |",
        "| 0    |              |",
        "| 2    |              |",
        "| 2    | non-null-mix |",
        "| 2    |              |",
        "| 2    | non-null-mix |",
        "| 2    |              |",
        "+------+--------------+",
    ];
    read_table_data_str(
        "./tests/data/mixed-nulls",
        Some(&["part", "n"]),
        Some(column_expr!("n").is_null()),
        expected,
    )?;
    Ok(())
}

#[test]
fn mixed_not_null() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+------+--------------+",
        "| part | n            |",
        "+------+--------------+",
        "| 1    | non-null     |",
        "| 1    | non-null     |",
        "| 1    | non-null     |",
        "| 1    | non-null     |",
        "| 1    | non-null     |",
        "| 2    |              |",
        "| 2    |              |",
        "| 2    |              |",
        "| 2    | non-null-mix |",
        "| 2    | non-null-mix |",
        "+------+--------------+",
    ];
    read_table_data_str(
        "./tests/data/mixed-nulls",
        Some(&["part", "n"]),
        Some(column_expr!("n").is_not_null()),
        expected,
    )?;
    Ok(())
}

#[test]
fn and_or_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            column_expr!("number")
                .gt(4i64)
                .and(column_expr!("a_float").gt(5.5)),
            table_for_numbers(vec![6]),
        ),
        (
            column_expr!("number")
                .gt(4i64)
                .and(Expression::not(column_expr!("a_float").gt(5.5))),
            table_for_numbers(vec![5]),
        ),
        (
            column_expr!("number")
                .gt(4i64)
                .or(column_expr!("a_float").gt(5.5)),
            table_for_numbers(vec![5, 6]),
        ),
        (
            column_expr!("number")
                .gt(4i64)
                .or(Expression::not(column_expr!("a_float").gt(5.5))),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
    ];
    for (expr, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(expr),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn not_and_or_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            Expression::not(
                column_expr!("number")
                    .gt(4i64)
                    .and(column_expr!("a_float").gt(5.5)),
            ),
            table_for_numbers(vec![1, 2, 3, 4, 5]),
        ),
        (
            Expression::not(
                column_expr!("number")
                    .gt(4i64)
                    .and(Expression::not(column_expr!("a_float").gt(5.5))),
            ),
            table_for_numbers(vec![1, 2, 3, 4, 6]),
        ),
        (
            Expression::not(
                column_expr!("number")
                    .gt(4i64)
                    .or(column_expr!("a_float").gt(5.5)),
            ),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            Expression::not(
                column_expr!("number")
                    .gt(4i64)
                    .or(Expression::not(column_expr!("a_float").gt(5.5))),
            ),
            vec![],
        ),
    ];
    for (expr, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(expr),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn invalid_skips_none_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let empty_struct = Expression::struct_from(vec![]);
    let cases = vec![
        (Expression::literal(false), table_for_numbers(vec![])),
        (
            Expression::and(column_expr!("number"), false),
            table_for_numbers(vec![]),
        ),
        (
            Expression::literal(true),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Expression::literal(3i64),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            column_expr!("number").distinct(3i64),
            table_for_numbers(vec![1, 2, 4, 5, 6]),
        ),
        (
            column_expr!("number").distinct(Expression::null_literal(DataType::LONG)),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Expression::not(column_expr!("number").distinct(3i64)),
            table_for_numbers(vec![3]),
        ),
        (
            Expression::not(
                column_expr!("number").distinct(Expression::null_literal(DataType::LONG)),
            ),
            table_for_numbers(vec![]),
        ),
        (
            column_expr!("number").gt(empty_struct.clone()),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Expression::not(column_expr!("number").gt(empty_struct.clone())),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
    ];
    for (expr, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(expr),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn with_predicate_and_removes() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 1     |",
        "| 2     |",
        "| 3     |",
        "| 4     |",
        "| 5     |",
        "| 6     |",
        "| 7     |",
        "| 8     |",
        "+-------+",
    ];
    read_table_data_str(
        "./tests/data/table-with-dv-small/",
        None,
        Some(Expression::gt(column_expr!("value"), 3)),
        expected,
    )?;
    Ok(())
}

#[test]
fn short_dv() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----+-------+--------------------------+---------------------+",
        "| id | value | timestamp                | rand                |",
        "+----+-------+--------------------------+---------------------+",
        "| 3  | 3     | 2023-05-31T18:58:33.633Z | 0.7918174793484931  |",
        "| 4  | 4     | 2023-05-31T18:58:33.633Z | 0.9281049271981882  |",
        "| 5  | 5     | 2023-05-31T18:58:33.633Z | 0.27796520310701633 |",
        "| 6  | 6     | 2023-05-31T18:58:33.633Z | 0.15263801464228832 |",
        "| 7  | 7     | 2023-05-31T18:58:33.633Z | 0.1981143710215575  |",
        "| 8  | 8     | 2023-05-31T18:58:33.633Z | 0.3069439236599195  |",
        "| 9  | 9     | 2023-05-31T18:58:33.633Z | 0.5175919190815845  |",
        "+----+-------+--------------------------+---------------------+",
    ];
    read_table_data_str("./tests/data/with-short-dv/", None, None, expected)?;
    Ok(())
}

#[test]
fn basic_decimal() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----------------+---------+--------------+------------------------+",
        "| part           | col1    | col2         | col3                   |",
        "+----------------+---------+--------------+------------------------+",
        "| -2342342.23423 | -999.99 | -99999.99999 | -9999999999.9999999999 |",
        "| 0.00004        | 0.00    | 0.00000      | 0.0000000000           |",
        "| 234.00000      | 1.00    | 2.00000      | 3.0000000000           |",
        "| 2342222.23454  | 111.11  | 22222.22222  | 3333333333.3333333333  |",
        "+----------------+---------+--------------+------------------------+",
    ];
    read_table_data_str("./tests/data/basic-decimal-table/", None, None, expected)?;
    Ok(())
}

#[test]
fn timestamp_ntz() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----+----------------------------+----------------------------+",
        "| id | tsNtz                      | tsNtzPartition             |",
        "+----+----------------------------+----------------------------+",
        "| 0  | 2021-11-18T02:30:00.123456 | 2021-11-18T02:30:00.123456 |",
        "| 1  | 2013-07-05T17:01:00.123456 | 2021-11-18T02:30:00.123456 |",
        "| 2  |                            | 2021-11-18T02:30:00.123456 |",
        "| 3  | 2021-11-18T02:30:00.123456 | 2013-07-05T17:01:00.123456 |",
        "| 4  | 2013-07-05T17:01:00.123456 | 2013-07-05T17:01:00.123456 |",
        "| 5  |                            | 2013-07-05T17:01:00.123456 |",
        "| 6  | 2021-11-18T02:30:00.123456 |                            |",
        "| 7  | 2013-07-05T17:01:00.123456 |                            |",
        "| 8  |                            |                            |",
        "+----+----------------------------+----------------------------+",
    ];
    read_table_data_str(
        "./tests/data/data-reader-timestamp_ntz/",
        None,
        None,
        expected,
    )?;
    Ok(())
}

#[test]
fn type_widening_basic() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+---------------------+---------------------+--------------------+----------------+----------------+----------------+----------------------------+",
        "| byte_long           | int_long            | float_double       | byte_double    | short_double   | int_double     | date_timestamp_ntz         |",
        "+---------------------+---------------------+--------------------+----------------+----------------+----------------+----------------------------+",
        "| 1                   | 2                   | 3.4000000953674316 | 5.0            | 6.0            | 7.0            | 2024-09-09T00:00:00        |",
        "| 9223372036854775807 | 9223372036854775807 | 1.234567890123     | 1.234567890123 | 1.234567890123 | 1.234567890123 | 2024-09-09T12:34:56.123456 |",
        "+---------------------+---------------------+--------------------+----------------+----------------+----------------+----------------------------+",
   ];
    let select_cols: Option<&[&str]> = Some(&[
        "byte_long",
        "int_long",
        "float_double",
        "byte_double",
        "short_double",
        "int_double",
        "date_timestamp_ntz",
    ]);

    read_table_data_str("./tests/data/type-widening/", select_cols, None, expected)
}

#[test]
fn type_widening_decimal() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----------------------------+-------------------------------+--------------+---------------+--------------+----------------------+",
        "| decimal_decimal_same_scale | decimal_decimal_greater_scale | byte_decimal | short_decimal | int_decimal  | long_decimal         |",
        "+----------------------------+-------------------------------+--------------+---------------+--------------+----------------------+",
        "| 123.45                     | 67.89000                      | 1.0          | 2.0           | 3.0          | 4.0                  |",
        "| 12345678901234.56          | 12345678901.23456             | 123.4        | 12345.6       | 1234567890.1 | 123456789012345678.9 |",
        "+----------------------------+-------------------------------+--------------+---------------+--------------+----------------------+",
    ];
    let select_cols: Option<&[&str]> = Some(&[
        "decimal_decimal_same_scale",
        "decimal_decimal_greater_scale",
        "byte_decimal",
        "short_decimal",
        "int_decimal",
        "long_decimal",
    ]);
    read_table_data_str("./tests/data/type-widening/", select_cols, None, expected)
}

// Verify that predicates over invalid/missing columns do not cause skipping.
#[test]
fn predicate_references_invalid_missing_column() -> Result<(), Box<dyn std::error::Error>> {
    // Attempted skipping over a logically valid but physically missing column. We should be able to
    // skip the data file because the missing column is inferred to be all-null.
    //
    // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- currently disabled.
    //
    //let expected = vec![
    //    "+--------+",
    //    "| chrono |",
    //    "+--------+",
    //    "+--------+",
    //];
    let columns = &["chrono", "missing"];
    let expected = vec![
        "+-------------------------------------------------------------------------------------------+---------+",
        "| chrono                                                                                    | missing |",
        "+-------------------------------------------------------------------------------------------+---------+",
        "| {date32: 1971-01-01, timestamp: 1970-02-01T08:00:00Z, timestamp_ntz: 1970-01-02T00:00:00} |         |",
        "| {date32: 1971-01-02, timestamp: 1970-02-01T09:00:00Z, timestamp_ntz: 1970-01-02T00:01:00} |         |",
        "| {date32: 1971-01-03, timestamp: 1970-02-01T10:00:00Z, timestamp_ntz: 1970-01-02T00:02:00} |         |",
        "| {date32: 1971-01-04, timestamp: 1970-02-01T11:00:00Z, timestamp_ntz: 1970-01-02T00:03:00} |         |",
        "| {date32: 1971-01-05, timestamp: 1970-02-01T12:00:00Z, timestamp_ntz: 1970-01-02T00:04:00} |         |",
        "+-------------------------------------------------------------------------------------------+---------+",
    ];
    let predicate = column_expr!("missing").lt(10i64);
    read_table_data_str(
        "./tests/data/parquet_row_group_skipping/",
        Some(columns),
        Some(predicate),
        expected,
    )?;

    // Attempted skipping over an invalid (logically missing) column. Ideally this should throw a
    // query error, but at a minimum it should not cause incorrect data skipping.
    let expected = vec![
        "+-------------------------------------------------------------------------------------------+",
        "| chrono                                                                                    |",
        "+-------------------------------------------------------------------------------------------+",
        "| {date32: 1971-01-01, timestamp: 1970-02-01T08:00:00Z, timestamp_ntz: 1970-01-02T00:00:00} |",
        "| {date32: 1971-01-02, timestamp: 1970-02-01T09:00:00Z, timestamp_ntz: 1970-01-02T00:01:00} |",
        "| {date32: 1971-01-03, timestamp: 1970-02-01T10:00:00Z, timestamp_ntz: 1970-01-02T00:02:00} |",
        "| {date32: 1971-01-04, timestamp: 1970-02-01T11:00:00Z, timestamp_ntz: 1970-01-02T00:03:00} |",
        "| {date32: 1971-01-05, timestamp: 1970-02-01T12:00:00Z, timestamp_ntz: 1970-01-02T00:04:00} |",
        "+-------------------------------------------------------------------------------------------+",
    ];
    let predicate = column_expr!("invalid").lt(10);
    read_table_data_str(
        "./tests/data/parquet_row_group_skipping/",
        Some(columns),
        Some(predicate),
        expected,
    )
    .expect_err("unknown column");
    Ok(())
}

// Note: This test is disabled for windows because it creates a directory with name
// `time=1971-07-22T03:06:40.000000Z`. This is disallowed in windows due to having a `:` in
// the name.
#[cfg(not(windows))]
#[test]
fn timestamp_partitioned_table() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----+-----+---+----------------------+",
        "| id | x   | s | time                 |",
        "+----+-----+---+----------------------+",
        "| 1  | 0.5 |   | 1971-07-22T03:06:40Z |",
        "+----+-----+---+----------------------+",
    ];
    let test_name = "timestamp-partitioned-table";
    let test_dir = common::load_test_data("./tests/data", test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    read_table_data_str(test_path.to_str().unwrap(), None, None, expected)
}
