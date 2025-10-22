use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::arrow::array::{AsArray as _, BooleanArray};
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::arrow::datatypes::{Int64Type, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{
    column_expr, column_pred, Expression as Expr, ExpressionRef, Predicate as Pred,
};
use delta_kernel::parquet::file::properties::{EnabledStatistics, WriterProperties};
use delta_kernel::scan::state::{transform_to_logical, DvInfo, Stats};
use delta_kernel::scan::{Scan, ScanResult};
use delta_kernel::schema::{DataType, MetadataColumnSpec, Schema, StructField, StructType};
use delta_kernel::{Engine, FileMeta, Snapshot};

use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use test_utils::{
    actions_to_string, add_commit, generate_batch, generate_simple_batch, into_record_batch,
    load_test_data, read_scan, record_batch_to_bytes, record_batch_to_bytes_with_props, to_arrow,
    IntoArray, TestAction, METADATA,
};
use url::Url;

mod common;

const PARQUET_FILE1: &str = "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";
const PARQUET_FILE2: &str = "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet";
const PARQUET_FILE3: &str = "part-00002-c506e79a-0bf8-4e2b-a42b-9731b2e490ff-c000.snappy.parquet";

/// Helper function to extract filtered data from a scan result, respecting row masks
fn extract_record_batch(
    scan_result: ScanResult,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mask = scan_result.full_mask();
    let record_batch = into_record_batch(scan_result.raw_data?);

    if let Some(mask) = mask {
        Ok(filter_record_batch(
            &record_batch,
            &BooleanArray::from(mask),
        )?)
    } else {
        Ok(record_batch)
    }
}

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
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let expected_data = vec![batch.clone(), batch];

    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

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
    let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));

    let expected_data = vec![batch.clone(), batch];

    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;

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
    let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));

    let expected_data = vec![batch];

    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;

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
        Arc::new(TokioBackgroundExecutor::new()),
    ));
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;

    // The first file has id between 1 and 3; the second has id between 5 and 7. For each operator,
    // we validate the boundary values where we expect the set of matched files to change.
    //
    // NOTE: For cases that match both batch1 and batch2, we list batch2 first because log replay
    // returns most recently added files first.
    #[allow(clippy::type_complexity)] // otherwise it's even more complex because no `_`
    let test_cases: Vec<(fn(Expr, Expr) -> _, _, _)> = vec![
        (Pred::eq, 0i32, vec![]),
        (Pred::eq, 1, vec![&batch1]),
        (Pred::eq, 3, vec![&batch1]),
        (Pred::eq, 4, vec![]),
        (Pred::eq, 5, vec![&batch2]),
        (Pred::eq, 7, vec![&batch2]),
        (Pred::eq, 8, vec![]),
        (Pred::lt, 1, vec![]),
        (Pred::lt, 2, vec![&batch1]),
        (Pred::lt, 5, vec![&batch1]),
        (Pred::lt, 6, vec![&batch2, &batch1]),
        (Pred::le, 0, vec![]),
        (Pred::le, 1, vec![&batch1]),
        (Pred::le, 4, vec![&batch1]),
        (Pred::le, 5, vec![&batch2, &batch1]),
        (Pred::gt, 2, vec![&batch2, &batch1]),
        (Pred::gt, 3, vec![&batch2]),
        (Pred::gt, 6, vec![&batch2]),
        (Pred::gt, 7, vec![]),
        (Pred::ge, 3, vec![&batch2, &batch1]),
        (Pred::ge, 4, vec![&batch2]),
        (Pred::ge, 7, vec![&batch2]),
        (Pred::ge, 8, vec![]),
        (Pred::ne, 0, vec![&batch2, &batch1]),
        (Pred::ne, 1, vec![&batch2, &batch1]),
        (Pred::ne, 3, vec![&batch2, &batch1]),
        (Pred::ne, 4, vec![&batch2, &batch1]),
        (Pred::ne, 5, vec![&batch2, &batch1]),
        (Pred::ne, 7, vec![&batch2, &batch1]),
        (Pred::ne, 8, vec![&batch2, &batch1]),
    ];
    for (pred_fn, value, expected_batches) in test_cases {
        let predicate = pred_fn(column_expr!("id"), Expr::literal(value));
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
    let result_schema = Arc::new(ArrowSchema::try_from_kernel(
        scan.logical_schema().as_ref(),
    )?);
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

fn scan_metadata_callback(
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

fn read_with_scan_metadata(
    location: &Url,
    engine: &dyn Engine,
    scan: &Scan,
    expected: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let result_schema = Arc::new(ArrowSchema::try_from_kernel(
        scan.logical_schema().as_ref(),
    )?);
    let scan_metadata = scan.scan_metadata(engine)?;
    let mut scan_files = vec![];
    for res in scan_metadata {
        let scan_metadata = res?;
        scan_files = scan_metadata.visit_scan_files(scan_files, scan_metadata_callback)?;
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
            size: scan_file.size.try_into().unwrap(),
            location: file_path,
        };
        let read_results = engine
            .parquet_handler()
            .read_parquet_files(
                &[meta],
                scan.physical_schema().clone(),
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
                scan.physical_schema(),
                scan.logical_schema(),
                scan_file.transform.clone(),
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
    predicate: Option<Pred>,
    mut expected: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from(path))?;
    let predicate = predicate.map(Arc::new);
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(DefaultEngine::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

    let read_schema = select_cols.map(|select_cols| {
        let table_schema = snapshot.schema();
        let selected_fields = select_cols
            .iter()
            .map(|col| table_schema.field(col).cloned().unwrap());
        Arc::new(Schema::new_unchecked(selected_fields))
    });
    println!("Read {url:?} with schema {read_schema:#?} and predicate {predicate:#?}");
    let scan = snapshot
        .scan_builder()
        .with_schema_opt(read_schema)
        .with_predicate(predicate.clone())
        .build()?;

    sort_lines!(expected);
    read_with_scan_metadata(&url, engine.as_ref(), &scan, &expected)?;
    read_with_execute(engine, &scan, &expected)?;
    Ok(())
}

// util to take a Vec<&str> and call read_table_data with Vec<String>
fn read_table_data_str(
    path: &str,
    select_cols: Option<&[&str]>,
    predicate: Option<Pred>,
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

// get the basic_partitioned table for a set of expected letters
fn table_for_letters(letters: &[char]) -> Vec<String> {
    let mut res: Vec<String> = vec![
        "+--------+--------+",
        "| letter | number |",
        "+--------+--------+",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let rows = vec![(1, 'a'), (2, 'b'), (3, 'c'), (4, 'a'), (5, 'e')];
    for (num, letter) in rows {
        if letters.contains(&letter) {
            res.push(format!("| {letter}      | {num}      |"));
        }
    }
    res.push("+--------+--------+".to_string());
    res
}

#[test]
fn predicate_on_number() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            column_expr!("number").lt(Expr::literal(4i64)),
            table_for_numbers(vec![1, 2, 3]),
        ),
        (
            column_expr!("number").le(Expr::literal(4i64)),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            column_expr!("number").gt(Expr::literal(4i64)),
            table_for_numbers(vec![5, 6]),
        ),
        (
            column_expr!("number").ge(Expr::literal(4i64)),
            table_for_numbers(vec![4, 5, 6]),
        ),
        (
            column_expr!("number").eq(Expr::literal(4i64)),
            table_for_numbers(vec![4]),
        ),
        (
            column_expr!("number").ne(Expr::literal(4i64)),
            table_for_numbers(vec![1, 2, 3, 5, 6]),
        ),
    ];

    for (pred, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(pred),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn predicate_on_letter() -> Result<(), Box<dyn std::error::Error>> {
    // Test basic column pruning. Note that the actual predicate machinery is already well-tested,
    // so we're just testing wiring here.
    let null_row_table: Vec<String> = vec![
        "+--------+--------+",
        "| letter | number |",
        "+--------+--------+",
        "|        | 6      |",
        "+--------+--------+",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let cases = vec![
        (column_expr!("letter").is_null(), null_row_table),
        (
            column_expr!("letter").is_not_null(),
            table_for_letters(&['a', 'b', 'c', 'e']),
        ),
        (
            column_expr!("letter").lt(Expr::literal("c")),
            table_for_letters(&['a', 'b']),
        ),
        (
            column_expr!("letter").le(Expr::literal("c")),
            table_for_letters(&['a', 'b', 'c']),
        ),
        (
            column_expr!("letter").gt(Expr::literal("c")),
            table_for_letters(&['e']),
        ),
        (
            column_expr!("letter").ge(Expr::literal("c")),
            table_for_letters(&['c', 'e']),
        ),
        (
            column_expr!("letter").eq(Expr::literal("c")),
            table_for_letters(&['c']),
        ),
        (
            column_expr!("letter").ne(Expr::literal("c")),
            table_for_letters(&['a', 'b', 'e']),
        ),
    ];

    for (pred, expected) in cases {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["letter", "number"]),
            Some(pred),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn predicate_on_letter_and_number() -> Result<(), Box<dyn std::error::Error>> {
    // Partition skipping and file skipping are currently implemented separately. Mixing them in an
    // AND clause will evaulate each separately, but mixing them in an OR clause disables both.
    let full_table: Vec<String> = vec![
        "+--------+--------+",
        "| letter | number |",
        "+--------+--------+",
        "|        | 6      |",
        "| a      | 1      |",
        "| a      | 4      |",
        "| b      | 2      |",
        "| c      | 3      |",
        "| e      | 5      |",
        "+--------+--------+",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let cases = vec![
        (
            Pred::or(
                // No pruning power
                column_expr!("letter").gt(Expr::literal("a")),
                column_expr!("number").gt(Expr::literal(3i64)),
            ),
            full_table,
        ),
        (
            Pred::and(
                column_expr!("letter").gt(Expr::literal("a")), // numbers 2, 3, 5
                column_expr!("number").gt(Expr::literal(3i64)), // letters a, e
            ),
            table_for_letters(&['e']),
        ),
        (
            Pred::and(
                column_expr!("letter").gt(Expr::literal("a")), // numbers 2, 3, 5
                Pred::or(
                    // No pruning power
                    column_expr!("letter").eq(Expr::literal("c")),
                    column_expr!("number").eq(Expr::literal(3i64)),
                ),
            ),
            table_for_letters(&['b', 'c', 'e']),
        ),
    ];

    for (pred, expected) in cases {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["letter", "number"]),
            Some(pred),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn predicate_on_number_not() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            Pred::not(column_expr!("number").lt(Expr::literal(4i64))),
            table_for_numbers(vec![4, 5, 6]),
        ),
        (
            Pred::not(column_expr!("number").le(Expr::literal(4i64))),
            table_for_numbers(vec![5, 6]),
        ),
        (
            Pred::not(column_expr!("number").gt(Expr::literal(4i64))),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            Pred::not(column_expr!("number").ge(Expr::literal(4i64))),
            table_for_numbers(vec![1, 2, 3]),
        ),
        (
            Pred::not(column_expr!("number").eq(Expr::literal(4i64))),
            table_for_numbers(vec![1, 2, 3, 5, 6]),
        ),
        (
            Pred::not(column_expr!("number").ne(Expr::literal(4i64))),
            table_for_numbers(vec![4]),
        ),
    ];
    for (pred, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(pred),
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
        Some(Pred::and(
            column_expr!("number").is_not_null(),
            column_expr!("number").lt(Expr::literal(3i64)),
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
            Pred::and(
                column_expr!("number").gt(Expr::literal(4i64)),
                column_expr!("a_float").gt(Expr::literal(5.5)),
            ),
            table_for_numbers(vec![6]),
        ),
        (
            Pred::and(
                column_expr!("number").gt(Expr::literal(4i64)),
                Pred::not(column_expr!("a_float").gt(Expr::literal(5.5))),
            ),
            table_for_numbers(vec![5]),
        ),
        (
            Pred::or(
                column_expr!("number").gt(Expr::literal(4i64)),
                column_expr!("a_float").gt(Expr::literal(5.5)),
            ),
            table_for_numbers(vec![5, 6]),
        ),
        (
            Pred::or(
                column_expr!("number").gt(Expr::literal(4i64)),
                Pred::not(column_expr!("a_float").gt(Expr::literal(5.5))),
            ),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
    ];
    for (pred, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(pred),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn not_and_or_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (
            Pred::not(Pred::and(
                column_expr!("number").gt(Expr::literal(4i64)),
                column_expr!("a_float").gt(Expr::literal(5.5)),
            )),
            table_for_numbers(vec![1, 2, 3, 4, 5]),
        ),
        (
            Pred::not(Pred::and(
                column_expr!("number").gt(Expr::literal(4i64)),
                Pred::not(column_expr!("a_float").gt(Expr::literal(5.5))),
            )),
            table_for_numbers(vec![1, 2, 3, 4, 6]),
        ),
        (
            Pred::not(Pred::or(
                column_expr!("number").gt(Expr::literal(4i64)),
                column_expr!("a_float").gt(Expr::literal(5.5)),
            )),
            table_for_numbers(vec![1, 2, 3, 4]),
        ),
        (
            Pred::not(Pred::or(
                column_expr!("number").gt(Expr::literal(4i64)),
                Pred::not(column_expr!("a_float").gt(Expr::literal(5.5))),
            )),
            vec![],
        ),
    ];
    for (pred, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(pred),
            expected,
        )?;
    }
    Ok(())
}

#[test]
fn invalid_skips_none_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let empty_struct = Expr::struct_from(Vec::<ExpressionRef>::new());
    let cases = vec![
        (Pred::literal(false), table_for_numbers(vec![])),
        (
            Pred::and(column_pred!("number"), Pred::literal(false)),
            table_for_numbers(vec![]),
        ),
        (
            Pred::literal(true),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Pred::from_expr(Expr::literal(3i64)),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            column_expr!("number").distinct(Expr::literal(3i64)),
            table_for_numbers(vec![1, 2, 4, 5, 6]),
        ),
        (
            column_expr!("number").distinct(Expr::null_literal(DataType::LONG)),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Pred::not(column_expr!("number").distinct(Expr::literal(3i64))),
            table_for_numbers(vec![3]),
        ),
        (
            Pred::not(column_expr!("number").distinct(Expr::null_literal(DataType::LONG))),
            table_for_numbers(vec![]),
        ),
        (
            column_expr!("number").gt(empty_struct.clone()),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            Pred::not(column_expr!("number").gt(empty_struct.clone())),
            table_for_numbers(vec![1, 2, 3, 4, 5, 6]),
        ),
    ];
    for (pred, expected) in cases.into_iter() {
        read_table_data(
            "./tests/data/basic_partitioned",
            Some(&["a_float", "number"]),
            Some(pred),
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
        Some(Pred::gt(column_expr!("value"), Expr::literal(3))),
        expected,
    )?;
    Ok(())
}

#[tokio::test]
async fn predicate_on_non_nullable_partition_column() -> Result<(), Box<dyn std::error::Error>> {
    // Test for https://github.com/delta-io/delta-kernel-rs/issues/698
    let batch = generate_batch(vec![("val", vec!["a", "b", "c"].into_array())])?;

    let storage = Arc::new(InMemory::new());
    let actions = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string(),
        r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[\"id\"]"},"isBlindAppend":true}}"#.to_string(),
        r#"{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":["id"],"configuration":{},"createdTime":1587968585495}}"#.to_string(),
        format!(r#"{{"add":{{"path":"id=1/{PARQUET_FILE1}","partitionValues":{{"id":"1"}},"size":0,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":3,\"nullCount\":{{\"val\":0}},\"minValues\":{{\"val\":\"a\"}},\"maxValues\":{{\"val\":\"c\"}}}}"}}}}"#),
        format!(r#"{{"add":{{"path":"id=2/{PARQUET_FILE2}","partitionValues":{{"id":"2"}},"size":0,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":3,\"nullCount\":{{\"val\":0}},\"minValues\":{{\"val\":\"a\"}},\"maxValues\":{{\"val\":\"c\"}}}}"}}}}"#),
    ];

    add_commit(storage.as_ref(), 0, actions.iter().join("\n")).await?;
    storage
        .put(
            &Path::from("id=1").child(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;
    storage
        .put(
            &Path::from("id=2").child(PARQUET_FILE2),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;

    let location = Url::parse("memory:///")?;

    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;

    let predicate = Pred::eq(column_expr!("id"), Expr::literal(2));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let stream = scan.execute(engine)?;

    let mut files_scanned = 0;
    for engine_data in stream {
        let mut result_batch = into_record_batch(engine_data?.raw_data?);
        let _ = result_batch.remove_column(result_batch.schema().index_of("id")?);
        assert_eq!(&batch, &result_batch);
        files_scanned += 1;
    }
    assert_eq!(1, files_scanned);
    Ok(())
}

#[tokio::test]
async fn predicate_on_non_nullable_column_missing_stats() -> Result<(), Box<dyn std::error::Error>>
{
    let batch_1 = generate_batch(vec![("val", vec!["a", "b", "c"].into_array())])?;
    let batch_2 = generate_batch(vec![("val", vec!["d", "e", "f"].into_array())])?;

    let storage = Arc::new(InMemory::new());
    let actions = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string(),
        r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}"#.to_string(),
        r#"{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"val\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#.to_string(),
        // Add one file with stats, one file without
        format!(r#"{{"add":{{"path":"{PARQUET_FILE1}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":3,\"nullCount\":{{\"val\":0}},\"minValues\":{{\"val\":\"a\"}},\"maxValues\":{{\"val\":\"c\"}}}}"}}}}"#),
        format!(r#"{{"add":{{"path":"{PARQUET_FILE2}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":3,\"nullCount\":{{}},\"minValues\":{{}},\"maxValues\":{{}}}}"}}}}"#),
    ];

    // Disable writing Parquet statistics so these cannot be used for pruning row groups
    let writer_props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    add_commit(storage.as_ref(), 0, actions.iter().join("\n")).await?;
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes_with_props(&batch_1, writer_props.clone()).into(),
        )
        .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE2),
            record_batch_to_bytes_with_props(&batch_2, writer_props).into(),
        )
        .await?;

    let location = Url::parse("memory:///")?;

    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;

    let predicate = Pred::eq(column_expr!("val"), Expr::literal("g"));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let stream = scan.execute(engine)?;

    let mut files_scanned = 0;
    for engine_data in stream {
        let result_batch = into_record_batch(engine_data?.raw_data?);
        assert_eq!(&batch_2, &result_batch);
        files_scanned += 1;
    }
    // One file is scanned as stats are missing so we don't know the predicate isn't satisfied
    assert_eq!(1, files_scanned);

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
    let predicate = column_expr!("missing").lt(Expr::literal(10i64));
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
    let predicate = column_expr!("invalid").lt(Expr::literal(10));
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
    let test_dir = load_test_data("./tests/data", test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    read_table_data_str(test_path.to_str().unwrap(), None, None, expected)
}

#[test]
fn compacted_log_files_table() -> Result<(), Box<dyn std::error::Error>> {
    let expected = vec![
        "+----+--------------------+",
        "| id | comment            |",
        "+----+--------------------+",
        "| 0  | new                |",
        "| 1  | after-large-delete |",
        "| 2  |                    |",
        "| 10 | merge1-insert      |",
        "| 12 | merge2-insert      |",
        "+----+--------------------+",
    ];
    let test_name = "compacted-log-files-table";
    let test_dir = load_test_data("./tests/data", test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    read_table_data_str(test_path.to_str().unwrap(), None, None, expected)
}

#[test]
fn unshredded_variant_table() -> Result<(), Box<dyn std::error::Error>> {
    let expected = include!("data/unshredded-variant.expected.in");
    let test_name = "unshredded-variant";
    let test_dir = load_test_data("./tests/data", test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    read_table_data_str(test_path.to_str().unwrap(), None, None, expected)
}

#[tokio::test]
async fn test_row_index_metadata_column() -> Result<(), Box<dyn std::error::Error>> {
    // Setup up an in-memory table with different numbers of rows in each file
    let batch1 = generate_batch(vec![
        ("id", vec![1i32, 2, 3, 4, 5].into_array()),
        ("value", vec!["a", "b", "c", "d", "e"].into_array()),
    ])?;
    let batch2 = generate_batch(vec![
        ("id", vec![10i32, 20, 30].into_array()),
        ("value", vec!["x", "y", "z"].into_array()),
    ])?;
    let batch3 = generate_batch(vec![
        ("id", vec![100i32, 200, 300, 400].into_array()),
        ("value", vec!["p", "q", "r", "s"].into_array()),
    ])?;

    let storage = Arc::new(InMemory::new());
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add(PARQUET_FILE1.to_string()),
            TestAction::Add(PARQUET_FILE2.to_string()),
            TestAction::Add(PARQUET_FILE3.to_string()),
        ]),
    )
    .await?;

    for (parquet_file, batch) in [
        (PARQUET_FILE1, &batch1),
        (PARQUET_FILE2, &batch2),
        (PARQUET_FILE3, &batch3),
    ] {
        storage
            .put(
                &Path::from(parquet_file),
                record_batch_to_bytes(batch).into(),
            )
            .await?;
    }

    let location = Url::parse("memory:///")?;
    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    // Create a schema that includes a row index metadata column
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex),
        StructField::nullable("value", DataType::STRING),
    ])?);

    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().with_schema(schema).build()?;

    let mut file_count = 0;
    let expected_row_counts = [5, 3, 4];
    let stream = scan.execute(engine.clone())?;

    for scan_result in stream {
        let batch = extract_record_batch(scan_result?)?;
        file_count += 1;

        // Verify the schema structure
        assert_eq!(batch.num_columns(), 3, "Expected 3 columns in the batch");
        assert_eq!(
            batch.schema().field(0).name(),
            "id",
            "First column should be 'id'"
        );
        assert_eq!(
            batch.schema().field(1).name(),
            "row_index",
            "Second column should be 'row_index'"
        );
        assert_eq!(
            batch.schema().field(2).name(),
            "value",
            "Third column should be 'value'"
        );

        // Each file should have row indexes starting from 0 (file-local indexing)
        let row_index_array = batch.column(1).as_primitive::<Int64Type>();
        let expected_values: Vec<i64> = (0..batch.num_rows() as i64).collect();
        assert_eq!(
            row_index_array.values().to_vec(),
            expected_values,
            "Row index values incorrect for file {} (expected {} rows)",
            file_count,
            expected_row_counts[file_count - 1]
        );
    }

    assert_eq!(file_count, 3, "Expected to scan 3 files");
    Ok(())
}

#[tokio::test]
async fn test_unsupported_metadata_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare an in-memory table with some data
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
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;

    let location = Url::parse("memory:///")?;
    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    // Test that unsupported metadata columns fail with appropriate errors
    let test_cases = [
        (
            "row_id",
            MetadataColumnSpec::RowId,
            "Row ids are not enabled on this table",
        ),
        (
            "row_commit_version",
            MetadataColumnSpec::RowCommitVersion,
            "Row commit versions not supported",
        ),
    ];

    for (column_name, metadata_spec, error_text) in test_cases {
        let snapshot = Snapshot::builder_for(location.clone()).build(engine.as_ref())?;
        let schema = Arc::new(StructType::try_new([
            StructField::nullable("id", DataType::INTEGER),
            StructField::create_metadata_column(column_name, metadata_spec),
        ])?);

        let scan_err = snapshot
            .scan_builder()
            .with_schema(schema)
            .build()
            .unwrap_err();
        let error_msg = scan_err.to_string();
        assert!(
            error_msg.contains(error_text),
            "Expected {error_msg} to contain {error_text}"
        );
    }

    Ok(())
}
