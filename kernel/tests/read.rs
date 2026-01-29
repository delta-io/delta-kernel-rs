use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::arrow::array::AsArray as _;
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::arrow::datatypes::{Int64Type, Schema as ArrowSchema};
use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::{
    column_expr, column_pred, Expression as Expr, ExpressionRef, Predicate as Pred,
};
use delta_kernel::log_segment::LogSegment;
use delta_kernel::parquet::file::properties::{EnabledStatistics, WriterProperties};
use delta_kernel::path::ParsedLogPath;
use delta_kernel::scan::state::{transform_to_logical, ScanFile};
use delta_kernel::scan::Scan;
use delta_kernel::schema::{DataType, MetadataColumnSpec, Schema, StructField, StructType};
use delta_kernel::{Engine, FileMeta, Snapshot};

use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use test_utils::{
    actions_to_string, add_commit, generate_batch, generate_simple_batch, into_record_batch,
    load_test_data, read_scan, record_batch_to_bytes, record_batch_to_bytes_with_props, IntoArray,
    TestAction, METADATA,
};
use url::Url;

mod common;

const PARQUET_FILE1: &str = "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";
const PARQUET_FILE2: &str = "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet";
const PARQUET_FILE3: &str = "part-00002-c506e79a-0bf8-4e2b-a42b-9731b2e490ff-c000.snappy.parquet";

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
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());

    let expected_data = vec![batch.clone(), batch];

    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let mut files = 0;
    let stream = scan.execute(engine)?.zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(into_record_batch(data?), expected);
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
    let engine = DefaultEngineBuilder::new(storage.clone()).build();

    let expected_data = vec![batch.clone(), batch];

    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;

    let mut files = 0;
    let stream = scan.execute(Arc::new(engine))?.zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(into_record_batch(data?), expected);
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
    let engine = DefaultEngineBuilder::new(storage.clone()).build();

    let expected_data = vec![batch];

    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;

    let stream = scan.execute(Arc::new(engine))?.zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        files += 1;
        assert_eq!(into_record_batch(data?), expected);
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
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
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
            files_scanned += 1;
            assert_eq!(into_record_batch(batch?), expected.clone());
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

fn scan_metadata_callback(batches: &mut Vec<ScanFile>, scan_file: ScanFile) {
    batches.push(scan_file);
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
            let record_batch = logical.try_into_record_batch()?;
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
    let engine = test_utils::create_default_engine(&url)?;

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

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;

    let predicate = Pred::eq(column_expr!("id"), Expr::literal(2));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let stream = scan.execute(engine)?;

    let mut files_scanned = 0;
    for engine_data in stream {
        let mut result_batch = into_record_batch(engine_data?);
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

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;

    let predicate = Pred::eq(column_expr!("val"), Expr::literal("g"));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let stream = scan.execute(engine)?;

    let mut files_scanned = 0;
    for engine_data in stream {
        let result_batch = into_record_batch(engine_data?);
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
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());

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

    for data in stream {
        let batch = into_record_batch(data?);
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
async fn test_file_path_metadata_column() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::array::{Array, AsArray, RunArray};

    // Set up an in-memory table with multiple data files
    let batch1 = generate_batch(vec![
        ("id", vec![1i32, 2, 3].into_array()),
        ("value", vec!["a", "b", "c"].into_array()),
    ])?;
    let batch2 = generate_batch(vec![
        ("id", vec![10i32, 20].into_array()),
        ("value", vec!["x", "y"].into_array()),
    ])?;

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

    for (parquet_file, batch) in [(PARQUET_FILE1, &batch1), (PARQUET_FILE2, &batch2)] {
        storage
            .put(
                &Path::from(parquet_file),
                record_batch_to_bytes(batch).into(),
            )
            .await?;
    }

    let location = Url::parse("memory:///")?;
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());

    // Create a schema that includes the file path metadata column
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        StructField::nullable("value", DataType::STRING),
    ])?);

    let snapshot = Snapshot::builder_for(location.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().with_schema(schema).build()?;

    let mut file_count = 0;
    let expected_files = [PARQUET_FILE1, PARQUET_FILE2];
    let expected_row_counts = [3, 2];
    let stream = scan.execute(engine.clone())?;

    for data in stream {
        let batch = into_record_batch(data?);

        // Verify the schema structure
        assert_eq!(batch.num_columns(), 3, "Expected 3 columns in the batch");
        assert_eq!(
            batch.schema().field(0).name(),
            "id",
            "First column should be 'id'"
        );
        assert_eq!(
            batch.schema().field(1).name(),
            "_file",
            "Second column should be '_file'"
        );
        assert_eq!(
            batch.schema().field(2).name(),
            "value",
            "Third column should be 'value'"
        );

        // Verify the file path column contains the expected file name
        let file_path_array = batch.column(1);
        let expected_file_name = expected_files[file_count];
        let expected_path = format!("{}{}", location, expected_file_name);

        // The file path array should be run-end encoded
        let run_array = file_path_array
            .as_any()
            .downcast_ref::<RunArray<Int64Type>>()
            .expect("File path column should be run-end encoded");

        // Verify each logical row has the correct file path
        assert_eq!(
            run_array.len(),
            expected_row_counts[file_count],
            "File {} should have {} rows",
            expected_file_name,
            expected_row_counts[file_count]
        );

        // Verify the physical representation is efficient (single run)
        let run_ends = run_array.run_ends().values();
        assert_eq!(
            run_ends.len(),
            1,
            "File path should be encoded as a single run"
        );
        assert_eq!(
            run_ends[0], expected_row_counts[file_count] as i64,
            "Run should end at position {}",
            expected_row_counts[file_count]
        );

        // Verify the value is the expected file path
        let values = run_array.values().as_string::<i32>();
        assert_eq!(values.len(), 1, "Should have only 1 unique file path value");
        assert_eq!(
            values.value(0),
            expected_path,
            "File path should be '{}'",
            expected_path
        );

        file_count += 1;
    }

    assert_eq!(file_count, 2, "Expected to scan 2 files");
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
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());

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

#[tokio::test]
async fn test_invalid_files_are_skipped() -> Result<(), Box<dyn std::error::Error>> {
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
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());

    let invalid_files = [
        "_delta_log/0.zip",
        "_delta_log/_copy_into_log/0.zip",
        "_delta_log/_ignore_me/00000000000000000000.json",
        "_delta_log/_and_me/00000000000000000000.checkpoint.parquet",
        "_delta_log/02184.json",
        "_delta_log/0x000000000000000000.checkpoint.parquet",
        "00000000000000000000.json",
        "_delta_log/_staged_commits/_staged_commits/00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        "_delta_log/my_random_dir/_staged_commits/00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        "_delta_log/my_random_dir/_delta_log/_staged_commits/00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        "_delta_log/_delta_log/00000000000000000000.json",
        "_delta_log/_delta_log/00000000000000000000.checkpoint.parquet",
        "_delta_log/something/_delta_log/00000000000000000000.crc",
        "_delta_log/something/_delta_log/00000000000000000000.json",
        "_delta_log/something/_delta_log/00000000000000000000.checkpoint.parquet",
    ];

    fn get_file_path_for_test(path: &ParsedLogPath) -> &str {
        &path.location.location.as_str()[10..]
    }

    fn ensure_segment_does_not_contain(invalid_files: &[&str], segment: &LogSegment) {
        assert!(
            !segment.ascending_commit_files.iter().any(|p| {
                let test_path = get_file_path_for_test(p);
                invalid_files.contains(&test_path)
            }),
            "ascending_commit_files contained invalid file"
        );
        assert!(
            !segment.ascending_compaction_files.iter().any(|p| {
                let test_path = get_file_path_for_test(p);
                invalid_files.contains(&test_path)
            }),
            "ascending_compaction_files contained invalid file"
        );
        assert!(
            !segment.checkpoint_parts.iter().any(|p| {
                let test_path = get_file_path_for_test(p);
                invalid_files.contains(&test_path)
            }),
            "checkpoint_parts contained invalid file"
        );
        if let Some(ref crc) = segment.latest_crc_file {
            assert!(
                !invalid_files.contains(&get_file_path_for_test(crc)),
                "Latest crc contained invalid file"
            );
        }
        if let Some(ref latest_commit) = segment.latest_commit_file {
            assert!(
                !invalid_files.contains(&get_file_path_for_test(latest_commit)),
                "Latest commit contained invalid file"
            );
        }
    }

    for invalid_file in invalid_files.iter() {
        let invalid_path = Path::from(*invalid_file);
        storage.put(&invalid_path, vec![1u8].into()).await?;
        let snapshot = Snapshot::builder_for(location.clone()).build(engine.as_ref())?;
        ensure_segment_does_not_contain(&invalid_files, snapshot.log_segment());
        storage.delete(&invalid_path).await?;
    }

    // final test with _all_ the files we should ignore
    for invalid_file in invalid_files.iter() {
        let invalid_path = Path::from(*invalid_file);
        storage.put(&invalid_path, vec![1u8].into()).await?;
    }
    let snapshot = Snapshot::builder_for(location).build(engine.as_ref())?;
    ensure_segment_does_not_contain(&invalid_files, snapshot.log_segment());

    Ok(())
}

/// Test that data skipping works with parsed stats from checkpoint.
/// The parsed-stats table has 6 files with id ranges (4 from checkpoint, 2 from commits):
/// - File 1: id 1-100, salary 50100-60000 (checkpoint)
/// - File 2: id 101-200, salary 60100-70000 (checkpoint)
/// - File 3: id 201-300, salary 70100-80000 (checkpoint)
/// - File 4: id 301-400, salary 80100-90000 (checkpoint)
/// - File 5: id 401-500, salary 90100-100000 (commit 4)
/// - File 6: id 501-600, salary 100100-110000 (commit 5)
#[test]
fn data_skipping_with_parsed_stats() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Test 1: Predicate that should skip all files (id > 700)
    // All files have max id of 600, so no files should match
    let predicate = Pred::gt(column_expr!("id"), Expr::literal(700i64));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let files_scanned: usize = scan.execute(engine.clone())?.count();
    assert_eq!(
        files_scanned, 0,
        "Expected 0 files when id > 700 (all files have max id 600)"
    );

    // Test 2: Predicate that should return only first file (id < 50)
    // Only file 1 has ids 1-100 and min < 50
    let predicate = Pred::lt(column_expr!("id"), Expr::literal(50i64));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let mut files_scanned = 0;
    for _data in scan.execute(engine.clone())? {
        files_scanned += 1;
    }
    assert_eq!(
        files_scanned, 1,
        "Expected 1 file when id < 50 (only file 1 has min id 1)"
    );

    // Test 3: Predicate using salary column (salary > 105000)
    // Only file 6 has salary range 100100-110000, with max 110000
    let predicate = Pred::gt(column_expr!("salary"), Expr::literal(105000i64));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let mut files_scanned = 0;
    for _data in scan.execute(engine.clone())? {
        files_scanned += 1;
    }
    assert_eq!(
        files_scanned, 1,
        "Expected 1 file when salary > 105000 (only file 6 has salary up to 110000)"
    );

    // Test 4: Predicate that matches multiple files (id > 350)
    // Files 4, 5, 6 have ids starting from 301, 401, 501
    let predicate = Pred::gt(column_expr!("id"), Expr::literal(350i64));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let mut files_scanned = 0;
    for _data in scan.execute(engine)? {
        files_scanned += 1;
    }
    assert_eq!(
        files_scanned, 3,
        "Expected 3 files when id > 350 (files 4, 5, 6 have ids > 350)"
    );

    Ok(())
}

/// Test that `stats_parsed` column is populated correctly in `scan_files`.
///
/// The behavior is:
/// - Neither predicate nor stats_columns → stats_parsed with default columns (from table properties)
/// - Predicate only → stats_parsed with predicate columns (kernel uses it for data skipping)
/// - stats_columns only → stats_parsed with user columns (user does their own data skipping)
/// - Both predicate AND stats_columns → error (mutually exclusive)
#[test]
fn scan_metadata_parsed_stats() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Test 1: Without predicate or stats_columns, stats_parsed is present (default from table properties)
    let scan = snapshot.clone().scan_builder().build()?;
    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut has_parsed_stats_default = false;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        if schema.column_with_name("stats_parsed").is_some() {
            has_parsed_stats_default = true;
        }
    }
    assert!(
        has_parsed_stats_default,
        "Expected stats_parsed column to be present by default"
    );

    // Test 2: With predicate only, stats_parsed should be present (kernel needs it for data skipping)
    let predicate =
        delta_kernel::expressions::Predicate::gt(column_expr!("id"), Expr::literal(100i64));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut has_parsed_stats_with_pred = false;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        if schema.column_with_name("stats_parsed").is_some() {
            has_parsed_stats_with_pred = true;
        }
    }
    assert!(
        has_parsed_stats_with_pred,
        "Expected stats_parsed column to be present with predicate"
    );

    // Test 3: With stats_columns only, stats_parsed column should be present with user columns
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_stats_columns(vec![ColumnName::new(["id"])])
        .build()?;
    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut has_parsed_stats_with_cols = false;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        if schema.column_with_name("stats_parsed").is_some() {
            has_parsed_stats_with_cols = true;
            // Verify it's a struct column
            let (_idx, field) = schema.column_with_name("stats_parsed").unwrap();
            assert!(
                matches!(
                    field.data_type(),
                    delta_kernel::arrow::datatypes::DataType::Struct(_)
                ),
                "stats_parsed should be a struct type"
            );
        }
    }
    assert!(
        has_parsed_stats_with_cols,
        "Expected stats_parsed column to be present with stats_columns"
    );

    // Test 4: Both predicate AND stats_columns should error
    let predicate =
        delta_kernel::expressions::Predicate::gt(column_expr!("id"), Expr::literal(100i64));
    let result = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .with_stats_columns(vec![ColumnName::new(["id"])])
        .build();

    assert!(
        result.is_err(),
        "Expected error when both predicate and stats_columns are specified"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Cannot specify both predicate and stats_columns"),
        "Expected specific error message, got: {}",
        err_msg
    );

    Ok(())
}

/// Test stats_columns with a single column - verify the schema structure
#[test]
fn stats_columns_single_column_schema() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Request stats for only the "id" column
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(vec![ColumnName::new(["id"])])
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        // Verify stats_parsed column exists
        let (idx, field) = schema
            .column_with_name("stats_parsed")
            .expect("stats_parsed column should exist");

        // Verify it's a struct with the expected fields
        if let ArrowDataType::Struct(fields) = field.data_type() {
            // Should have: numRecords, nullCount, minValues, maxValues
            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            assert!(
                field_names.contains(&"numRecords"),
                "Should have numRecords field"
            );
            assert!(
                field_names.contains(&"nullCount"),
                "Should have nullCount field"
            );
            assert!(
                field_names.contains(&"minValues"),
                "Should have minValues field"
            );
            assert!(
                field_names.contains(&"maxValues"),
                "Should have maxValues field"
            );

            // Verify minValues only contains "id" column
            let min_values_field = fields.iter().find(|f| f.name() == "minValues").unwrap();
            if let ArrowDataType::Struct(min_fields) = min_values_field.data_type() {
                let min_field_names: Vec<&str> =
                    min_fields.iter().map(|f| f.name().as_str()).collect();
                assert_eq!(
                    min_field_names,
                    vec!["id"],
                    "minValues should only contain 'id' column"
                );
            } else {
                panic!("minValues should be a struct");
            }
        } else {
            panic!("stats_parsed should be a struct type");
        }

        // Verify the column has data
        assert!(batch.column(idx).len() > 0, "stats_parsed should have data");
    }

    Ok(())
}

/// Test stats_columns with multiple columns
#[test]
fn stats_columns_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Request stats for multiple columns: id, name, salary
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(vec![
            ColumnName::new(["id"]),
            ColumnName::new(["name"]),
            ColumnName::new(["salary"]),
        ])
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        let (_idx, field) = schema
            .column_with_name("stats_parsed")
            .expect("stats_parsed column should exist");

        if let ArrowDataType::Struct(fields) = field.data_type() {
            // Verify minValues contains all requested columns
            let min_values_field = fields.iter().find(|f| f.name() == "minValues").unwrap();
            if let ArrowDataType::Struct(min_fields) = min_values_field.data_type() {
                let min_field_names: std::collections::HashSet<&str> =
                    min_fields.iter().map(|f| f.name().as_str()).collect();
                assert!(
                    min_field_names.contains("id"),
                    "minValues should contain 'id'"
                );
                assert!(
                    min_field_names.contains("name"),
                    "minValues should contain 'name'"
                );
                assert!(
                    min_field_names.contains("salary"),
                    "minValues should contain 'salary'"
                );
                assert!(
                    !min_field_names.contains("age"),
                    "minValues should NOT contain 'age' (not requested)"
                );
            }
        }
    }

    Ok(())
}

/// Test stats_columns with empty vector - should use default stats
#[test]
fn stats_columns_empty_vector() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Empty stats_columns vector should fall back to default behavior
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(Vec::<ColumnName>::new())
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    // Should still have stats_parsed (from default table properties)
    let mut has_stats = false;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();
        let schema = batch.schema();

        if schema.column_with_name("stats_parsed").is_some() {
            has_stats = true;
        }
    }
    assert!(has_stats, "Empty stats_columns should use default stats");

    Ok(())
}

/// Test that stats_columns actually contains parsed values
#[test]
fn stats_columns_values_are_parsed() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::array::{Array, Int64Array, StringArray, StructArray};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    let scan = snapshot
        .scan_builder()
        .with_stats_columns(vec![ColumnName::new(["id"]), ColumnName::new(["name"])])
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;
    let mut found_valid_stats = false;

    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();

        let (stats_idx, _) = batch
            .schema()
            .column_with_name("stats_parsed")
            .expect("stats_parsed should exist");

        let stats_col = batch.column(stats_idx);
        let stats_struct = stats_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("stats_parsed should be StructArray");

        // Get numRecords - should be non-null integers
        let num_records_idx = stats_struct
            .column_by_name("numRecords")
            .expect("Should have numRecords");
        let num_records = num_records_idx
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("numRecords should be Int64Array");

        // Verify numRecords has valid values (test data has 100 records per file)
        for i in 0..num_records.len() {
            if !num_records.is_null(i) {
                let value = num_records.value(i);
                assert_eq!(value, 100, "Each file should have 100 records");
                found_valid_stats = true;
            }
        }

        // Get minValues struct
        let min_values = stats_struct
            .column_by_name("minValues")
            .expect("Should have minValues");
        let min_values_struct = min_values
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("minValues should be StructArray");

        // Check that minValues.id is an Int64Array with values
        if let Some(min_id) = min_values_struct.column_by_name("id") {
            let min_id_arr = min_id
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("minValues.id should be Int64Array");
            // First file has min id = 1
            if !min_id_arr.is_null(0) {
                assert!(min_id_arr.value(0) >= 1, "min id should be >= 1");
            }
        }

        // Check minValues.name is a StringArray
        if let Some(min_name) = min_values_struct.column_by_name("name") {
            let min_name_arr = min_name
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("minValues.name should be StringArray");
            if !min_name_arr.is_null(0) {
                assert!(
                    min_name_arr.value(0).starts_with("name_"),
                    "min name should start with 'name_'"
                );
            }
        }
    }

    assert!(found_valid_stats, "Should have found valid parsed stats");
    Ok(())
}

/// Test that stats_columns doesn't perform data skipping (no predicate)
#[test]
fn stats_columns_no_data_skipping() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // With stats_columns only (no predicate), all files should be returned
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(vec![ColumnName::new(["id"])])
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut file_count = 0;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;

        // Count files (each row is a file)
        let selection = scan_metadata.scan_files.selection_vector();
        for selected in selection.iter() {
            if *selected {
                file_count += 1;
            }
        }
    }

    // Test data has 6 files - all should be returned without data skipping
    assert_eq!(
        file_count, 6,
        "All 6 files should be returned without predicate"
    );

    Ok(())
}

/// Test that predicate with data skipping works (for comparison)
#[test]
fn predicate_with_data_skipping() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // With predicate, data skipping should filter files
    // id > 500 should only match the last file (id 501-600)
    let predicate = Pred::gt(column_expr!("id"), Expr::literal(500i64));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut file_count = 0;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let selection = scan_metadata.scan_files.selection_vector();
        for selected in selection.iter() {
            if *selected {
                file_count += 1;
            }
        }
    }

    // Only files with max(id) > 500 should be selected
    // File 6 has id 501-600, so it should be selected
    // Data skipping should filter out files where max(id) <= 500
    assert!(
        file_count < 6,
        "Data skipping should filter some files (got {} files)",
        file_count
    );

    Ok(())
}

/// Test with_stats_columns using None (should use default behavior)
#[test]
fn stats_columns_none_uses_default() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Passing None should be same as not calling with_stats_columns
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(None::<Vec<ColumnName>>)
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let mut has_stats = false;
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();

        if batch.schema().column_with_name("stats_parsed").is_some() {
            has_stats = true;
        }
    }

    assert!(has_stats, "None should use default stats behavior");
    Ok(())
}

/// Test stats_columns with non-existent column (should handle gracefully)
#[test]
fn stats_columns_nonexistent_column() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::ColumnName;

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Request stats for a column that doesn't exist
    let scan = snapshot
        .scan_builder()
        .with_stats_columns(vec![ColumnName::new(["nonexistent_column"])])
        .build()?;

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

    // Should still work, but stats_parsed might be empty/null for the non-existent column
    for scan_metadata in scan_metadata_iter {
        let scan_metadata = scan_metadata?;
        let data = scan_metadata.scan_files.data();
        let arrow_data = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Expected ArrowEngineData");
        let batch = arrow_data.record_batch();

        // The scan should complete without error
        // stats_parsed schema will be determined by what columns actually exist
        assert!(batch.num_rows() > 0, "Should have rows");
    }

    Ok(())
}
