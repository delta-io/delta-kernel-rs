use std::error;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use itertools::Itertools;

use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{DeltaResult, Error, PredicateRef, Version};

mod common;

use test_utils::DefaultEngineExtension;
use test_utils::{load_test_data, to_arrow};

fn read_cdf_for_table(
    test_name: impl AsRef<str>,
    start_version: Version,
    end_version: impl Into<Option<Version>>,
    predicate: impl Into<Option<PredicateRef>>,
) -> DeltaResult<Vec<RecordBatch>> {
    let test_dir = load_test_data("tests/data", test_name.as_ref()).unwrap();
    let test_path = test_dir.path().join(test_name.as_ref());
    let test_path = delta_kernel::try_parse_uri(test_path.to_str().expect("table path to string"))?;
    let engine = DefaultEngine::new_local();
    let table_changes = TableChanges::try_new(
        test_path,
        engine.as_ref(),
        start_version,
        end_version.into(),
    )?;

    // Project out the commit timestamp since file modification time may change anytime git clones
    // or switches branches
    let names = table_changes
        .schema()
        .fields()
        .map(|field| field.name())
        .filter(|name| *name != "_commit_timestamp")
        .collect_vec();
    let schema = table_changes.schema().project(&names)?;
    let scan = table_changes
        .into_scan_builder()
        .with_schema(schema)
        .with_predicate(predicate)
        .build()?;
    let scan_schema_as_arrow =
        ArrowSchema::try_from_kernel(scan.logical_schema().as_ref()).unwrap();
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch = to_arrow(data)?;
            // Verify that the arrow record batches match the expected schema
            assert!(record_batch.schema().as_ref() == &scan_schema_as_arrow);
            match mask {
                Some(mask) => Ok(filter_record_batch(&record_batch, &mask.into())?),
                None => Ok(record_batch),
            }
        })
        .try_collect()?;
    Ok(batches)
}

#[test]
fn cdf_with_deletion_vector() -> Result<(), Box<dyn error::Error>> {
    let batches = read_cdf_for_table("cdf-table-with-dv", 0, None, None)?;
    // Each commit performs the following:
    // 0. Insert  0..=9
    // 1. Remove  [0, 9]
    // 2. Restore [0, 9]
    // 3. Remove  [0, 1, 4, 5]
    // 4. Restore [1, 4]
    // 5. Restore [0, 5] and Remove [3]
    // 6. Restore 3
    let mut expected = vec![
        "+-------+--------------+-----------------+",
        "| value | _change_type | _commit_version |",
        "+-------+--------------+-----------------+",
        "| 0     | insert       | 0               |",
        "| 1     | insert       | 0               |",
        "| 2     | insert       | 0               |",
        "| 3     | insert       | 0               |",
        "| 4     | insert       | 0               |",
        "| 5     | insert       | 0               |",
        "| 6     | insert       | 0               |",
        "| 7     | insert       | 0               |",
        "| 8     | insert       | 0               |",
        "| 9     | insert       | 0               |",
        "| 0     | delete       | 1               |",
        "| 9     | delete       | 1               |",
        "| 0     | insert       | 2               |",
        "| 9     | insert       | 2               |",
        "| 0     | delete       | 3               |",
        "| 1     | delete       | 3               |",
        "| 4     | delete       | 3               |",
        "| 5     | delete       | 3               |",
        "| 1     | insert       | 4               |",
        "| 4     | insert       | 4               |",
        "| 3     | delete       | 5               |",
        "| 0     | insert       | 5               |",
        "| 5     | insert       | 5               |",
        "| 3     | insert       | 6               |",
        "+-------+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn basic_cdf() -> Result<(), Box<dyn error::Error>> {
    let batches = read_cdf_for_table("cdf-table", 0, None, None)?;
    let mut expected = vec![
        "+----+--------+------------+------------------+-----------------+",
        "| id | name   | birthday   | _change_type     | _commit_version |",
        "+----+--------+------------+------------------+-----------------+",
        "| 1  | Steve  | 2023-12-22 | insert           | 0               |",
        "| 2  | Bob    | 2023-12-23 | insert           | 0               |",
        "| 3  | Dave   | 2023-12-23 | insert           | 0               |",
        "| 4  | Kate   | 2023-12-23 | insert           | 0               |",
        "| 5  | Emily  | 2023-12-24 | insert           | 0               |",
        "| 6  | Carl   | 2023-12-24 | insert           | 0               |",
        "| 7  | Dennis | 2023-12-24 | insert           | 0               |",
        "| 8  | Claire | 2023-12-25 | insert           | 0               |",
        "| 9  | Ada    | 2023-12-25 | insert           | 0               |",
        "| 10 | Borb   | 2023-12-25 | insert           | 0               |",
        "| 3  | Dave   | 2023-12-22 | update_postimage | 1               |",
        "| 3  | Dave   | 2023-12-23 | update_preimage  | 1               |",
        "| 4  | Kate   | 2023-12-22 | update_postimage | 1               |",
        "| 4  | Kate   | 2023-12-23 | update_preimage  | 1               |",
        "| 2  | Bob    | 2023-12-22 | update_postimage | 1               |",
        "| 2  | Bob    | 2023-12-23 | update_preimage  | 1               |",
        "| 7  | Dennis | 2023-12-24 | update_preimage  | 2               |",
        "| 7  | Dennis | 2023-12-29 | update_postimage | 2               |",
        "| 5  | Emily  | 2023-12-24 | update_preimage  | 2               |",
        "| 5  | Emily  | 2023-12-29 | update_postimage | 2               |",
        "| 6  | Carl   | 2023-12-24 | update_preimage  | 2               |",
        "| 6  | Carl   | 2023-12-29 | update_postimage | 2               |",
        "| 7  | Dennis | 2023-12-29 | delete           | 3               |",
        "+----+--------+------------+------------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn cdf_non_partitioned() -> Result<(), Box<dyn error::Error>> {
    let batches = read_cdf_for_table("cdf-table-non-partitioned", 0, None, None)?;
    let mut expected = vec![
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+",
             "| id | name   | birthday   | long_field        | boolean_field | double_field | smallint_field | _change_type     | _commit_version |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+",
             "| 1  | Steve  | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 8  | Claire | 2024-04-17 | 7                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 9  | Ada    | 2024-04-17 | 8                 | true          | 3.14         | 1              | insert           | 0               |",
             "| 10 | Borb   | 2024-04-17 | 99999999999999999 | true          | 3.14         | 1              | insert           | 0               |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | update_preimage  | 1               |",
             "| 3  | Dave   | 2024-04-14 | 2                 | true          | 3.14         | 1              | update_postimage | 1               |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | update_preimage  | 1               |",
             "| 4  | Kate   | 2024-04-14 | 3                 | true          | 3.14         | 1              | update_postimage | 1               |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | update_preimage  | 1               |",
             "| 2  | Bob    | 2024-04-14 | 1                 | true          | 3.14         | 1              | update_postimage | 1               |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | update_preimage  | 2               |",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | update_postimage | 2               |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | update_preimage  | 2               |",
             "| 5  | Emily  | 2024-04-14 | 4                 | true          | 3.14         | 1              | update_postimage | 2               |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | update_preimage  | 2               |",
             "| 6  | Carl   | 2024-04-14 | 5                 | true          | 3.14         | 1              | update_postimage | 2               |",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | delete           | 3               |",
             "| 1  | Alex   | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 4               |",
             "| 2  | Alan   | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 4               |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+"
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn cdf_with_cdc_and_dvs() -> Result<(), Box<dyn error::Error>> {
    let batches = read_cdf_for_table("cdf-table-with-cdc-and-dvs", 0, None, None)?;
    let mut expected = vec![
        "+----+--------------------+------------------+-----------------+",
        "| id | comment            | _change_type     | _commit_version |",
        "+----+--------------------+------------------+-----------------+",
        "| 1  | initial            | insert           | 0               |",
        "| 2  | insert1            | insert           | 1               |",
        "| 3  | insert1-delete1    | insert           | 1               |",
        "| 4  | insert1-delete2    | insert           | 1               |",
        "| 5  | insert1-delete2    | insert           | 1               |",
        "| 3  | insert1-delete1    | delete           | 2               |",
        "| 3  | insert1-delete1    | insert           | 4               |",
        "| 4  | insert1-delete2    | delete           | 5               |",
        "| 5  | insert1-delete2    | delete           | 5               |",
        "| 4  | insert1-delete2    | insert           | 7               |",
        "| 5  | insert2            | insert           | 8               |",
        "| 1  | initial            | update_preimage  | 9               |",
        "| 1  | update1            | update_postimage | 9               |",
        "| 2  | insert1            | update_preimage  | 9               |",
        "| 2  | update1            | update_postimage | 9               |",
        "| 3  | insert1-delete1    | update_preimage  | 9               |",
        "| 3  | update1            | update_postimage | 9               |",
        "| 1  | update1            | delete           | 10              |",
        "| 2  | update1            | update_preimage  | 12              |",
        "| 2  | update2            | update_postimage | 12              |",
        "| 6  | insert3            | insert           | 14              |",
        "| 7  | insert3            | insert           | 14              |",
        "| 8  | insert4            | insert           | 15              |",
        "| 9  | insert4            | insert           | 15              |",
        "| 8  | insert4            | delete           | 16              |",
        "| 7  | insert3            | delete           | 16              |",
        "| 10 | merge1-insert      | insert           | 18              |",
        "| 11 | merge1-insert      | insert           | 18              |",
        "| 9  | merge1-update      | update_postimage | 18              |",
        "| 9  | insert4            | update_preimage  | 18              |",
        "| 11 | merge1-insert      | update_preimage  | 20              |",
        "| 11 |                    | update_postimage | 20              |",
        "| 12 | merge2-insert      | insert           | 22              |",
        "| 11 |                    | delete           | 22              |",
        "| 3  | update1            | delete           | 24              |",
        "| 4  | insert1-delete2    | delete           | 24              |",
        "| 5  | insert2            | delete           | 24              |",
        "| 2  | update2            | delete           | 24              |",
        "| 6  | insert3            | delete           | 24              |",
        "| 9  | merge1-update      | delete           | 24              |",
        "| 0  | new                | insert           | 25              |",
        "| 1  | after-large-delete | insert           | 25              |",
        "| 2  |                    | insert           | 25              |",
        "+----+--------------------+------------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn simple_cdf_version_ranges() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-simple", 0, 0, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);

    let batches = read_cdf_for_table("cdf-table-simple", 1, 1, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | delete       | 1               |",
        "| 1  | delete       | 1               |",
        "| 2  | delete       | 1               |",
        "| 3  | delete       | 1               |",
        "| 4  | delete       | 1               |",
        "| 5  | delete       | 1               |",
        "| 6  | delete       | 1               |",
        "| 7  | delete       | 1               |",
        "| 8  | delete       | 1               |",
        "| 9  | delete       | 1               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);

    let batches = read_cdf_for_table("cdf-table-simple", 2, 2, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 20 | insert       | 2               |",
        "| 21 | insert       | 2               |",
        "| 22 | insert       | 2               |",
        "| 23 | insert       | 2               |",
        "| 24 | insert       | 2               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);

    let batches = read_cdf_for_table("cdf-table-simple", 0, 2, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "| 0  | delete       | 1               |",
        "| 1  | delete       | 1               |",
        "| 2  | delete       | 1               |",
        "| 3  | delete       | 1               |",
        "| 4  | delete       | 1               |",
        "| 5  | delete       | 1               |",
        "| 6  | delete       | 1               |",
        "| 7  | delete       | 1               |",
        "| 8  | delete       | 1               |",
        "| 9  | delete       | 1               |",
        "| 20 | insert       | 2               |",
        "| 21 | insert       | 2               |",
        "| 22 | insert       | 2               |",
        "| 23 | insert       | 2               |",
        "| 24 | insert       | 2               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn update_operations() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-update-ops", 0, 2, None)?;
    // Note: `update_pre` and `update_post` are technically not part of the delta spec, and instead
    // should be `update_preimage` and `update_postimage` respectively. However, the tests in
    // delta-spark use the post and pre.
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "| 20 | update_pre   | 1               |",
        "| 21 | update_pre   | 1               |",
        "| 22 | update_pre   | 1               |",
        "| 23 | update_pre   | 1               |",
        "| 24 | update_pre   | 1               |",
        "| 30 | update_post  | 2               |",
        "| 31 | update_post  | 2               |",
        "| 32 | update_post  | 2               |",
        "| 33 | update_post  | 2               |",
        "| 34 | update_post  | 2               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn false_data_change_is_ignored() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-data-change", 0, 1, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn invalid_range_end_before_start() {
    let res = read_cdf_for_table("cdf-table-simple", 1, 0, None);
    let expected_msg =
        "Failed to build LogSegment: start_version cannot be greater than end_version";
    assert!(matches!(res, Err(Error::Generic(msg)) if msg == expected_msg));
}

#[test]
fn invalid_range_start_after_last_version_of_table() {
    let res = read_cdf_for_table("cdf-table-simple", 3, 4, None);
    let expected_msg = "Expected the first commit to have version 3, got None";
    assert!(matches!(res, Err(Error::Generic(msg)) if msg == expected_msg));
}

#[test]
fn partition_table() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-partitioned", 0, 2, None)?;
    let mut expected = vec![
        "+----+------+------+------------------+-----------------+",
        "| id | text | part | _change_type     | _commit_version |",
        "+----+------+------+------------------+-----------------+",
        "| 0  | old  | 0    | insert           | 0               |",
        "| 1  | old  | 1    | insert           | 0               |",
        "| 2  | old  | 0    | insert           | 0               |",
        "| 3  | old  | 1    | insert           | 0               |",
        "| 4  | old  | 0    | insert           | 0               |",
        "| 5  | old  | 1    | insert           | 0               |",
        "| 3  | old  | 1    | delete           | 1               |",
        "| 1  | old  | 1    | update_preimage  | 1               |",
        "| 1  | new  | 1    | update_postimage | 1               |",
        "| 0  | old  | 0    | delete           | 2               |",
        "| 2  | old  | 0    | delete           | 2               |",
        "| 4  | old  | 0    | delete           | 2               |",
        "+----+------+------+------------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn backtick_column_names() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-backtick-column-names", 0, None, None)?;
    let mut expected = vec![
        "+--------+----------+--------------------------+--------------+-----------------+",
        "| id.num | id.num`s | struct_col               | _change_type | _commit_version |",
        "+--------+----------+--------------------------+--------------+-----------------+",
        "| 2      | 10       | {field: 1, field.one: 2} | insert       | 0               |",
        "| 4      | 10       | {field: 1, field.one: 2} | insert       | 0               |",
        "| 1      | 10       | {field: 1, field.one: 2} | insert       | 1               |",
        "| 3      | 10       | {field: 1, field.one: 2} | insert       | 1               |",
        "| 5      | 10       | {field: 1, field.one: 2} | insert       | 1               |",
        "+--------+----------+--------------------------+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn unconditional_delete() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-delete-unconditional", 0, None, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "| 0  | delete       | 1               |",
        "| 1  | delete       | 1               |",
        "| 2  | delete       | 1               |",
        "| 3  | delete       | 1               |",
        "| 4  | delete       | 1               |",
        "| 5  | delete       | 1               |",
        "| 6  | delete       | 1               |",
        "| 7  | delete       | 1               |",
        "| 8  | delete       | 1               |",
        "| 9  | delete       | 1               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn conditional_delete_all_rows() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-delete-conditional-all-rows", 0, None, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "| 0  | delete       | 1               |",
        "| 1  | delete       | 1               |",
        "| 2  | delete       | 1               |",
        "| 3  | delete       | 1               |",
        "| 4  | delete       | 1               |",
        "| 5  | delete       | 1               |",
        "| 6  | delete       | 1               |",
        "| 7  | delete       | 1               |",
        "| 8  | delete       | 1               |",
        "| 9  | delete       | 1               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn conditional_delete_two_rows() -> DeltaResult<()> {
    let batches = read_cdf_for_table("cdf-table-delete-conditional-two-rows", 0, None, None)?;
    let mut expected = vec![
        "+----+--------------+-----------------+",
        "| id | _change_type | _commit_version |",
        "+----+--------------+-----------------+",
        "| 0  | insert       | 0               |",
        "| 1  | insert       | 0               |",
        "| 2  | insert       | 0               |",
        "| 3  | insert       | 0               |",
        "| 4  | insert       | 0               |",
        "| 5  | insert       | 0               |",
        "| 6  | insert       | 0               |",
        "| 7  | insert       | 0               |",
        "| 8  | insert       | 0               |",
        "| 9  | insert       | 0               |",
        "| 2  | delete       | 1               |",
        "| 8  | delete       | 1               |",
        "+----+--------------+-----------------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}
