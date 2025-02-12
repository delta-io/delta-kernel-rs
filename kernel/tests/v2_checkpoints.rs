// rustfmt::skip
use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::engine::sync::SyncEngine;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{DeltaResult, Table};

mod common;
use common::{load_test_data, read_scan};

fn read_v2_checkpoint_table(test_name: impl AsRef<str>) -> DeltaResult<Vec<RecordBatch>> {
    let test_dir = load_test_data("tests/data", test_name.as_ref()).unwrap();
    let test_path = test_dir.path().join(test_name.as_ref());

    let table = Table::try_from_uri(test_path.to_str().expect("table path to string")).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let scan = snapshot.into_scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;

    Ok(batches)
}

fn test_v2_checkpoints(table_name: &str) -> DeltaResult<()> {
    let num_of_max_digits = 999.to_string().len();
    let batches = read_v2_checkpoint_table(table_name)?;

    fn generate_rows(count: usize, width: usize) -> Vec<String> {
        (0..count)
            .map(|id| format!("| {:<width$} |", id, width = width))
            .collect()
    }

    let mut expected = vec![
        "+-----+".to_string(),
        "| id  |".to_string(),
        "+-----+".to_string(),
    ];

    // Generate rows of expected table in the same manner as delta-spark creates them
    expected.extend(vec!["| 0   |".to_string(); 3]);
    let all_batches = [
        generate_rows(30, num_of_max_digits),
        generate_rows(100, num_of_max_digits),
        generate_rows(100, num_of_max_digits),
        generate_rows(1000, num_of_max_digits),
    ];
    
    for batch in all_batches {
        expected.extend(batch);
    }

    expected.push("+-----+".to_string());

    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_checkpoints_json_with_sidecars() -> DeltaResult<()> {
    test_v2_checkpoints("v2-checkpoints-json-with-sidecars")
}

#[test]
fn v2_checkpoints_parquet_with_sidecars() -> DeltaResult<()> {
    test_v2_checkpoints("v2-checkpoints-parquet-with-sidecars")
}

#[test]
fn v2_checkpoints_json_without_sidecars() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-checkpoints-json-without-sidecars")?;
    let mut expected = vec![
        "+------+",
        "| id   |",
        "+------+",
        "| 0    |",
        "| 1    |",
        "| 2    |",
        "| 3    |",
        "| 4    |",
        "| 5    |",
        "| 6    |",
        "| 7    |",
        "| 8    |",
        "| 9    |",
        "| 2718 |",
        "+------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_checkpoints_parquet_without_sidecars() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-checkpoints-json-without-sidecars")?;
    let mut expected = vec![
        "+------+",
        "| id   |",
        "+------+",
        "| 0    |",
        "| 1    |",
        "| 2    |",
        "| 3    |",
        "| 4    |",
        "| 5    |",
        "| 6    |",
        "| 7    |",
        "| 8    |",
        "| 9    |",
        "| 2718 |",
        "+------+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_classic_checkpoint_json() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-classic-checkpoint-json")?;
    let mut expected = vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "| 10 |",
        "| 11 |",
        "| 12 |",
        "| 13 |",
        "| 14 |",
        "| 15 |",
        "| 16 |",
        "| 17 |",
        "| 18 |",
        "| 19 |",
        "+----+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_classic_checkpoint_parquet() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-classic-checkpoint-parquet")?;
    let mut expected = vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "| 10 |",
        "| 11 |",
        "| 12 |",
        "| 13 |",
        "| 14 |",
        "| 15 |",
        "| 16 |",
        "| 17 |",
        "| 18 |",
        "| 19 |",
        "+----+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_checkpoints_parquet_with_last_checkpoint() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-checkpoints-parquet-with-last-checkpoint")?;
    let mut expected = vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "+----+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[test]
fn v2_checkpoints_json_with_last_checkpoint() -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table("v2-checkpoints-json-with-last-checkpoint")?;
    let mut expected = vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "+----+",
    ];
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}
