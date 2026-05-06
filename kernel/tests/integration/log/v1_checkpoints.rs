//! Integration tests for V1 checkpoints written via `Snapshot::checkpoint`.

use std::collections::HashMap;

use delta_kernel::arrow::array::RecordBatchReader;
use delta_kernel::checkpoint::CheckpointSpec;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::test_utils::{create_table_and_load_snapshot, test_table_setup_mt, write_batch_to_table};

use crate::common::write_utils::{
    get_simple_schema, load_existing_single_file_checkpoint_path, simple_id_batch,
};

/// On a V1 table (no `v2Checkpoint` feature), passing either `None` or
/// `Some(CheckpointSpec::V1)` to `snapshot.checkpoint()` produces a V1 checkpoint: the main
/// parquet schema must not contain a `checkpointMetadata` column.
#[rstest::rstest]
#[case::none(None)]
#[case::v1(Some(CheckpointSpec::V1))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_checkpoint_on_v1_table(
    #[case] spec: Option<CheckpointSpec>,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_schema();
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let mut snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    snapshot = write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        simple_id_batch(&schema, vec![1, 2]),
        HashMap::new(),
    )
    .await?;

    let version = snapshot.version();
    snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;

    let ckpt_path = load_existing_single_file_checkpoint_path(&table_path, version);
    let file = std::fs::File::open(&ckpt_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let schema = reader.schema();
    assert!(
        schema.field_with_name("checkpointMetadata").is_err(),
        "V1 checkpoint must not contain `checkpointMetadata` column, found schema: {schema:?}"
    );

    Ok(())
}
