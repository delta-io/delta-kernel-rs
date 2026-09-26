use std::sync::Arc;

use delta_kernel::arrow::array::Int64Array;
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::{assert_result_error_with_message, begin_transaction};

use super::{
    assert_row_tracking_files, create_row_tracking_table, modified_row_tracking_batches,
    row_tracking_file_metadata, row_tracking_metadata_cases, selected_file_paths, RowTrackingState,
    ScanFileModification,
};
use crate::common::write_utils::sequential_dv_descriptors;

#[apply(row_tracking_metadata_cases)]
#[tokio::test]
async fn commit_validates_dv_row_tracking_metadata(
    row_tracking_state: RowTrackingState,
    selection_vector: &[bool],
    expected_error: Option<&str>,
    value: Option<i64>,
    field: &'static str,
    batch_index: usize,
    cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // === Given a table with row tracking in the requested state ===
    let (_temp_dir, engine, snapshot) = create_row_tracking_table(row_tracking_state, cm_mode)?;
    let (original, modified, batches) = modified_row_tracking_batches(
        snapshot.clone(),
        engine.as_ref(),
        ScanFileModification {
            field_name: field,
            value: Arc::new(Int64Array::from(vec![value])),
            row_id: 1,
        },
        selection_vector,
        batch_index,
    )?;
    let selected_paths = selected_file_paths(&original, selection_vector);
    let mut expected = row_tracking_file_metadata(&original);
    let modified_metadata = row_tracking_file_metadata(&modified);
    for path in &selected_paths {
        let mut metadata = modified_metadata[path];
        metadata.has_deletion_vector = true;
        expected.insert(path.clone(), metadata);
    }

    // === When committing DV updates for the selected files ===
    let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
    txn.ack_row_tracking_preservation();
    txn.update_deletion_vectors(
        sequential_dv_descriptors(&selected_paths),
        batches.into_iter().map(Ok),
    )?;
    let result = txn.commit(engine.as_ref());

    // === Expect a validation error or the exact surviving file metadata ===
    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(result, &format!("{expected_error} '{field}'"));
    } else {
        assert_row_tracking_files(
            result?.unwrap_post_commit_snapshot(),
            engine.as_ref(),
            &expected,
        )?;
    }
    Ok(())
}
