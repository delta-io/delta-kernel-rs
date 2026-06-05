//! Shared test helpers for `delta_kernel_default_engine`'s own test suite.
//!
//! This crate sits inside `default-engine/` as a workspace member so default-engine's tests can
//! pull in common helpers (record-batch conversion, error assertions, etc.) without duplicating
//! them. It cannot live in the workspace `test_utils` crate because `test_utils` itself depends
//! on `delta_kernel_default_engine`, which would form an `default_engine -> test_utils ->
//! default_engine` cycle if added to default-engine as a normal dep. Keeping these helpers in
//! a sibling crate dev-depended by default-engine sidesteps that.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel::arrow::array::{RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{DeltaResult, EngineData, Error};

/// Convert an `EngineData` into a `RecordBatch`. Panics if the underlying engine data is not
/// `ArrowEngineData`.
pub fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
    ArrowEngineData::try_from_engine_data(engine_data)
        .unwrap()
        .into()
}

/// `?`-friendly variant of [`into_record_batch`] for use inside iterators that yield
/// `DeltaResult<Box<dyn EngineData>>`.
pub fn try_into_record_batch(
    engine_data: DeltaResult<Box<dyn EngineData>>,
) -> DeltaResult<RecordBatch> {
    engine_data
        .and_then(ArrowEngineData::try_from_engine_data)
        .map(Into::into)
}

/// Wrap a `StringArray` into the single-column `EngineData` shape expected by `parse_json`.
pub fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
    let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
    let schema = Arc::new(ArrowSchema::new(vec![string_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
        .expect("Can't convert to record batch");
    Box::new(ArrowEngineData::new(batch))
}

/// Returns the current time as a `Duration` since Unix epoch.
pub fn current_time_duration() -> DeltaResult<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("System time before Unix epoch: {e}")))
}

/// Returns the current time in milliseconds since Unix epoch.
pub fn current_time_ms() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    i64::try_from(duration.as_millis())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))
}

/// Assert that `res` is an `Err` whose `Display` contains `message`.
#[track_caller]
pub fn assert_result_error_with_message<T, E: ToString>(res: Result<T, E>, message: &str) {
    match res {
        Ok(_) => panic!("Expected error containing '{message}', but got Ok result"),
        Err(e) => {
            let actual = e.to_string();
            assert!(
                actual.contains(message),
                "Expected error containing '{message}', got: '{actual}'"
            );
        }
    }
}
