use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::actions::schemas::GetStructField;
use crate::schema::{SchemaRef, StructType};

pub mod iterators;
pub mod log_replay;

pub(crate) static CHECKPOINT_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![<Option<i64>>::get_struct_field(
        "sizeInBytes",
    )]))
});

/// Get the expected schema for engine data passed to [`into_checkpoint_metadata_iter`].
pub fn get_checkpoint_metadata_schema() -> &'static SchemaRef {
    &CHECKPOINT_METADATA_SCHEMA
}

pub(crate) static LAST_CHECKPOINT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        <i64>::get_struct_field("version"), // TODO: Should maybe be u64
        <i64>::get_struct_field("size"),
        <Option<i64>>::get_struct_field("parts"), // TODO: Should maybe be usize
        <Option<i64>>::get_struct_field("sizeInBytes"),
        <Option<i64>>::get_struct_field("numOfAddFiles"),
        <Option<String>>::get_struct_field("checksum"),
        // TODO: Add the checkpoint_schema field
    ]))
});

/// Get the expected schema for engine data to be written in the last checkpoint file.
pub fn get_last_checkpoint_schema() -> &'static SchemaRef {
    &LAST_CHECKPOINT_SCHEMA
}
