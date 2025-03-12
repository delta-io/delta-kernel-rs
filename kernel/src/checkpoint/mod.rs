use std::sync::{Arc, LazyLock};

use crate::actions::schemas::GetStructField;
use crate::actions::{
    Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, METADATA_NAME,
    PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::schema::{SchemaRef, StructType};

pub mod iterators;
pub mod log_replay;

pub(crate) static CHECKPOINT_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![<Option<i64>>::get_struct_field(
        "sizeInBytes",
    )]))
});

static V1_CHECKPOINT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
    ])
    .into()
});

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

pub(crate) static V2_CHECKPOINT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<Sidecar>::get_struct_field(SIDECAR_NAME),
    ])
    .into()
});

pub(crate) static SIDECAR_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
    ])
    .into()
});

pub(crate) static SIDECAR_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| StructType::new([Option::<Sidecar>::get_struct_field(SIDECAR_NAME)]).into());

/// Get the expected schema for engine data passed to [`into_checkpoint_metadata_iter`].
pub fn get_checkpoint_metadata_schema() -> &'static SchemaRef {
    &CHECKPOINT_METADATA_SCHEMA
}

/// Get the expected schema for engine data to be written in the last checkpoint file.
pub fn get_last_checkpoint_schema() -> &'static SchemaRef {
    &LAST_CHECKPOINT_SCHEMA
}

/// Get the expected schema for engine data to be written in the V1 checkpoint file.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_v1_checkpoint_schema() -> &'static SchemaRef {
    &V1_CHECKPOINT_SCHEMA
}

/// Get the expected schema for engine data to be written in the V2 checkpoint file.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_v2_checkpoint_schema() -> &'static SchemaRef {
    &V2_CHECKPOINT_SCHEMA
}

/// Get the expected schema for engine data to be written in sidecar files.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_sidecar_file_schema() -> &'static SchemaRef {
    &SIDECAR_FILE_SCHEMA
}

fn get_sidecar_schema() -> &'static SchemaRef {
    &SIDECAR_SCHEMA
}
