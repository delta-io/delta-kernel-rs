//! Output schemas for declarative plan nodes.

use std::sync::{Arc, LazyLock};

use crate::schema::{DataType, SchemaRef, StructField, StructType};

/// Schema for file metadata returned by storage-related plan nodes.
///
/// Columns:
/// - `path` (String, not null): fully qualified URL of the file
/// - `last_modified` (Long, not null): milliseconds since Unix epoch
/// - `size` (Long, not null): file size in bytes
// Safety: all field names are unique, so `try_new` cannot fail. The `expect` is acceptable
// because this is a statically-defined schema whose validity is a compile-time invariant.
#[allow(clippy::panic, clippy::expect_used)]
pub static FILE_META_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("last_modified", DataType::LONG),
            StructField::not_null("size", DataType::LONG),
        ])
        .expect("FILE_META_SCHEMA has unique field names"),
    )
});
