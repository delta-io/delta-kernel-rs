//! Tiny kernel [`StructType`] schema builders used across engine integration tests.

use std::sync::Arc;

use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

/// Schema with a single non-null `LONG` field named `x`.
///
/// Used as the canonical one-column LONG schema for scan/parity/window/state-machine tests.
pub fn single_long_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "x",
        DataType::LONG,
    )]))
}
