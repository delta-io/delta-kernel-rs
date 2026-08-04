//! Engine infrastructure shared by `Engine` implementations.
//!
//! The default Arrow/Tokio engine lives in the separate `delta_kernel_default_engine` crate.
//! `SyncEngine` is included only in test builds.

#[cfg(feature = "arrow-expression")]
use delta_kernel_derive::internal_api;

#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_reader::ArrowReaderOptions;
#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_writer::ArrowWriterOptions;

/// Returns the standard [`ArrowReaderOptions`] for all default engine parquet reads.
///
/// Skipping the embedded Arrow IPC schema avoids dependence on Arrow-specific metadata and
/// ensures that type resolution is driven by the kernel schema rather than the file's schema.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn reader_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_skip_arrow_metadata(true)
}

/// Returns the standard [`ArrowWriterOptions`] for all kernel parquet writes.
///
/// Omitting the Arrow IPC schema from the file metadata keeps Delta files interoperable with
/// non-Arrow readers and avoids encoding Arrow-specific type information.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn writer_options() -> ArrowWriterOptions {
    ArrowWriterOptions::new().with_skip_arrow_metadata(true)
}

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "arrow-expression", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "arrow-expression", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(all(feature = "internal-api", feature = "arrow-expression"))]
pub use self::arrow_utils::{parse_json, to_json_bytes};

// The plan executor support modules read Arrow data (`arrow_utils`, `arrow_data`), so they
// require the Arrow engine base in addition to the declarative-plans IR.
#[cfg(all(feature = "declarative-plans", feature = "default-engine-base"))]
pub mod plans;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;

/// Shared fixtures for the `arrow_data` / `arrow_get_data` struct-list tests.
#[cfg(all(test, feature = "default-engine-base"))]
pub(crate) mod struct_list_test_support {
    use std::sync::{Arc, LazyLock};

    use crate::arrow::array::{ArrayRef, GenericListArray, Int32Array, StructArray};
    use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
    use crate::engine_data::{GetData, RowVisitor};
    use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
    use crate::DeltaResult;

    /// A [`RowVisitor`] that collects the `n` (int) field of each visited element struct.
    #[derive(Default)]
    pub(crate) struct CollectNVisitor {
        pub(crate) values: Vec<i32>,
    }

    impl RowVisitor for CollectNVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NT: LazyLock<ColumnNamesAndTypes> =
                LazyLock::new(|| (vec![ColumnName::new(["n"])], vec![DataType::INTEGER]).into());
            NT.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                if let Some(n) = getters[0].get_int(i, "n")? {
                    self.values.push(n);
                }
            }
            Ok(())
        }
    }

    /// Build an `array<struct<n: int>>` from per-row element values (e.g. `[[10, 20], [30]]`).
    pub(crate) fn struct_list_fixture(rows: &[&[i32]]) -> GenericListArray<i32> {
        let flat: Vec<i32> = rows.iter().flat_map(|r| r.iter().copied()).collect();
        let n = Arc::new(Int32Array::from(flat)) as ArrayRef;
        let element_fields: Fields = vec![ArrowField::new("n", ArrowDataType::Int32, false)].into();
        let elements = StructArray::new(element_fields.clone(), vec![n], None);
        let item_field = Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Struct(element_fields),
            false,
        ));
        let mut offsets = vec![0i32];
        for r in rows {
            offsets.push(offsets.last().unwrap() + r.len() as i32);
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        GenericListArray::new(item_field, offsets, Arc::new(elements), None)
    }
}
#[cfg(all(feature = "default-engine-base", feature = "internal-api"))]
pub mod ensure_data_types;
#[cfg(all(feature = "default-engine-base", not(feature = "internal-api")))]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
// module is always pub; trait inside is gated by #[internal_api]
pub mod parquet_row_group_skipping;
