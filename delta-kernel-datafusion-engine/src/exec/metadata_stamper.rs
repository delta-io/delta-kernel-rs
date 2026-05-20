//! Re-stamp arrow field-declaration metadata so engine batches match the kernel-declared
//! schema byte-for-byte (DataFusion's native struct/list/map primitives don't carry the
//! Delta-protocol `delta.columnMapping.*` / `parquet.field.id` metadata).

use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use delta_kernel::arrow::array::{ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::cast;
use delta_kernel::arrow::datatypes::SchemaRef;

/// Zero-copy re-stamp every top-level column of `batch` so the resulting schema matches
/// `target` byte-for-byte. Shape (column count, names, leaf types, nullability) must already
/// agree with `target`.
pub(crate) fn stamp_batch_metadata(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(target.fields().iter())
        .map(|(col, field)| cast(col.as_ref(), field.data_type()).map_err(DataFusionError::from))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(Arc::clone(target), cols).map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use delta_kernel::arrow::array::{Array, Int64Array, StringArray, StructArray};
    use delta_kernel::arrow::buffer::NullBuffer;
    use delta_kernel::arrow::datatypes::{DataType, Field, Fields, Schema};

    use super::*;

    fn meta(field: Field, k: &str, v: &str) -> Field {
        field.with_metadata(HashMap::from([(k.to_string(), v.to_string())]))
    }

    #[test]
    fn stamps_nested_struct_metadata_and_preserves_parent_null_bitmap() {
        let inner_meta = |name: &str, ty, id: &str| {
            meta(Field::new(name, ty, true), "delta.columnMapping.id", id)
        };
        let target_inner = Fields::from(vec![
            inner_meta("name", DataType::Utf8, "3"),
            inner_meta("score", DataType::Int64, "4"),
        ]);
        let target = Arc::new(Schema::new(vec![Field::new(
            "info",
            DataType::Struct(target_inner.clone()),
            true,
        )]));

        let source_inner = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
        ]);
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let scores: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let nulls = NullBuffer::from(vec![true, false]);
        let struct_arr = StructArray::new(source_inner.clone(), vec![names, scores], Some(nulls));
        let source = Arc::new(Schema::new(vec![Field::new(
            "info",
            struct_arr.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(source, vec![Arc::new(struct_arr)]).unwrap();

        let stamped = stamp_batch_metadata(&batch, &target).unwrap();
        assert_eq!(stamped.schema(), target);
        let out = stamped
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(
            !out.is_valid(1),
            "parent struct null bitmap must survive cast"
        );
    }
}
