//! [`ScalarUDF`] that stamps the kernel's logical [`Field`] (logical name + Delta
//! metadata + nested struct/list/map field names + per-level metadata) onto a single
//! input column.
//!
//! Sits at the top of
//! [`DataFusionExecutor::read_relation`](crate::DataFusionExecutor::read_relation)'s projection: at
//! runtime it casts the input array to the target [`DataType`] via [`arrow::compute::cast`]
//! (positional, metadata-preserving -- the same primitive the historical `stamp_batch_metadata`
//! used post-collect); at plan time [`return_field_from_args`] declares the target [`FieldRef`]
//! verbatim so DataFusion's projection output schema carries the full nested metadata declaration
//! too.
//!
//! Why a UDF instead of `Expr::Cast` or `Expr::Alias`:
//! - `Expr::Cast(_, ArrowDataType)` carries only the data type, no metadata, and DataFusion's
//!   logical-cast validation rejects struct-to-struct casts whose source and target field names
//!   don't overlap (column-mapping renames trip this).
//! - `Expr::Alias(_, name)` carries only the name, no metadata.
//! - `Expr::ScalarFunction(udf)` with [`ScalarUDFImpl::return_field_from_args`] overridden can
//!   declare the projection's output [`FieldRef`] verbatim. Per the `return_field_from_args` doc
//!   (`datafusion/expr/src/udf.rs:644`) the top-level field name is ignored for primitives (callers
//!   must `.alias(...)`) but is honored for structured types and the field's metadata flows through
//!   to the projection's output schema in either case. The runtime `cast` keeps the materialized
//!   batch's schema byte-for-byte aligned with the declaration so DataFusion's projection
//!   `result_data_type == expected_type` assertion holds.

use std::sync::Arc;

use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Fields as ArrowFields,
};
use datafusion_common::Result as DfResult;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// A no-op [`ScalarUDF`] whose only job is to re-declare its argument's output [`Field`]
/// (and therefore its metadata + nested field names) without touching the underlying
/// array data. See module docs.
///
/// `Hash`/`Eq` are required by [`ScalarUDFImpl`] (DataFusion uses them for UDF dedup in
/// the optimizer). `Field` implements both, so the derives flow through `FieldRef`.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StampFieldUdf {
    /// The [`FieldRef`] this UDF will return from [`Self::return_field_from_args`].
    /// Held in an `Arc` so wrapping it into multiple `ScalarFunction` expressions stays
    /// cheap.
    target_field: FieldRef,
    /// One-arg signature: takes any single type, returns the same type at runtime
    /// (data passes through unchanged).
    signature: Signature,
}

impl StampFieldUdf {
    /// Build a UDF that stamps `target_field` onto its single argument. The runtime
    /// invocation is a passthrough; the projection's *schema* picks up
    /// `target_field` verbatim.
    pub fn new(target_field: FieldRef) -> Self {
        Self {
            target_field,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Wrap this UDF over `arg` as a `ScalarFunction` expression. The resulting `Expr`
    /// is suitable to feed directly to `DataFrame::select`.
    pub fn call(self, arg: Expr) -> Expr {
        let udf = Arc::new(ScalarUDF::from(self));
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![arg]))
    }
}

impl ScalarUDFImpl for StampFieldUdf {
    fn name(&self) -> &str {
        "kernel_stamp_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DfResult<ArrowDataType> {
        Ok(self.target_field.data_type().clone())
    }

    /// Build the projection's output [`FieldRef`] by merging the kernel target's
    /// names + metadata with the input's runtime nullability via [`merge_field`]:
    ///
    /// - The outer field name + metadata + nested field names + nested metadata come from
    ///   `self.target_field` so the projection's schema carries the full kernel logical declaration
    ///   (recursively).
    /// - Nullability at every level comes from the input field so the cast in
    ///   [`Self::invoke_with_args`] -- which respects the *outer* target type but does NOT tighten
    ///   the nullability of the array's actual null buffer -- can produce an output whose
    ///   `data_type()` matches what we declare here. The alternative (claim target's non-null
    ///   promises) trips DataFusion's projection-executor `result_data_type == expected_type`
    ///   assertion when the runtime data has nulls (e.g. `named_struct(...)` always produces
    ///   nullable fields).
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        let input = args.arg_fields.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{}: expected one argument field; got {}",
                self.name(),
                args.arg_fields.len()
            ))
        })?;
        Ok(merge_field(input, &self.target_field))
    }

    /// Cast the input array to the merged target data type carried on
    /// `args.return_field` (the same field we built in
    /// [`Self::return_field_from_args`]). The cast uses [`arrow::compute::cast`]
    /// (positional, metadata-preserving -- the same primitive the historical
    /// `stamp_batch_metadata` used post-collect):
    ///
    /// - Primitive cast: type coercion (`Int64` -> `Long` is a noop) with the outer projection
    ///   [`Expr::alias`](datafusion_expr::Expr::alias) supplying the column name.
    /// - Struct cast: positional rebuild of the struct array with the target's field names +
    ///   metadata (matches by position, NOT by name -- this is the whole reason we use
    ///   [`arrow::compute::cast`] over DataFusion's logical `Expr::Cast`, which name-validates and
    ///   rejects column-mapping renames).
    /// - List / Map cast: recursive descent that stamps each nested element field.
    ///
    /// Scalar inputs flow through unchanged: cast on a length-1 array reshapes the
    /// scalar but `ScalarValue::try_from_array` of a freshly cast scalar can lose
    /// timezone/precision info on some types, and the projection executor handles
    /// scalar-to-array materialization itself when the surrounding plan needs an
    /// array form. Pass-through is the safe baseline.
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg = args.args.swap_remove(0);
        let target_dt = args.return_field.data_type();
        match arg {
            ColumnarValue::Array(arr) if arr.data_type() == target_dt => {
                Ok(ColumnarValue::Array(arr))
            }
            ColumnarValue::Array(arr) => {
                let casted = cast(arr.as_ref(), target_dt)?;
                Ok(ColumnarValue::Array(casted))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s)),
        }
    }
}

/// Merge `target`'s logical name + metadata onto `input`'s runtime nullability,
/// recursively into nested struct / list / large-list / fixed-size-list / map
/// types. The merge is asymmetric on purpose:
///
/// - **Names + metadata come from `target`.** This is the kernel's logical schema declaration
///   (`delta.columnMapping.*` / `parquet.field.id`) that the projection needs to surface on its
///   output schema and on every batch downstream.
/// - **Nullability comes from `input`.** DataFusion's projection executor asserts that the runtime
///   array's `data_type()` matches what [`ScalarUDFImpl::return_field_from_args`] declared. The
///   runtime cast in [`StampFieldUdf::invoke_with_args`] preserves the array's actual null buffer
///   without re-checking it against tighter nullability promises, so claiming `non-null` on a level
///   where the runtime can carry nulls would trip the assertion.
///
/// When source and target arities don't line up at a particular level (only
/// possible if upstream rename is buggy), the merge falls back to the input field
/// at that level; the cast in `invoke_with_args` will then fail with a clearer
/// arrow error.
fn merge_field(input: &FieldRef, target: &FieldRef) -> FieldRef {
    let merged_dt = merge_data_type(input.data_type(), target.data_type());
    let mut field = ArrowField::new(target.name(), merged_dt, input.is_nullable());
    if !target.metadata().is_empty() {
        field = field.with_metadata(target.metadata().clone());
    }
    Arc::new(field)
}

fn merge_data_type(input_dt: &ArrowDataType, target_dt: &ArrowDataType) -> ArrowDataType {
    match (input_dt, target_dt) {
        (ArrowDataType::Struct(input_fields), ArrowDataType::Struct(target_fields))
            if input_fields.len() == target_fields.len() =>
        {
            ArrowDataType::Struct(merge_fields(input_fields, target_fields))
        }
        (ArrowDataType::List(input_elem), ArrowDataType::List(target_elem)) => {
            ArrowDataType::List(merge_field(input_elem, target_elem))
        }
        (ArrowDataType::LargeList(input_elem), ArrowDataType::LargeList(target_elem)) => {
            ArrowDataType::LargeList(merge_field(input_elem, target_elem))
        }
        (
            ArrowDataType::FixedSizeList(input_elem, input_n),
            ArrowDataType::FixedSizeList(target_elem, _),
        ) => ArrowDataType::FixedSizeList(merge_field(input_elem, target_elem), *input_n),
        (ArrowDataType::Map(input_kv, input_sorted), ArrowDataType::Map(target_kv, _)) => {
            ArrowDataType::Map(merge_field(input_kv, target_kv), *input_sorted)
        }
        // Primitive or mismatched-shape: keep the input's runtime type. Metadata at
        // this level flows through the parent `merge_field` call regardless.
        _ => input_dt.clone(),
    }
}

fn merge_fields(input_fields: &ArrowFields, target_fields: &ArrowFields) -> ArrowFields {
    input_fields
        .iter()
        .zip(target_fields.iter())
        .map(|(i, t)| merge_field(i, t))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::execution::context::SessionContext;
    use datafusion_common::arrow::array::{Int64Array, RecordBatch};
    use datafusion_common::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion_expr::col;

    use super::*;

    fn stamped_field(id: &str) -> FieldRef {
        Arc::new(
            ArrowField::new("x", ArrowDataType::Int64, true).with_metadata(HashMap::from([(
                "delta.columnMapping.id".to_string(),
                id.to_string(),
            )])),
        )
    }

    #[tokio::test]
    async fn stamp_udf_carries_field_metadata_through_projection() {
        let ctx = SessionContext::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            ArrowDataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();

        // Project `x` through the stamp UDF declaring metadata on the output Field;
        // then collect to confirm the metadata reaches the materialized batch schema.
        let df = ctx
            .read_batch(batch)
            .expect("read batch")
            .select(vec![StampFieldUdf::new(stamped_field("7"))
                .call(col("x"))
                .alias("x")])
            .expect("select");

        let logical_meta = df.schema().field(0).metadata().clone();
        assert_eq!(
            logical_meta.get("delta.columnMapping.id"),
            Some(&"7".to_string()),
            "stamp UDF must surface the declared FieldRef metadata into the \
             projection's output schema"
        );

        let collected = df.collect().await.expect("collect");
        let runtime_meta = collected[0].schema().field(0).metadata().clone();
        assert_eq!(
            runtime_meta.get("delta.columnMapping.id"),
            Some(&"7".to_string()),
            "stamped metadata must also flow through to the materialized batch schema"
        );
    }
}
