//! `ArrowOpaque*Op` impls for the FFI's `Named*` opaque ops.
//!
//! Kernel's `evaluate_expression` / `evaluate_predicate` already recurse
//! through compound shapes (Binary, Junction, etc.) and produce
//! `ArrayRef`s natively. This module is the leaf: when kernel hits an
//! opaque node, we evaluate each arg recursively, bundle the resulting
//! `ArrayRef`s as a `RecordBatch`, export through Arrow C Data Interface,
//! and hand them to the engine's callback. Engine only ever sees
//! pre-computed columns -- never the AST.
//!
//! Available only under `default-engine-base` (requires the kernel-side
//! arrow expression evaluator).

use std::sync::Arc;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{make_array, Array, ArrayRef, BooleanArray, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaqueExpressionOp, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{Expression, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error, Predicate};

use super::opaque_eval::OpaqueEvalCallbacks;
use super::{NamedOpaqueExpressionOp, NamedOpaquePredicateOp};
use crate::engine_data::ArrowFFIData;
use crate::kernel_string_slice;

/// Evaluate each arg using kernel's standard evaluator, then bundle the
/// resulting `ArrayRef`s as a single-batch `RecordBatch` keyed by
/// `arg0..argN`.
fn evaluate_args(args: &[Expression], batch: &RecordBatch) -> DeltaResult<RecordBatch> {
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|arg| evaluate_expression(arg, batch, None))
        .collect::<DeltaResult<_>>()?;

    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // If there are zero args, fall back to a single-column dummy of n_rows
    // so the engine still receives a batch with the correct row count.
    if arrays.is_empty() {
        let n_rows = batch.num_rows();
        return RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &delta_kernel::arrow::array::RecordBatchOptions::new().with_row_count(Some(n_rows)),
        )
        .map_err(|e| Error::Generic(format!("zero-arg opaque eval batch construction: {e}")));
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Generic(format!("opaque eval batch construction: {e}")))
}

/// Import an `ArrowFFIData` written by the engine back into an `ArrayRef`.
fn import_ffi_array(ffi: &mut ArrowFFIData) -> DeltaResult<ArrayRef> {
    // Take ownership of the FFI structs out of the result buffer so the
    // engine's release callbacks fire exactly once.
    let array = std::mem::replace(&mut ffi.array, FFI_ArrowArray::empty());
    let schema = std::mem::replace(&mut ffi.schema, FFI_ArrowSchema::empty());

    // SAFETY: the engine promised these structs are valid Arrow C Data
    // Interface payloads it produced and handed to us.
    let array_data = unsafe { from_ffi(array, &schema) }
        .map_err(|e| Error::Generic(format!("from_ffi: {e}")))?;
    Ok(make_array(array_data))
}

/// Engine returned an `ArrayRef` of the wrong top-level shape -- raise an
/// error rather than silently coercing.
fn require_boolean_array(arr: ArrayRef) -> DeltaResult<BooleanArray> {
    match arr.data_type() {
        ArrowDataType::Boolean => Ok(arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| Error::Generic("downcast to BooleanArray failed".into()))?
            .clone()),
        other => Err(Error::Generic(format!(
            "opaque predicate eval_pred returned non-boolean array of type {other:?}"
        ))),
    }
}

fn call_eval_pred(
    cb: &OpaqueEvalCallbacks,
    op_name: &str,
    args: &[Expression],
    batch: &RecordBatch,
    inverted: bool,
) -> DeltaResult<BooleanArray> {
    let eval_pred = cb.eval_pred.ok_or_else(|| {
        Error::Generic(format!(
            "opaque predicate `{op_name}` has no engine eval_pred callback registered"
        ))
    })?;

    let args_batch = evaluate_args(args, batch)?;
    let mut args_ffi = ArrowFFIData::try_from_record_batch(&args_batch)?;
    let mut result_ffi = ArrowFFIData::empty();

    // SAFETY: callback was supplied by the engine. We pass a writable
    // args slot (engine takes ownership of the Arrow FFI handles) and a
    // writable result slot (engine writes its output handles here).
    let ok = unsafe {
        eval_pred(
            cb.engine_state,
            kernel_string_slice!(op_name),
            &mut args_ffi,
            inverted,
            &mut result_ffi,
        )
    };
    if !ok {
        return Err(Error::Generic(format!(
            "engine eval_pred reported failure for `{op_name}`"
        )));
    }

    let arr = import_ffi_array(&mut result_ffi)?;
    require_boolean_array(arr)
}

fn call_eval_expr(
    cb: &OpaqueEvalCallbacks,
    op_name: &str,
    args: &[Expression],
    batch: &RecordBatch,
) -> DeltaResult<ArrayRef> {
    let eval_expr = cb.eval_expr.ok_or_else(|| {
        Error::Generic(format!(
            "opaque expression `{op_name}` has no engine eval_expr callback registered"
        ))
    })?;

    let args_batch = evaluate_args(args, batch)?;
    let mut args_ffi = ArrowFFIData::try_from_record_batch(&args_batch)?;
    let mut result_ffi = ArrowFFIData::empty();

    let ok = unsafe {
        eval_expr(
            cb.engine_state,
            kernel_string_slice!(op_name),
            &mut args_ffi,
            &mut result_ffi,
        )
    };
    if !ok {
        return Err(Error::Generic(format!(
            "engine eval_expr reported failure for `{op_name}`"
        )));
    }

    import_ffi_array(&mut result_ffi)
}

// === ArrowOpaquePredicateOp ====================================================

impl ArrowOpaquePredicateOp for NamedOpaquePredicateOp {
    fn name(&self) -> &str {
        self.op_name()
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let cb = self.callbacks_clone().ok_or_else(|| {
            Error::Generic(format!(
                "opaque predicate `{}` has no eval context attached",
                self.op_name()
            ))
        })?;
        call_eval_pred(&cb, self.op_name(), args, batch, inverted)
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        Ok(None)
    }

    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        None
    }

    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        None
    }
}

// === ArrowOpaqueExpressionOp ===================================================

impl ArrowOpaqueExpressionOp for NamedOpaqueExpressionOp {
    fn name(&self) -> &str {
        self.op_name()
    }

    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        let cb = self.callbacks_clone().ok_or_else(|| {
            Error::Generic(format!(
                "opaque expression `{}` has no eval context attached",
                self.op_name()
            ))
        })?;
        call_eval_expr(&cb, self.op_name(), args, batch)
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expression],
    ) -> DeltaResult<delta_kernel::expressions::Scalar> {
        Err(Error::Generic(format!(
            "opaque FFI expression `{}` does not support scalar evaluation",
            self.op_name()
        )))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ffi::c_void;
    use std::ptr;
    use std::sync::Arc;

    use delta_kernel::arrow::array::ffi::from_ffi;
    use delta_kernel::arrow::array::{
        ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
    use delta_kernel::engine::arrow_expression::opaque::{
        ArrowOpaqueExpression as _, ArrowOpaquePredicate as _,
    };
    use delta_kernel::expressions::{column_expr, Expression, Predicate};

    use super::*;
    use crate::expressions::opaque_eval::OpaqueEvalCallbacks;
    use crate::KernelStringSlice;

    // ============================================================================
    // Engine-side test stubs: a tiny "engine" written in Rust that does row-time
    // STARTS_WITH(args[0], args[1]) and LOWER(args[0]) by walking the
    // pre-evaluated arg columns. No AST involved.
    // ============================================================================

    /// Take ownership of the FFI handles in `args_in`, leaving the slot empty,
    /// then import as a `RecordBatch`. Mirrors how a real engine consumes
    /// kernel-produced FFI args (move out -> from_ffi -> array import).
    unsafe fn take_ffi_record_batch(args_in: *mut ArrowFFIData) -> RecordBatch {
        // SAFETY: kernel exported a valid ArrowFFIData via try_from_record_batch
        // and handed us a writable slot; we replace the live handles with empties
        // so kernel's Drop on the slot is a no-op (no double-free).
        let array = std::mem::replace(unsafe { &mut (*args_in).array }, FFI_ArrowArray::empty());
        let schema = std::mem::replace(unsafe { &mut (*args_in).schema }, FFI_ArrowSchema::empty());
        let array_data = unsafe { from_ffi(array, &schema) }.unwrap();
        let sa = StructArray::from(array_data);
        sa.into()
    }

    /// Write a result `ArrayRef` back into the engine's `result_out` slot,
    /// exporting it through Arrow C Data Interface.
    fn write_result(result_out: *mut ArrowFFIData, arr: ArrayRef) {
        // Wrap as a top-level array via try_from_record_batch (single-column batch).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            arr.data_type().clone(),
            true,
        )]));
        let _batch = RecordBatch::try_new(schema, vec![arr.clone()]).unwrap();

        // Convert ArrayRef directly to FFI struct, NOT struct-wrapped, since our
        // import path in arrow_eval expects a top-level array shape.
        let array_data = arr.to_data();
        let array_ffi = delta_kernel::arrow::array::ffi::FFI_ArrowArray::new(&array_data);
        let schema_ffi =
            delta_kernel::arrow::array::ffi::FFI_ArrowSchema::try_from(array_data.data_type())
                .unwrap();
        // SAFETY: result_out is a writable slot kernel handed to the engine.
        unsafe {
            (*result_out).array = array_ffi;
            (*result_out).schema = schema_ffi;
        }
    }

    /// Engine eval_pred stub: implements `STARTS_WITH(args[0], args[1])`.
    /// Both args arrive as pre-evaluated String columns. Engine produces a
    /// `BooleanArray` of n_rows.
    unsafe extern "C" fn engine_starts_with(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        inverted: bool,
        result_out: *mut ArrowFFIData,
    ) -> bool {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        assert_eq!(batch.num_columns(), 2);
        let lhs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg0 should be StringArray after kernel pre-eval");
        let rhs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg1 should be StringArray after kernel pre-eval");
        let out: BooleanArray = (0..batch.num_rows())
            .map(|i| match (lhs.is_valid(i), rhs.is_valid(i)) {
                (true, true) => {
                    let m = lhs.value(i).starts_with(rhs.value(i));
                    Some(if inverted { !m } else { m })
                }
                _ => None,
            })
            .collect();
        write_result(result_out, Arc::new(out));
        true
    }

    /// Engine eval_expr stub: implements `LOWER(args[0])` on String inputs.
    unsafe extern "C" fn engine_lower(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        result_out: *mut ArrowFFIData,
    ) -> bool {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        assert_eq!(batch.num_columns(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("LOWER arg0 should be StringArray");
        let out: StringArray = (0..col.len())
            .map(|i| col.is_valid(i).then(|| col.value(i).to_lowercase()))
            .collect();
        write_result(result_out, Arc::new(out));
        true
    }

    unsafe extern "C" fn noop_free(_state: *mut c_void) {}

    fn lower_starts_with_callbacks() -> Arc<OpaqueEvalCallbacks> {
        Arc::new(OpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred: Some(engine_starts_with),
            eval_expr: Some(engine_lower),
            free_state: noop_free,
        })
    }

    fn batch_with_col(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Utf8,
            true,
        )]));
        let arr: ArrayRef = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    // ============================================================================
    // End-to-end: kernel pre-evaluates LOWER(col), hands engine the lowercased
    // column + the literal, engine does STARTS_WITH.
    // ============================================================================

    #[test]
    fn end_to_end_starts_with_lower_col_foo() {
        let cb = lower_starts_with_callbacks();
        let lower_op = NamedOpaqueExpressionOp::with_callbacks("LOWER", cb.clone());
        let starts_with_op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb);

        // STARTS_WITH(LOWER(col), "foo")
        let pred = Predicate::arrow_opaque(
            starts_with_op,
            [
                Expression::arrow_opaque(lower_op, [column_expr!("col")]),
                Expression::literal("foo"),
            ],
        );

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("Apple"), None, Some("foozzz")]);
        let result = evaluate_predicate(&pred, &batch, false).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0), "FOOBAR -> foobar starts_with foo");
        assert!(!result.value(1), "Apple -> apple does not start_with foo");
        assert!(result.is_null(2), "null col -> null verdict");
        assert!(result.value(3), "foozzz starts_with foo");
    }

    #[test]
    fn end_to_end_inversion_flips_verdicts() {
        let cb = lower_starts_with_callbacks();
        let lower_op = NamedOpaqueExpressionOp::with_callbacks("LOWER", cb.clone());
        let starts_with_op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb);

        let pred = Predicate::arrow_opaque(
            starts_with_op,
            [
                Expression::arrow_opaque(lower_op, [column_expr!("col")]),
                Expression::literal("foo"),
            ],
        );

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("Apple")]);
        let result = evaluate_predicate(&pred, &batch, true).unwrap();
        assert!(!result.value(0), "inverted: FOOBAR now does not match");
        assert!(result.value(1), "inverted: Apple now matches");
    }

    // ============================================================================
    // Composability: kernel handles AND of two opaque predicates natively. Engine
    // gets two independent callbacks, kernel combines via standard Arrow boolean AND.
    // ============================================================================

    #[test]
    fn composition_and_of_two_opaque_predicates() {
        let cb = lower_starts_with_callbacks();
        let lhs = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb.clone()),
            [column_expr!("col"), Expression::literal("F")],
        );
        let rhs = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb),
            [column_expr!("col"), Expression::literal("FO")],
        );
        let and_pred = Predicate::and_from([lhs, rhs]);

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("FBAR"), Some("BAR")]);
        let result = evaluate_predicate(&and_pred, &batch, false).unwrap();
        assert!(result.value(0), "FOOBAR starts with F AND FO");
        assert!(!result.value(1), "FBAR starts with F but not FO");
        assert!(!result.value(2), "BAR starts with neither");
    }

    // ============================================================================
    // Negative paths: missing callback errors cleanly.
    // ============================================================================

    #[test]
    fn missing_eval_pred_callback_errors() {
        // Callbacks struct provides eval_expr but not eval_pred.
        let cb = Arc::new(OpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred: None,
            eval_expr: Some(engine_lower),
            free_state: noop_free,
        });
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("MISSING_PRED", cb),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(
            format!("{err}").contains("has no engine eval_pred callback"),
            "expected eval_pred missing error, got: {err}"
        );
    }

    #[test]
    fn engine_returning_false_surfaces_error() {
        unsafe extern "C" fn engine_fail(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            _args_in: *mut ArrowFFIData,
            _inverted: bool,
            _result_out: *mut ArrowFFIData,
        ) -> bool {
            false
        }
        let cb = Arc::new(OpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred: Some(engine_fail),
            eval_expr: None,
            free_state: noop_free,
        });
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("ALWAYS_FAIL", cb),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("engine eval_pred reported failure"));
    }
}
