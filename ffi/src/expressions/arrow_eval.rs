//! `FfiOpaquePredicateOp`: an engine-defined opaque predicate evaluated through callbacks.
//!
//! Kernel's `evaluate_predicate` recurses through compound expressions (Binary, Junction, etc.)
//! natively. This module is the leaf: when kernel hits an opaque predicate, we evaluate each arg
//! via `evaluate_expression`, bundle the resulting `ArrayRef`s as a `RecordBatch`, export through
//! Arrow C Data Interface, and hand them to the engine's callback. Engine receives pre-computed
//! columns, never the AST.
//!
//! Available only under `default-engine-base` (requires the kernel-side arrow expression
//! evaluator).

use std::sync::Arc;

use delta_kernel::arrow::array::ffi::from_ffi;
use delta_kernel::arrow::array::{
    make_array, Array, ArrayRef, BooleanArray, RecordBatch, StructArray,
};
use delta_kernel::arrow::datatypes::{Field, Fields, Schema};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaquePredicate as _, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{Expression, ExpressionRef, Scalar, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator, KernelPredicateEvaluator,
};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error, Predicate};

use super::opaque_eval::{COpaqueEvalCallbacks, FfiOpaqueEvalCallbacks};
use crate::engine_data::ArrowFFIData;
use crate::{kernel_string_slice, OptionalValue};

// === FfiOpaquePredicateOp ===================================================

/// Which engine callback an opaque op dispatches to (set by the stats rewrite; internal to the
/// FFI layer, not part of the ABI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvalMode {
    /// Row-time evaluation -> `eval_pred_rows`.
    RowMode,
    /// Stats-based evaluation (file data skipping) -> `eval_pred_stats`.
    StatsMode,
}

/// Engine-defined opaque predicate identified by a name (e.g. `STARTS_WITH`, `LIKE`). Kernel
/// routes evaluation through the engine's [`COpaqueEvalCallbacks`]: the engine receives
/// pre-evaluated args as Arrow arrays and returns one verdict per row (or per file, in stats
/// mode).
///
/// # Data skipping
///
/// `as_data_skipping_predicate` rewrites each `Column` arg into a
/// `Struct[min, max, nullcount, rowcount]` and tags the op with `EvalMode::StatsMode`. Literals
/// pass through; `Predicate` children recurse. This is the only mode that prunes files -- RowMode
/// evaluates per data row and never drops files, and the scalar / partition-pruning paths always
/// abstain. See [`EngineEvalRowsFn`] / [`EngineEvalStatsFn`] for the per-callback arg shapes.
///
/// Inverted predicates (`NOT op`) are rewritten too: the inversion is recorded on the op and
/// forwarded to the engine callback's `inverted` flag, which must reason about the negated op (see
/// [`EngineEvalStatsFn`]). The StatsMode rewrite abstains (keeps all files) when a `Column` arg has
/// no sibling literal to infer its type from, or an arg is some kind other than `Column`,
/// `Literal`, or `Predicate`.
///
/// [`EngineEvalRowsFn`]: super::opaque_eval::EngineEvalRowsFn
/// [`EngineEvalStatsFn`]: super::opaque_eval::EngineEvalStatsFn
#[derive(Debug, Clone)]
pub(crate) struct FfiOpaquePredicateOp {
    name: String,
    callbacks: Arc<FfiOpaqueEvalCallbacks>,
    /// Which engine callback eval dispatches to.
    mode: EvalMode,
    /// Whether the op is negated. Set by the StatsMode rewrite so the eval callback can reason
    /// about the negated op (see [`EngineEvalStatsFn`]); XORed with the eval-time `inverted`.
    ///
    /// [`EngineEvalStatsFn`]: super::opaque_eval::EngineEvalStatsFn
    inverted: bool,
}

impl FfiOpaquePredicateOp {
    /// Build a RowMode, non-inverted op identified by `name`, evaluated via `callbacks`.
    pub(crate) fn new(name: impl Into<String>, callbacks: Arc<FfiOpaqueEvalCallbacks>) -> Self {
        Self {
            name: name.into(),
            callbacks,
            mode: EvalMode::RowMode,
            inverted: false,
        }
    }

    /// Consume `self` and return it with `mode` overridden.
    fn with_mode(mut self, mode: EvalMode) -> Self {
        self.mode = mode;
        self
    }

    /// Consume `self` and return it with `inverted` overridden.
    fn with_inverted(mut self, inverted: bool) -> Self {
        self.inverted = inverted;
        self
    }
}

impl PartialEq for FfiOpaquePredicateOp {
    fn eq(&self, other: &Self) -> bool {
        // Mode, inversion, and callbacks all change how the op evaluates, so equality includes
        // them. Callbacks compare by `Arc` pointer identity (not otherwise comparable).
        self.name == other.name
            && self.mode == other.mode
            && self.inverted == other.inverted
            && Arc::ptr_eq(&self.callbacks, &other.callbacks)
    }
}

impl Eq for FfiOpaquePredicateOp {}

/// Pre-evaluate each arg into an `ArrayRef`, bundle them as a `RecordBatch` keyed `arg0..argN`.
///
/// `Expression::Struct` args (produced by the stats-mode rewrite) get a dedicated path because
/// kernel's evaluator needs a `DataType::Struct` result type to name fields, which we don't have.
fn evaluate_args(args: &[Expression], batch: &RecordBatch) -> DeltaResult<RecordBatch> {
    // Zero-arg ops (e.g. NOW(), RAND()): empty-schema batch with explicit row count so the
    // engine knows how many rows to emit.
    if args.is_empty() {
        let n_rows = batch.num_rows();
        return RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &delta_kernel::arrow::array::RecordBatchOptions::new().with_row_count(Some(n_rows)),
        )
        .map_err(|e| Error::Generic(format!("zero-arg opaque eval batch construction: {e}")));
    }

    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|arg| match arg {
            Expression::Struct(fields, _nullability) => evaluate_struct_arg(fields, batch),
            _ => evaluate_expression(arg, batch, None),
        })
        .collect::<DeltaResult<_>>()?;

    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Generic(format!("opaque eval batch construction: {e}")))
}

/// Lift a column type hint from the first `Literal` arg (kernel-native lifts the same way from
/// the right-hand `Scalar` in `col < val`). Returns `None` when there's no literal to borrow.
fn pick_column_type_hint(exprs: &[Expression]) -> Option<DataType> {
    exprs.iter().find_map(|e| match e {
        Expression::Literal(scalar) => Some(scalar.data_type()),
        _ => None,
    })
}

/// Rewrite a single opaque-predicate arg into its stats-mode form. `Column` -> a positional
/// `Struct[min, max, nullcount, rowcount]`; `Literal` passes through unchanged; `Predicate`
/// recurses through kernel's standard data-skipping evaluator.
///
/// Returns `None` (abstaining the whole predicate) when the arg can't be expressed against stats:
/// an unsupported expression kind, or a `Column` with no sibling literal to infer its type.
fn rewrite_stat_arg(
    evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
    arg: &Expression,
    type_hint: Option<&DataType>,
) -> Option<Expression> {
    let null_long = || Expression::literal(Scalar::Null(DataType::LONG));
    match arg {
        Expression::Column(col) => {
            // One typed path for both column kinds: the evaluator returns the exact partition
            // value for a partition column (ignoring the type) and `minValues`/`maxValues` refs
            // for a data column. With no sibling literal to supply the type, abstain.
            let ty = type_hint?;
            let min = evaluator.get_min_stat(col, ty).unwrap_or_else(null_long);
            let max = evaluator.get_max_stat(col, ty).unwrap_or_else(null_long);
            let nullcount = evaluator.get_nullcount_stat(col).unwrap_or_else(null_long);
            let rowcount = evaluator.get_rowcount_stat().unwrap_or_else(null_long);
            Some(Expression::struct_from([min, max, nullcount, rowcount]))
        }
        Expression::Literal(_) => Some(arg.clone()),
        Expression::Predicate(p) => Some(Expression::Predicate(Box::new(
            evaluator.eval_pred(p, false)?,
        ))),
        _ => None,
    }
}

/// Build a `StructArray` from a struct expression's children, naming fields positionally
/// (`f0`, `f1`, ...). Engines index into the struct by position; field names are placeholders.
fn evaluate_struct_arg(fields: &[ExpressionRef], batch: &RecordBatch) -> DeltaResult<ArrayRef> {
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|f| evaluate_expression(f, batch, None))
        .collect::<DeltaResult<_>>()?;
    let arrow_fields: Fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("f{i}"), a.data_type().clone(), true))
        .collect();
    StructArray::try_new(arrow_fields, arrays, None)
        .map(|sa| Arc::new(sa) as ArrayRef)
        .map_err(|e| Error::Generic(format!("struct arg construction: {e}")))
}

/// Import an engine-produced `ArrowFFIData` into an `ArrayRef`, consuming the Arrow C Data
/// Interface handles.
fn import_ffi_array(ffi: ArrowFFIData) -> DeltaResult<ArrayRef> {
    // A released (empty) array means the engine reported success without populating the result
    // slot. This check is load-bearing: `from_ffi` asserts on the empty structs' null pointers,
    // and kernel must never panic -- so reject the unpopulated case with an error up front.
    if ffi.array.is_released() {
        return Err(Error::Generic(
            "engine callback returned success but no result array".into(),
        ));
    }

    let ArrowFFIData { array, schema } = ffi;

    // SAFETY: the engine promised these structs are valid Arrow C Data
    // Interface payloads it produced and handed to us.
    let array_data = unsafe { from_ffi(array, &schema) }
        .map_err(|e| Error::Generic(format!("from_ffi: {e}")))?;
    Ok(make_array(array_data))
}

fn require_boolean_array(arr: ArrayRef, expected_rows: usize) -> DeltaResult<BooleanArray> {
    if arr.len() != expected_rows {
        return Err(Error::Generic(format!(
            "opaque predicate eval_pred returned {} rows, expected {expected_rows}",
            arr.len()
        )));
    }
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            Error::Generic(format!(
                "opaque predicate eval_pred returned non-boolean array of type {:?}",
                arr.data_type()
            ))
        })
}

fn call_eval_pred(
    cb: &COpaqueEvalCallbacks,
    op_name: &str,
    args_batch: RecordBatch,
    mode: EvalMode,
    inverted: bool,
) -> DeltaResult<BooleanArray> {
    let num_rows = args_batch.num_rows();
    let mut args_ffi = ArrowFFIData::try_from_record_batch(args_batch)?;

    // Out-pointer convention: kernel owns the result slot on its stack, pre-initialized to None;
    // the engine overwrites it with Some(result) on success, transferring ownership of the written
    // Arrow C Data Interface structs to kernel.
    let mut result = OptionalValue::None;

    // SAFETY: callback was supplied by the engine. We pass a writable args slot (the engine takes
    // ownership of the Arrow FFI handles by moving them out of it) and a writable result slot
    // valid for the duration of the call.
    unsafe {
        let name = kernel_string_slice!(op_name);
        let eval = match mode {
            EvalMode::RowMode => cb.eval_pred_rows,
            EvalMode::StatsMode => cb.eval_pred_stats,
        };
        eval(cb.engine_state, name, &mut args_ffi, inverted, &mut result);
    }
    let OptionalValue::Some(result_ffi) = result else {
        return Err(Error::Generic(format!(
            "engine opaque-eval callback reported failure for `{op_name}`"
        )));
    };

    let arr = import_ffi_array(result_ffi)?;
    require_boolean_array(arr, num_rows)
}

// === ArrowOpaquePredicateOp ====================================================

impl ArrowOpaquePredicateOp for FfiOpaquePredicateOp {
    fn name(&self) -> &str {
        &self.name
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        // Materialize the args batch. In StatsMode this can fail when the rewrite referenced a
        // stats column the batch doesn't carry -- e.g. an opaque op pairs a min/max-ineligible
        // column (boolean, binary, or a complex type) with an eligible literal, so the lifted type
        // hint points at an absent minValues/maxValues field. File pruning is best-effort, so
        // abstain (keep every file) rather than abort the scan. RowMode has no safe abstain, so its
        // errors propagate.
        let args_batch = match evaluate_args(args, batch) {
            Ok(args_batch) => args_batch,
            Err(e) if self.mode == EvalMode::StatsMode => {
                tracing::warn!(
                    "opaque predicate `{}`: cannot materialize stats args ({e}); keeping all files",
                    self.name
                );
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            }
            Err(e) => return Err(e),
        };
        // The StatsMode rewrite records inversion on the op (the stats predicate is then evaluated
        // non-inverted by kernel); RowMode carries it via the eval-time flag. XOR composes both.
        let inverted = inverted ^ self.inverted;
        call_eval_pred(
            &self.callbacks.inner,
            &self.name,
            args_batch,
            self.mode,
            inverted,
        )
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        // Abstains from scalar evaluation (e.g. partition pruning).
        // TODO: support it by invoking the engine callback with a one-row stats batch.
        Ok(None)
    }

    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        // Row-group skipping: abstain. Opaque predicates prune at the file level instead -- via the
        // `as_data_skipping_predicate` rewrite, then `evaluate_predicate` -> `eval_pred`.
        // TODO: support row-group skipping by invoking the engine callback with a one-row stats
        // batch.
        None
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        // Each `Column` arg becomes a `Struct[min, max, nullcount, rowcount]`; literals pass
        // through; `Predicate` children recurse. If any arg can't be rewritten (e.g. a data
        // column with no type hint), the whole predicate abstains and kernel keeps every file.
        let type_hint = pick_column_type_hint(exprs);
        let rewritten = exprs
            .iter()
            .map(|arg| rewrite_stat_arg(evaluator, arg, type_hint.as_ref()))
            .collect::<Option<Vec<_>>>()?;

        // Record inversion on the op, not as a surrounding `Predicate::not` (kernel would flip the
        // verdict, unsound for stats). Use arrow_opaque so dispatch reaches our callback.
        let stats_op = self
            .clone()
            .with_mode(EvalMode::StatsMode)
            .with_inverted(inverted);
        Some(Predicate::arrow_opaque(stats_op, rewritten))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ffi::c_void;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use delta_kernel::arrow::array::{
        ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
    use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate as _;
    use delta_kernel::expressions::{column_expr, Expression, Predicate};

    use super::*;
    use crate::expressions::opaque_eval::EngineEvalRowsFn;
    use crate::KernelStringSlice;

    // === Engine-side test stubs ===================================================
    //
    // Tiny "engine" written in Rust that demonstrates composite opaque predicates.
    // `STARTS_WITH_LOWER(col, prefix)` folds the LOWER step into the predicate
    // callback, so no `Expression::Opaque` ever appears in the tree.

    /// Take ownership of args FFI handles, leaving the slot empty so kernel's
    /// drop is a no-op.
    unsafe fn take_ffi_record_batch(args_in: *mut ArrowFFIData) -> RecordBatch {
        let array = std::mem::replace(unsafe { &mut (*args_in).array }, FFI_ArrowArray::empty());
        let schema = std::mem::replace(unsafe { &mut (*args_in).schema }, FFI_ArrowSchema::empty());
        let array_data = unsafe { from_ffi(array, &schema) }.unwrap();
        let sa = StructArray::from(array_data);
        sa.into()
    }

    /// Write an `ArrayRef` into kernel's result slot as Arrow C Data Interface structs, the way a
    /// real engine returns its result from `eval_pred` (out-pointer convention).
    unsafe fn write_result(out: *mut OptionalValue<ArrowFFIData>, arr: ArrayRef) {
        let array_data = arr.to_data();
        let ffi = ArrowFFIData {
            array: FFI_ArrowArray::new(&array_data),
            schema: FFI_ArrowSchema::try_from(array_data.data_type()).unwrap(),
        };
        unsafe { *out = OptionalValue::Some(ffi) };
    }

    /// Composite opaque predicate: `STARTS_WITH(LOWER(args[0]), args[1])`. Both
    /// args arrive as pre-evaluated String columns; the engine applies LOWER
    /// then prefix-match per row.
    unsafe extern "C" fn engine_starts_with_lower(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        inverted: bool,
        out: *mut OptionalValue<ArrowFFIData>,
    ) {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        assert_eq!(batch.num_columns(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg0 should be StringArray");
        let prefix = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg1 should be StringArray");
        let verdicts: BooleanArray = (0..batch.num_rows())
            .map(|i| match (col.is_valid(i), prefix.is_valid(i)) {
                (true, true) => {
                    let m = col.value(i).to_lowercase().starts_with(prefix.value(i));
                    Some(if inverted { !m } else { m })
                }
                _ => None,
            })
            .collect();
        unsafe { write_result(out, Arc::new(verdicts)) }
    }

    /// Simpler opaque predicate: `STARTS_WITH(args[0], args[1])` with no LOWER.
    /// Used by the composition test where we need two ops sharing the same op
    /// name function.
    unsafe extern "C" fn engine_starts_with(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        inverted: bool,
        out: *mut OptionalValue<ArrowFFIData>,
    ) {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        let lhs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg0 should be StringArray");
        let rhs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg1 should be StringArray");
        let verdicts: BooleanArray = (0..batch.num_rows())
            .map(|i| match (lhs.is_valid(i), rhs.is_valid(i)) {
                (true, true) => {
                    let m = lhs.value(i).starts_with(rhs.value(i));
                    Some(if inverted { !m } else { m })
                }
                _ => None,
            })
            .collect();
        unsafe { write_result(out, Arc::new(verdicts)) }
    }

    unsafe extern "C" fn noop_free(_state: *mut c_void) {}

    fn callbacks_for(eval_pred: EngineEvalRowsFn) -> Arc<FfiOpaqueEvalCallbacks> {
        // Each test exercises one mode; the unused slot is never invoked, so wire the same stub to
        // both row and stats callbacks.
        Arc::new(FfiOpaqueEvalCallbacks::new(COpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred_rows: eval_pred,
            eval_pred_stats: eval_pred,
            free_state: noop_free,
        }))
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

    // === Op identity =============================================================

    #[test]
    fn op_equality_includes_name_mode_inversion_and_callback_identity() {
        let cb = callbacks_for(engine_starts_with);
        let op = FfiOpaquePredicateOp::new("LIKE", cb.clone());

        // Same name + same callbacks Arc -> equal.
        assert_eq!(op, FfiOpaquePredicateOp::new("LIKE", cb.clone()));
        // Different name -> not equal.
        assert_ne!(op, FfiOpaquePredicateOp::new("OTHER", cb.clone()));
        // Mode and inversion change how the op evaluates -> not equal.
        assert_ne!(
            op,
            FfiOpaquePredicateOp::new("LIKE", cb.clone()).with_mode(EvalMode::StatsMode)
        );
        assert_ne!(
            op,
            FfiOpaquePredicateOp::new("LIKE", cb).with_inverted(true)
        );
        // Distinct callback Arcs -> not equal (pointer identity).
        assert_ne!(
            op,
            FfiOpaquePredicateOp::new("LIKE", callbacks_for(engine_starts_with))
        );
    }

    // === End-to-end: STARTS_WITH(LOWER(col), "foo") as one composite op =========

    #[test]
    fn composite_starts_with_lower_predicate_round_trips() {
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("STARTS_WITH_LOWER", callbacks_for(engine_starts_with_lower)),
            [column_expr!("col"), Expression::literal("foo")],
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
    fn inversion_flips_verdicts() {
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("STARTS_WITH_LOWER", callbacks_for(engine_starts_with_lower)),
            [column_expr!("col"), Expression::literal("foo")],
        );
        let batch = batch_with_col(vec![Some("FOOBAR"), Some("Apple")]);
        let result = evaluate_predicate(&pred, &batch, true).unwrap();
        assert!(!result.value(0), "inverted: FOOBAR no longer matches");
        assert!(result.value(1), "inverted: Apple now matches");
    }

    // === Composition: AND of two opaque predicates ==============================

    #[test]
    fn composition_and_of_two_opaque_predicates() {
        let cb = callbacks_for(engine_starts_with);
        let lhs = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("STARTS_WITH", cb.clone()),
            [column_expr!("col"), Expression::literal("F")],
        );
        let rhs = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("STARTS_WITH", cb),
            [column_expr!("col"), Expression::literal("FO")],
        );
        let and_pred = Predicate::and_from([lhs, rhs]);

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("FBAR"), Some("BAR")]);
        let result = evaluate_predicate(&and_pred, &batch, false).unwrap();
        assert!(result.value(0), "FOOBAR starts with F AND FO");
        assert!(!result.value(1), "FBAR starts with F but not FO");
        assert!(!result.value(2), "BAR starts with neither");
    }

    // === Negative paths ==========================================================

    #[test]
    fn engine_returning_none_surfaces_error() {
        unsafe extern "C" fn engine_fail(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            _args_in: *mut ArrowFFIData,
            _inverted: bool,
            _out: *mut OptionalValue<ArrowFFIData>,
        ) {
            // Leave *out untouched (kernel pre-initialized it to None) to signal failure.
        }
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("ALWAYS_FAIL", callbacks_for(engine_fail)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("engine opaque-eval callback reported failure"));
    }

    #[test]
    fn engine_returning_non_boolean_array_errors() {
        unsafe extern "C" fn engine_int(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            let _ = unsafe { take_ffi_record_batch(args_in) };
            let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
            unsafe { write_result(out, arr) }
        }
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("RETURNS_INT", callbacks_for(engine_int)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x"), Some("y"), Some("z")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("non-boolean array"), "got: {err}");
    }

    #[test]
    fn engine_returning_wrong_length_array_errors() {
        unsafe extern "C" fn engine_short(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            let _ = unsafe { take_ffi_record_batch(args_in) };
            let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
            unsafe { write_result(out, arr) }
        }
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("RETURNS_SHORT", callbacks_for(engine_short)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x"), Some("y"), Some("z")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(
            format!("{err}").contains("returned 1 rows, expected 3"),
            "got: {err}"
        );
    }

    #[test]
    fn engine_returning_success_without_result_errors() {
        unsafe extern "C" fn engine_empty(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            // Consume args but hand back an empty (unpopulated) result.
            let _ = unsafe { take_ffi_record_batch(args_in) };
            unsafe { *out = OptionalValue::Some(ArrowFFIData::empty()) };
        }
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("EMPTY", callbacks_for(engine_empty)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("no result array"), "got: {err}");
    }

    // === Zero-arg opaque predicate ===============================================

    #[test]
    fn zero_arg_opaque_predicate_propagates_row_count() {
        unsafe extern "C" fn engine_always_true(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            let batch = unsafe { take_ffi_record_batch(args_in) };
            assert_eq!(batch.num_columns(), 0, "zero-arg => no columns");
            let n = batch.num_rows();
            let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
            unsafe { write_result(out, arr) }
        }
        let pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("ZERO_ARG", callbacks_for(engine_always_true)),
            [] as [Expression; 0],
        );
        let batch = batch_with_col(vec![Some("a"); 5]);
        let result = evaluate_predicate(&pred, &batch, false).unwrap();
        assert_eq!(result.len(), 5);
        assert!((0..5).all(|i| result.value(i)));
    }

    /// `call_eval_pred` must route by mode: a RowMode op hits `eval_pred_rows`, a StatsMode op hits
    /// `eval_pred_stats`. Every other test wires the same stub into both slots, so a swapped match
    /// arm would pass them -- this pins the routing with two distinct stubs.
    #[test]
    fn dispatch_routes_each_mode_to_its_callback() {
        static ROWS: AtomicUsize = AtomicUsize::new(0);
        static STATS: AtomicUsize = AtomicUsize::new(0);

        unsafe extern "C" fn rows(
            _: *mut c_void,
            _: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            ROWS.fetch_add(1, AtomicOrdering::SeqCst);
            let batch = unsafe { take_ffi_record_batch(args_in) };
            unsafe {
                write_result(
                    out,
                    Arc::new(BooleanArray::from(vec![true; batch.num_rows()])),
                )
            }
        }
        unsafe extern "C" fn stats(
            _: *mut c_void,
            _: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _: bool,
            out: *mut OptionalValue<ArrowFFIData>,
        ) {
            STATS.fetch_add(1, AtomicOrdering::SeqCst);
            let batch = unsafe { take_ffi_record_batch(args_in) };
            unsafe {
                write_result(
                    out,
                    Arc::new(BooleanArray::from(vec![true; batch.num_rows()])),
                )
            }
        }
        let callbacks = || {
            Arc::new(FfiOpaqueEvalCallbacks::new(COpaqueEvalCallbacks {
                engine_state: ptr::null_mut(),
                eval_pred_rows: rows,
                eval_pred_stats: stats,
                free_state: noop_free,
            }))
        };
        let batch = batch_with_col(vec![Some("x")]);

        // RowMode (default) routes to eval_pred_rows only.
        let row_pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("OP", callbacks()),
            [column_expr!("col")],
        );
        evaluate_predicate(&row_pred, &batch, false).unwrap();
        assert_eq!(ROWS.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            STATS.load(AtomicOrdering::SeqCst),
            0,
            "row mode must not hit the stats slot"
        );

        // StatsMode routes to eval_pred_stats only.
        let stats_pred = Predicate::arrow_opaque(
            FfiOpaquePredicateOp::new("OP", callbacks()).with_mode(EvalMode::StatsMode),
            [column_expr!("col")],
        );
        evaluate_predicate(&stats_pred, &batch, false).unwrap();
        assert_eq!(
            ROWS.load(AtomicOrdering::SeqCst),
            1,
            "stats mode must not hit the rows slot"
        );
        assert_eq!(STATS.load(AtomicOrdering::SeqCst), 1);
    }

    // === Data-skipping rewrite ==================================================

    mod rewrite {
        use std::cmp::Ordering;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        use delta_kernel::arrow::array::Int64Array;
        use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
        use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicateOp as _;
        use delta_kernel::expressions::{
            column_expr, BinaryExpressionOp, BinaryPredicateOp, ColumnName, Expression,
            JunctionPredicateOp, OpaquePredicateOpRef, Scalar,
        };
        use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
        use delta_kernel::schema::DataType;
        use delta_kernel::Predicate;

        use super::*;

        /// Stub evaluator mirroring `DataSkippingPredicateCreator`: data columns produce
        /// `stats_parsed.{min,max}Values.<col>` refs, partition columns produce
        /// `partitionValues_parsed.<col>`.
        struct StatsRewriter {
            has_stats: bool,
            partition: bool,
        }

        fn stats_col(prefix: &str, col: &ColumnName) -> Expression {
            let mut name = ColumnName::new(["stats_parsed", prefix]);
            name = name.join(col);
            Expression::from(name)
        }

        fn partition_col(col: &ColumnName) -> Expression {
            let mut name = ColumnName::new(["partitionValues_parsed"]);
            name = name.join(col);
            Expression::from(name)
        }

        fn stats_col_numrecords() -> Expression {
            Expression::from(ColumnName::new(["stats_parsed", "numRecords"]))
        }

        /// Single-file stats batch: `stats_parsed { minValues.col=1, maxValues.col=10,
        /// numRecords=1 }`. Shared by the rewrite-dispatch tests below.
        fn single_col_int64_stats_batch() -> RecordBatch {
            let col_field = Arc::new(Field::new("col", ArrowDataType::Int64, true));
            let min_struct = StructArray::from(vec![(
                col_field.clone(),
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            )]);
            let max_struct = StructArray::from(vec![(
                col_field,
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            )]);
            let num_records: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
            let stats_parsed = StructArray::from(vec![
                (
                    Arc::new(Field::new(
                        "minValues",
                        min_struct.data_type().clone(),
                        true,
                    )),
                    Arc::new(min_struct) as ArrayRef,
                ),
                (
                    Arc::new(Field::new(
                        "maxValues",
                        max_struct.data_type().clone(),
                        true,
                    )),
                    Arc::new(max_struct) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                    num_records,
                ),
            ]);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "stats_parsed",
                stats_parsed.data_type().clone(),
                true,
            )]));
            RecordBatch::try_new(schema, vec![Arc::new(stats_parsed) as ArrayRef]).unwrap()
        }

        impl DataSkippingPredicateEvaluator for StatsRewriter {
            type Output = Predicate;
            type ColumnStat = Expression;

            fn get_min_stat(&self, col: &ColumnName, _: &DataType) -> Option<Expression> {
                if self.partition {
                    return Some(partition_col(col));
                }
                self.has_stats.then(|| stats_col("minValues", col))
            }
            fn get_max_stat(&self, col: &ColumnName, _: &DataType) -> Option<Expression> {
                if self.partition {
                    return Some(partition_col(col));
                }
                self.has_stats.then(|| stats_col("maxValues", col))
            }
            fn get_nullcount_stat(&self, _: &ColumnName) -> Option<Expression> {
                None
            }
            fn get_rowcount_stat(&self) -> Option<Expression> {
                Some(stats_col_numrecords())
            }
            fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Predicate> {
                if let Scalar::Boolean(b) = val {
                    Some(Predicate::literal(*b != inverted))
                } else {
                    None
                }
            }
            fn eval_pred_scalar_is_null(&self, _: &Scalar, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_is_null(&self, _: &ColumnName, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_binary_scalars(
                &self,
                _: BinaryPredicateOp,
                _: &Scalar,
                _: &Scalar,
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn eval_pred_opaque(
                &self,
                _: &OpaquePredicateOpRef,
                _: &[Expression],
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn finish_eval_pred_junction(
                &self,
                op: JunctionPredicateOp,
                preds: &mut dyn Iterator<Item = Option<Predicate>>,
                inverted: bool,
            ) -> Option<Predicate> {
                let collected: Vec<Predicate> = preds.collect::<Option<Vec<_>>>()?;
                let pred = Predicate::junction(op, collected);
                Some(if inverted { Predicate::not(pred) } else { pred })
            }
            fn eval_partial_cmp(
                &self,
                ord: Ordering,
                col: Expression,
                val: &Scalar,
                inverted: bool,
            ) -> Option<Predicate> {
                let base_op = match ord {
                    Ordering::Less => BinaryPredicateOp::LessThan,
                    Ordering::Greater => BinaryPredicateOp::GreaterThan,
                    Ordering::Equal => BinaryPredicateOp::Equal,
                };
                let pred = Predicate::binary(base_op, col, Expression::literal(val.clone()));
                Some(if inverted { Predicate::not(pred) } else { pred })
            }
        }

        fn rewriter() -> StatsRewriter {
            StatsRewriter {
                has_stats: true,
                partition: false,
            }
        }

        fn no_stats_rewriter() -> StatsRewriter {
            StatsRewriter {
                has_stats: false,
                partition: false,
            }
        }

        fn partition_rewriter() -> StatsRewriter {
            StatsRewriter {
                has_stats: true,
                partition: true,
            }
        }

        fn op_with_callbacks(name: &str) -> FfiOpaquePredicateOp {
            FfiOpaquePredicateOp::new(name, callbacks_for(engine_starts_with))
        }

        fn rewrite(
            op: &FfiOpaquePredicateOp,
            args: &[Expression],
            rewriter: &StatsRewriter,
            inverted: bool,
        ) -> Option<Predicate> {
            op.as_data_skipping_predicate(
                rewriter
                    as &dyn DataSkippingPredicateEvaluator<
                        Output = Predicate,
                        ColumnStat = Expression,
                    >,
                args,
                inverted,
            )
        }

        #[test]
        fn inverted_range_op_prunes_complement() {
            // Range op `GE(col, t)`: non-inverted keeps a file iff some value could be `>= t`
            // (max >= t); inverted (`col < t`) keeps iff some value could be `< t` (min < t).
            // Same stats, different computation -- not a verdict flip.
            unsafe extern "C" fn engine_ge(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                inverted: bool,
                out: *mut OptionalValue<ArrowFFIData>,
            ) {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                let stats = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let min = stats
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let max = stats
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let target = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let keep: BooleanArray = (0..batch.num_rows())
                    .map(|i| {
                        let t = target.value(i);
                        Some(if inverted {
                            min.value(i) < t
                        } else {
                            max.value(i) >= t
                        })
                    })
                    .collect();
                unsafe { write_result(out, Arc::new(keep)) }
            }

            // Stats batch: a single file with id in [1, 10]. Target 11 is above the whole range.
            let op = FfiOpaquePredicateOp::new("GE", callbacks_for(engine_ge));
            let args = [column_expr!("col"), Expression::literal(11i64)];
            let stats_batch = single_col_int64_stats_batch();

            // GE(col, 11): max 10 < 11 -> prune.
            let non_inverted =
                rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let r = evaluate_predicate(&non_inverted, &stats_batch, false).unwrap();
            assert!(!r.value(0), "GE(col,11): max 10 < 11 prunes the file");

            // NOT GE(col, 11) == col < 11: min 1 < 11 -> keep. Inverted is forwarded, not
            // abstained.
            let inverted =
                rewrite(&op, &args, &rewriter(), true).expect("inverted rewrite should succeed");
            let r = evaluate_predicate(&inverted, &stats_batch, false).unwrap();
            assert!(
                r.value(0),
                "NOT GE(col,11) == col < 11: min 1 < 11 keeps the file"
            );
        }

        #[test]
        fn wraps_column_in_min_max_nullcount_rowcount_struct() {
            let op = op_with_callbacks("STARTS_WITH");
            let args = [column_expr!("col"), Expression::literal("foo")];
            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque, got {pred:?}");
            };
            assert_eq!(opaque.exprs.len(), 2);
            let Expression::Struct(fields, _) = &opaque.exprs[0] else {
                panic!("expected Struct arg, got {:?}", opaque.exprs[0]);
            };
            assert_eq!(fields.len(), 4);
            assert_eq!(
                *fields[0],
                stats_col("minValues", &ColumnName::new(["col"]))
            );
            assert_eq!(
                *fields[1],
                stats_col("maxValues", &ColumnName::new(["col"]))
            );
            assert_eq!(
                *fields[2],
                Expression::literal(Scalar::Null(DataType::LONG))
            );
            assert_eq!(*fields[3], stats_col_numrecords());
            assert_eq!(opaque.exprs[1], Expression::literal("foo"));
        }

        /// A data column with no sibling literal gives no type hint, so min/max stats can't be
        /// read. We abstain for the whole predicate rather than emit a partial wrapper.
        #[test]
        fn abstains_when_data_column_has_no_type_hint() {
            let op = op_with_callbacks("IS_DEFINED");
            let args = [column_expr!("col")];
            assert!(rewrite(&op, &args, &rewriter(), false).is_none());
        }

        #[test]
        fn passes_literals_through_unchanged() {
            let op = op_with_callbacks("OP");
            let args = [Expression::literal("foo")];
            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            assert_eq!(opaque.exprs.len(), 1, "arity preserved");
            assert_eq!(opaque.exprs[0], Expression::literal("foo"));
        }

        #[test]
        fn recursively_rewrites_predicate_child() {
            let op = op_with_callbacks("OP");
            let inner = Predicate::lt(column_expr!("col"), Expression::literal(5i64));
            let args = [Expression::Predicate(Box::new(inner))];

            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            let Expression::Predicate(rewritten) = &opaque.exprs[0] else {
                panic!("expected Predicate arg, got {:?}", opaque.exprs[0]);
            };
            // The inner `col < 5` rewrites through `eval_partial_cmp` to a stats column ref.
            let serialized = format!("{rewritten:?}");
            assert!(
                serialized.contains("minValues") || serialized.contains("maxValues"),
                "predicate child should reference stats columns; got: {serialized}"
            );
        }

        #[test]
        fn abstains_on_unsupported_expr_kinds() {
            let op = op_with_callbacks("OP");
            let args = [Expression::binary(
                BinaryExpressionOp::Plus,
                column_expr!("col"),
                Expression::literal(1i64),
            )];
            assert!(rewrite(&op, &args, &rewriter(), false).is_none());
        }

        /// Column with no min/max stats still produces a wrapper -- the engine sees nulls for
        /// min/max but can still reason about rowcount (e.g. small-file filtering).
        #[test]
        fn column_without_stats_falls_back_to_rowcount_only() {
            let op = op_with_callbacks("OP");
            let args = [column_expr!("col"), Expression::literal(1i64)];
            let pred =
                rewrite(&op, &args, &no_stats_rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            let Expression::Struct(fields, _) = &opaque.exprs[0] else {
                panic!("expected Struct arg");
            };
            assert_eq!(
                *fields[0],
                Expression::literal(Scalar::Null(DataType::LONG))
            );
            assert_eq!(
                *fields[1],
                Expression::literal(Scalar::Null(DataType::LONG))
            );
            assert_eq!(
                *fields[2],
                Expression::literal(Scalar::Null(DataType::LONG))
            );
            assert_eq!(*fields[3], stats_col_numrecords());
        }

        #[test]
        fn preserves_arity_for_mixed_args() {
            let op = op_with_callbacks("OP");
            let inner = Predicate::lt(column_expr!("y"), Expression::literal(5i64));
            let args = [
                column_expr!("col"),
                Expression::literal(1i64),
                Expression::Predicate(Box::new(inner)),
            ];

            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            assert_eq!(
                opaque.exprs.len(),
                3,
                "arity should be preserved (3 in, 3 out)"
            );
            assert!(matches!(opaque.exprs[0], Expression::Struct(_, _)));
            assert!(matches!(opaque.exprs[1], Expression::Literal(_)));
            assert!(matches!(opaque.exprs[2], Expression::Predicate(_)));
        }

        /// End-to-end: rewrite -> evaluate against a synthesized stats batch -> callback fires.
        /// Bare `Predicate::opaque` would fail the adapter downcast and silently skip the callback.
        #[test]
        fn rewritten_predicate_dispatches_to_engine_callback() {
            static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

            unsafe extern "C" fn engine_count(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                _inverted: bool,
                out: *mut OptionalValue<ArrowFFIData>,
            ) {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                CALL_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; batch.num_rows()]));
                unsafe { write_result(out, arr) }
            }

            let op = FfiOpaquePredicateOp::new("OP", callbacks_for(engine_count));
            let args = [column_expr!("col"), Expression::literal(5i64)];
            let rewritten =
                rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            let stats_batch = single_col_int64_stats_batch();

            CALL_COUNT.store(0, AtomicOrdering::SeqCst);
            let result = evaluate_predicate(&rewritten, &stats_batch, false).unwrap();
            assert_eq!(result.len(), 1);
            assert!(result.value(0));
            assert_eq!(
                CALL_COUNT.load(AtomicOrdering::SeqCst),
                1,
                "callback must fire exactly once"
            );
        }

        /// Engine inspects slot 2 (nullcount), sees all-null (stub returns None),
        /// and falls back to keep-the-file.
        #[test]
        fn engine_handles_missing_nullcount_via_null_placeholder() {
            static SAW_NULLCOUNT: AtomicUsize = AtomicUsize::new(0);

            unsafe extern "C" fn engine_check_nullcount(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                _inverted: bool,
                out: *mut OptionalValue<ArrowFFIData>,
            ) {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("arg0 should be StructArray");
                assert_eq!(
                    col.num_columns(),
                    4,
                    "wrapper must be [min, max, nullcount, rowcount]"
                );
                let nullcount = col.column(2);
                let usable = (0..nullcount.len()).any(|i| !nullcount.is_null(i));
                if !usable {
                    SAW_NULLCOUNT.fetch_add(1, AtomicOrdering::SeqCst);
                }
                // Without nullcount, we can't prune -- keep every file.
                let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; batch.num_rows()]));
                unsafe { write_result(out, arr) }
            }

            let op =
                FfiOpaquePredicateOp::new("NEEDS_NULLCOUNT", callbacks_for(engine_check_nullcount));
            let args = [column_expr!("col"), Expression::literal(5i64)];
            let rewritten =
                rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            // Stats batch carries only min/max; the rewriter's literal-null nullcount/rowcount
            // resolve in-expression without needing schema columns for them.
            let stats_batch = single_col_int64_stats_batch();

            SAW_NULLCOUNT.store(0, AtomicOrdering::SeqCst);
            let result = evaluate_predicate(&rewritten, &stats_batch, false).unwrap();
            assert!(result.value(0));
            assert_eq!(
                SAW_NULLCOUNT.load(AtomicOrdering::SeqCst),
                1,
                "engine should observe nullcount as all-null and fall back to keep"
            );
        }

        /// A row with no collected stats rewrites to null min/max, and a conservative engine
        /// keeps it -- pins the keep-on-null-bounds contract that pruning accuracy relies on.
        /// Log-replay safety does not depend on it: kernel's `OR(NOT is_add, ...)` guard keeps
        /// Remove rows regardless of the verdict (see
        /// `misbehaving_stats_callback_cannot_resurrect_removed_file`).
        #[test]
        fn null_bounds_keep_file_for_remove_row_safety() {
            unsafe extern "C" fn engine_keep_on_null_bounds(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                _inverted: bool,
                out: *mut OptionalValue<ArrowFFIData>,
            ) {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                let stats = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("arg0 should be StructArray");
                let (min, max) = (stats.column(0), stats.column(1));
                let keep: BooleanArray = (0..batch.num_rows())
                    // Null bounds can't prove the predicate impossible, so keep the file (true).
                    .map(|i| Some(min.is_null(i) || max.is_null(i)))
                    .collect();
                unsafe { write_result(out, Arc::new(keep)) }
            }

            let op = FfiOpaquePredicateOp::new("OP", callbacks_for(engine_keep_on_null_bounds));
            let args = [column_expr!("col"), Expression::literal(5i64)];
            // `no_stats_rewriter` models a row with no collected stats (e.g. a Remove): null
            // min/max.
            let rewritten = rewrite(&op, &args, &no_stats_rewriter(), false)
                .expect("rewrite should succeed with a type hint");
            let stats_batch = single_col_int64_stats_batch();
            let result = evaluate_predicate(&rewritten, &stats_batch, false).unwrap();
            assert!(
                result.value(0),
                "row with null bounds must be kept, not pruned"
            );
        }

        /// A partition column goes through the same path as a data column: it resolves to its
        /// exact value as both min and max, with the uniform `numRecords` rowcount ref (no
        /// partition special-case).
        #[test]
        fn partition_column_rewrites_to_exact_value_bounds() {
            let op = op_with_callbacks("OP");
            let args = [column_expr!("part_col"), Expression::literal("X")];
            let pred = rewrite(&op, &args, &partition_rewriter(), false)
                .expect("rewrite should succeed for partition column");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            let Expression::Struct(fields, _) = &opaque.exprs[0] else {
                panic!("expected Struct arg, got {:?}", opaque.exprs[0]);
            };
            assert_eq!(fields.len(), 4);
            // Exact partition value serves as both bounds.
            assert_eq!(*fields[0], partition_col(&ColumnName::new(["part_col"])));
            assert_eq!(*fields[1], partition_col(&ColumnName::new(["part_col"])));
            // Rowcount is the same `numRecords` ref as for data columns.
            assert_eq!(*fields[3], stats_col_numrecords());
        }

        #[test]
        fn type_hint_picks_first_literal_when_multiple_present() {
            let exprs = [
                column_expr!("c"),
                Expression::literal("s"),
                Expression::literal(1i64),
            ];
            assert_eq!(
                super::super::pick_column_type_hint(&exprs),
                Some(DataType::STRING),
                "the first literal's type wins"
            );
        }

        /// A min/max-ineligible column paired with an eligible literal makes the rewrite emit a
        /// min/max stats ref the batch doesn't carry. Materializing the args fails, so StatsMode
        /// abstains (keeps every file) instead of aborting the scan -- and the engine callback
        /// never fires.
        #[test]
        fn stats_eval_abstains_when_min_max_ref_is_unbacked() {
            static CALLS: AtomicUsize = AtomicUsize::new(0);

            unsafe extern "C" fn engine_count(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                _inverted: bool,
                out: *mut OptionalValue<ArrowFFIData>,
            ) {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                CALLS.fetch_add(1, AtomicOrdering::SeqCst);
                unsafe {
                    write_result(
                        out,
                        Arc::new(BooleanArray::from(vec![true; batch.num_rows()])),
                    )
                }
            }

            let op = FfiOpaquePredicateOp::new("OP", callbacks_for(engine_count));
            let args = [column_expr!("col"), Expression::literal(5i64)];
            let rewritten =
                rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            // Stats batch carries only numRecords -- no minValues/maxValues -- so materializing the
            // rewritten min/max refs fails and StatsMode abstains.
            let num_records: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
            let stats_parsed = StructArray::from(vec![(
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                num_records,
            )]);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "stats_parsed",
                stats_parsed.data_type().clone(),
                true,
            )]));
            let stats_batch =
                RecordBatch::try_new(schema, vec![Arc::new(stats_parsed) as ArrayRef]).unwrap();

            CALLS.store(0, AtomicOrdering::SeqCst);
            let result = evaluate_predicate(&rewritten, &stats_batch, false).unwrap();
            assert!(result.value(0), "abstain keeps the file");
            assert_eq!(
                CALLS.load(AtomicOrdering::SeqCst),
                0,
                "callback must not fire when the args can't be materialized"
            );
        }
    }
}
