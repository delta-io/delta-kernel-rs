//! Engine callback framework for opaque-predicate data skipping.
//!
//! Engines register [`OpaquePruningCallbacks`] alongside their
//! `EvaluationHandler`; kernel invokes them every time it needs a pruning
//! decision for a [`Predicate::Opaque`] node. The engine evaluates the op
//! in its own language using kernel-provided accessors over the op's
//! children and the relevant stats.
//!
//! See `doc/design/opaque-predicate-data-skipping.md` for the design.
//!
//! [`Predicate::Opaque`]: delta_kernel::expressions::Predicate::Opaque

use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel::expressions::{Expression, Scalar, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
use delta_kernel::schema::DataType;
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice, TryFromStringSlice};

// === Verdict =================================================================

/// Pruning verdict written by the engine.
///
/// `Unknown` is the default and is treated as "keep" by kernel; engines that
/// cannot evaluate a particular op should leave the verdict at `Unknown`
/// rather than guessing.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaquePruneVerdict {
    /// Predicate evaluates true; keep the file/row-group/partition.
    Keep = 1,
    /// Predicate evaluates false; skip.
    Skip = 2,
    /// Engine could not determine; keep conservatively.
    Unknown = 3,
}

/// Write handle the engine populates inside a callback.
#[repr(C)]
pub struct OpaquePruneResult {
    verdict: u32,
}

impl Default for OpaquePruneResult {
    fn default() -> Self {
        Self {
            verdict: OpaquePruneVerdict::Unknown as u32,
        }
    }
}

impl OpaquePruneResult {
    pub(crate) fn into_option(self) -> Option<bool> {
        match self.verdict {
            v if v == OpaquePruneVerdict::Keep as u32 => Some(true),
            v if v == OpaquePruneVerdict::Skip as u32 => Some(false),
            _ => None,
        }
    }
}

/// Record a "keep" verdict.
///
/// # Safety
/// `out` must be a valid pointer to an [`OpaquePruneResult`].
#[no_mangle]
pub unsafe extern "C" fn opaque_prune_result_keep(out: *mut OpaquePruneResult) {
    if !out.is_null() {
        unsafe { (*out).verdict = OpaquePruneVerdict::Keep as u32 };
    }
}

/// Record a "skip" verdict.
///
/// # Safety
/// `out` must be a valid pointer to an [`OpaquePruneResult`].
#[no_mangle]
pub unsafe extern "C" fn opaque_prune_result_skip(out: *mut OpaquePruneResult) {
    if !out.is_null() {
        unsafe { (*out).verdict = OpaquePruneVerdict::Skip as u32 };
    }
}

// === ChildAccessor ===========================================================

/// Tag indicating the shape of an opaque op's child argument.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaqueChildKind {
    /// Argument shape not supported by the accessor (nested expressions,
    /// multi-segment column refs, types this enum doesn't enumerate).
    Unsupported = 0,
    /// Single-segment column reference; read the name via
    /// [`child_accessor_column_name`].
    Column = 1,
    /// String literal; read via [`child_accessor_literal_string`].
    LiteralString = 2,
    /// 64-bit integer literal (also widened from `Integer`); read via
    /// [`child_accessor_literal_long`].
    LiteralLong = 3,
    /// 64-bit floating-point literal (also widened from `Float`); read via
    /// [`child_accessor_literal_double`].
    LiteralDouble = 4,
    /// Boolean literal; read via [`child_accessor_literal_bool`].
    LiteralBool = 5,
    /// SQL NULL literal of any type; no value to read.
    LiteralNull = 6,
}

/// Read accessor over an opaque op's child expressions. Borrowed for the
/// duration of the callback; engines must not retain it.
pub struct ChildAccessor<'a> {
    children: &'a [Expression],
}

impl<'a> ChildAccessor<'a> {
    pub(crate) fn new(children: &'a [Expression]) -> Self {
        Self { children }
    }

    fn child(&self, idx: usize) -> Option<&Expression> {
        self.children.get(idx)
    }
}

/// Number of children carried by the opaque op.
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`] valid for the
/// current callback.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_count(acc: *const ChildAccessor<'_>) -> usize {
    if acc.is_null() {
        return 0;
    }
    unsafe { (&*acc).children.len() }
}

/// Returns the kind of the child at `idx`. Returns
/// [`OpaqueChildKind::Unsupported`] if `idx` is out of range or the shape
/// isn't recognized (e.g., a nested expression).
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`] valid for the
/// current callback.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_kind(acc: *const ChildAccessor<'_>, idx: usize) -> u32 {
    if acc.is_null() {
        return OpaqueChildKind::Unsupported as u32;
    }
    let Some(child) = (unsafe { &*acc }).child(idx) else {
        return OpaqueChildKind::Unsupported as u32;
    };
    let kind = match child {
        Expression::Column(c) if c.path().len() == 1 => OpaqueChildKind::Column,
        Expression::Literal(Scalar::String(_)) => OpaqueChildKind::LiteralString,
        Expression::Literal(Scalar::Long(_)) => OpaqueChildKind::LiteralLong,
        Expression::Literal(Scalar::Integer(_)) => OpaqueChildKind::LiteralLong,
        Expression::Literal(Scalar::Double(_)) => OpaqueChildKind::LiteralDouble,
        Expression::Literal(Scalar::Float(_)) => OpaqueChildKind::LiteralDouble,
        Expression::Literal(Scalar::Boolean(_)) => OpaqueChildKind::LiteralBool,
        Expression::Literal(Scalar::Null(_)) => OpaqueChildKind::LiteralNull,
        _ => OpaqueChildKind::Unsupported,
    };
    kind as u32
}

/// Read the single-segment column name of child `idx`. Returns `false` if
/// the child isn't a column or `idx` is out of range; otherwise writes the
/// name slice to `*out` (borrowed for the callback's lifetime).
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_column_name(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Column(c)) = acc.child(idx) else {
        return false;
    };
    let path = c.path();
    if path.len() != 1 {
        return false;
    }
    let name = path[0].as_str();
    unsafe { *out = kernel_string_slice!(name) };
    true
}

/// Read a string-literal child. Returns `false` if the child isn't a
/// string literal.
///
/// # Safety
/// `acc` valid, `out` valid pointer. Slice points into kernel-owned data
/// valid for the callback's lifetime.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_string(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Literal(Scalar::String(s))) = acc.child(idx) else {
        return false;
    };
    let view = s.as_str();
    unsafe { *out = kernel_string_slice!(view) };
    true
}

/// Read an integer-literal child as i64. Accepts both `Long` and `Integer`
/// scalars (the latter widened to i64). Returns `false` otherwise.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_long(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let value = match acc.child(idx) {
        Some(Expression::Literal(Scalar::Long(v))) => *v,
        Some(Expression::Literal(Scalar::Integer(v))) => i64::from(*v),
        _ => return false,
    };
    unsafe { *out = value };
    true
}

/// Read a floating-point-literal child as f64. Accepts both `Double` and
/// `Float` scalars.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_double(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut f64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let value = match acc.child(idx) {
        Some(Expression::Literal(Scalar::Double(v))) => *v,
        Some(Expression::Literal(Scalar::Float(v))) => f64::from(*v),
        _ => return false,
    };
    unsafe { *out = value };
    true
}

/// Read a boolean-literal child.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_bool(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut bool,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Literal(Scalar::Boolean(v))) = acc.child(idx) else {
        return false;
    };
    unsafe { *out = *v };
    true
}

// === StatsAccessor (file pruning + row-group skipping) =======================

/// Source of column statistics for a single file or row group. Kernel
/// implementations bridge Delta-log Add-action stats (file pruning) or
/// parquet footer stats (row-group skipping). The engine only sees this
/// trait through FFI accessor functions; it cannot be implemented across
/// the FFI boundary.
pub(crate) trait StatsProvider {
    fn min(&self, col: &str, dtype: &DataType) -> Option<Scalar>;
    fn max(&self, col: &str, dtype: &DataType) -> Option<Scalar>;
    fn null_count(&self, col: &str) -> Option<i64>;
    fn row_count(&self) -> Option<i64>;
}

/// Append-only arena for strings handed across the FFI boundary. Each
/// interned string lives in a `Box<str>` whose heap allocation outlives
/// the arena (the `Box` may move within the backing `Vec`, but the bytes
/// it points to do not).
#[derive(Default)]
pub(crate) struct StrArena {
    boxes: std::cell::RefCell<Vec<Box<str>>>,
}

impl StrArena {
    pub(crate) fn intern(&self, s: String) -> &str {
        let boxed = s.into_boxed_str();
        let ptr = boxed.as_ptr();
        let len = boxed.len();
        self.boxes.borrow_mut().push(boxed);
        // SAFETY: `ptr` points to the heap allocation owned by the
        // `Box<str>` we just pushed. The Vec may reallocate its backing
        // buffer (moving the Box's position), but the heap allocation
        // never relocates, so the slice stays valid for `&self`'s
        // lifetime.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }
    }
}

/// Borrowed handle wrapping a [`StatsProvider`] for FFI consumption.
///
/// String slices returned from the stats getters point into a small
/// per-accessor arena freed when the accessor drops (at the end of the
/// engine callback). The engine must copy slices it needs to retain.
pub struct StatsAccessor<'a> {
    provider: &'a dyn StatsProvider,
    arena: StrArena,
}

impl<'a> StatsAccessor<'a> {
    pub(crate) fn new(provider: &'a dyn StatsProvider) -> Self {
        Self {
            provider,
            arena: StrArena::default(),
        }
    }

    fn intern(&self, s: String) -> &str {
        self.arena.intern(s)
    }
}

unsafe fn column_name_from_slice(col: &KernelStringSlice) -> Option<String> {
    // SAFETY: caller guarantees slice validity for the duration of the call.
    unsafe { String::try_from_slice(col) }.ok()
}

/// Read string min stat for the given column.
///
/// # Safety
/// `acc` valid, `col` a valid string slice, `out` a valid pointer. The
/// returned slice points into the accessor's per-callback arena and is
/// valid only for the lifetime of the accessor (typically the engine
/// callback's duration). The engine must copy if it needs to retain past
/// the callback.
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_min_string(
    acc: *const StatsAccessor<'_>,
    col: KernelStringSlice,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(Scalar::String(s)) = acc.provider.min(&name, &DataType::STRING) else {
        return false;
    };
    let interned = acc.intern(s);
    unsafe { *out = kernel_string_slice!(interned) };
    true
}

/// Read string max stat for the given column.
///
/// # Safety
/// See [`stats_accessor_min_string`].
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_max_string(
    acc: *const StatsAccessor<'_>,
    col: KernelStringSlice,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(Scalar::String(s)) = acc.provider.max(&name, &DataType::STRING) else {
        return false;
    };
    let interned = acc.intern(s);
    unsafe { *out = kernel_string_slice!(interned) };
    true
}

/// Read i64 min stat. Accepts Long or Integer-typed stats.
///
/// # Safety
/// `acc` valid, `col` valid slice, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_min_long(
    acc: *const StatsAccessor<'_>,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let value = match acc.provider.min(&name, &DataType::LONG) {
        Some(Scalar::Long(v)) => v,
        Some(Scalar::Integer(v)) => i64::from(v),
        _ => return false,
    };
    unsafe { *out = value };
    true
}

/// Read i64 max stat.
///
/// # Safety
/// See [`stats_accessor_min_long`].
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_max_long(
    acc: *const StatsAccessor<'_>,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let value = match acc.provider.max(&name, &DataType::LONG) {
        Some(Scalar::Long(v)) => v,
        Some(Scalar::Integer(v)) => i64::from(v),
        _ => return false,
    };
    unsafe { *out = value };
    true
}

/// Read the column's null count.
///
/// # Safety
/// `acc` valid, `col` valid slice, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_null_count(
    acc: *const StatsAccessor<'_>,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(value) = acc.provider.null_count(&name) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read the total row count for the file/row-group.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn stats_accessor_row_count(
    acc: *const StatsAccessor<'_>,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(value) = acc.provider.row_count() else {
        return false;
    };
    unsafe { *out = value };
    true
}

// === ScalarResolver (partition pruning) ======================================

/// Bridge from kernel's partition-value lookup machinery to the FFI accessor
/// surface engines use during partition pruning. Holds a reference to the
/// kernel-side [`ScalarExpressionEvaluator`] and a small per-callback arena
/// for strings handed back to the engine.
pub struct ScalarResolver<'a> {
    eval_expr: &'a ScalarExpressionEvaluator<'a>,
    arena: StrArena,
}

impl<'a> ScalarResolver<'a> {
    pub(crate) fn new(eval_expr: &'a ScalarExpressionEvaluator<'a>) -> Self {
        Self {
            eval_expr,
            arena: StrArena::default(),
        }
    }

    fn resolve_column(&self, col: &str) -> Option<Scalar> {
        let expr = Expression::column([col]);
        (self.eval_expr)(&expr)
    }

    fn intern(&self, s: String) -> &str {
        self.arena.intern(s)
    }
}

/// Resolve a column reference to a string scalar (partition value lookup).
///
/// # Safety
/// `acc` valid, `col` valid slice, `out` valid pointer. The returned slice
/// points into the resolver's per-callback arena; the engine must copy if
/// it needs to retain past the callback.
#[no_mangle]
pub unsafe extern "C" fn scalar_resolver_string(
    acc: *const ScalarResolver<'_>,
    col: KernelStringSlice,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(Scalar::String(s)) = acc.resolve_column(&name) else {
        return false;
    };
    let interned = acc.intern(s);
    unsafe { *out = kernel_string_slice!(interned) };
    true
}

/// Resolve a column reference to an i64 scalar.
///
/// # Safety
/// `acc` valid, `col` valid slice, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn scalar_resolver_long(
    acc: *const ScalarResolver<'_>,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let value = match acc.resolve_column(&name) {
        Some(Scalar::Long(v)) => v,
        Some(Scalar::Integer(v)) => i64::from(v),
        _ => return false,
    };
    unsafe { *out = value };
    true
}

// === Callback struct =========================================================

/// Signature: kernel passes the engine the op name, the op's children, and
/// a stats accessor for the file or row-group under consideration. Engine
/// writes its verdict to `*out`.
pub type EvalAgainstStatsFn = extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    children: *const ChildAccessor<'_>,
    stats: *const StatsAccessor<'_>,
    inverted: bool,
    out: *mut OpaquePruneResult,
);

/// Signature: kernel passes the engine the op name, children, and a
/// scalar-resolver bridge for partition values.
pub type EvalOnPartitionValuesFn = extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    children: *const ChildAccessor<'_>,
    resolver: *const ScalarResolver<'_>,
    inverted: bool,
    out: *mut OpaquePruneResult,
);

/// Signature: same as `EvalAgainstStatsFn` but the accessor reads parquet
/// footer stats. Kept distinct so engines can route differently if
/// row-group pruning has different semantics than file-level pruning.
pub type EvalOnRowGroupStatsFn = EvalAgainstStatsFn;

/// Bundle of engine callbacks for opaque-predicate pruning. All three
/// callbacks must be non-null when the struct is passed into kernel;
/// engines that don't support a particular pass should provide a callback
/// that immediately writes `Unknown`.
#[repr(C)]
#[derive(Debug)]
pub struct OpaquePruningCallbacks {
    /// Opaque engine state; passed back as the first argument to every
    /// callback.
    pub engine_state: *mut c_void,
    /// File-pruning callback.
    pub eval_against_stats: EvalAgainstStatsFn,
    /// Partition-pruning callback.
    pub eval_on_partition_values: EvalOnPartitionValuesFn,
    /// Row-group-skipping callback.
    pub eval_on_row_group_stats: EvalOnRowGroupStatsFn,
    /// Destructor for `engine_state`. Called when the last reference to
    /// the pruning context is dropped.
    pub free_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: engine is responsible for ensuring engine_state is sound to
// access from any thread; kernel makes no thread guarantees beyond
// "callbacks may be invoked concurrently".
unsafe impl Send for OpaquePruningCallbacks {}
unsafe impl Sync for OpaquePruningCallbacks {}

impl Drop for OpaquePruningCallbacks {
    fn drop(&mut self) {
        (self.free_state)(self.engine_state);
    }
}

// === Shared context handle ===================================================

#[handle_descriptor(target=OpaquePruningCallbacks, mutable=false, sized=true)]
pub struct SharedOpaquePruningContext;

/// Create a shared pruning context from a callback bundle. The returned
/// handle can be passed to `visit_predicate_opaque_with_pruning` for each
/// opaque op in a query; kernel arc-clones internally.
///
/// # Safety
/// All function pointers in `callbacks` must be valid. `engine_state` must
/// be valid until `free_state` is invoked (which happens when the last
/// kernel-side reference drops).
#[no_mangle]
pub unsafe extern "C" fn create_opaque_pruning_context(
    callbacks: OpaquePruningCallbacks,
) -> Handle<SharedOpaquePruningContext> {
    Arc::new(callbacks).into()
}

/// Drop a pruning context handle. The engine's `free_state` fires when
/// the last reference is released.
///
/// # Safety
/// `ctx` must be a valid handle produced by `create_opaque_pruning_context`.
#[no_mangle]
pub unsafe extern "C" fn free_opaque_pruning_context(ctx: Handle<SharedOpaquePruningContext>) {
    ctx.drop_handle();
}

// === Used by NamedOpaquePredicateOp ==========================================

/// Invoke the engine's stats-based pruning callback. Used by the arrow
/// adapter's `eval_pred` and by any future kernel-side per-file
/// refinement pass.
#[allow(clippy::needless_lifetimes)]
pub(crate) fn invoke_eval_against_stats<'a>(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: ChildAccessor<'a>,
    stats: StatsAccessor<'a>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_against_stats)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        &children as *const _,
        &stats as *const _,
        inverted,
        &mut out as *mut _,
    );
    out.into_option()
}

#[allow(clippy::needless_lifetimes)]
pub(crate) fn invoke_eval_on_partition_values<'a>(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: ChildAccessor<'a>,
    resolver: ScalarResolver<'a>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_on_partition_values)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        &children as *const _,
        &resolver as *const _,
        inverted,
        &mut out as *mut _,
    );
    out.into_option()
}

#[allow(clippy::needless_lifetimes)]
pub(crate) fn invoke_eval_on_row_group_stats<'a>(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: ChildAccessor<'a>,
    stats: StatsAccessor<'a>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_on_row_group_stats)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        &children as *const _,
        &stats as *const _,
        inverted,
        &mut out as *mut _,
    );
    out.into_option()
}

// === Adapters bridging kernel's DataSkippingPredicateEvaluator to FFI ========

/// `StatsProvider` impl that reads from a kernel
/// [`DataSkippingPredicateEvaluator`] with `Output = bool, ColumnStat =
/// Scalar` (the direct evaluator used for row-group skipping and the
/// per-file refinement pass).
pub(crate) struct DirectStatsProvider<'a, E: ?Sized> {
    evaluator: &'a E,
}

impl<'a, E: ?Sized> DirectStatsProvider<'a, E> {
    pub(crate) fn new(evaluator: &'a E) -> Self {
        Self { evaluator }
    }
}

impl<'a, E> StatsProvider for DirectStatsProvider<'a, E>
where
    E: DataSkippingPredicateEvaluator<Output = bool, ColumnStat = Scalar> + ?Sized,
{
    fn min(&self, col: &str, dtype: &DataType) -> Option<Scalar> {
        let col = delta_kernel::expressions::ColumnName::new([col]);
        self.evaluator.get_min_stat(&col, dtype)
    }
    fn max(&self, col: &str, dtype: &DataType) -> Option<Scalar> {
        let col = delta_kernel::expressions::ColumnName::new([col]);
        self.evaluator.get_max_stat(&col, dtype)
    }
    fn null_count(&self, col: &str) -> Option<i64> {
        let col = delta_kernel::expressions::ColumnName::new([col]);
        let stat = self.evaluator.get_nullcount_stat(&col)?;
        scalar_as_i64(&stat)
    }
    fn row_count(&self) -> Option<i64> {
        let stat = self.evaluator.get_rowcount_stat()?;
        scalar_as_i64(&stat)
    }
}

/// Widen a `Long`/`Integer` scalar to `i64`. Returns `None` for any other
/// variant.
pub(crate) fn scalar_as_i64(scalar: &Scalar) -> Option<i64> {
    match scalar {
        Scalar::Long(v) => Some(*v),
        Scalar::Integer(v) => Some(i64::from(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cell::Cell;
    use std::ptr;

    use super::*;

    // === OpaquePruneResult ====================================================

    #[test]
    fn prune_result_defaults_to_unknown() {
        let r = OpaquePruneResult::default();
        assert_eq!(r.into_option(), None);
    }

    #[test]
    fn prune_result_records_keep() {
        let mut r = OpaquePruneResult::default();
        unsafe { opaque_prune_result_keep(&mut r) };
        assert_eq!(r.into_option(), Some(true));
    }

    #[test]
    fn prune_result_records_skip() {
        let mut r = OpaquePruneResult::default();
        unsafe { opaque_prune_result_skip(&mut r) };
        assert_eq!(r.into_option(), Some(false));
    }

    #[test]
    fn prune_result_null_pointer_is_noop() {
        unsafe { opaque_prune_result_keep(ptr::null_mut()) };
        unsafe { opaque_prune_result_skip(ptr::null_mut()) };
    }

    // === ChildAccessor ========================================================

    #[test]
    fn child_accessor_reads_column_and_literal() {
        use delta_kernel::expressions::{column_expr, Expression};

        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        assert_eq!(unsafe { child_accessor_count(&acc) }, 2);
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 0) },
            OpaqueChildKind::Column as u32
        );
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 1) },
            OpaqueChildKind::LiteralString as u32
        );

        let mut col_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        let ok = unsafe { child_accessor_column_name(&acc, 0, &mut col_out) };
        assert!(ok);
        let s = unsafe { String::try_from_slice(&col_out) }.unwrap();
        assert_eq!(s, "name");

        let mut lit_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        let ok = unsafe { child_accessor_literal_string(&acc, 1, &mut lit_out) };
        assert!(ok);
        let s = unsafe { String::try_from_slice(&lit_out) }.unwrap();
        assert_eq!(s, "foo");
    }

    #[test]
    fn child_accessor_rejects_wrong_kind() {
        use delta_kernel::expressions::{column_expr, Expression};

        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        let mut out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        // Index 1 is a literal, not a column.
        assert!(!unsafe { child_accessor_column_name(&acc, 1, &mut out) });
        // Index 0 is a column, not a string literal.
        assert!(!unsafe { child_accessor_literal_string(&acc, 0, &mut out) });
        // Out-of-range index.
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 99) },
            OpaqueChildKind::Unsupported as u32
        );
    }

    #[test]
    fn child_accessor_reads_long_and_double() {
        let exprs = [
            Expression::literal(42_i64),
            Expression::literal(2.5_f64),
            Expression::literal(true),
        ];
        let acc = ChildAccessor::new(&exprs);

        let mut long_out = 0_i64;
        assert!(unsafe { child_accessor_literal_long(&acc, 0, &mut long_out) });
        assert_eq!(long_out, 42);

        let mut double_out = 0_f64;
        assert!(unsafe { child_accessor_literal_double(&acc, 1, &mut double_out) });
        assert!((double_out - 2.5).abs() < 1e-9);

        let mut bool_out = false;
        assert!(unsafe { child_accessor_literal_bool(&acc, 2, &mut bool_out) });
        assert!(bool_out);
    }

    // === StatsAccessor + callback dispatch ===================================

    struct FixedStats {
        min: Option<Scalar>,
        max: Option<Scalar>,
        nulls: Option<i64>,
        rows: Option<i64>,
    }

    impl StatsProvider for FixedStats {
        fn min(&self, _col: &str, _dtype: &DataType) -> Option<Scalar> {
            self.min.clone()
        }
        fn max(&self, _col: &str, _dtype: &DataType) -> Option<Scalar> {
            self.max.clone()
        }
        fn null_count(&self, _col: &str) -> Option<i64> {
            self.nulls
        }
        fn row_count(&self) -> Option<i64> {
            self.rows
        }
    }

    #[test]
    fn stats_accessor_reads_string_min_max() {
        let stats = FixedStats {
            min: Some(Scalar::from("apple")),
            max: Some(Scalar::from("zebra")),
            nulls: Some(0),
            rows: Some(100),
        };
        let acc = StatsAccessor::new(&stats);
        let col_name = "any_col";

        let mut out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(unsafe {
            stats_accessor_min_string(&acc, kernel_string_slice!(col_name), &mut out)
        });
        let s = unsafe { String::try_from_slice(&out) }.unwrap();
        assert_eq!(s, "apple");

        assert!(unsafe {
            stats_accessor_max_string(&acc, kernel_string_slice!(col_name), &mut out)
        });
        let s = unsafe { String::try_from_slice(&out) }.unwrap();
        assert_eq!(s, "zebra");

        let mut row_count = 0_i64;
        assert!(unsafe { stats_accessor_row_count(&acc, &mut row_count) });
        assert_eq!(row_count, 100);
    }

    // === End-to-end callback round-trip ======================================

    // A captured invocation -- proves the callback fired with the expected args.
    thread_local! {
        static CALLBACK_LOG: Cell<Option<(String, bool)>> = const { Cell::new(None) };
        static CALLBACK_VERDICT: Cell<OpaquePruneVerdict> = const { Cell::new(OpaquePruneVerdict::Unknown) };
    }

    extern "C" fn test_eval_against_stats(
        _state: *mut c_void,
        op_name: KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const StatsAccessor<'_>,
        inverted: bool,
        out: *mut OpaquePruneResult,
    ) {
        let name = unsafe { String::try_from_slice(&op_name) }.unwrap();
        CALLBACK_LOG.with(|c| c.set(Some((name, inverted))));
        let verdict = CALLBACK_VERDICT.with(Cell::get);
        match verdict {
            OpaquePruneVerdict::Keep => unsafe { opaque_prune_result_keep(out) },
            OpaquePruneVerdict::Skip => unsafe { opaque_prune_result_skip(out) },
            OpaquePruneVerdict::Unknown => {}
        }
    }

    extern "C" fn test_eval_on_partition_values(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _resolver: *const ScalarResolver<'_>,
        _inverted: bool,
        _out: *mut OpaquePruneResult,
    ) {
    }

    extern "C" fn test_free_state(_state: *mut c_void) {}

    fn build_test_callbacks() -> OpaquePruningCallbacks {
        OpaquePruningCallbacks {
            engine_state: ptr::null_mut(),
            eval_against_stats: test_eval_against_stats,
            eval_on_partition_values: test_eval_on_partition_values,
            eval_on_row_group_stats: test_eval_against_stats,
            free_state: test_free_state,
        }
    }

    #[test]
    fn invoke_eval_against_stats_round_trips_name_and_inverted() {
        CALLBACK_LOG.with(|c| c.set(None));
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Skip));

        let cb = build_test_callbacks();
        let stats = FixedStats {
            min: None,
            max: None,
            nulls: None,
            rows: None,
        };
        let exprs: Vec<Expression> = vec![];
        let result = invoke_eval_against_stats(
            &cb,
            "STARTS_WITH",
            ChildAccessor::new(&exprs),
            StatsAccessor::new(&stats),
            true,
        );
        let logged = CALLBACK_LOG.with(Cell::take);
        assert_eq!(logged, Some(("STARTS_WITH".to_string(), true)));
        assert_eq!(result, Some(false));
    }

    #[test]
    fn invoke_eval_against_stats_keep_verdict() {
        CALLBACK_LOG.with(|c| c.set(None));
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Keep));

        let cb = build_test_callbacks();
        let stats = FixedStats {
            min: None,
            max: None,
            nulls: None,
            rows: None,
        };
        let exprs: Vec<Expression> = vec![];
        let result = invoke_eval_against_stats(
            &cb,
            "FOO",
            ChildAccessor::new(&exprs),
            StatsAccessor::new(&stats),
            false,
        );
        assert_eq!(result, Some(true));
    }

    #[test]
    fn invoke_eval_against_stats_unknown_verdict_passes_through() {
        CALLBACK_LOG.with(|c| c.set(None));
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Unknown));

        let cb = build_test_callbacks();
        let stats = FixedStats {
            min: None,
            max: None,
            nulls: None,
            rows: None,
        };
        let exprs: Vec<Expression> = vec![];
        let result = invoke_eval_against_stats(
            &cb,
            "FOO",
            ChildAccessor::new(&exprs),
            StatsAccessor::new(&stats),
            false,
        );
        assert_eq!(result, None);
    }
}
