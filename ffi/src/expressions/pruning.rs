//! Engine callback framework for opaque-predicate data skipping.
//!
//! ## Data flow
//!
//! ```text
//! engine                                                     kernel
//! ------                                                     ------
//! 1. fill OpaquePruningCallbacks { eval_against_stats,
//!                                  eval_on_partition_values,
//!                                  eval_on_row_group_stats,
//!                                  engine_state, free_state }
//! 2. create_opaque_pruning_context(callbacks) -> ctx handle
//! 3. visit_predicate_opaque_with_pruning(name, children, ctx)
//!         |
//!         v
//!    builds NamedOpaquePredicateOp { name, Arc<callbacks> }, then wraps via
//!    Predicate::arrow_opaque (when `default-engine-base` is on) or
//!    Predicate::opaque (otherwise). engine receives a predicate id.
//!         |
//!         v
//!    kernel pruning passes invoke the op's callback:
//!      - partition prune:  eval_pred_scalar -> eval_on_partition_values
//!      - row-group skip:   eval_as_data_skipping_predicate -> eval_on_row_group_stats
//!      - file prune:       indirect rewrite keeps the opaque branch live, then either
//!                            * default engine: ArrowOpaquePredicateOp::eval_pred runs
//!                              per metadata-batch row -> eval_against_stats
//!                            * other engines: the engine's own EvaluationHandler must
//!                              recognize Predicate::Opaque and dispatch to the callback;
//!                              otherwise file pruning conservatively keeps every file.
//!         |
//!         v
//!    each invocation hands the engine:
//!      - ChildAccessor    (read-only view of the op's children)
//!      - StatsAccessor    (typed stats lookups; row-group + file passes)
//!      - ScalarResolver   (typed scalar lookups; partition pass)
//!      - OpaquePruneResult (write-only verdict slot)
//!    engine fills the verdict; kernel maps Keep/Skip/Unknown to its
//!    Option<bool> pruning decision.
//! ```
//!
//! ## Contract
//!
//! - **Column names are physical.** Stats and partition values are keyed by physical column names
//!   (the names that appear in parquet files), not logical schema names. Engines that build
//!   predicates from a logical schema must resolve to physical names before constructing the opaque
//!   op; otherwise lookups silently miss and every file is kept.
//! - **Single-segment columns only.** [`ChildAccessor`] reports nested column references (`a.b.c`)
//!   as [`OpaqueChildKind::Unsupported`]. Nested-column opaque predicates degrade to no pruning.
//!   Engines that need nested-column pruning must add accessor surface or fall back to row-time
//!   filtering.
//! - **Supported scalar types.** Accessors cover `String`, integer types widened to `i64` (`Long`,
//!   `Integer`, `Short`, `Byte`), and floats widened to `f64` (`Double`, `Float`). `Date`,
//!   `Timestamp`, `Decimal`, and `Binary` are not yet wired through; engines requesting them get
//!   `false` from the accessor.
//!
//! [`Predicate::Opaque`]: delta_kernel::expressions::Predicate::Opaque
//! [`visit_predicate_opaque_with_pruning`]: crate::expressions::kernel_visitor::visit_predicate_opaque_with_pruning

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
/// `Unknown` is the zero value, so a default-initialized [`OpaquePruneResult`]
/// is already "I don't know" -- engines that cannot evaluate the op leave the
/// result alone and kernel keeps the file/row-group/partition conservatively.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaquePruneVerdict {
    /// Engine could not determine; keep conservatively.
    Unknown = 0,
    /// Predicate evaluates true; keep the file/row-group/partition.
    Keep = 1,
    /// Predicate evaluates false; skip.
    Skip = 2,
}

/// Write handle the engine populates inside a callback. Zero-initialized
/// (`verdict = 0`) means [`OpaquePruneVerdict::Unknown`].
#[repr(C)]
#[derive(Default)]
pub struct OpaquePruneResult {
    verdict: u32,
}

impl OpaquePruneResult {
    /// Translate the engine-written verdict into kernel's `Option<bool>`.
    /// `Keep` -> `Some(true)`, `Skip` -> `Some(false)`. Any other value
    /// (including the default `Unknown`, or an out-of-range value from a
    /// buggy engine) maps to `None` -- treated as "keep conservatively"
    /// downstream.
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
    /// Argument shape not supported by the accessor: nested expressions
    /// (`a + b`, `f(x)`), multi-segment column refs (`a.b.c`), or scalar
    /// types this enum does not enumerate (decimal, date, timestamp, binary).
    Unsupported = 0,
    /// Single-segment column reference; read the name via
    /// [`child_accessor_column_name`].
    Column = 1,
    /// String literal; read via [`child_accessor_literal_string`].
    LiteralString = 2,
    /// Integer literal (`Long`, `Integer`, `Short`, or `Byte` -- widened to
    /// `i64`); read via [`child_accessor_literal_long`].
    LiteralLong = 3,
    /// Floating-point literal (`Double` or `Float` -- widened to `f64`); read
    /// via [`child_accessor_literal_double`].
    LiteralDouble = 4,
    /// Boolean literal; read via [`child_accessor_literal_bool`].
    LiteralBool = 5,
    /// SQL NULL literal of any type; no value to read.
    LiteralNull = 6,
}

/// Read accessor over an opaque op's child expressions. Borrowed for the
/// duration of the callback; engines must not retain it.
///
/// Only single-segment column references are visible to the engine: a child
/// like `Expression::Column("a.b.c")` (path length > 1) reports as
/// [`OpaqueChildKind::Unsupported`] and the engine has no way to read its
/// path. Predicates that reference nested struct fields therefore degrade
/// to no kernel-side pruning -- the engine should fall back to row-time
/// filtering for such cases.
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
        Expression::Literal(Scalar::Long(_) | Scalar::Integer(_))
        | Expression::Literal(Scalar::Short(_) | Scalar::Byte(_)) => OpaqueChildKind::LiteralLong,
        Expression::Literal(Scalar::Double(_) | Scalar::Float(_)) => OpaqueChildKind::LiteralDouble,
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

/// Read an integer-literal child as i64. Accepts any signed integer scalar
/// (`Long`, `Integer`, `Short`, `Byte`), widened to i64. Returns `false` for
/// any non-integer literal.
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
    let Some(Expression::Literal(scalar)) = acc.child(idx) else {
        return false;
    };
    let Some(value) = scalar_as_i64(scalar) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read a floating-point-literal child as f64. Accepts both `Double` and
/// `Float` scalars (the latter widened).
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

/// Append-only arena for strings handed across the FFI boundary. The
/// `RefCell` makes `StrArena: !Sync`, which is fine because each
/// [`StatsAccessor`]/[`ScalarResolver`] is constructed and consumed within
/// a single engine callback on a single kernel thread.
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
        // SAFETY: A `Box<str>` is a (ptr, len) pair pointing to a heap
        // allocation it owns. When the backing `Vec<Box<str>>` reallocates,
        // only the (ptr, len) pairs move; the heap allocations each box
        // points to do not. The `Box` we just pushed will not be popped or
        // freed for `&self`'s lifetime (the arena is append-only). The
        // source `String` was valid UTF-8, so the bytes are too.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }
    }
}

/// Borrowed handle wrapping a `StatsProvider` for FFI consumption.
///
/// String slices returned from the stats getters point into a small
/// per-accessor arena freed when the accessor drops (at the end of the
/// engine callback). The engine must copy slices it needs to retain.
///
/// Column names passed to the accessors must be **physical** names -- the
/// names that appear in parquet files and in `Add.partitionValues`. Engines
/// that built the opaque predicate from a logical (column-mapped) schema
/// must resolve to physical names before constructing the children;
/// otherwise lookups silently miss and every file is kept.
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
    let Some(value) = acc
        .provider
        .min(&name, &DataType::LONG)
        .as_ref()
        .and_then(scalar_as_i64)
    else {
        return false;
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
    let Some(value) = acc
        .provider
        .max(&name, &DataType::LONG)
        .as_ref()
        .and_then(scalar_as_i64)
    else {
        return false;
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
///
/// Column names passed to the resolver must be **physical** names. Partition
/// values in `Add.partitionValues` are keyed by physical name; a logical name
/// from a column-mapped schema will silently miss every lookup.
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
    let Some(value) = acc.resolve_column(&name).as_ref().and_then(scalar_as_i64) else {
        return false;
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
/// callbacks are required (the `extern "C" fn` field type forbids null);
/// engines that do not support a given pass should supply a callback that
/// leaves the verdict at [`OpaquePruneVerdict::Unknown`].
///
/// Engines that pass `engine_state` to multiple
/// [`create_opaque_pruning_context`] calls must arrange for `free_state` to
/// be idempotent: every context owns one drop, and a shared raw state would
/// be freed multiple times.
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
    /// Destructor for `engine_state`. Called once when the last reference
    /// to the pruning context is dropped; may run on any kernel thread.
    pub free_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: `engine_state` and all four function pointers may be touched from
// any kernel thread. The struct lives behind `Arc<...>` via
// `SharedOpaquePruningContext`, and `SharedHandle` requires `T: Sync`, so
// these impls are not optional. Engines must therefore ensure:
//   - Every callback is safe to invoke from any thread, possibly concurrently (kernel currently
//     invokes them sequentially per query, but does not promise to keep doing so).
//   - `engine_state` is safe to access from whichever thread releases the last `Arc` reference --
//     that is the thread that runs `free_state`.
unsafe impl Send for OpaquePruningCallbacks {}
unsafe impl Sync for OpaquePruningCallbacks {}

impl Drop for OpaquePruningCallbacks {
    /// Invokes `free_state` exactly once, on whichever thread releases the
    /// last `Arc<OpaquePruningCallbacks>` reference. Engines that pin
    /// `engine_state` to a particular thread (JNI handles, GC roots, etc.)
    /// must arrange for thread-safe destruction.
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
pub(crate) fn invoke_eval_against_stats(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: &ChildAccessor<'_>,
    stats: &StatsAccessor<'_>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_against_stats)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        children,
        stats,
        inverted,
        &mut out,
    );
    out.into_option()
}

pub(crate) fn invoke_eval_on_partition_values(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: &ChildAccessor<'_>,
    resolver: &ScalarResolver<'_>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_on_partition_values)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        children,
        resolver,
        inverted,
        &mut out,
    );
    out.into_option()
}

pub(crate) fn invoke_eval_on_row_group_stats(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: &ChildAccessor<'_>,
    stats: &StatsAccessor<'_>,
    inverted: bool,
) -> Option<bool> {
    let mut out = OpaquePruneResult::default();
    (cb.eval_on_row_group_stats)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        children,
        stats,
        inverted,
        &mut out,
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

/// Widen any signed integer scalar (`Long`, `Integer`, `Short`, `Byte`) to
/// `i64`. Returns `None` for any other variant.
pub(crate) fn scalar_as_i64(scalar: &Scalar) -> Option<i64> {
    match scalar {
        Scalar::Long(v) => Some(*v),
        Scalar::Integer(v) => Some(i64::from(*v)),
        Scalar::Short(v) => Some(i64::from(*v)),
        Scalar::Byte(v) => Some(i64::from(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cell::Cell;
    use std::ptr;

    use delta_kernel::expressions::column_expr;

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
            &ChildAccessor::new(&exprs),
            &StatsAccessor::new(&stats),
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
            &ChildAccessor::new(&exprs),
            &StatsAccessor::new(&stats),
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
            &ChildAccessor::new(&exprs),
            &StatsAccessor::new(&stats),
            false,
        );
        assert_eq!(result, None);
    }
}
