//! Schema/expression utilities shared by plan builders.
//!
//! These tools operate on [`StructType`][crate::schema::StructType] and
//! [`Expression`][crate::expressions::Expression] without depending on the plan-construction
//! state machinery. They live here -- separate from `ir` (IR data types) and the SM framework
//! -- because they're builder-time helpers reused by multiple call sites (the kernel
//! [`PlanBuilder`][crate::plans::state_machines::framework::plan_context::PlanBuilder] *and*
//! engine-side lowering).
//!
//! - `check` -- bidirectional type checker for builder schema derivation (`check_expression`
//!   under strict structural [`DataType`][crate::schema::DataType] equality, plus the
//!   operator-aligned helpers `infer_projection_schema`, `check_projection`, `check_select`,
//!   `validate_exprs`). Kernel-internal.
//! - [`field_op`] -- nested struct edits (`FieldOp` + `compile_field_op`) plus a small set of
//!   schema/expression conveniences (`arc_struct_or_invariant`, `identity_named_expr`, plus the
//!   publicly re-exported [`field_op::load_output_schema`] shared with engine-side lowering).
//!
//! # Errors
//!
//! These modules are deliberately Delta-agnostic: they're generic schema/expression utilities
//! and shouldn't know about [`DeltaError`][crate::plans::errors::DeltaError] /
//! [`DeltaErrorCode`][crate::plans::errors::DeltaErrorCode]. Instead they return
//! `SchemaExprResult` -- an anyhow-style boxed `dyn Error` -- and callers at the plan-builder
//! boundary wrap that into a `DeltaError` with the appropriate code via
//! [`DeltaResultExt::or_delta`][crate::plans::errors::DeltaResultExt::or_delta]. The boxed
//! source is preserved through `std::error::Error::source()`.

// schema_expr is the type-checker that `PlanBuilder::project` / `select` / `field_op` rely on
// (see `state_machines::framework::plan_context`). The consumer module is intentionally not
// declared in this slice; the module-wide allow keeps the diff quiet on its own and is
// expected to be removed when the consumer is added.
#![allow(dead_code)]

pub(crate) mod check;
pub mod field_op;

/// Boxed, sendable error type returned by the `check` and `field_op` primitives. Identical
/// to [`crate::plans::errors::BoxedSource`]; aliased here so call sites read as "schema-expr's
/// error" without leaking the Delta error vocabulary into a generic type-checker.
pub(crate) type SchemaExprError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` alias paired with `SchemaExprError`.
pub(crate) type SchemaExprResult<T> = std::result::Result<T, SchemaExprError>;
