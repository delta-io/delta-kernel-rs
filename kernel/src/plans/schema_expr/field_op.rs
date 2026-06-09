//! Field-level schema edits and matching projection expressions.
//!
//! The kernel `PlanBuilder` exposes point-edit primitives (`replace_col`, `insert_col_after`,
//! `drop_col`, `append_col_typed`) that all reduce to one operation: walk a parent struct
//! inside the input schema, apply an edit (replace / insert / drop), then emit the projection
//! expression list that produces the edited output from the input.
//!
//! `FieldOp` captures the edit; `compile_field_op` applies it, returning both the output
//! schema and the per-leaf projection expressions. Construction helpers (`FieldOp::replace`,
//! `FieldOp::drop_`, `FieldOp::insert_after`) absorb up-front argument validation so call
//! sites stay one line.
//!
//! A few unrelated schema/expression conveniences live here too -- they're used by both
//! `field_op`'s internals and by other `PlanBuilder` methods:
//! - `arc_struct_or_invariant` -- wrap a field list as `SchemaRef`, surfacing the constructor error
//!   verbatim.
//! - `identity_named_expr` -- `(name, col(name))` pair for identity projections.
//! - [`load_output_schema`] -- `NodeKind::Load`'s output type rule, shared between
//!   `PlanBuilder::load` and the datafusion lowering path. Publicly re-exported so engines can
//!   share the kernel's computation rather than duplicate it.
//!
//! # Errors
//!
//! All public functions return `SchemaExprResult` -- an anyhow-style boxed `dyn Error`.
//! Boundary callers (the [`PlanBuilder`] methods and the engine-side lowering path) attach the
//! appropriate `DeltaErrorCode` via
//! [`DeltaResultExt::or_delta`][crate::plans::errors::DeltaResultExt::or_delta]; engines that
//! don't speak `DeltaError` (e.g. the datafusion lowering) consume the boxed source directly.
//!
//! [`PlanBuilder`]: crate::plans::state_machines::framework::plan_context::PlanBuilder

use std::sync::Arc;

use crate::expressions::{ColumnName, Expression, ExpressionRef};
use crate::plans::schema_expr::{check, SchemaExprResult};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::Error;

/// Build a [`SchemaExprError`] from a `format!`-style message. See `check.rs`'s `type_err!` for
/// the rationale.
macro_rules! field_err {
    ($($arg:tt)*) => {
        $crate::plans::schema_expr::SchemaExprError::from(::std::format!($($arg)*))
    };
}

// ============================================================================
// Schema / expression conveniences
// ============================================================================

/// Build a `SchemaRef` from a field list, propagating [`StructType::try_new`] errors as the
/// module's boxed error (the boundary caller attaches the Delta error code).
pub(crate) fn arc_struct_or_invariant(fields: Vec<StructField>) -> SchemaExprResult<SchemaRef> {
    Ok(Arc::new(StructType::try_new(fields)?))
}

/// `(name, col(name))` -- one identity entry for a top-level projection list.
pub(crate) fn identity_named_expr(name: impl Into<String>) -> (String, ExpressionRef) {
    let name = name.into();
    let expr = Arc::new(Expression::column([&name]));
    (name, expr)
}

/// Subset `input_schema` to the fields named in `names`, preserving each field's full
/// `StructField` (type + nullability + metadata) as it appears in the input. Used by
/// builder methods that narrow the row shape to a caller-named subset (e.g.
/// [`PlanBuilder::max_by_version`]'s `value_columns`). Errors if any name does not
/// resolve in `input_schema`. `op_name` is embedded in the error so the failure points
/// back at the original public method.
///
/// [`PlanBuilder::max_by_version`]:
///     crate::plans::state_machines::framework::plan_context::PlanBuilder::max_by_version
pub(crate) fn narrow_schema_to(
    input_schema: &StructType,
    names: &[String],
    op_name: &'static str,
) -> SchemaExprResult<SchemaRef> {
    let fields: Vec<StructField> = names
        .iter()
        .map(|name| {
            input_schema
                .field(name)
                .cloned()
                .ok_or_else(|| field_err!("{op_name}: column {name:?} not in input schema"))
        })
        .collect::<SchemaExprResult<_>>()?;
    arc_struct_or_invariant(fields)
}

/// Build the output schema for a `NodeKind::Load`: `file_schema`'s fields followed by one
/// field per `passthrough_columns` entry, with the passthrough type resolved by walking
/// `input_schema` and the passthrough name taken from the column path's leaf.
///
/// Used by both kernel-side builder validation (`PlanBuilder::load`) and engine-side
/// lowering (`delta-kernel-datafusion-engine`'s `lower_load`) so the rule stays one place.
pub fn load_output_schema(
    file_schema: &StructType,
    passthrough_columns: &[ColumnName],
    input_schema: &StructType,
) -> SchemaExprResult<SchemaRef> {
    let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
    for col in passthrough_columns {
        let leaf_name = col
            .path()
            .last()
            .ok_or_else(|| field_err!("load: passthrough column path is empty"))?
            .clone();
        let walk = input_schema.walk_column_fields(col)?;
        let leaf = walk
            .last()
            .ok_or_else(|| field_err!("load: passthrough column resolved to empty path"))?;
        fields.push(StructField::nullable(leaf_name, leaf.data_type().clone()));
    }
    arc_struct_or_invariant(fields)
}

// ============================================================================
// FieldOp
// ============================================================================

/// Field-level edit applied to the parent struct identified by a path prefix.
///
/// Each variant carries the schema-level edit and the per-leaf expression that will populate
/// the new column at the matching position in the projection list. Construct via the
/// position-specific constructors -- [`Self::append`], [`Self::insert_after_sibling`],
/// [`Self::replace`], [`Self::drop_`] -- which absorb argument validation so
/// [`compile_field_op`] takes a well-formed op.
pub(crate) enum FieldOp {
    /// Insert `new_field` into the struct at `parent`. `after = Some(name)` places it
    /// immediately after the named sibling; `after = None` appends at the end. Mirrors
    /// [`StructType::with_field_inserted_after`]'s `after: Option<&str>` contract.
    /// Constructed only via [`Self::append`] (root/parent-append) or
    /// [`Self::insert_after_sibling`] (insert-after-sibling).
    InsertAfter {
        parent: ColumnName,
        after: Option<String>,
        new_field: StructField,
        new_expr: ExpressionRef,
    },
    /// Replace the field at `target` (full path, leaf included) with `new_field`.
    /// Constructed only via [`Self::replace`], which validates `target` is non-empty.
    Replace {
        target: ColumnName,
        new_field: StructField,
        new_expr: ExpressionRef,
    },
    /// Remove the field at `target` (full path, leaf included) from its parent struct.
    /// Constructed only via [`Self::drop_`], which validates `target` is non-empty.
    Drop { target: ColumnName },
}

impl FieldOp {
    /// Append `new_field` at the end of the struct at `parent`. `parent` may be empty (root).
    pub(crate) fn append(
        parent: ColumnName,
        new_field: StructField,
        new_expr: ExpressionRef,
    ) -> Self {
        Self::InsertAfter {
            parent,
            after: None,
            new_field,
            new_expr,
        }
    }

    /// Insert `new_field` immediately after `sibling`'s position within its parent
    /// struct. Splits `sibling` into `(parent, leaf)` internally so call sites
    /// don't have to. Errors if `sibling` is empty (a sibling at the root is
    /// meaningless because the root is a struct, not a field).
    pub(crate) fn insert_after_sibling(
        sibling: ColumnName,
        new_field: StructField,
        new_expr: ExpressionRef,
    ) -> SchemaExprResult<Self> {
        let (leaf, parent) = sibling
            .path()
            .split_last()
            .ok_or_else(|| field_err!("insert_col_after: sibling path is empty"))?;
        Ok(Self::InsertAfter {
            parent: ColumnName::new(parent),
            after: Some(leaf.clone()),
            new_field,
            new_expr,
        })
    }

    /// Replace the field at `target` (full path, leaf included). Errors if `target`
    /// is empty (a replace at the root is meaningless because the input is always a
    /// struct, not a single field).
    pub(crate) fn replace(
        target: ColumnName,
        new_field: StructField,
        new_expr: ExpressionRef,
    ) -> SchemaExprResult<Self> {
        let target = ensure_nonempty_path(target, "replace_col")?;
        Ok(Self::Replace {
            target,
            new_field,
            new_expr,
        })
    }

    /// Drop the field at `target` (full path, leaf included). Errors if `target` is empty.
    pub(crate) fn drop_(target: ColumnName) -> SchemaExprResult<Self> {
        let target = ensure_nonempty_path(target, "drop_col")?;
        Ok(Self::Drop { target })
    }

    /// The parent struct path where the edit takes effect. For `Replace` / `Drop` this is
    /// the target's path with the leaf stripped; for `InsertAfter` it is the carried
    /// `parent` directly. Empty slice = root of the input schema.
    fn parent_path(&self) -> &[String] {
        match self {
            Self::InsertAfter { parent, .. } => parent.path(),
            Self::Replace { target, .. } | Self::Drop { target, .. } => target
                .path()
                .split_last()
                .map(|(_, parent)| parent)
                .unwrap_or(&[]),
        }
    }

    /// Bidirectionally check the op's `new_expr` against `input_schema` with
    /// `new_field.data_type()` as the expected output type. `Drop` has no expression to
    /// validate.
    fn validate_new_expr(&self, input_schema: &StructType) -> SchemaExprResult<()> {
        let (new_field, new_expr) = match self {
            Self::InsertAfter {
                new_field,
                new_expr,
                ..
            }
            | Self::Replace {
                new_field,
                new_expr,
                ..
            } => (new_field, new_expr),
            Self::Drop { .. } => return Ok(()),
        };
        check::check_expression(new_expr.as_ref(), input_schema, new_field).map(drop)
    }
}

/// Apply `op` to `input_schema` and return both halves a `NodeKind::Project` needs: the
/// output schema and the per-output-field projection expressions.
///
/// Validates `op.new_expr` against `input_schema` up front via
/// [`check::check_expression`]. Schema-level existence/collision checks are deferred to
/// kernel's `with_field_*` constructors.
pub(crate) fn compile_field_op(
    input_schema: &StructType,
    op: &FieldOp,
) -> SchemaExprResult<(SchemaRef, Vec<(String, ExpressionRef)>)> {
    op.validate_new_expr(input_schema)?;
    let parent_path = op.parent_path();
    let output_schema = schema_after_field_op(input_schema, parent_path, op)?;
    let named_exprs = projection_for_path(&output_schema, &[], parent_path, op)?;
    Ok((output_schema, named_exprs))
}

// ============================================================================
// FieldOp internals (private)
// ============================================================================

/// Schema half of a [`FieldOp`]: walk `parent_path` into `input_schema` via
/// [`StructType::with_struct_at`] and apply the schema-level edit at the leaf parent.
/// Defers existence and collision checks to the underlying `with_field_*` methods.
fn schema_after_field_op(
    input_schema: &StructType,
    parent_path: &[String],
    op: &FieldOp,
) -> SchemaExprResult<SchemaRef> {
    let new_struct = input_schema.with_struct_at(parent_path, |s| match op {
        FieldOp::InsertAfter {
            after, new_field, ..
        } => s.with_field_inserted_after(after.as_deref(), new_field.clone()),
        FieldOp::Replace {
            target, new_field, ..
        } => s.with_field_replaced(target_leaf(target)?, new_field.clone()),
        FieldOp::Drop { target } => {
            let leaf = target_leaf(target)?;
            if s.field(leaf).is_none() {
                // Mirror the `op:` prefix that `with_field_replaced` /
                // `with_field_inserted_after` errors carry, so `drop_col:` shows up at the
                // top of the chain when the closure error bubbles out of with_struct_at.
                return Err(Error::generic(format!(
                    "drop_col: field {leaf:?} not found"
                )));
            }
            Ok(s.with_field_removed(leaf))
        }
    })?;
    Ok(Arc::new(new_struct))
}

/// Last component of a non-empty target path. Returns a kernel error (not a `DeltaError`)
/// so it can be returned from inside `with_struct_at`'s closure. Callers of
/// [`compile_field_op`] are expected to validate non-emptiness up front, so this is a
/// defensive guard against a misconstructed [`FieldOp::Replace`] / [`FieldOp::Drop`].
fn target_leaf(target: &ColumnName) -> Result<&str, Error> {
    target
        .path()
        .last()
        .map(String::as_str)
        .ok_or_else(|| Error::generic("FieldOp target path is empty"))
}

/// Validate `path` is non-empty and return it for use as a `Replace` / `Drop` target.
/// `op_name` is embedded in the error message so the failure points back at the original
/// public method.
fn ensure_nonempty_path(path: ColumnName, op_name: &'static str) -> SchemaExprResult<ColumnName> {
    if path.path().is_empty() {
        return Err(field_err!("{op_name}: path is empty"));
    }
    Ok(path)
}

/// Projection half of a [`FieldOp`]: walk the *output* schema along `remaining`. Drops,
/// inserts, and renames are already baked into the output, so each output field needs
/// exactly one expression: a freshly-introduced field uses the op's `new_expr`; the
/// on-path ancestor recurses and re-emits via [`Expression::struct_from`]; everything
/// else identity-projects from the input via [`col_ref_at`].
///
/// `path_so_far` accumulates the absolute prefix so identity references resolve from the
/// data root rather than the inner struct.
fn projection_for_path(
    output: &StructType,
    path_so_far: &[String],
    remaining: &[String],
    op: &FieldOp,
) -> SchemaExprResult<Vec<(String, ExpressionRef)>> {
    output
        .fields()
        .map(|f| {
            let fname = f.name();
            match (remaining.split_first(), op) {
                // On the path inward: descend into the matching struct field.
                (Some((target, rest)), _) if fname == target => {
                    let DataType::Struct(sub) = f.data_type() else {
                        return Err(field_err!(
                            "projection_for_path: field {fname:?} is not a struct",
                        ));
                    };
                    let mut sub_path = path_so_far.to_vec();
                    sub_path.push(fname.clone());
                    let exprs: Vec<ExpressionRef> = projection_for_path(sub, &sub_path, rest, op)?
                        .into_iter()
                        .map(|(_, e)| e)
                        .collect();
                    Ok((fname.clone(), Arc::new(Expression::struct_from(exprs))))
                }
                // At the target struct: Replace/InsertAfter introduce `new_expr` for the
                // freshly-named field at this level.
                (
                    None,
                    FieldOp::Replace {
                        new_field,
                        new_expr,
                        ..
                    }
                    | FieldOp::InsertAfter {
                        new_field,
                        new_expr,
                        ..
                    },
                ) if fname == new_field.name() => Ok((fname.clone(), Arc::clone(new_expr))),
                // Identity passthrough: off-path field, or at target but not the new one.
                _ => Ok((fname.clone(), col_ref_at(path_so_far, fname))),
            }
        })
        .collect()
}

/// Build a `col(...)` reference to `leaf` within the struct rooted at `prefix`. When
/// `prefix` is empty this is a top-level column reference.
fn col_ref_at(prefix: &[String], leaf: &str) -> ExpressionRef {
    let mut path: Vec<String> = Vec::with_capacity(prefix.len() + 1);
    path.extend(prefix.iter().cloned());
    path.push(leaf.to_string());
    Arc::new(Expression::column(path))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{col, lit, Expression};
    use crate::schema::{DataType, StructField, StructType};

    fn input_schema() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("x", DataType::STRING),
                    StructField::nullable("y", DataType::INTEGER),
                ]))),
            ),
        ])
    }

    fn col_path(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn compile_field_op_root_append_returns_extended_schema_and_identity_projection() {
        let input = input_schema();
        let op = FieldOp::InsertAfter {
            after: None,
            new_field: StructField::nullable("c", DataType::INTEGER),
            new_expr: Arc::new(lit(0i32)),
        };
        let (out_schema, named_exprs) = compile_field_op(&input, &op).unwrap();
        let names: Vec<_> = out_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "s", "c"]);
        // Identity projection for unchanged columns, expression for the new one.
        let new_proj = named_exprs.iter().find(|(n, _)| n == "c").unwrap();
        assert_eq!(new_proj.1.as_ref(), &Expression::literal(0i32));
        assert!(named_exprs.iter().any(|(n, _)| n == "a"));
        assert!(named_exprs.iter().any(|(n, _)| n == "s"));
    }

    #[test]
    fn compile_field_op_nested_replace_keeps_outer_passthrough() {
        let input = input_schema();
        let op = FieldOp::Replace {
            target: col_path(&["s", "x"]),
            new_field: StructField::nullable("x", DataType::STRING),
            new_expr: Arc::new(col(["s", "x"])),
        };
        let (out_schema, named_exprs) = compile_field_op(&input, &op).unwrap();
        // Top-level fields unchanged.
        let names: Vec<_> = out_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "s"]);
        // Top-level projection still references `a` and `s` (struct rewritten under s).
        assert!(named_exprs.iter().any(|(n, _)| n == "a"));
        assert!(named_exprs.iter().any(|(n, _)| n == "s"));
    }

    #[test]
    fn compile_field_op_nested_drop_removes_only_target_leaf() {
        let input = input_schema();
        let op = FieldOp::Drop {
            target: col_path(&["s", "y"]),
        };
        let (out_schema, _named_exprs) = compile_field_op(&input, &op).unwrap();
        let s_field = out_schema.field("s").unwrap();
        let DataType::Struct(s_type) = s_field.data_type() else {
            panic!("s should still be a struct");
        };
        let s_field_names: Vec<_> = s_type.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(s_field_names, vec!["x"]);
    }

    #[test]
    fn compile_field_op_insert_after_sibling_orders_correctly() {
        let input = input_schema();
        let op = FieldOp::InsertAfter {
            after: Some(col_path(&["s", "x"])),
            new_field: StructField::nullable("x_alias", DataType::STRING),
            new_expr: Arc::new(col(["s", "x"])),
        };
        let (out_schema, _) = compile_field_op(&input, &op).unwrap();
        let DataType::Struct(s_type) = out_schema.field("s").unwrap().data_type() else {
            panic!("s should be a struct");
        };
        let s_names: Vec<_> = s_type.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(s_names, vec!["x", "x_alias", "y"]);
    }

    #[test]
    fn compile_field_op_drop_missing_leaf_carries_op_prefix() {
        let input = input_schema();
        let op = FieldOp::Drop {
            target: col_path(&["s", "missing"]),
        };
        let err = compile_field_op(&input, &op)
            .err()
            .expect("missing leaf must error");
        // `drop_col:` is the prefix the closure attaches via Error::generic(format!).
        assert!(
            err.to_string().contains("drop_col:"),
            "expected drop_col: prefix on error, got: {err}"
        );
    }

    #[test]
    fn compile_field_op_replace_missing_root_reports_unresolved_column() {
        let input = input_schema();
        // `col(["missing"])` won't resolve via check_expression's walk_column_refs.
        let op = FieldOp::Replace {
            target: vec!["missing".to_string()],
            new_field: StructField::nullable("missing", DataType::INTEGER),
            new_expr: Arc::new(col(["missing"])),
        };
        let err = compile_field_op(&input, &op)
            .err()
            .expect("missing root must error");
        assert!(
            err.to_string().contains("unresolved column reference"),
            "got: {err}"
        );
    }
}
