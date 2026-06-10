//! Expression patches: sparse, `O(changes)` edits to the fields of an input struct.
//!
//! An [`ExpressionStructPatch`] keeps, drops, replaces, or inserts fields relative to an
//! input struct without enumerating untouched fields. Build one with
//! [`ExpressionStructPatchBuilder`], which validates conflicting operations and lowers nested
//! field paths into recursive raw patches.

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{ColumnName, Expression, ExpressionRef};
use crate::utils::CollectInto;
use crate::{DeltaResult, Error};

/// A patch affecting a single input field.
///
/// A field patch can keep or omit its input field, then insert zero or more expressions after the
/// input field's output position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpressionFieldPatch {
    /// If true, the original input field is emitted before this patch's insertions. If false, the
    /// input field is omitted and the first insertion, if present, occupies the input field's
    /// output position.
    pub keep_input: bool,
    /// Expressions emitted after this field's output position.
    pub insertions: Vec<ExpressionRef>,
    /// If true, this patch is silently ignored when the input field does not exist. Otherwise, a
    /// missing input field produces an error.
    pub optional: bool,
}

/// A sparse expression patch over the fields of one input struct.
///
/// `ExpressionStructPatch` achieves `O(changes)` space complexity instead of `O(schema_width)` by
/// only specifying fields that actually change (inserted, replaced, or deleted). Any input field
/// not specifically mentioned by the patch is passed through, unmodified and with the same relative
/// field ordering. This is particularly useful for wide schemas where only a few columns need to be
/// modified and/or dropped, or where a small number of columns need to be injected.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ExpressionStructPatch {
    /// The path to the nested input struct this patch operates on (if any). If no path is given,
    /// the patch operates directly on top-level columns.
    pub input_path: Option<ColumnName>,
    /// A mapping from named input fields to the patch to be performed on each field.
    pub field_patches: HashMap<String, ExpressionFieldPatch>,
    /// A list of new fields to emit before processing the first input field.
    pub prepended_fields: Vec<ExpressionRef>,
    /// A list of new fields to emit after all input fields and field-specific insertions.
    pub appended_fields: Vec<ExpressionRef>,
}

impl ExpressionStructPatch {
    /// True if this patch makes no changes to the selected input struct.
    pub fn is_empty(&self) -> bool {
        self.prepended_fields.is_empty()
            && self.appended_fields.is_empty()
            && self.field_patches.is_empty()
    }

    /// None if this is a top-level patch. Otherwise, the path of this nested patch.
    pub fn input_path(&self) -> Option<&ColumnName> {
        self.input_path.as_ref()
    }
}

/// Builds a raw [`ExpressionStructPatch`] from a sequence of requested patch operations.
///
/// The builder records user intent, checks for conflicting destructive operations, and lowers
/// nested field paths into recursive raw struct patches. The resulting raw patch only needs to
/// describe whether each input field is kept and which expressions are emitted at that field's
/// output position.
#[derive(Debug)]
pub struct ExpressionStructPatchBuilder {
    /// None for a top-level patch; otherwise the path of the nested struct this patch targets.
    input_path: Option<ColumnName>,
    /// The patch tree assembled so far, with each `with_*` call applied eagerly.
    root: PatchNode,
    /// The first error produced by a `with_*` call, surfaced by [`build`](Self::build). Once set,
    /// later `with_*` calls are skipped so the original (most relevant) error is preserved.
    error: DeltaResult<()>,
}

#[derive(Debug, PartialEq, Default)]
struct PatchNode {
    prepended_fields: Vec<ExpressionRef>,
    appended_fields: Vec<ExpressionRef>,
    fields: HashMap<String, PatchNodeField>,
}

#[derive(Debug, PartialEq, Default)]
struct PatchNodeField {
    action: InputFieldAction,
    insert_after: Vec<ExpressionRef>,
}

#[derive(Debug, PartialEq, Default)]
enum InputFieldAction {
    #[default]
    Keep,
    Drop {
        optional: bool,
    },
    Replace(ExpressionRef),
    Patch(Box<PatchNode>),
}

impl InputFieldAction {
    fn is_optional_drop(&self) -> bool {
        matches!(self, InputFieldAction::Drop { optional: true })
    }
}

impl ExpressionStructPatchBuilder {
    /// Creates a new top-level patch builder.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            input_path: None,
            root: PatchNode::default(),
            error: Ok(()),
        }
    }

    /// Creates a new builder that operates on fields of a nested struct identified by `path`.
    pub fn new_nested(path: impl CollectInto<ColumnName>) -> Self {
        Self {
            input_path: Some(path.collect_into()),
            root: PatchNode::default(),
            error: Ok(()),
        }
    }

    /// Records a field drop.
    pub fn with_dropped_field(self, field_name: impl Into<String>) -> Self {
        self.with_dropped_field_at(ColumnName::default(), field_name)
    }

    /// Records a field drop in a nested struct.
    pub fn with_dropped_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
    ) -> Self {
        self.apply_at(struct_path, |node| node.drop(field_name, false))
    }

    /// Records an optional field drop.
    pub fn with_dropped_field_if_exists(self, field_name: impl Into<String>) -> Self {
        self.with_dropped_field_if_exists_at(ColumnName::default(), field_name)
    }

    /// Records an optional field drop in a nested struct.
    pub fn with_dropped_field_if_exists_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
    ) -> Self {
        self.apply_at(struct_path, |node| node.drop(field_name, true))
    }

    /// Records a field replacement.
    pub fn with_replaced_field(self, field_name: impl Into<String>, expr: ExpressionRef) -> Self {
        self.with_replaced_field_at(ColumnName::default(), field_name, expr)
    }

    /// Records a field replacement in a nested struct.
    pub fn with_replaced_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.set_action(field_name, InputFieldAction::Replace(expr))
        })
    }

    /// Records an expression to emit before processing the first input field.
    pub fn with_prepended_field(self, expr: ExpressionRef) -> Self {
        self.with_prepended_field_at(ColumnName::default(), expr)
    }

    /// Records an expression to emit before processing the first input field of a nested struct.
    pub fn with_prepended_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        expr: ExpressionRef,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.prepended_fields.push(expr);
            Ok(())
        })
    }

    /// Records an expression to insert after the field identified by `path`.
    pub fn with_inserted_field_after(
        self,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> Self {
        self.with_inserted_field_after_at(ColumnName::default(), field_name, expr)
    }

    /// Records an expression to insert after the named field in a nested struct.
    pub fn with_inserted_field_after_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> Self {
        self.apply_at(struct_path, |node| node.insert_after(field_name, expr))
    }

    /// Records an expression to append after all input fields and field-specific insertions.
    pub fn with_appended_field(self, expr: ExpressionRef) -> Self {
        self.with_appended_field_at(ColumnName::default(), expr)
    }

    /// Records an expression to append after all fields of a nested struct.
    pub fn with_appended_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        expr: ExpressionRef,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.appended_fields.push(expr);
            Ok(())
        })
    }

    // Applies `op` to the (possibly nested) struct node identified by `struct_path` (an empty path
    // targets the input struct directly). Errors are deferred: the first failure is stashed in
    // `self.error` and surfaced by `build`, and once set, later operations are skipped so the
    // original error is preserved.
    fn apply_at(
        mut self,
        struct_path: impl CollectInto<ColumnName>,
        op: impl FnOnce(&mut PatchNode) -> DeltaResult<()>,
    ) -> Self {
        if self.error.is_ok() {
            let path = struct_path.collect_into();
            self.error = self.root.child_at_mut(&path).and_then(op);
        }
        self
    }

    /// Builds the raw patch.
    ///
    /// # Errors
    ///
    /// Returns an error when a `with_*` call requested multiple drop/replace operations for the
    /// same field, or when a destructive operation on one field overlapped with an operation on a
    /// nested child field.
    pub fn build(self) -> DeltaResult<ExpressionStructPatch> {
        self.error?;
        self.root.into_raw_patch(self.input_path)
    }
}

impl TryFrom<ExpressionStructPatchBuilder> for ExpressionStructPatch {
    type Error = Error;

    fn try_from(builder: ExpressionStructPatchBuilder) -> DeltaResult<Self> {
        builder.build()
    }
}

impl PatchNode {
    /// Records an expression to insert immediately after the named input field.
    fn insert_after(
        &mut self,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> DeltaResult<()> {
        let entry = self.field_state_mut(field_name.into(), |field_name, entry| {
            if entry.action.is_optional_drop() {
                return Err(Error::generic(format!(
                    "Field '{field_name}' cannot combine optional drop with insert-after"
                )));
            }
            Ok(())
        })?;
        entry.insert_after.push(expr);
        Ok(())
    }

    /// Records a drop of the named input field. `optional` tolerates an absent field at evaluation
    /// time, but cannot combine with insertions after that field.
    fn drop(&mut self, field_name: impl Into<String>, optional: bool) -> DeltaResult<()> {
        self.set_action(field_name, InputFieldAction::Drop { optional })
    }

    /// Records the input field action (drop/replace/patch) for the named input field. Only one such
    /// action is allowed per field.
    fn set_action(
        &mut self,
        field_name: impl Into<String>,
        action: InputFieldAction,
    ) -> DeltaResult<()> {
        let entry = self.field_state_mut(field_name.into(), |field_name, entry| {
            if entry.action != InputFieldAction::Keep {
                return Err(Error::generic(format!(
                    "Field '{field_name}' has multiple input field actions"
                )));
            }
            if action.is_optional_drop() && !entry.insert_after.is_empty() {
                return Err(Error::generic(format!(
                    "Field '{field_name}' cannot combine optional drop with insert-after"
                )));
            }
            Ok(())
        })?;
        entry.action = action;
        Ok(())
    }

    fn child_at_mut(&mut self, path: &[String]) -> DeltaResult<&mut Self> {
        let Some((field_name, remaining)) = path.split_first() else {
            return Ok(self);
        };
        // Descending into a child field forces its ancestor field's action to Patch. We can convert
        // Keep (a newly-created no-op or existing insert-after) to Patch, but not Drop or Replace.
        let state = self.fields.entry(field_name.to_string()).or_default();
        if state.action == InputFieldAction::Keep {
            state.action = InputFieldAction::Patch(Box::default());
        }
        let InputFieldAction::Patch(node) = &mut state.action else {
            return Err(Error::generic(format!(
                "Cannot patch nested fields under dropped/replaced field '{field_name}'"
            )));
        };
        node.child_at_mut(remaining)
    }

    /// Fetches mutable state for the requested field.
    /// * If not yet present, create and return a new defaulted entry
    /// * If already existing, invoke the validator on it and return the entry on success
    fn field_state_mut(
        &mut self,
        field_name: String,
        validate_existing: impl FnOnce(&str, &PatchNodeField) -> DeltaResult<()>,
    ) -> DeltaResult<&mut PatchNodeField> {
        match self.fields.entry(field_name) {
            hash_map::Entry::Vacant(entry) => Ok(entry.insert(PatchNodeField::default())),
            hash_map::Entry::Occupied(entry) => {
                validate_existing(entry.key(), entry.get())?;
                Ok(entry.into_mut())
            }
        }
    }

    fn into_raw_patch(self, input_path: Option<ColumnName>) -> DeltaResult<ExpressionStructPatch> {
        let mut field_patches = HashMap::with_capacity(self.fields.len());
        for (field_name, state) in self.fields {
            let patch = state.into_raw_field_patch(input_path.as_ref(), &field_name)?;
            field_patches.insert(field_name, patch);
        }

        Ok(ExpressionStructPatch {
            field_patches,
            prepended_fields: self.prepended_fields,
            appended_fields: self.appended_fields,
            input_path,
        })
    }
}

impl PatchNodeField {
    fn into_raw_field_patch(
        self,
        parent_input_path: Option<&ColumnName>,
        field_name: &str,
    ) -> DeltaResult<ExpressionFieldPatch> {
        let mut insertions = self.insert_after;
        let (keep_input, optional) = match self.action {
            InputFieldAction::Keep => {
                if insertions.is_empty() {
                    return Err(Error::generic(format!(
                        "Internal error: builder produced a no-op patch for field '{field_name}'"
                    )));
                }
                (true, false)
            }
            InputFieldAction::Drop { optional } => (false, optional),
            InputFieldAction::Replace(expr) => {
                insertions.insert(0, expr);
                (false, false)
            }
            InputFieldAction::Patch(node) => {
                if node.prepended_fields.is_empty()
                    && node.appended_fields.is_empty()
                    && node.fields.is_empty()
                {
                    return Err(Error::generic(format!(
                        "Internal error: builder produced an empty nested patch for field \
                         '{field_name}'"
                    )));
                }
                let field_name = ColumnName::new([field_name]);
                let child_input_path = match parent_input_path {
                    Some(parent) => parent.join(&field_name),
                    None => field_name,
                };
                let child_patch = node.into_raw_patch(Some(child_input_path))?;
                insertions.insert(0, Arc::new(Expression::StructPatch(child_patch)));
                (false, false)
            }
        };

        Ok(ExpressionFieldPatch {
            keep_input,
            insertions,
            optional,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ExpressionStructPatchBuilder, InputFieldAction, PatchNode, PatchNodeField};
    use crate::expressions::Expression as Expr;

    #[test]
    fn struct_patch_builder_lowers_nested_paths_to_raw_patches() {
        let patch = ExpressionStructPatchBuilder::new()
            .with_dropped_field_at(["add"], "gone")
            .with_replaced_field_at(["add"], "stub", Arc::new(Expr::literal("replaced")))
            .with_inserted_field_after_at(["add"], "x", Arc::new(Expr::literal(true)))
            .with_inserted_field_after("add", Arc::new(Expr::literal("after_add")))
            .build()
            .unwrap();

        let add_patch = patch.field_patches.get("add").unwrap();
        assert!(!add_patch.keep_input);
        assert_eq!(add_patch.insertions.len(), 2);

        let Expr::StructPatch(inner) = add_patch.insertions[0].as_ref() else {
            panic!("Expected nested struct patch");
        };
        assert_eq!(
            inner.input_path.as_ref().map(|p| p.to_string()).as_deref(),
            Some("add")
        );

        let gone = inner.field_patches.get("gone").unwrap();
        assert!(!gone.keep_input);
        assert!(gone.insertions.is_empty());

        let stub = inner.field_patches.get("stub").unwrap();
        assert!(!stub.keep_input);
        assert_eq!(stub.insertions, vec![Arc::new(Expr::literal("replaced"))]);

        let x = inner.field_patches.get("x").unwrap();
        assert!(x.keep_input);
        assert_eq!(x.insertions, vec![Arc::new(Expr::literal(true))]);

        assert_eq!(
            add_patch.insertions[1],
            Arc::new(Expr::literal("after_add"))
        );
    }

    #[test]
    fn struct_patch_builder_allows_empty_root_patches() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        assert!(patch.is_empty());
        assert!(patch.input_path().is_none());

        let nested_patch = ExpressionStructPatchBuilder::new_nested(["nested"])
            .build()
            .unwrap();
        assert!(nested_patch.is_empty());
        assert_eq!(
            nested_patch
                .input_path()
                .map(ToString::to_string)
                .as_deref(),
            Some("nested")
        );
    }

    #[test]
    fn struct_patch_builder_rejects_impossible_empty_nested_patch() {
        let result = PatchNodeField {
            action: InputFieldAction::Patch(Box::<PatchNode>::default()),
            insert_after: vec![],
        }
        .into_raw_field_patch(None, "add");

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("empty nested patch"));
    }

    #[test]
    fn struct_patch_builder_rejects_destructive_overlaps() {
        let result = ExpressionStructPatchBuilder::new()
            .with_dropped_field("a")
            .with_replaced_field("a", Arc::new(Expr::literal(1)))
            .build();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiple input field actions"));

        let result = ExpressionStructPatchBuilder::new()
            .with_replaced_field("a", Arc::new(Expr::literal(1)))
            .with_dropped_field("a")
            .build();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiple input field actions"));

        let result = ExpressionStructPatchBuilder::new()
            .with_dropped_field("add")
            .with_inserted_field_after_at(["add"], "x", Arc::new(Expr::literal(true)))
            .build();
        assert!(result.unwrap_err().to_string().contains("nested fields"));

        let result = ExpressionStructPatchBuilder::new()
            .with_replaced_field_at(["add"], "x", Arc::new(Expr::literal("one")))
            .with_dropped_field_at(["add"], "x")
            .build();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiple input field actions"));
    }
}
