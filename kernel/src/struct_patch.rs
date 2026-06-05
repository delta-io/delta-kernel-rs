//! Struct patches: sparse, `O(changes)` edits to the fields of an input struct.
//!
//! A struct patch keeps, drops, replaces, or inserts fields relative to an input struct without
//! enumerating untouched fields. Patches are built with a [`StructPatchBuilder`], which validates
//! conflicting operations and lowers nested field paths into recursive patches. Two flavors share
//! the same builder surface, each surfaced as a type alias in its own domain module:
//!
//! * [`ExpressionStructPatchBuilder`](crate::expressions::ExpressionStructPatchBuilder) emits
//!   expressions and produces an [`ExpressionStructPatch`] that is embedded in an [`Expression`]
//!   and applied to data at evaluation time.
//! * [`SchemaStructPatchBuilder`](crate::schema::SchemaStructPatchBuilder) emits schema fields and
//!   produces an output [`StructType`] directly from an input schema.

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::expressions::{ColumnName, Expression, ExpressionRef};
use crate::schema::{DataType, StructField, StructType};
use crate::utils::CollectInto;
use crate::{DeltaResult, Error};

// === Raw expression patch ===

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

// === Builder ===

/// Builds a sparse struct patch from a sequence of requested patch operations.
///
/// The builder records user intent, checks for conflicting destructive operations, and lowers
/// nested field paths into recursive struct patches. The same builder surface drives both
/// expression patching
/// ([`ExpressionStructPatchBuilder`](crate::expressions::ExpressionStructPatchBuilder)) and schema
/// patching ([`SchemaStructPatchBuilder`](crate::schema::SchemaStructPatchBuilder)); only the
/// terminal `build` step differs.
#[derive(Debug)]
pub struct StructPatchBuilder<Item> {
    /// None for a top-level patch; otherwise the path of the nested struct this patch targets.
    input_path: Option<ColumnName>,
    /// The patch tree assembled so far, with each `with_*` call applied eagerly.
    root: PatchNode<Item>,
    /// The first error produced by a `with_*` call, surfaced by `build`. Once set, later `with_*`
    /// calls are skipped so the original (most relevant) error is preserved.
    error: DeltaResult<()>,
}

#[derive(Debug)]
struct PatchNode<Item> {
    prepended_fields: Vec<Item>,
    appended_fields: Vec<Item>,
    fields: HashMap<String, PatchNodeField<Item>>,
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though none of the
// fields' defaults actually requires `Item: Default`.
impl<Item> Default for PatchNode<Item> {
    fn default() -> Self {
        Self {
            prepended_fields: Vec::new(),
            appended_fields: Vec::new(),
            fields: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct PatchNodeField<Item> {
    action: InputFieldAction<Item>,
    insert_after: Vec<Item>,
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though none of the
// fields' defaults actually requires `Item: Default`.
impl<Item> Default for PatchNodeField<Item> {
    fn default() -> Self {
        Self {
            action: InputFieldAction::default(),
            insert_after: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum InputFieldAction<Item> {
    Keep,
    Drop { optional: bool },
    Replace(Item),
    Patch(Box<PatchNode<Item>>),
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though the default enum
// variant does not contain any `Item`. Also suppress the clippy suggestion to define a `#[default]`
// enum variant, since that's unrelated to the problematic `Item: Default` bound.
#[allow(clippy::derivable_impls)]
impl<Item> Default for InputFieldAction<Item> {
    fn default() -> Self {
        InputFieldAction::Keep
    }
}

impl<Item> InputFieldAction<Item> {
    fn is_keep(&self) -> bool {
        matches!(self, InputFieldAction::Keep)
    }

    fn is_optional_drop(&self) -> bool {
        matches!(self, InputFieldAction::Drop { optional: true })
    }
}

// Empty path passed by StructPatchBuilder::with_xxx wrappers to with_xxx_at
const TOP_LEVEL: &[String] = &[];

impl<Item> StructPatchBuilder<Item> {
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
        self.with_dropped_field_at(TOP_LEVEL, field_name)
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
        self.with_dropped_field_if_exists_at(TOP_LEVEL, field_name)
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
    pub fn with_replaced_field(self, field_name: impl Into<String>, item: Item) -> Self {
        self.with_replaced_field_at(TOP_LEVEL, field_name, item)
    }

    /// Records a field replacement in a nested struct.
    pub fn with_replaced_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        item: Item,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.set_action(field_name, InputFieldAction::Replace(item))
        })
    }

    /// Records an item to emit before processing the first input field.
    pub fn with_prepended_field(self, item: Item) -> Self {
        self.with_prepended_field_at(TOP_LEVEL, item)
    }

    /// Records an item to emit before processing the first input field of a nested struct.
    pub fn with_prepended_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        item: Item,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.prepended_fields.push(item);
            Ok(())
        })
    }

    /// Records an item to insert after the named field.
    pub fn with_inserted_field_after(self, field_name: impl Into<String>, item: Item) -> Self {
        self.with_inserted_field_after_at(TOP_LEVEL, field_name, item)
    }

    /// Records an item to insert after the named field in a nested struct.
    pub fn with_inserted_field_after_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        item: Item,
    ) -> Self {
        self.apply_at(struct_path, |node| node.insert_after(field_name, item))
    }

    /// Records an item to append after all input fields and field-specific insertions.
    pub fn with_appended_field(self, item: Item) -> Self {
        self.with_appended_field_at(TOP_LEVEL, item)
    }

    /// Records an item to append after all fields of a nested struct.
    pub fn with_appended_field_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        item: Item,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.appended_fields.push(item);
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
        op: impl FnOnce(&mut PatchNode<Item>) -> DeltaResult<()>,
    ) -> Self {
        if self.error.is_ok() {
            let path = struct_path.collect_into();
            self.error = self.root.child_at_mut(&path).and_then(op);
        }
        self
    }
}

impl<Item> PatchNode<Item> {
    /// Records an item to insert immediately after the named input field.
    fn insert_after(&mut self, field_name: impl Into<String>, item: Item) -> DeltaResult<()> {
        let entry = self.field_state_mut(field_name.into(), |field_name, entry| {
            if entry.action.is_optional_drop() {
                return Err(Error::generic(format!(
                    "Field '{field_name}' cannot combine optional drop with insert-after"
                )));
            }
            Ok(())
        })?;
        entry.insert_after.push(item);
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
        action: InputFieldAction<Item>,
    ) -> DeltaResult<()> {
        let entry = self.field_state_mut(field_name.into(), |field_name, entry| {
            if !entry.action.is_keep() {
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
        if state.action.is_keep() {
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
        validate_existing: impl FnOnce(&str, &PatchNodeField<Item>) -> DeltaResult<()>,
    ) -> DeltaResult<&mut PatchNodeField<Item>> {
        match self.fields.entry(field_name) {
            hash_map::Entry::Vacant(entry) => Ok(entry.insert(PatchNodeField::default())),
            hash_map::Entry::Occupied(entry) => {
                validate_existing(entry.key(), entry.get())?;
                Ok(entry.into_mut())
            }
        }
    }
}

// === Expression lowering ===

impl StructPatchBuilder<ExpressionRef> {
    /// Builds the raw expression patch.
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

impl TryFrom<StructPatchBuilder<ExpressionRef>> for ExpressionStructPatch {
    type Error = Error;

    fn try_from(builder: StructPatchBuilder<ExpressionRef>) -> DeltaResult<Self> {
        builder.build()
    }
}

impl PatchNode<ExpressionRef> {
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

impl PatchNodeField<ExpressionRef> {
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

// === Schema lowering ===

impl StructPatchBuilder<StructField> {
    /// Builds the output struct schema for this patch over `input_schema`.
    ///
    /// If this builder targets a nested path (via [`new_nested`](Self::new_nested)), the returned
    /// schema is the patched schema for the nested struct at that path, not the full top-level
    /// input schema.
    ///
    /// # Errors
    ///
    /// Returns an error if a `with_*` call produced a conflicting operation, the input path cannot
    /// be resolved to a struct, a required field patch references a missing input field, a nested
    /// field patch targets a non-struct field, or the resulting output schema is invalid.
    pub fn build(self, input_schema: &StructType) -> DeltaResult<StructType> {
        self.error?;
        let source_schema = resolve_input_schema(input_schema, self.input_path.as_ref())?;
        self.root.build_schema(source_schema)
    }
}

impl PatchNode<StructField> {
    fn build_schema(self, input_schema: &StructType) -> DeltaResult<StructType> {
        let PatchNode {
            prepended_fields,
            appended_fields,
            mut fields,
        } = self;

        let mut output_fields = prepended_fields;
        output_fields.reserve(input_schema.num_fields() + fields.len());

        for input_field in input_schema.fields() {
            let Some(PatchNodeField {
                action,
                insert_after,
            }) = fields.remove(input_field.name())
            else {
                output_fields.push(input_field.clone());
                continue;
            };
            match action {
                InputFieldAction::Keep => output_fields.push(input_field.clone()),
                InputFieldAction::Drop { .. } => {}
                InputFieldAction::Replace(field) => output_fields.push(field),
                InputFieldAction::Patch(node) => {
                    let DataType::Struct(nested_schema) = input_field.data_type() else {
                        return Err(Error::generic(format!(
                            "Cannot patch nested fields under non-struct field '{}'",
                            input_field.name()
                        )));
                    };
                    let patched = node.build_schema(nested_schema)?;
                    output_fields.push(StructField {
                        name: input_field.name().clone(),
                        data_type: DataType::Struct(Box::new(patched)),
                        nullable: input_field.nullable,
                        metadata: input_field.metadata.clone(),
                    });
                }
            }
            output_fields.extend(insert_after);
        }

        // Any field patch that did not match an input field is an error, unless it is an optional
        // drop (which tolerates a missing input field).
        if let Some((field_name, _)) = fields
            .iter()
            .find(|(_, state)| !state.action.is_optional_drop())
        {
            return Err(Error::generic(format!(
                "Field to patch does not exist: {field_name}"
            )));
        }

        output_fields.extend(appended_fields);
        StructType::try_new(output_fields)
    }
}

/// Resolves the struct schema targeted by a top-level or nested struct patch.
fn resolve_input_schema<'a>(
    input_schema: &'a StructType,
    input_path: Option<&ColumnName>,
) -> DeltaResult<&'a StructType> {
    let input_path = match input_path {
        Some(input_path) if !input_path.path().is_empty() => input_path,
        _ => return Ok(input_schema),
    };
    let fields = input_schema.walk_column_fields(input_path)?;
    let Some(leaf) = fields.last() else {
        return Err(Error::internal_error(format!(
            "walk_column_fields returned an empty field list for input path '{input_path}'"
        )));
    };
    let DataType::Struct(nested_schema) = leaf.data_type() else {
        return Err(Error::generic(format!(
            "Patching failed: input path '{input_path}' references a non-struct field"
        )));
    };
    Ok(nested_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{InputFieldAction, PatchNode, PatchNodeField};
    use crate::expressions::{Expression as Expr, ExpressionStructPatchBuilder};
    use crate::schema::{DataType, SchemaStructPatchBuilder, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;

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
            action: InputFieldAction::Patch(Box::<PatchNode<_>>::default()),
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

    // === Schema patch tests ===

    fn field(name: impl Into<String>) -> StructField {
        StructField::nullable(name, DataType::INTEGER)
    }

    fn schema(names: &[&str]) -> StructType {
        StructType::new_unchecked(names.iter().map(|name| field(*name)).collect::<Vec<_>>())
    }

    fn field_names(schema: &StructType) -> Vec<String> {
        schema
            .fields()
            .map(|field| field.name().to_string())
            .collect()
    }

    #[test]
    fn schema_build_empty_patch_preserves_input_schema() {
        let input_schema = schema(&["a", "b"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .build(&input_schema)
            .unwrap();
        assert_eq!(output_schema, input_schema);
    }

    #[test]
    fn schema_build_empty_nested_path_patches_top_level_schema() {
        let input_schema = schema(&["a", "b"]);
        let output_schema = SchemaStructPatchBuilder::new_nested(Vec::<String>::new())
            .with_inserted_field_after("a", field("after_a"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(field_names(&output_schema), ["a", "after_a", "b"]);
    }

    #[test]
    fn schema_build_inserts_fields_before_and_after_input_fields() {
        let input_schema = schema(&["a", "b"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_prepended_field(field("prepended"))
            .with_inserted_field_after("a", field("after_a"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(
            field_names(&output_schema),
            ["prepended", "a", "after_a", "b"]
        );
    }

    #[test]
    fn schema_build_appends_fields_after_all_input_fields() {
        let input_schema = schema(&["a", "b"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_appended_field(field("appended_1"))
            .with_appended_field(field("appended_2"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(
            field_names(&output_schema),
            ["a", "b", "appended_1", "appended_2"]
        );
    }

    #[test]
    fn schema_build_appends_to_empty_input_schema() {
        let input_schema = StructType::new_unchecked(Vec::<StructField>::new());
        let output_schema = SchemaStructPatchBuilder::new()
            .with_appended_field(field("only"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(field_names(&output_schema), ["only"]);
    }

    #[test]
    fn schema_build_replaces_field_at_input_position() {
        let input_schema = schema(&["a", "b", "c"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_replaced_field("b", field("bb"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(field_names(&output_schema), ["a", "bb", "c"]);
    }

    #[test]
    fn schema_build_drops_field() {
        let input_schema = schema(&["a", "b", "c"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_dropped_field("b")
            .build(&input_schema)
            .unwrap();

        assert_eq!(field_names(&output_schema), ["a", "c"]);
    }

    #[test]
    fn schema_build_nested_path_targets_nested_struct_schema() {
        let nested_schema = schema(&["nested_a", "nested_b"]);
        let input_schema = StructType::new_unchecked(vec![
            StructField::nullable("nested", DataType::Struct(Box::new(nested_schema))),
            field("top"),
        ]);
        let output_schema = SchemaStructPatchBuilder::new_nested(["nested"])
            .with_inserted_field_after("nested_a", field("nested_inserted"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(
            field_names(&output_schema),
            ["nested_a", "nested_inserted", "nested_b"]
        );
    }

    #[test]
    fn schema_build_nested_field_patch_patches_in_place() {
        let nested_schema = schema(&["nested_a", "nested_b"]);
        let input_schema = StructType::new_unchecked(vec![
            StructField::nullable("nested", DataType::Struct(Box::new(nested_schema))),
            field("top"),
        ]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_inserted_field_after_at(["nested"], "nested_a", field("nested_inserted"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(field_names(&output_schema), ["nested", "top"]);
        let DataType::Struct(nested) = output_schema.field("nested").unwrap().data_type() else {
            panic!("Expected nested struct field");
        };
        assert_eq!(
            field_names(nested),
            ["nested_a", "nested_inserted", "nested_b"]
        );
    }

    #[test]
    fn schema_build_optional_missing_drop_is_ignored() {
        let input_schema = schema(&["a", "b"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_dropped_field_if_exists("missing")
            .build(&input_schema)
            .unwrap();

        assert_eq!(output_schema, input_schema);
    }

    #[test]
    fn schema_build_required_missing_field_returns_error() {
        let input_schema = schema(&["a", "b"]);
        let result = SchemaStructPatchBuilder::new()
            .with_dropped_field("missing")
            .build(&input_schema);

        assert_result_error_with_message(result, "Field to patch does not exist: missing");
    }

    #[test]
    fn schema_build_required_missing_field_errors_even_when_optional_patch_matches() {
        let input_schema = schema(&["a", "b"]);
        let result = SchemaStructPatchBuilder::new()
            .with_dropped_field_if_exists("a")
            .with_dropped_field("missing")
            .build(&input_schema);

        assert_result_error_with_message(result, "Field to patch does not exist: missing");
    }

    #[test]
    fn schema_build_preserves_patch_ordering() {
        let input_schema = schema(&["a", "b", "c"]);
        let output_schema = SchemaStructPatchBuilder::new()
            .with_prepended_field(field("prepended"))
            .with_inserted_field_after("a", field("after_a"))
            .with_replaced_field("b", field("bb"))
            .with_dropped_field("c")
            .with_appended_field(field("appended"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(
            field_names(&output_schema),
            ["prepended", "a", "after_a", "bb", "appended"]
        );
    }
}
