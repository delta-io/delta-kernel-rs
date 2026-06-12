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
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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
    /// The patch tree assembled so far, with each builder call applied eagerly.
    root: StructPatchNode<Item>,
    /// The first error produced by a builder call, surfaced by `build`. Once set, later calls are
    /// skipped so the original (most relevant) error is preserved.
    error: DeltaResult<()>,
}

/// The patch builder internally represents the in-progress patch specification as a tree of struct
/// and field patches, incrementally built up by validated builder calls. For example, the
/// following sequence of calls (in any order):
///
/// ```ignore
/// let builder = StructPatchBuilder::new()
///     .prepend(item1)
///     .append(item2)
///     .drop("a")
///     .replace("b", item3)
///     .insert_after("b", item4)
///     .insert_after("b", item5)
///     .insert_after_at(["c"], "x", item6);
/// ```
///
/// ... produces the following tree structure:
///
/// ```text
/// root: StructPatchNode               (patches applied to the top-level struct)
/// ├── prepended_fields: [item1]
/// ├── appended_fields: [item2]
/// └── fields:
///     ├── "a" → FieldPatchNode
///     │         ├── op: Drop
///     │         └── insert_after: []
///     │
///     ├── "b" → FieldPatchNode
///     │         ├── op: Replace(item3)
///     │         └── insert_after: [item4, item5]
///     │
///     └── "c" → FieldPatchNode
///               ├── op: Nested(StructPatchNode)    (recurse into nested struct)
///               │   ├── prepended_fields: []
///               │   ├── appended_fields: []
///               │   └── fields:
///               │       └── "x" → FieldPatchNode
///               │                 ├── op: Keep
///               │                 └── insert_after: [item6]
///               └── insert_after: []
/// ```
///
/// Conflicting operations ({Replace, Drop, Nested} x {Replace, Drop}) are detected and rejected
/// along the way, so the tree is always valid. If no conflicts were detected, the final patch is
/// produced by recursively lowering the internal tree into the output patch type, at which point it
/// is no longer possible to distinguish e.g. Drop+Insert from Replace.
#[derive(Debug)]
struct StructPatchNode<Item> {
    prepended_fields: Vec<Item>,
    appended_fields: Vec<Item>,
    fields: HashMap<String, FieldPatchNode<Item>>,
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though none of the
// fields' defaults actually requires `Item: Default`.
impl<Item> Default for StructPatchNode<Item> {
    fn default() -> Self {
        Self {
            prepended_fields: Vec::new(),
            appended_fields: Vec::new(),
            fields: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct FieldPatchNode<Item> {
    action: FieldPatchOp<Item>,
    insert_after: Vec<Item>,
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though none of the
// fields' defaults actually requires `Item: Default`.
impl<Item> Default for FieldPatchNode<Item> {
    fn default() -> Self {
        Self {
            action: FieldPatchOp::default(),
            insert_after: Vec::new(),
        }
    }
}

/// Describes what happens to the input field itself; any insertions after the input are field
/// tracked separately. A node with `Keep` is either freshly-inserted into the patch tree (with the
/// true op to be applied immediately after), or is the otherwise unmodified named anchor of 1+
/// insert-after operations. A `Nested` operation is like special `Replace`, where the replacement
/// is the computed result of applying patches to one or more of the field's children.
#[derive(Debug)]
enum FieldPatchOp<Item> {
    Keep,
    Drop { optional: bool },
    Replace(Item),
    Nested(Box<StructPatchNode<Item>>),
}

// Do not derive `Default` -- it emits `impl<Item: Default> Default`, even though the default enum
// variant does not contain any `Item`. Also suppress the clippy suggestion to define a `#[default]`
// enum variant, since that's unrelated to the problematic `Item: Default` bound.
#[allow(clippy::derivable_impls)]
impl<Item> Default for FieldPatchOp<Item> {
    fn default() -> Self {
        FieldPatchOp::Keep
    }
}

impl<Item> FieldPatchOp<Item> {
    fn is_keep(&self) -> bool {
        matches!(self, FieldPatchOp::Keep)
    }

    fn is_optional_drop(&self) -> bool {
        matches!(self, FieldPatchOp::Drop { optional: true })
    }
}

// Empty path passed by top-level methods to their nested-path counterparts.
const TOP_LEVEL: &[String] = &[];

impl<Item> StructPatchBuilder<Item> {
    /// Creates a new top-level patch builder.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            input_path: None,
            root: StructPatchNode::default(),
            error: Ok(()),
        }
    }

    /// Creates a new builder that operates on fields of a nested struct identified by `path`.
    pub fn new_nested(path: impl CollectInto<ColumnName>) -> Self {
        Self {
            input_path: Some(path.collect_into()),
            root: StructPatchNode::default(),
            error: Ok(()),
        }
    }

    /// Records a field drop.
    pub fn drop(self, field_name: impl Into<String>) -> Self {
        self.drop_at(TOP_LEVEL, field_name)
    }

    /// Records a field drop in a nested struct.
    pub fn drop_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
    ) -> Self {
        self.apply_at(struct_path, |node| node.drop(field_name, false))
    }

    /// Records an optional field drop.
    pub fn drop_if_exists(self, field_name: impl Into<String>) -> Self {
        self.drop_if_exists_at(TOP_LEVEL, field_name)
    }

    /// Records an optional field drop in a nested struct.
    pub fn drop_if_exists_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
    ) -> Self {
        self.apply_at(struct_path, |node| node.drop(field_name, true))
    }

    /// Records a field replacement.
    pub fn replace(self, field_name: impl Into<String>, item: impl Into<Item>) -> Self {
        self.replace_at(TOP_LEVEL, field_name, item)
    }

    /// Records a field replacement in a nested struct.
    pub fn replace_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        item: impl Into<Item>,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.set_action(field_name, FieldPatchOp::Replace(item.into()))
        })
    }

    /// Records an item to emit before processing the first input field.
    pub fn prepend(self, item: impl Into<Item>) -> Self {
        self.prepend_at(TOP_LEVEL, item)
    }

    /// Records an item to emit before processing the first input field of a nested struct.
    pub fn prepend_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        item: impl Into<Item>,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.prepended_fields.push(item.into());
            Ok(())
        })
    }

    /// Records an item to insert after the named field.
    pub fn insert_after(self, field_name: impl Into<String>, item: impl Into<Item>) -> Self {
        self.insert_after_at(TOP_LEVEL, field_name, item)
    }

    /// Records an item to insert after the named field in a nested struct.
    pub fn insert_after_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        field_name: impl Into<String>,
        item: impl Into<Item>,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.insert_after(field_name, item.into())
        })
    }

    /// Records an item to append after all input fields and field-specific insertions.
    pub fn append(self, item: impl Into<Item>) -> Self {
        self.append_at(TOP_LEVEL, item)
    }

    /// Records an item to append after all fields of a nested struct.
    pub fn append_at(
        self,
        struct_path: impl CollectInto<ColumnName>,
        item: impl Into<Item>,
    ) -> Self {
        self.apply_at(struct_path, |node| {
            node.appended_fields.push(item.into());
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
        op: impl FnOnce(&mut StructPatchNode<Item>) -> DeltaResult<()>,
    ) -> Self {
        if self.error.is_ok() {
            let path = struct_path.collect_into();
            self.error = self.root.child_at_mut(&path).and_then(op);
        }
        self
    }
}

impl<Item> StructPatchNode<Item> {
    /// Records an item to insert immediately after the named input field.
    fn insert_after(
        &mut self,
        field_name: impl Into<String>,
        item: impl Into<Item>,
    ) -> DeltaResult<()> {
        let entry = self.field_patch_mut(field_name.into(), |field_name, entry| {
            if entry.action.is_optional_drop() {
                return Err(Error::generic(format!(
                    "Field '{field_name}' cannot combine optional drop with insert-after"
                )));
            }
            Ok(())
        })?;
        entry.insert_after.push(item.into());
        Ok(())
    }

    /// Records a drop of the named input field. `optional` tolerates an absent field at evaluation
    /// time, but cannot combine with insertions after that field.
    fn drop(&mut self, field_name: impl Into<String>, optional: bool) -> DeltaResult<()> {
        self.set_action(field_name, FieldPatchOp::Drop { optional })
    }

    /// Records the input field action (drop/replace/patch) for the named input field. Only one such
    /// action is allowed per field.
    fn set_action(
        &mut self,
        field_name: impl Into<String>,
        action: FieldPatchOp<Item>,
    ) -> DeltaResult<()> {
        let entry = self.field_patch_mut(field_name.into(), |field_name, entry| {
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
        // All ancestors along the path to a given field must be Nested. We can convert Keep (a
        // newly-created no-op or existing insert-after) to Nested, but not Drop or Replace.
        let state = self.fields.entry(field_name.to_string()).or_default();
        if state.action.is_keep() {
            state.action = FieldPatchOp::Nested(Box::default());
        }
        let FieldPatchOp::Nested(node) = &mut state.action else {
            return Err(Error::generic(format!(
                "Cannot patch nested fields under dropped/replaced field '{field_name}'"
            )));
        };
        node.child_at_mut(remaining)
    }

    /// Fetches mutable state for the requested field.
    /// * If not yet present, create and return a new defaulted entry
    /// * If already existing, invoke the validator on it and return the entry on success
    fn field_patch_mut(
        &mut self,
        field_name: String,
        validate_existing: impl FnOnce(&str, &FieldPatchNode<Item>) -> DeltaResult<()>,
    ) -> DeltaResult<&mut FieldPatchNode<Item>> {
        match self.fields.entry(field_name) {
            hash_map::Entry::Vacant(entry) => Ok(entry.insert(FieldPatchNode::default())),
            hash_map::Entry::Occupied(entry) => {
                validate_existing(entry.key(), entry.get())?;
                Ok(entry.into_mut())
            }
        }
    }
}

// === Expression lowering ===

impl StructPatchBuilder<ExpressionRef> {
    /// Builds the final expression patch.
    ///
    /// # Errors
    ///
    /// Returns an error when builder calls request multiple drop/replace operations for the
    /// same field, or when a destructive operation on one field overlapped with an operation on a
    /// nested child field.
    pub fn build(self) -> DeltaResult<ExpressionStructPatch> {
        self.error?;
        Ok(self.root.into_struct_patch(self.input_path))
    }
}

impl TryFrom<StructPatchBuilder<ExpressionRef>> for ExpressionStructPatch {
    type Error = Error;

    fn try_from(builder: StructPatchBuilder<ExpressionRef>) -> DeltaResult<Self> {
        builder.build()
    }
}

impl StructPatchNode<ExpressionRef> {
    fn into_struct_patch(self, input_path: Option<ColumnName>) -> ExpressionStructPatch {
        let mut field_patches = HashMap::with_capacity(self.fields.len());
        for (field_name, state) in self.fields {
            let patch = state.into_field_patch(input_path.as_ref(), &field_name);
            field_patches.insert(field_name, patch);
        }

        ExpressionStructPatch {
            field_patches,
            prepended_fields: self.prepended_fields,
            appended_fields: self.appended_fields,
            input_path,
        }
    }
}

impl FieldPatchNode<ExpressionRef> {
    fn into_field_patch(
        self,
        parent_input_path: Option<&ColumnName>,
        field_name: &str,
    ) -> ExpressionFieldPatch {
        let mut field_patch = ExpressionFieldPatch::default();
        match self.action {
            FieldPatchOp::Keep => {
                field_patch.keep_input = true;
            }
            FieldPatchOp::Drop { optional } => {
                field_patch.optional = optional;
            }
            FieldPatchOp::Replace(expr) => {
                field_patch.insertions.push(expr);
            }
            FieldPatchOp::Nested(node) => {
                let field_name = ColumnName::new([field_name]);
                let child_input_path = match parent_input_path {
                    Some(parent) => parent.join(&field_name),
                    None => field_name,
                };
                let child_patch = node.into_struct_patch(Some(child_input_path));
                let child_patch = Arc::new(Expression::StructPatch(child_patch));
                field_patch.insertions.push(child_patch);
            }
        }

        field_patch.insertions.extend(self.insert_after);
        field_patch
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
    /// Returns an error if a builder call produced a conflicting operation, the input path cannot
    /// be resolved to a struct, a required field patch references a missing input field, a nested
    /// field patch targets a non-struct field, or the resulting output schema is invalid.
    pub fn build(self, input_schema: &StructType) -> DeltaResult<StructType> {
        self.error?;
        let source_schema = resolve_input_schema(input_schema, self.input_path.as_ref())?;
        self.root.build_schema(source_schema)
    }
}

impl StructPatchNode<StructField> {
    fn build_schema(self, input_schema: &StructType) -> DeltaResult<StructType> {
        let mut fields = self.fields;
        let mut output_fields = self.prepended_fields;
        output_fields.reserve(input_schema.num_fields() + fields.len());

        for input_field in input_schema.fields() {
            let Some(field_patch) = fields.remove(input_field.name()) else {
                output_fields.push(input_field.clone());
                continue;
            };
            match field_patch.action {
                FieldPatchOp::Keep => output_fields.push(input_field.clone()),
                FieldPatchOp::Drop { .. } => {}
                FieldPatchOp::Replace(field) => output_fields.push(field),
                FieldPatchOp::Nested(node) => {
                    let DataType::Struct(nested_schema) = input_field.data_type() else {
                        return Err(Error::generic(format!(
                            "Cannot patch nested fields under non-struct field '{}'",
                            input_field.name()
                        )));
                    };
                    let field = node.build_schema(nested_schema)?;
                    let field = StructField::new(input_field.name(), field, input_field.nullable);
                    output_fields.push(field.with_metadata(input_field.metadata.clone()));
                }
            }
            output_fields.extend(field_patch.insert_after);
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

        output_fields.extend(self.appended_fields);
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
    let field = input_schema.field_at(input_path)?;
    let DataType::Struct(nested_schema) = field.data_type() else {
        return Err(Error::generic(format!(
            "Patching failed: input path '{input_path}' references a non-struct field"
        )));
    };
    Ok(nested_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use crate::expressions::{lit, Expression as Expr, ExpressionStructPatchBuilder};
    use crate::schema::{DataType, SchemaStructPatchBuilder, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;

    #[test]
    fn struct_patch_builder_lowers_nested_paths_to_raw_patches() {
        let patch = ExpressionStructPatchBuilder::new()
            .drop_at(["add"], "gone")
            .replace_at(["add"], "stub", lit("replaced"))
            .insert_after_at(["add"], "x", lit(true))
            .insert_after("add", lit("after_add"))
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
        assert_eq!(stub.insertions, vec![Arc::new(lit("replaced"))]);

        let x = inner.field_patches.get("x").unwrap();
        assert!(x.keep_input);
        assert_eq!(x.insertions, vec![Arc::new(lit(true))]);

        assert_eq!(add_patch.insertions[1], Arc::new(lit("after_add")));
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

    #[rstest]
    #[case::drop_then_replace(
        ExpressionStructPatchBuilder::new().drop("a").replace("a", lit(1)),
        "multiple input field actions")]
    #[case::replace_then_drop(
        ExpressionStructPatchBuilder::new().replace("a", lit(1)).drop("a"),
        "multiple input field actions")]
    #[case::drop_with_nested_insert(
        ExpressionStructPatchBuilder::new().drop("add").insert_after_at(["add"], "x", lit(true)),
        "nested fields")]
    #[case::nested_replace_then_drop(
        ExpressionStructPatchBuilder::new().replace_at(["add"], "x", lit("one")).drop_at(["add"], "x"),
        "multiple input field actions")]
    fn expression_build_rejects_conflicting_field_actions(
        #[case] builder: ExpressionStructPatchBuilder,
        #[case] expected_msg: &str,
    ) {
        assert_result_error_with_message(builder.build(), expected_msg);
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

    // Top-level schema with a "nested" struct field (holding `nested_a`, `nested_b`) and a
    // sibling top-level field, used to exercise nested-path patching.
    fn nested_input_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::nullable("nested", schema(&["nested_a", "nested_b"])),
            field("top"),
        ])
    }

    // Patches that leave the input schema fully unchanged (same fields, types, and order).
    #[rstest]
    #[case::empty_patch(SchemaStructPatchBuilder::new())]
    #[case::optional_missing_drop(SchemaStructPatchBuilder::new().drop_if_exists("missing"))]
    fn schema_build_preserves_input_schema(#[case] builder: SchemaStructPatchBuilder) {
        let input_schema = schema(&["a", "b"]);
        assert_eq!(builder.build(&input_schema).unwrap(), input_schema);
    }

    // Patches that reorder/insert/replace/drop fields, asserted by the resulting field order.
    #[rstest]
    #[case::empty_nested_path_targets_top_level(
        schema(&["a", "b"]),
        SchemaStructPatchBuilder::new_nested(Vec::<String>::new()).insert_after("a", field("after_a")),
        &["a", "after_a", "b"])]
    #[case::inserts_before_and_after(
        schema(&["a", "b"]),
        SchemaStructPatchBuilder::new().prepend(field("prepended")).insert_after("a", field("after_a")),
        &["prepended", "a", "after_a", "b"])]
    #[case::appends_after_all_input_fields(
        schema(&["a", "b"]),
        SchemaStructPatchBuilder::new().append(field("appended_1")).append(field("appended_2")),
        &["a", "b", "appended_1", "appended_2"])]
    #[case::appends_to_empty_input(
        StructType::new_unchecked(Vec::<StructField>::new()),
        SchemaStructPatchBuilder::new().append(field("only")),
        &["only"])]
    #[case::replaces_field_at_input_position(
        schema(&["a", "b", "c"]),
        SchemaStructPatchBuilder::new().replace("b", field("bb")),
        &["a", "bb", "c"])]
    #[case::drops_field(
        schema(&["a", "b", "c"]),
        SchemaStructPatchBuilder::new().drop("b"),
        &["a", "c"])]
    #[case::preserves_patch_ordering(
        schema(&["a", "b", "c"]),
        SchemaStructPatchBuilder::new()
            .prepend(field("prepended"))
            .insert_after("a", field("after_a"))
            .replace("b", field("bb"))
            .drop("c")
            .append(field("appended")),
        &["prepended", "a", "after_a", "bb", "appended"])]
    fn schema_build_produces_expected_field_order(
        #[case] input_schema: StructType,
        #[case] builder: SchemaStructPatchBuilder,
        #[case] expected_names: &[&str],
    ) {
        let output_schema = builder.build(&input_schema).unwrap();
        assert_eq!(field_names(&output_schema), expected_names);
    }

    #[test]
    fn schema_build_nested_path_targets_nested_struct_schema() {
        let input_schema = nested_input_schema();
        let output_schema = SchemaStructPatchBuilder::new_nested(["nested"])
            .insert_after("nested_a", field("nested_inserted"))
            .build(&input_schema)
            .unwrap();

        assert_eq!(
            field_names(&output_schema),
            ["nested_a", "nested_inserted", "nested_b"]
        );
    }

    #[test]
    fn schema_build_nested_field_patch_patches_in_place() {
        let input_schema = nested_input_schema();
        let output_schema = SchemaStructPatchBuilder::new()
            .insert_after_at(["nested"], "nested_a", field("nested_inserted"))
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

    #[rstest]
    #[case::required_missing(SchemaStructPatchBuilder::new().drop("missing"))]
    #[case::required_missing_with_optional_match(
        SchemaStructPatchBuilder::new().drop_if_exists("a").drop("missing"))]
    fn schema_build_required_missing_field_errors(#[case] builder: SchemaStructPatchBuilder) {
        let result = builder.build(&schema(&["a", "b"]));
        assert_result_error_with_message(result, "Field to patch does not exist: missing");
    }
}
