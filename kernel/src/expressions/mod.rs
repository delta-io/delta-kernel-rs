//! Definitions and functions to create and manipulate kernel expressions

use std::collections::{hash_map, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use itertools::Itertools;
use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

pub use self::column_names::{
    column_expr, column_expr_ref, column_name, column_pred, joined_column_expr, joined_column_name,
    ColumnName,
};
pub use self::scalars::{ArrayData, DecimalData, MapData, Scalar, StructData};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::SchemaRef;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::{DataType, DeltaResult, DynPartialEq, Error};

mod column_names;
pub(crate) mod literal_expression_transform;
pub(crate) use literal_expression_transform::literal_expression_transform;
mod scalars;

pub type ExpressionRef = std::sync::Arc<Expression>;
pub type PredicateRef = std::sync::Arc<Predicate>;

////////////////////////////////////////////////////////////////////////
// Operators
////////////////////////////////////////////////////////////////////////

/// A unary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UnaryPredicateOp {
    /// Unary Is Null
    IsNull,
}

/// A binary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryPredicateOp {
    /// Comparison Less Than
    LessThan,
    /// Comparison Greater Than
    GreaterThan,
    /// Comparison Equal
    Equal,
    /// Distinct
    Distinct,
    /// IN
    In,
}

/// A unary expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryExpressionOp {
    /// Convert struct data to JSON-encoded strings
    ToJson,
}

/// A binary expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryExpressionOp {
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
}

/// A variadic expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VariadicExpressionOp {
    /// Collapse multiple values into one by taking the first non-null value
    Coalesce,
}

/// A junction (AND/OR) predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JunctionPredicateOp {
    /// Conjunction
    And,
    /// Disjunction
    Or,
}

/// A kernel-supplied scalar expression evaluator which in particular can convert column references
/// (i.e. [`Expression::Column`]) to [`Scalar`] values. [`OpaqueExpressionOp::eval_expr_scalar`] and
/// [`OpaquePredicateOp::eval_pred_scalar`] rely on this evaluator.
///
/// If the evaluator produces `None`, it means kernel was unable to evaluate
/// the input expression. Otherwise, `Some(Scalar)` is the result of that evaluation (possibly
/// `Scalar::Null` if the output was NULL).
pub type ScalarExpressionEvaluator<'a> = dyn Fn(&Expression) -> Option<Scalar> + 'a;

/// An opaque expression operation (ie defined and implemented by the engine).
pub trait OpaqueExpressionOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this opaque expression, e.g. for partition pruning.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`ScalarExpressionEvaluator`],
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. with the wrong number and/or types of arguments, None input,
    /// etc); the operation is disqualified from participating in partition pruning.
    ///
    /// `Ok(Scalar::Null)` means the operation actually produced a legitimately NULL result.
    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar>;
}

/// An opaque predicate operation (ie defined and implemented by the engine).
pub trait OpaquePredicateOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this (possibly inverted) opaque predicate on behalf of a
    /// [`DirectPredicateEvaluator`], e.g. for partition pruning or to evaluate an opaque data
    /// skipping predicate produced previously by an [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by calling
    /// back to the provided [`ScalarExpressionEvaluator`] and/or [`DirectPredicateEvaluator`].
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. wrong number and/or types of arguments, None input, etc); the
    /// operation is disqualified from participating in partition pruning and/or data skipping.
    ///
    /// `Ok(None)` means the operation actually produced a legitimately NULL output.
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>>;

    /// Evaluates this (possibly inverted) opaque predicate for data skipping on behalf of a
    /// [`DirectDataSkippingPredicateEvaluator`], e.g. for parquet row group skipping.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`DirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support evaluation as a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in row group skipping.
    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool>;

    /// Converts this (possibly inverted) opaque predicate to a data skipping predicate on behalf of
    /// an [`IndirectDataSkippingPredicateEvaluator`], e.g. for stats-based file pruning.
    ///
    /// Implementations can transform the predicate and its child expressions however they see fit,
    /// possibly by calling back to the owning [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support conversion to a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in file pruning.
    //
    // NOTE: It would be nicer if this method could accept an `Arc<Self>`, in case the data skipping
    // predicate rewrite can reuse the same operation. But sadly, that would not be dyn-compatible.
    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate>;
}

/// A shared reference to an [`OpaqueExpressionOp`] instance.
pub type OpaqueExpressionOpRef = Arc<dyn OpaqueExpressionOp>;

/// A shared reference to an [`OpaquePredicateOp`] instance.
pub type OpaquePredicateOpRef = Arc<dyn OpaquePredicateOp>;

////////////////////////////////////////////////////////////////////////
// Expressions and predicates
////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryPredicate {
    /// The operator.
    pub op: UnaryPredicateOp,
    /// The input expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryPredicate {
    /// The operator.
    pub op: BinaryPredicateOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryExpression {
    /// The operator.
    pub op: UnaryExpressionOp,
    /// The input expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryExpression {
    /// The operator.
    pub op: BinaryExpressionOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariadicExpression {
    /// The operator.
    pub op: VariadicExpressionOp,
    /// The input expressions.
    pub exprs: Vec<Expression>,
}

/// An expression that parses a JSON string into a struct with the given schema.
/// This is the inverse of `ToJson` - it converts a JSON-encoded string column into a
/// struct column.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParseJsonExpression {
    /// The expression that evaluates to a STRING column containing JSON objects.
    pub json_expr: Box<Expression>,
    /// The schema defining the structure to parse the JSON into.
    pub output_schema: SchemaRef,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JunctionPredicate {
    /// The operator.
    pub op: JunctionPredicateOp,
    /// The input predicates.
    pub preds: Vec<Predicate>,
}

// NOTE: We have to use `Arc<dyn OpaquePredicateOp>` instead of `Box<dyn OpaquePredicateOp>` because
// we cannot require `OpaquePredicateOp: Clone` (not a dyn-compatible trait). Instead, we must rely
// on cheap `Arc` clone, which does not duplicate the inner object.
//
// TODO(#1564): OpaquePredicate currently does not support serialization or deserialization. In the
// future, the [`OpaquePredicateOp`] trait can be extended to support ser/de.
#[derive(Clone, Debug)]
pub struct OpaquePredicate {
    pub op: OpaquePredicateOpRef,
    pub exprs: Vec<Expression>,
}
fn fail_serialize_opaque_predicate<S>(
    _value: &OpaquePredicate,
    _serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(ser::Error::custom("Cannot serialize an Opaque Predicate"))
}

fn fail_deserialize_opaque_predicate<'de, D>(_deserializer: D) -> Result<OpaquePredicate, D::Error>
where
    D: Deserializer<'de>,
{
    Err(de::Error::custom("Cannot deserialize an Opaque Predicate"))
}

impl OpaquePredicate {
    pub(crate) fn new(
        op: OpaquePredicateOpRef,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        let exprs = exprs.into_iter().collect();
        Self { op, exprs }
    }
}

// NOTE: We have to use `Arc<dyn OpaqueExpressionOp>` instead of `Box<dyn OpaqueExpressionOp>`
// because we cannot require `OpaqueExpressionOp: Clone` (not a dyn-compatible trait). Instead, we
// must rely on cheap `Arc` clone, which does not duplicate the inner object.
//
// TODO(#1564): OpaqueExpression currently does not support serialization or deserialization. In the
// future, the [`OpaqueExpressionOp`] trait can be extended to support ser/de.
#[derive(Clone, Debug)]
pub struct OpaqueExpression {
    pub op: OpaqueExpressionOpRef,
    pub exprs: Vec<Expression>,
}

impl OpaqueExpression {
    pub(crate) fn new(
        op: OpaqueExpressionOpRef,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        let exprs = exprs.into_iter().collect();
        Self { op, exprs }
    }
}

fn fail_serialize_opaque_expression<S>(
    _value: &OpaqueExpression,
    _serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(ser::Error::custom("Cannot serialize an Opaque Expression"))
}

fn fail_deserialize_opaque_expression<'de, D>(
    _deserializer: D,
) -> Result<OpaqueExpression, D::Error>
where
    D: Deserializer<'de>,
{
    Err(de::Error::custom("Cannot deserialize an Opaque Expression"))
}

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

impl Default for ExpressionFieldPatch {
    fn default() -> Self {
        Self {
            keep_input: true,
            insertions: Vec::new(),
            optional: false,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Default)]
struct PatchNode {
    prepended_fields: Vec<ExpressionRef>,
    appended_fields: Vec<ExpressionRef>,
    fields: HashMap<String, FieldPatchBuildState>,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct FieldPatchBuildState {
    action: InputFieldAction,
    insert_after: Vec<ExpressionRef>,
}

#[derive(Debug, Clone, PartialEq, Default)]
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
    pub fn new_nested<A>(path: impl IntoIterator<Item = A>) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        Self {
            input_path: Some(ColumnName::new(path)),
            root: PatchNode::default(),
            error: Ok(()),
        }
    }

    /// Records a field drop.
    pub fn with_dropped_field(self, field_name: impl Into<String>) -> Self {
        self.with_dropped_field_at(ColumnName::default(), field_name)
    }

    /// Records a field drop in a nested struct.
    pub fn with_dropped_field_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        field_name: impl Into<String>,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        self.apply_at(struct_path, |node| node.drop(field_name, false))
    }

    /// Records an optional field drop.
    pub fn with_dropped_field_if_exists(self, field_name: impl Into<String>) -> Self {
        self.with_dropped_field_if_exists_at(ColumnName::default(), field_name)
    }

    /// Records an optional field drop in a nested struct.
    pub fn with_dropped_field_if_exists_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        field_name: impl Into<String>,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        self.apply_at(struct_path, |node| node.drop(field_name, true))
    }

    /// Records a field replacement.
    pub fn with_replaced_field(self, field_name: impl Into<String>, expr: ExpressionRef) -> Self {
        self.with_replaced_field_at(ColumnName::default(), field_name, expr)
    }

    /// Records a field replacement in a nested struct.
    pub fn with_replaced_field_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        self.apply_at(struct_path, |node| {
            node.set_action(field_name, InputFieldAction::Replace(expr))
        })
    }

    /// Records an expression to emit before processing the first input field.
    pub fn with_prepended_field(self, expr: ExpressionRef) -> Self {
        self.with_prepended_field_at(ColumnName::default(), expr)
    }

    /// Records an expression to emit before processing the first input field of a nested struct.
    pub fn with_prepended_field_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        expr: ExpressionRef,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
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
    pub fn with_inserted_field_after_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        field_name: impl Into<String>,
        expr: ExpressionRef,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        self.apply_at(struct_path, |node| node.insert_after(field_name, expr))
    }

    /// Records an expression to append after all input fields and field-specific insertions.
    pub fn with_appended_field(self, expr: ExpressionRef) -> Self {
        self.with_appended_field_at(ColumnName::default(), expr)
    }

    /// Records an expression to append after all fields of a nested struct.
    pub fn with_appended_field_at<A>(
        self,
        struct_path: impl IntoIterator<Item = A>,
        expr: ExpressionRef,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        self.apply_at(struct_path, |node| {
            node.appended_fields.push(expr);
            Ok(())
        })
    }

    // Applies `op` to the (possibly nested) struct node identified by `struct_path` (an empty path
    // targets the input struct directly). Errors are deferred: the first failure is stashed in
    // `self.error` and surfaced by `build`, and once set, later operations are skipped so the
    // original error is preserved.
    fn apply_at<A>(
        mut self,
        struct_path: impl IntoIterator<Item = A>,
        op: impl FnOnce(&mut PatchNode) -> DeltaResult<()>,
    ) -> Self
    where
        ColumnName: FromIterator<A>,
    {
        if self.error.is_ok() {
            let path = ColumnName::new(struct_path);
            self.error = self.root.node_at_mut(&path).and_then(op);
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

    fn node_at_mut(&mut self, path: &[String]) -> DeltaResult<&mut Self> {
        let Some((field_name, remaining)) = path.split_first() else {
            return Ok(self);
        };
        // NOTE: No need to validate existing entries, `patch_node_mut` handles it.
        self.fields
            .entry(field_name.to_string())
            .or_default()
            .patch_node_mut(field_name)?
            .node_at_mut(remaining)
    }

    /// Fetches mutable state for the requested field.
    /// * If not yet present, create and return a new defaulted entry
    /// * If already existing, invoke the validator on it and return the entry on success
    fn field_state_mut(
        &mut self,
        field_name: String,
        validate_existing: impl FnOnce(&str, &FieldPatchBuildState) -> DeltaResult<()>,
    ) -> DeltaResult<&mut FieldPatchBuildState> {
        match self.fields.entry(field_name) {
            hash_map::Entry::Vacant(entry) => Ok(entry.insert(FieldPatchBuildState::default())),
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

impl FieldPatchBuildState {
    /// Every ancestor field of a field we want to patch must have a Patch field action. We can
    /// convert from Keep (no-op or insert-after) to Patch, but not Drop or Replace.
    fn patch_node_mut(&mut self, field_name: &str) -> DeltaResult<&mut PatchNode> {
        if self.action == InputFieldAction::Keep {
            self.action = InputFieldAction::Patch(Box::default());
        }
        let InputFieldAction::Patch(node) = &mut self.action else {
            return Err(Error::generic(format!(
                "Cannot patch nested fields under dropped/replaced field '{field_name}'"
            )));
        };
        Ok(node)
    }

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

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name.
    Column(ColumnName),
    /// A predicate treated as a boolean expression
    Predicate(Box<Predicate>), // should this be Arc?
    /// A struct computed from a Vec of expressions.
    /// The optional nullability predicate, if provided and evaluates to false/null, makes the
    /// entire struct null.
    Struct(Vec<ExpressionRef>, Option<ExpressionRef>),
    /// A sparse patch of a struct. More efficient than `Struct` for wide schemas
    /// where only a few fields change, achieving O(changes) instead of O(schema_width) complexity.
    #[serde(alias = "Transform")]
    StructPatch(ExpressionStructPatch),
    /// An expression that takes one expression as input.
    Unary(UnaryExpression),
    /// An expression that takes two expressions as input.
    Binary(BinaryExpression),
    /// An expression that takes a variable number of expressions as input.
    Variadic(VariadicExpression),
    /// An expression that the engine defines and implements. Kernel interacts with the expression
    /// only through methods provided by the [`OpaqueExpressionOp`] trait.
    #[serde(serialize_with = "fail_serialize_opaque_expression")]
    #[serde(deserialize_with = "fail_deserialize_opaque_expression")]
    Opaque(OpaqueExpression),
    /// An unknown expression (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown expressions as if they were literal NULL
    /// values (which may disable skipping if it "poisons" the predicate), but engines MUST NOT
    /// attempt to interpret them as NULL when evaluating query filters because it could produce
    /// incorrect results. For example, converting `WHERE <fancy-udf-invocation> IS NULL` to `WHERE
    /// <unknown> IS NULL` to `WHERE NULL IS NULL` is equivalent to `WHERE TRUE` and would include
    /// all rows -- almost certainly NOT what the query author intended. Use `Expression::Opaque`
    /// for expressions kernel doesn't understand but which engine can still evaluate.
    Unknown(String),
    /// Parse a JSON string expression into a struct with the given schema.
    ParseJson(ParseJsonExpression),
    /// Extract keys from a `Map<String, String>` and parse values into a typed struct using
    /// Delta's partition value serialization rules.
    MapToStruct(MapToStructExpression),
}

/// A SQL predicate.
///
/// These predicates do not track or validate data types, other than the type
/// of literals. It is up to the predicate evaluator to validate the
/// predicate against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    /// A boolean-valued expression, useful for e.g. `AND(<boolean_col1>, <boolean_col2>)`.
    BooleanExpression(Expression),
    /// Boolean inversion (true <-> false)
    ///
    /// NOTE: NOT is not a normal unary predicate, because it requires a predicate as input (not an
    /// expression), and is never directly evaluated. Instead, observing that all predicates are
    /// invertible, NOT is always pushed down into its child predicate, inverting it. For example,
    /// `NOT (a < b)` pushes down and inverts `<` to `>=`, producing `a >= b`.
    Not(Box<Predicate>),
    /// A unary operation.
    Unary(UnaryPredicate),
    /// A binary operation.
    Binary(BinaryPredicate),
    /// A junction operation (AND/OR).
    Junction(JunctionPredicate),
    /// A predicate that the engine defines and implements. Kernel interacts with the predicate
    /// only through methods provided by the [`OpaquePredicateOp`] trait.
    #[serde(serialize_with = "fail_serialize_opaque_predicate")]
    #[serde(deserialize_with = "fail_deserialize_opaque_predicate")]
    Opaque(OpaquePredicate),
    /// An unknown predicate (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown predicates as if they were literal NULL
    /// values (which may disable skipping if it "poisons" the predicate), but engines MUST NOT
    /// attempt to interpret them as NULL when evaluating query filters because it could
    /// produce incorrect results. For example, converting `WHERE <fancy-udf-invocation>` to
    /// `WHERE NULL` is equivalent to `WHERE FALSE` and would filter out all rows -- almost
    /// certainly NOT what the query author intended. Use `Predicate::Opaque` for predicates
    /// kernel doesn't understand but which engine can still evaluate.
    Unknown(String),
}

////////////////////////////////////////////////////////////////////////
// Struct/Enum impls
////////////////////////////////////////////////////////////////////////

impl BinaryPredicateOp {
    /// True if this is a comparison for which NULL input always produces NULL output
    pub(crate) fn is_null_intolerant(&self) -> bool {
        use BinaryPredicateOp::*;
        match self {
            LessThan | GreaterThan | Equal => true,
            Distinct | In => false, // tolerates NULL input
        }
    }
}

impl JunctionPredicateOp {
    pub(crate) fn invert(&self) -> JunctionPredicateOp {
        use JunctionPredicateOp::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

impl UnaryExpression {
    pub(crate) fn new(op: UnaryExpressionOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl UnaryPredicate {
    pub(crate) fn new(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl BinaryExpression {
    pub(crate) fn new(
        op: BinaryExpressionOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl BinaryPredicate {
    pub(crate) fn new(
        op: BinaryPredicateOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl VariadicExpression {
    pub(crate) fn new(
        op: VariadicExpressionOp,
        exprs: impl IntoIterator<Item = impl Into<Expression>>,
    ) -> Self {
        let exprs = exprs.into_iter().map(Into::into).collect();
        Self { op, exprs }
    }
}

impl ParseJsonExpression {
    pub(crate) fn new(json_expr: impl Into<Expression>, output_schema: SchemaRef) -> Self {
        Self {
            json_expr: Box::new(json_expr.into()),
            output_schema,
        }
    }
}

/// Transforms a `Map<String, String>` column into a struct whose schema is provided by the
/// evaluator's output type (via `result_type`). Each row in the map column becomes one row in
/// the output struct column: a `key` -> `value` mapping in the map means the struct field named
/// `key` receives `value`, parsed into the field's target type using Delta's partition value
/// serialization rules ([`PrimitiveType::parse_scalar`]).
///
/// - Missing keys produce null values
/// - Parse errors are propagated (indicating a broken table)
/// - Duplicate map keys are resolved by taking the rightmost entry
///
/// [`PrimitiveType::parse_scalar`]: crate::schema::PrimitiveType::parse_scalar
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MapToStructExpression {
    /// The expression that evaluates to a `Map<String, String>` column.
    pub map_expr: Box<Expression>,
}

impl MapToStructExpression {
    pub(crate) fn new(map_expr: impl Into<Expression>) -> Self {
        Self {
            map_expr: Box::new(map_expr.into()),
        }
    }
}

impl JunctionPredicate {
    pub(crate) fn new(op: JunctionPredicateOp, preds: Vec<Predicate>) -> Self {
        Self { op, preds }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        references.transform_expr(self);
        references.0
    }

    /// Create a new column name expression from input satisfying `FromIterator for ColumnName`.
    pub fn column<A>(field_names: impl IntoIterator<Item = A>) -> Expression
    where
        ColumnName: FromIterator<A>,
    {
        ColumnName::new(field_names).into()
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    /// Creates a NULL literal expression
    pub const fn null_literal(data_type: DataType) -> Self {
        Self::Literal(Scalar::Null(data_type))
    }

    /// Wraps a predicate as a boolean-valued expression
    pub fn from_pred(value: Predicate) -> Self {
        match value {
            Predicate::BooleanExpression(expr) => expr,
            _ => Self::Predicate(Box::new(value)),
        }
    }

    /// Create a new struct expression.
    ///
    /// The field names and types are supplied by the caller at evaluation time via the
    /// `result_type` parameter of the expression evaluator. Use this when the schema is
    /// always available from external context (e.g. the expression is the top-level output
    /// of [`crate::ExpressionEvaluator`]).
    pub fn struct_from(exprs: impl IntoIterator<Item = impl Into<Arc<Self>>>) -> Self {
        Self::Struct(exprs.into_iter().map(Into::into).collect(), None)
    }

    /// Create a new struct expression with a nullability predicate.
    ///
    /// When the predicate evaluates to false or null for a row, the entire struct is null
    /// for that row.
    pub fn struct_with_nullability_from(
        exprs: impl IntoIterator<Item = impl Into<Arc<Self>>>,
        nullability_predicate: impl Into<Arc<Self>>,
    ) -> Self {
        Self::Struct(
            exprs.into_iter().map(Into::into).collect(),
            Some(nullability_predicate.into()),
        )
    }

    /// Creates a new struct patch expression from a raw patch or patch builder.
    ///
    /// Returns an expression that applies the supplied sparse patch to an input struct. Passing a
    /// raw [`ExpressionStructPatch`] is infallible; passing an [`ExpressionStructPatchBuilder`]
    /// validates and lowers the recorded operations before constructing the expression.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied patch builder contains conflicting operations.
    pub fn struct_patch<P>(patch: P) -> DeltaResult<Self>
    where
        P: TryInto<ExpressionStructPatch>,
        Error: From<P::Error>,
    {
        Ok(Self::StructPatch(patch.try_into()?))
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(self) -> Predicate {
        Predicate::is_null(self)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(self) -> Predicate {
        Predicate::is_not_null(self)
    }

    /// Create a new predicate `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Predicate {
        Predicate::eq(self, other)
    }

    /// Create a new predicate `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Predicate {
        Predicate::ne(self, other)
    }

    /// Create a new predicate `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Predicate {
        Predicate::le(self, other)
    }

    /// Create a new predicate `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Predicate {
        Predicate::lt(self, other)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Predicate {
        Predicate::ge(self, other)
    }

    /// Create a new predicate `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Predicate {
        Predicate::gt(self, other)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Predicate {
        Predicate::distinct(self, other)
    }

    /// Creates a new unary expression
    pub fn unary(op: UnaryExpressionOp, expr: impl Into<Expression>) -> Self {
        Self::Unary(UnaryExpression::new(op, expr))
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryExpressionOp,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryExpression::new(op, lhs, rhs))
    }

    /// Creates a new variadic expression
    pub fn variadic(
        op: VariadicExpressionOp,
        exprs: impl IntoIterator<Item = impl Into<Expression>>,
    ) -> Self {
        Self::Variadic(VariadicExpression::new(op, exprs))
    }

    /// Creates a new COALESCE expression that returns the first non-null value.
    ///
    /// COALESCE evaluates expressions in order and returns the first non-null result.
    /// If all expressions evaluate to null, the result is null.
    pub fn coalesce(exprs: impl IntoIterator<Item = impl Into<Expression>>) -> Self {
        Self::variadic(VariadicExpressionOp::Coalesce, exprs)
    }

    /// Creates a new opaque expression
    pub fn opaque(
        op: impl OpaqueExpressionOp,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        Self::Opaque(OpaqueExpression::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown expression
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }

    /// Creates a new ParseJson expression that parses a JSON string column into a struct.
    /// This is the inverse of `ToJson` - it converts a JSON-encoded string into a struct.
    pub fn parse_json(json_expr: impl Into<Expression>, output_schema: SchemaRef) -> Self {
        Self::ParseJson(ParseJsonExpression::new(json_expr, output_schema))
    }

    /// Extracts keys from a `Map<String, String>` and parses values into a typed struct using
    /// Delta's partition value serialization rules. The output struct schema is determined by the
    /// evaluator's `result_type`.
    pub fn map_to_struct(map_expr: impl Into<Expression>) -> Self {
        Self::MapToStruct(MapToStructExpression::new(map_expr))
    }
}

impl Predicate {
    /// Returns a set of columns referenced by this predicate.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        references.transform_pred(self);
        references.0
    }

    /// Creates a new boolean column reference. See also [`Expression::column`].
    pub fn column<A>(field_names: impl IntoIterator<Item = A>) -> Predicate
    where
        ColumnName: FromIterator<A>,
    {
        Self::from_expr(ColumnName::new(field_names))
    }

    /// Create a new literal boolean value
    pub const fn literal(value: bool) -> Self {
        Self::BooleanExpression(Expression::Literal(Scalar::Boolean(value)))
    }

    /// Creates a NULL literal boolean value
    pub const fn null_literal() -> Self {
        Self::BooleanExpression(Expression::Literal(Scalar::Null(DataType::BOOLEAN)))
    }

    /// Converts a boolean-valued expression into a predicate
    pub fn from_expr(expr: impl Into<Expression>) -> Self {
        match expr.into() {
            Expression::Predicate(p) => *p,
            expr => Predicate::BooleanExpression(expr),
        }
    }

    /// Logical NOT (boolean inversion)
    pub fn not(pred: impl Into<Self>) -> Self {
        Self::Not(Box::new(pred.into()))
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(expr: impl Into<Expression>) -> Predicate {
        Self::unary(UnaryPredicateOp::IsNull, expr)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(expr: impl Into<Expression>) -> Predicate {
        Self::not(Self::is_null(expr))
    }

    /// Create a new predicate `self == other`
    pub fn eq(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Equal, a, b)
    }

    /// Create a new predicate `self != other`
    pub fn ne(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::Equal, a, b))
    }

    /// Create a new predicate `self <= other`
    pub fn le(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::GreaterThan, a, b))
    }

    /// Create a new predicate `self < other`
    pub fn lt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::LessThan, a, b)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::LessThan, a, b))
    }

    /// Create a new predicate `self > other`
    pub fn gt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::GreaterThan, a, b)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Distinct, a, b)
    }

    /// Create a new predicate `self AND other`
    pub fn and(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::and_from([a.into(), b.into()])
    }

    /// Create a new predicate `self OR other`
    pub fn or(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::or_from([a.into(), b.into()])
    }

    /// Creates a new predicate AND(preds...). See [`Self::junction`] for normalization of
    /// empty and single-element inputs.
    pub fn and_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionPredicateOp::And, preds)
    }

    /// Creates a new predicate OR(preds...). See [`Self::junction`] for normalization of
    /// empty and single-element inputs.
    pub fn or_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionPredicateOp::Or, preds)
    }

    /// Creates a new unary predicate OP expr
    pub fn unary(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self::Unary(UnaryPredicate { op, expr })
    }

    /// Creates a new binary predicate lhs OP rhs
    pub fn binary(
        op: BinaryPredicateOp,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryPredicate {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }

    /// Creates a new junction predicate OP(preds...). Normalizes degenerate cases:
    ///
    /// - Empty junction returns the identity element (the value that has no effect when combined
    ///   with other predicates under the same operator):
    ///   - `AND()` -> `true`, because `true AND p` == `p` for any predicate `p`.
    ///   - `OR()` -> `false`, because `false OR p` == `p` for any predicate `p`.
    /// - Single-element junction unwraps the element: `AND(p)` / `OR(p)` -> `p`.
    pub fn junction(op: JunctionPredicateOp, preds: impl IntoIterator<Item = Self>) -> Self {
        let mut preds: Vec<_> = preds.into_iter().collect();
        match preds.len() {
            0 => match op {
                JunctionPredicateOp::And => Self::literal(true),
                JunctionPredicateOp::Or => Self::literal(false),
            },
            // A junction of one predicate is just that predicate.
            1 => preds.remove(0),
            _ => Self::Junction(JunctionPredicate { op, preds }),
        }
    }

    /// Creates a new opaque predicate
    pub fn opaque(op: impl OpaquePredicateOp, exprs: impl IntoIterator<Item = Expression>) -> Self {
        Self::Opaque(OpaquePredicate::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown predicate
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }
}

////////////////////////////////////////////////////////////////////////
// Trait impls
////////////////////////////////////////////////////////////////////////

impl PartialEq for OpaquePredicate {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

impl PartialEq for OpaqueExpression {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

impl Display for UnaryExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use UnaryExpressionOp::*;
        match self {
            ToJson => write!(f, "TO_JSON"),
        }
    }
}

impl Display for BinaryExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryExpressionOp::*;
        match self {
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
        }
    }
}

impl Display for VariadicExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use VariadicExpressionOp::*;
        match self {
            Coalesce => write!(f, "COALESCE"),
        }
    }
}

impl Display for BinaryPredicateOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryPredicateOp::*;
        match self {
            LessThan => write!(f, "<"),
            GreaterThan => write!(f, ">"),
            Equal => write!(f, "="),
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Distinct => write!(f, "DISTINCT"),
            In => write!(f, "IN"),
        }
    }
}

// Helper for displaying the children of variadic expressions and predicates
fn format_child_list<T: Display>(children: &[T]) -> String {
    children.iter().map(|c| format!("{c}")).join(", ")
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Literal(l) => write!(f, "{l}"),
            Column(name) => write!(f, "Column({name})"),
            Predicate(p) => write!(f, "{p}"),
            Struct(exprs, _) => write!(f, "Struct({})", format_child_list(exprs)),
            StructPatch(patch) => {
                write!(f, "StructPatch(")?;
                let mut sep = "";
                if !patch.prepended_fields.is_empty() {
                    let prepended_fields = format_child_list(&patch.prepended_fields);
                    write!(f, "prepend [{prepended_fields}]")?;
                    sep = ", ";
                }
                for (field_name, field_patch) in &patch.field_patches {
                    if !field_patch.keep_input && field_patch.insertions.is_empty() {
                        write!(f, "{sep}drop {field_name}")?;
                        sep = ", ";
                    }
                    if !field_patch.insertions.is_empty() {
                        let insertions = format_child_list(&field_patch.insertions);
                        let action = if field_patch.keep_input {
                            "after"
                        } else {
                            "replace/after"
                        };
                        write!(f, "{sep}{action} {field_name} insert [{insertions}]")?;
                        sep = ", ";
                    }
                }
                if !patch.appended_fields.is_empty() {
                    let appended_fields = format_child_list(&patch.appended_fields);
                    write!(f, "{sep}append [{appended_fields}]")?;
                }
                write!(f, ")")
            }
            Unary(UnaryExpression { op, expr }) => write!(f, "{op}({expr})"),
            Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
            Variadic(VariadicExpression { op, exprs }) => {
                write!(f, "{op}({})", format_child_list(exprs))
            }
            Opaque(OpaqueExpression { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
            ParseJson(p) => {
                write!(
                    f,
                    "PARSE_JSON({}, <schema:{} fields>)",
                    p.json_expr,
                    p.output_schema.fields().len()
                )
            }
            MapToStruct(m) => write!(f, "MAP_TO_STRUCT({})", m.map_expr),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Predicate::*;
        match self {
            BooleanExpression(expr) => write!(f, "{expr}"),
            Not(pred) => write!(f, "NOT({pred})"),
            Binary(BinaryPredicate {
                op: BinaryPredicateOp::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Binary(BinaryPredicate { op, left, right }) => write!(f, "{left} {op} {right}"),
            Unary(UnaryPredicate { op, expr }) => match op {
                UnaryPredicateOp::IsNull => write!(f, "{expr} IS NULL"),
            },
            Junction(JunctionPredicate { op, preds }) => {
                let op = match op {
                    JunctionPredicateOp::And => "AND",
                    JunctionPredicateOp::Or => "OR",
                };
                write!(f, "{op}({})", format_child_list(preds))
            }
            Opaque(OpaquePredicate { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
        }
    }
}

impl From<Scalar> for Expression {
    fn from(value: Scalar) -> Self {
        Self::literal(value)
    }
}

impl From<ColumnName> for Expression {
    fn from(value: ColumnName) -> Self {
        Self::Column(value)
    }
}

impl From<Predicate> for Expression {
    fn from(value: Predicate) -> Self {
        Self::from_pred(value)
    }
}

impl From<ColumnName> for Predicate {
    fn from(value: ColumnName) -> Self {
        Self::from_expr(value)
    }
}

impl<R: Into<Expression>> std::ops::Add<R> for Expression {
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        Self::binary(BinaryExpressionOp::Plus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Sub<R> for Expression {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Minus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Mul<R> for Expression {
    type Output = Self;

    fn mul(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Multiply, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Div<R> for Expression {
    type Output = Self;

    fn div(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Divide, self, rhs)
    }
}

/// Retrieves the set of column names referenced by an expression.
#[derive(Default)]
struct GetColumnReferences<'a>(HashSet<&'a ColumnName>);

impl<'a> ExpressionTransform<'a> for GetColumnReferences<'a> {
    transform_output_type!(|'a, T| ());

    fn transform_expr_column(&mut self, name: &'a ColumnName) {
        self.0.insert(name);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;

    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use super::{
        column_expr, column_pred, Expression as Expr, ExpressionStructPatchBuilder,
        FieldPatchBuildState, InputFieldAction, PatchNode, Predicate as Pred,
    };

    /// Helper function to verify roundtrip serialization/deserialization
    fn assert_roundtrip<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T) {
        let json = serde_json::to_string(value).expect("serialization should succeed");
        let deserialized: T = serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(value, &deserialized, "roundtrip should preserve value");
    }

    #[test]
    fn test_expression_format() {
        let cases = [
            (column_expr!("x"), "Column(x)"),
            (
                (column_expr!("x") + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (
                Expr::struct_from([column_expr!("x"), Expr::literal(2), Expr::literal(10)]),
                "Struct(Column(x), 2, 10)",
            ),
        ];

        for (expr, expected) in cases {
            let result = format!("{expr}");
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_predicate_format() {
        let cases = [
            (column_pred!("x"), "Column(x)"),
            (column_expr!("x").eq(Expr::literal(2)), "Column(x) = 2"),
            (
                (column_expr!("x") - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                Pred::and(
                    column_expr!("x").ge(Expr::literal(2)),
                    column_expr!("x").le(Expr::literal(10)),
                ),
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10))",
            ),
            (
                Pred::and_from([
                    column_expr!("x").ge(Expr::literal(2)),
                    column_expr!("x").le(Expr::literal(10)),
                    column_expr!("x").le(Expr::literal(100)),
                ]),
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10), NOT(Column(x) > 100))",
            ),
            (
                Pred::or(
                    column_expr!("x").gt(Expr::literal(2)),
                    column_expr!("x").lt(Expr::literal(10)),
                ),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (
                column_expr!("x").eq(Expr::literal("foo")),
                "Column(x) = 'foo'",
            ),
        ];

        for (pred, expected) in cases {
            let result = format!("{pred}");
            assert_eq!(result, expected);
        }
    }

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
        let result = FieldPatchBuildState {
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

    // ==================== Serde Roundtrip Tests ====================

    mod serde_tests {
        use std::sync::Arc;

        use super::assert_roundtrip;
        use crate::expressions::scalars::{ArrayData, DecimalData, MapData, StructData};
        use crate::expressions::{
            column_expr, column_name, BinaryExpressionOp, BinaryPredicateOp, ColumnName,
            Expression, ExpressionStructPatchBuilder, Predicate, Scalar, UnaryExpressionOp,
        };
        use crate::schema::{ArrayType, DataType, DecimalType, MapType, StructField};
        use crate::utils::test_utils::assert_result_error_with_message;

        // ==================== Expression::Literal Tests ====================

        #[test]
        fn test_literal_scalars_roundtrip() {
            // Test all primitive scalar types that have proper PartialEq
            let cases: Vec<Expression> = vec![
                // Numeric types
                Expression::literal(42i32),         // Integer
                Expression::literal(9999999999i64), // Long
                Expression::literal(123i16),        // Short
                Expression::literal(42i8),          // Byte
                Expression::literal(1.12345677_32), // Float
                Expression::literal(1.12345667_64), // Double
                // String and Boolean
                Expression::literal("hello world"),
                Expression::literal(true),
                Expression::literal(false),
                // Temporal types
                Expression::Literal(Scalar::Timestamp(1234567890000000)),
                Expression::Literal(Scalar::TimestampNtz(1234567890000000)),
                Expression::Literal(Scalar::Date(19000)),
                // Binary
                Expression::Literal(Scalar::Binary(vec![1, 2, 3, 4, 5])),
                // Decimal
                Expression::Literal(Scalar::Decimal(
                    DecimalData::try_new(12345i128, DecimalType::try_new(10, 2).unwrap()).unwrap(),
                )),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_literal_complex_scalars_roundtrip() {
            // Test complex scalar types that need JSON comparison (partial_cmp returns None)
            let cases: Vec<Expression> = vec![
                // Null with different types
                Expression::null_literal(DataType::INTEGER),
                Expression::null_literal(DataType::STRING),
                Expression::null_literal(DataType::BOOLEAN),
                // Array
                Expression::Literal(Scalar::Array(
                    ArrayData::try_new(
                        ArrayType::new(DataType::INTEGER, false),
                        vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
                    )
                    .unwrap(),
                )),
                // Map
                Expression::Literal(Scalar::Map(
                    MapData::try_new(
                        MapType::new(DataType::STRING, DataType::INTEGER, false),
                        vec![
                            (Scalar::String("a".to_string()), Scalar::Integer(1)),
                            (Scalar::String("b".to_string()), Scalar::Integer(2)),
                        ],
                    )
                    .unwrap(),
                )),
                // Struct
                Expression::Literal(Scalar::Struct(
                    StructData::try_new(
                        vec![
                            StructField::nullable("x", DataType::INTEGER),
                            StructField::nullable("y", DataType::STRING),
                        ],
                        vec![Scalar::Integer(42), Scalar::String("hello".to_string())],
                    )
                    .unwrap(),
                )),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        // ==================== Expression::Column Tests ====================

        #[test]
        fn test_column_expressions_roundtrip() {
            let cases: Vec<Expression> = vec![
                column_expr!("my_column"),
                Expression::column(["parent", "child"]),
                Expression::column(["a", "b", "c", "d"]),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_column_names_roundtrip() {
            let cases: Vec<ColumnName> = vec![
                column_name!("simple"),
                ColumnName::new(["a", "b", "c"]),
                ColumnName::new::<&str>([]),
            ];

            for col in &cases {
                assert_roundtrip(col);
            }
        }

        // ==================== Expression Operations Tests ====================

        #[test]
        fn test_unary_expression_roundtrip() {
            let expr = Expression::unary(UnaryExpressionOp::ToJson, column_expr!("data"));
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_binary_expressions_roundtrip() {
            let ops = [
                BinaryExpressionOp::Plus,
                BinaryExpressionOp::Minus,
                BinaryExpressionOp::Multiply,
                BinaryExpressionOp::Divide,
            ];

            for op in ops {
                let expr = Expression::binary(op, column_expr!("a"), Expression::literal(10));
                assert_roundtrip(&expr);
            }
        }

        #[test]
        fn test_variadic_expression_roundtrip() {
            let expr = Expression::coalesce([
                column_expr!("a"),
                column_expr!("b"),
                Expression::literal("default"),
            ]);
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_nested_arithmetic_expression_roundtrip() {
            // (a + b) * (c - d) / 2
            let left = Expression::binary(
                BinaryExpressionOp::Plus,
                column_expr!("a"),
                column_expr!("b"),
            );
            let right = Expression::binary(
                BinaryExpressionOp::Minus,
                column_expr!("c"),
                column_expr!("d"),
            );
            let mul = Expression::binary(BinaryExpressionOp::Multiply, left, right);
            let expr = Expression::binary(BinaryExpressionOp::Divide, mul, Expression::literal(2));
            assert_roundtrip(&expr);
        }

        // ==================== Expression::Struct/StructPatch/Other Tests ====================

        #[test]
        fn test_struct_expression_roundtrip() {
            let expr = Expression::struct_from([
                Arc::new(column_expr!("x")),
                Arc::new(Expression::literal(42)),
                Arc::new(Expression::literal("hello")),
            ]);
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_transform_expressions_roundtrip() {
            let cases: Vec<Expression> = vec![
                // Identity transform
                Expression::struct_patch(ExpressionStructPatchBuilder::new()).unwrap(),
                // Drop field
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new().with_dropped_field("old_column"),
                )
                .unwrap(),
                // Replace field
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new()
                        .with_replaced_field("original", Arc::new(Expression::literal(0))),
                )
                .unwrap(),
                // Insert fields
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new()
                        .with_inserted_field_after("after_col", Arc::new(column_expr!("new_col")))
                        .with_prepended_field(Arc::new(Expression::literal("prepended")))
                        .with_appended_field(Arc::new(Expression::literal("appended"))),
                )
                .unwrap(),
                // Nested transform
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new_nested(["parent", "child"])
                        .with_dropped_field("to_drop"),
                )
                .unwrap(),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_expression_wrapping_predicate_roundtrip() {
            let pred = Predicate::eq(column_expr!("x"), Expression::literal(10));
            let expr = Expression::from_pred(pred);
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_expression_unknown_roundtrip() {
            let expr = Expression::unknown("some_unknown_function()");
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_map_to_struct_expression_roundtrip() {
            let cases: Vec<Expression> = vec![
                Expression::map_to_struct(column_expr!("pv")),
                Expression::map_to_struct(Expression::literal("ignored")),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        // ==================== Predicate Tests ====================

        #[test]
        fn test_predicate_basics_roundtrip() {
            let cases: Vec<Predicate> = vec![
                // Boolean expression
                Predicate::from_expr(column_expr!("is_active")),
                // Literals
                Predicate::literal(true),
                Predicate::literal(false),
                // NOT
                Predicate::not(Predicate::from_expr(column_expr!("x"))),
                // Nested NOT
                Predicate::not(Predicate::not(Predicate::gt(
                    column_expr!("x"),
                    Expression::literal(5),
                ))),
                // Unknown
                Predicate::unknown("some_unknown_predicate()"),
                // Unary predicates
                Predicate::is_null(column_expr!("nullable_col")),
                Predicate::is_not_null(column_expr!("nullable_col")),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        #[test]
        fn test_predicate_null_literal_roundtrip() {
            let pred = Predicate::null_literal();
            assert_roundtrip(&pred);
        }

        #[test]
        fn test_predicate_comparisons_roundtrip() {
            let cases: Vec<Predicate> = vec![
                Predicate::eq(column_expr!("x"), Expression::literal(42)),
                Predicate::ne(column_expr!("status"), Expression::literal("active")),
                Predicate::lt(column_expr!("age"), Expression::literal(18)),
                Predicate::le(column_expr!("price"), Expression::literal(100)),
                Predicate::gt(column_expr!("score"), Expression::literal(90)),
                Predicate::ge(column_expr!("quantity"), Expression::literal(1)),
                Predicate::distinct(column_expr!("a"), column_expr!("b")),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        #[test]
        fn test_predicate_in_roundtrip() {
            let array_data = ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
            )
            .unwrap();
            let pred = Predicate::binary(
                BinaryPredicateOp::In,
                column_expr!("x"),
                Expression::Literal(Scalar::Array(array_data)),
            );
            assert_roundtrip(&pred);
        }

        #[test]
        fn test_predicate_junctions_roundtrip() {
            let cases: Vec<Predicate> = vec![
                // Simple AND
                Predicate::and(
                    Predicate::gt(column_expr!("x"), Expression::literal(0)),
                    Predicate::lt(column_expr!("x"), Expression::literal(100)),
                ),
                // Simple OR
                Predicate::or(
                    Predicate::eq(column_expr!("status"), Expression::literal("active")),
                    Predicate::eq(column_expr!("status"), Expression::literal("pending")),
                ),
                // Multiple AND
                Predicate::and_from([
                    Predicate::gt(column_expr!("x"), Expression::literal(0)),
                    Predicate::lt(column_expr!("x"), Expression::literal(100)),
                    Predicate::is_not_null(column_expr!("x")),
                ]),
                // Multiple OR
                Predicate::or_from([
                    Predicate::eq(column_expr!("type"), Expression::literal("A")),
                    Predicate::eq(column_expr!("type"), Expression::literal("B")),
                    Predicate::eq(column_expr!("type"), Expression::literal("C")),
                ]),
                // Nested: (a > 0 AND b < 100) OR (c = 'special')
                Predicate::or(
                    Predicate::and(
                        Predicate::gt(column_expr!("a"), Expression::literal(0)),
                        Predicate::lt(column_expr!("b"), Expression::literal(100)),
                    ),
                    Predicate::eq(column_expr!("c"), Expression::literal("special")),
                ),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        // ==================== Complex Nested Structures ====================

        #[test]
        fn test_deeply_nested_structures_roundtrip() {
            // COALESCE(a + b, c * d, 0) > 100
            let add = Expression::binary(
                BinaryExpressionOp::Plus,
                column_expr!("a"),
                column_expr!("b"),
            );
            let mul = Expression::binary(
                BinaryExpressionOp::Multiply,
                column_expr!("c"),
                column_expr!("d"),
            );
            let coalesce = Expression::coalesce([add, mul, Expression::literal(0)]);
            let pred = Predicate::gt(coalesce, Expression::literal(100));
            assert_roundtrip(&pred);

            // Expression wrapping a predicate that references expressions
            let inner_pred = Predicate::and(
                Predicate::eq(column_expr!("x"), Expression::literal(1)),
                Predicate::gt(
                    Expression::binary(
                        BinaryExpressionOp::Plus,
                        column_expr!("y"),
                        column_expr!("z"),
                    ),
                    Expression::literal(10),
                ),
            );
            let expr = Expression::from_pred(inner_pred);
            assert_roundtrip(&expr);
        }

        // ==================== Opaque Variant Failure Tests ====================

        #[test]
        fn test_opaque_expression_serialize_fails() {
            use crate::expressions::{OpaqueExpressionOp, ScalarExpressionEvaluator};
            use crate::DeltaResult;

            #[derive(Debug, PartialEq)]
            struct TestOpaqueExprOp;

            impl OpaqueExpressionOp for TestOpaqueExprOp {
                fn name(&self) -> &str {
                    "test_opaque"
                }
                fn eval_expr_scalar(
                    &self,
                    _eval_expr: &ScalarExpressionEvaluator<'_>,
                    _exprs: &[Expression],
                ) -> DeltaResult<Scalar> {
                    Ok(Scalar::Integer(0))
                }
            }

            let expr = Expression::opaque(TestOpaqueExprOp, [Expression::literal(1)]);
            let result = serde_json::to_string(&expr);
            assert_result_error_with_message(result, "Cannot serialize an Opaque Expression");
        }

        #[test]
        fn test_opaque_predicate_serialize_fails() {
            use crate::expressions::{OpaquePredicateOp, ScalarExpressionEvaluator};
            use crate::kernel_predicates::{
                DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
                IndirectDataSkippingPredicateEvaluator,
            };
            use crate::DeltaResult;

            #[derive(Debug, PartialEq)]
            struct TestOpaquePredOp;

            impl OpaquePredicateOp for TestOpaquePredOp {
                fn name(&self) -> &str {
                    "test_opaque_pred"
                }
                fn eval_pred_scalar(
                    &self,
                    _eval_expr: &ScalarExpressionEvaluator<'_>,
                    _eval_pred: &DirectPredicateEvaluator<'_>,
                    _exprs: &[Expression],
                    _inverted: bool,
                ) -> DeltaResult<Option<bool>> {
                    Ok(Some(true))
                }
                fn eval_as_data_skipping_predicate(
                    &self,
                    _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
                    _exprs: &[Expression],
                    _inverted: bool,
                ) -> Option<bool> {
                    Some(true)
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

            let pred = Predicate::opaque(TestOpaquePredOp, [Expression::literal(1)]);
            let result = serde_json::to_string(&pred);
            assert_result_error_with_message(result, "Cannot serialize an Opaque Predicate");
        }
    }

    #[test]
    fn single_element_and_from_returns_unwrapped_predicate() {
        let inner = Pred::gt(column_expr!("x"), Expr::literal(0));
        let result = Pred::and_from([inner.clone()]);
        assert_eq!(result, inner);
    }

    #[test]
    fn single_element_or_from_returns_unwrapped_predicate() {
        let inner = Pred::gt(column_expr!("x"), Expr::literal(0));
        let result = Pred::or_from([inner.clone()]);
        assert_eq!(result, inner);
    }

    #[test]
    fn multi_element_and_from_returns_junction() {
        let p1 = Pred::gt(column_expr!("x"), Expr::literal(0));
        let p2 = Pred::lt(column_expr!("x"), Expr::literal(100));
        let result = Pred::and_from([p1.clone(), p2.clone()]);
        assert!(matches!(result, Pred::Junction(ref j) if j.preds.len() == 2));
        assert_eq!(result, Pred::and(p1, p2));
    }

    #[test]
    fn empty_and_from_returns_identity_literal() {
        let result = Pred::and_from(std::iter::empty());
        assert_eq!(result, Pred::literal(true));
    }

    #[test]
    fn empty_or_from_returns_identity_literal() {
        let result = Pred::or_from(std::iter::empty());
        assert_eq!(result, Pred::literal(false));
    }
}
