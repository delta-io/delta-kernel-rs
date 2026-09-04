//! Resolved expressions and predicates accepted by declarative plans.
//!
//! Public [`Expression`] values are a broad source language. Plan
//! construction resolves that language against an input schema and rejects nodes outside the
//! mandatory replay dialect before an executor receives the plan.
//!
//! Resolution makes runtime schema facts explicit. For example, given:
//!
//! ```text
//! add: nullable struct {
//!     path: non-null string
//! }
//! ```
//!
//! `add.path` resolves to `STRING, nullable`: the non-null leaf can still be null when its parent
//! is null. Every executor sees that same result through [`PlanColumn`] instead of independently
//! rediscovering it.
//!
//! The source language remains broader than this module. A caller predicate containing `x / 2`
//! may be carried by an optional [`DataSkippingSite`](super::nodes::DataSkippingSite) for native
//! range analysis, but `Divide` cannot appear in [`PlanExpressionKind`] and is never mandatory for
//! replay.

use std::collections::HashMap;
use std::sync::Arc;

use crate::expressions::{
    BinaryPredicateOp, ColumnName, Expression, ExpressionRef, ExpressionStructPatch,
    JunctionPredicateOp, Predicate, PredicateRef, Scalar, UnaryPredicateOp, VariadicExpressionOp,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error};

/// A shared reference to a resolved [`PlanExpression`].
pub type PlanExpressionRef = Arc<PlanExpression>;

/// A shared reference to a resolved [`PlanPredicate`].
pub type PlanPredicateRef = Arc<PlanPredicate>;

/// A column path resolved against the schema at a declarative-plan boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanColumn {
    name: ColumnName,
    data_type: DataType,
    nullable: bool,
}

impl PlanColumn {
    pub(crate) fn resolve(input_schema: &StructType, name: ColumnName) -> DeltaResult<Self> {
        let (data_type, nullable) = resolve_column(input_schema, &name)?;
        Ok(Self {
            name,
            data_type,
            nullable,
        })
    }

    /// Returns the resolved column path.
    pub fn name(&self) -> &ColumnName {
        &self.name
    }

    /// Returns the resolved column's leaf type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns whether the leaf or any parent struct is nullable.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// An expression resolved and validated for execution inside a declarative plan.
#[derive(Debug, Clone)]
pub struct PlanExpression {
    source: ExpressionRef,
    node: PlanExpressionNode,
    data_type: DataType,
    nullable: bool,
}

/// A borrowed view of a resolved plan expression.
#[derive(Debug, Clone, Copy)]
pub enum PlanExpressionKind<'a> {
    /// A typed literal.
    Literal(&'a Scalar),
    /// A column path proved to exist in the input schema.
    Column(&'a PlanColumn),
    /// A Boolean predicate used as an expression.
    Predicate(&'a PlanPredicate),
    /// A struct constructor whose children are paired with the output fields in order.
    Struct {
        /// Resolved expressions for the struct fields.
        fields: &'a [PlanExpressionRef],
        /// Optional Boolean expression controlling whether the struct value is null.
        nullability: Option<&'a PlanExpression>,
    },
    /// A sparse struct projection resolved against its input and output schemas.
    StructPatch(&'a PlanStructPatch),
    /// A non-empty, same-type coalesce operation.
    Coalesce(&'a [PlanExpressionRef]),
    /// Permissive JSON parsing into the attached output schema.
    ParseJson {
        /// Resolved string input.
        json: &'a PlanExpression,
        /// Struct schema produced by parsing.
        output_schema: &'a SchemaRef,
    },
    /// Conversion of a string map into the attached typed struct.
    MapToStruct {
        /// Resolved map input.
        map: &'a PlanExpression,
        /// Struct schema produced from map entries.
        output_schema: &'a StructType,
    },
}

#[derive(Debug, Clone)]
enum PlanExpressionNode {
    Literal(Scalar),
    Column(PlanColumn),
    Predicate(PlanPredicateRef),
    Struct {
        fields: Vec<PlanExpressionRef>,
        nullability: Option<PlanExpressionRef>,
    },
    StructPatch(PlanStructPatch),
    Coalesce(Vec<PlanExpressionRef>),
    ParseJson {
        json: PlanExpressionRef,
        output_schema: SchemaRef,
    },
    MapToStruct {
        map: PlanExpressionRef,
        output_schema: StructType,
    },
}

impl PlanExpression {
    /// Resolves `source` against `input_schema` for use in a declarative plan.
    ///
    /// When `expected_type` is present, the resolved output type must match it exactly. The method
    /// rejects every source-language node outside the mandatory replay dialect.
    pub fn resolve(
        source: impl Into<ExpressionRef>,
        input_schema: &StructType,
        expected_type: Option<&DataType>,
    ) -> DeltaResult<PlanExpressionRef> {
        let source = source.into();
        let (node, data_type, nullable) =
            resolve_expression_node(&source, input_schema, expected_type)?;
        require_expected_type(&data_type, expected_type, "expression")?;
        Ok(Arc::new(Self {
            source,
            node,
            data_type,
            nullable,
        }))
    }

    /// Returns an exhaustive borrowed view of this expression's resolved node.
    pub fn kind(&self) -> PlanExpressionKind<'_> {
        match &self.node {
            PlanExpressionNode::Literal(value) => PlanExpressionKind::Literal(value),
            PlanExpressionNode::Column(name) => PlanExpressionKind::Column(name),
            PlanExpressionNode::Predicate(predicate) => {
                PlanExpressionKind::Predicate(predicate.as_ref())
            }
            PlanExpressionNode::Struct {
                fields,
                nullability,
            } => PlanExpressionKind::Struct {
                fields,
                nullability: nullability.as_deref(),
            },
            PlanExpressionNode::StructPatch(patch) => PlanExpressionKind::StructPatch(patch),
            PlanExpressionNode::Coalesce(expressions) => PlanExpressionKind::Coalesce(expressions),
            PlanExpressionNode::ParseJson {
                json,
                output_schema,
            } => PlanExpressionKind::ParseJson {
                json,
                output_schema,
            },
            PlanExpressionNode::MapToStruct { map, output_schema } => {
                PlanExpressionKind::MapToStruct { map, output_schema }
            }
        }
    }

    /// Returns this expression's resolved output type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns whether this expression can produce a null value.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the validated source expression for delegation to an imperative evaluator.
    ///
    /// Plan executors should prefer [`Self::kind`]. Executors that delegate evaluation to an
    /// [`EvaluationHandler`](crate::EvaluationHandler) can use this tree after Kernel validates it.
    pub fn source_expression(&self) -> &ExpressionRef {
        &self.source
    }
}

/// A sparse struct projection whose computed children have been recursively resolved.
#[derive(Debug, Clone)]
pub struct PlanStructPatch {
    input_path: Option<ColumnName>,
    field_patches: HashMap<String, PlanFieldPatch>,
    prepended_fields: Vec<PlanExpressionRef>,
    appended_fields: Vec<PlanExpressionRef>,
}

impl PlanStructPatch {
    /// Returns the nested input path, or `None` for a top-level patch.
    pub fn input_path(&self) -> Option<&ColumnName> {
        self.input_path.as_ref()
    }

    /// Returns the resolved patch associated with each source field name.
    pub fn field_patches(&self) -> &HashMap<String, PlanFieldPatch> {
        &self.field_patches
    }

    /// Returns fields inserted before all source fields.
    pub fn prepended_fields(&self) -> &[PlanExpressionRef] {
        &self.prepended_fields
    }

    /// Returns fields inserted after all source fields.
    pub fn appended_fields(&self) -> &[PlanExpressionRef] {
        &self.appended_fields
    }
}

/// A resolved edit associated with one source struct field.
#[derive(Debug, Clone)]
pub struct PlanFieldPatch {
    keep_input: bool,
    insertions: Vec<PlanExpressionRef>,
    optional: bool,
}

impl PlanFieldPatch {
    /// Returns whether the source field is preserved before the insertions.
    pub fn keeps_input(&self) -> bool {
        self.keep_input
    }

    /// Returns computed fields inserted after the source field's output position.
    pub fn insertions(&self) -> &[PlanExpressionRef] {
        &self.insertions
    }

    /// Returns whether a missing source field causes this patch to be ignored.
    pub fn is_optional(&self) -> bool {
        self.optional
    }
}

/// A Boolean predicate resolved and validated for execution inside a declarative plan.
#[derive(Debug, Clone)]
pub struct PlanPredicate {
    source: PredicateRef,
    node: PlanPredicateNode,
    nullable: bool,
}

/// A borrowed view of a resolved plan predicate.
#[derive(Debug, Clone, Copy)]
pub enum PlanPredicateKind<'a> {
    /// A resolved Boolean expression.
    Boolean(&'a PlanExpression),
    /// Boolean inversion.
    Not(&'a PlanPredicate),
    /// A null test, optionally inverted to `IS NOT NULL`.
    IsNull {
        /// Expression being tested.
        expression: &'a PlanExpression,
        /// Whether the result is inverted.
        inverted: bool,
    },
    /// A comparison over exact-type operands.
    Compare {
        /// Comparison operation.
        op: PlanComparison,
        /// Left operand.
        left: &'a PlanExpression,
        /// Right operand.
        right: &'a PlanExpression,
    },
    /// A non-empty SQL three-valued conjunction.
    And(&'a [PlanPredicateRef]),
    /// A non-empty SQL three-valued disjunction.
    Or(&'a [PlanPredicateRef]),
}

#[derive(Debug, Clone)]
enum PlanPredicateNode {
    Boolean(PlanExpressionRef),
    Not(PlanPredicateRef),
    IsNull {
        expression: PlanExpressionRef,
        inverted: bool,
    },
    Compare {
        op: PlanComparison,
        left: PlanExpressionRef,
        right: PlanExpressionRef,
    },
    And(Vec<PlanPredicateRef>),
    Or(Vec<PlanPredicateRef>),
}

/// Comparisons in the mandatory declarative-plan dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanComparison {
    /// SQL less-than using three-valued logic.
    LessThan,
    /// SQL greater-than using three-valued logic.
    GreaterThan,
    /// SQL equality using three-valued logic.
    Equal,
    /// SQL `IS DISTINCT FROM`, which always produces a non-null Boolean.
    Distinct,
}

impl PlanPredicate {
    /// Resolves `source` against `input_schema` for use in a declarative plan.
    pub fn resolve(
        source: impl Into<PredicateRef>,
        input_schema: &StructType,
    ) -> DeltaResult<PlanPredicateRef> {
        let source = source.into();
        let (node, nullable) = resolve_predicate_node(&source, input_schema, false)?;
        Ok(Arc::new(Self {
            source,
            node,
            nullable,
        }))
    }

    /// Returns an exhaustive borrowed view of this predicate's resolved node.
    pub fn kind(&self) -> PlanPredicateKind<'_> {
        match &self.node {
            PlanPredicateNode::Boolean(expression) => {
                PlanPredicateKind::Boolean(expression.as_ref())
            }
            PlanPredicateNode::Not(predicate) => PlanPredicateKind::Not(predicate.as_ref()),
            PlanPredicateNode::IsNull {
                expression,
                inverted,
            } => PlanPredicateKind::IsNull {
                expression,
                inverted: *inverted,
            },
            PlanPredicateNode::Compare { op, left, right } => PlanPredicateKind::Compare {
                op: *op,
                left,
                right,
            },
            PlanPredicateNode::And(predicates) => PlanPredicateKind::And(predicates),
            PlanPredicateNode::Or(predicates) => PlanPredicateKind::Or(predicates),
        }
    }

    /// Returns whether this predicate can evaluate to SQL unknown.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the validated source predicate for delegation to an imperative evaluator.
    ///
    /// Plan executors should prefer [`Self::kind`].
    pub fn source_predicate(&self) -> &PredicateRef {
        &self.source
    }
}

fn resolve_expression_node(
    source: &ExpressionRef,
    input_schema: &StructType,
    expected_type: Option<&DataType>,
) -> DeltaResult<(PlanExpressionNode, DataType, bool)> {
    match source.as_ref() {
        Expression::Literal(value) => Ok((
            PlanExpressionNode::Literal(value.clone()),
            value.data_type(),
            value.is_null(),
        )),
        Expression::Column(name) => {
            let column = PlanColumn::resolve(input_schema, name.clone())?;
            Ok((
                PlanExpressionNode::Column(column.clone()),
                column.data_type().clone(),
                column.is_nullable(),
            ))
        }
        Expression::Predicate(predicate) => {
            let predicate = PlanPredicate::resolve(predicate.as_ref().clone(), input_schema)?;
            let nullable = predicate.is_nullable();
            Ok((
                PlanExpressionNode::Predicate(predicate),
                DataType::BOOLEAN,
                nullable,
            ))
        }
        Expression::Struct(fields, nullability) => {
            let output_schema = require_struct_type(expected_type, "Struct")?;
            if fields.len() != output_schema.num_fields() {
                return Err(Error::generic(format!(
                    "plan Struct has {} fields but its output schema has {}",
                    fields.len(),
                    output_schema.num_fields()
                )));
            }
            let nullability = nullability
                .as_ref()
                .map(|expression| {
                    PlanExpression::resolve(
                        Arc::clone(expression),
                        input_schema,
                        Some(&DataType::BOOLEAN),
                    )
                })
                .transpose()?;
            let fields = fields
                .iter()
                .zip(output_schema.fields())
                .map(|(expression, field)| {
                    let expression = PlanExpression::resolve(
                        Arc::clone(expression),
                        input_schema,
                        Some(field.data_type()),
                    )?;
                    require_field_assignment(
                        &expression,
                        field,
                        nullability.as_deref(),
                        input_schema,
                    )?;
                    Ok(expression)
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let nullable = nullability.is_some();
            Ok((
                PlanExpressionNode::Struct {
                    fields,
                    nullability,
                },
                DataType::from(output_schema.clone()),
                nullable,
            ))
        }
        Expression::StructPatch(patch) => {
            let output_schema = require_struct_type(expected_type, "StructPatch")?;
            let (patch, nullable) = resolve_struct_patch(patch, input_schema, output_schema)?;
            Ok((
                PlanExpressionNode::StructPatch(patch),
                DataType::from(output_schema.clone()),
                nullable,
            ))
        }
        Expression::Variadic(variadic) if variadic.op == VariadicExpressionOp::Coalesce => {
            let first = variadic
                .exprs
                .first()
                .ok_or_else(|| Error::generic("plan Coalesce requires at least one expression"))?;
            let first = PlanExpression::resolve(first.clone(), input_schema, expected_type)?;
            let data_type = first.data_type().clone();
            let mut expressions = Vec::with_capacity(variadic.exprs.len());
            expressions.push(first);
            for expression in variadic.exprs.iter().skip(1) {
                expressions.push(PlanExpression::resolve(
                    expression.clone(),
                    input_schema,
                    Some(&data_type),
                )?);
            }
            let nullable = expressions
                .iter()
                .all(|expression| expression.is_nullable());
            Ok((
                PlanExpressionNode::Coalesce(expressions),
                data_type,
                nullable,
            ))
        }
        Expression::ParseJson(parse) => {
            let json = PlanExpression::resolve(
                parse.json_expr.as_ref().clone(),
                input_schema,
                Some(&DataType::STRING),
            )?;
            let data_type = DataType::from(parse.output_schema.as_ref().clone());
            Ok((
                PlanExpressionNode::ParseJson {
                    json,
                    output_schema: Arc::clone(&parse.output_schema),
                },
                data_type,
                true,
            ))
        }
        Expression::MapToStruct(map_to_struct) => {
            let output_schema = require_struct_type(expected_type, "MapToStruct")?;
            let map = PlanExpression::resolve(
                map_to_struct.map_expr.as_ref().clone(),
                input_schema,
                None,
            )?;
            require_string_map(map.data_type())?;
            for field in output_schema.fields() {
                if !matches!(field.data_type(), DataType::Primitive(_)) {
                    return Err(Error::generic(format!(
                        "plan MapToStruct field `{}` must be primitive, got {}",
                        field.name(),
                        field.data_type()
                    )));
                }
                if !field.is_nullable() {
                    return Err(Error::generic(format!(
                        "plan MapToStruct field `{}` must be nullable because a key may be missing",
                        field.name()
                    )));
                }
            }
            let nullable = map.is_nullable();
            Ok((
                PlanExpressionNode::MapToStruct {
                    map,
                    output_schema: output_schema.clone(),
                },
                DataType::from(output_schema.clone()),
                nullable,
            ))
        }
        Expression::Unary(_) => Err(unsupported_plan_expression("Unary")),
        Expression::Binary(_) => Err(unsupported_plan_expression("Binary arithmetic")),
        Expression::Variadic(_) => Err(unsupported_plan_expression("Array")),
        Expression::Opaque(_) => Err(unsupported_plan_expression("Opaque")),
        Expression::Unknown(_) => Err(unsupported_plan_expression("Unknown")),
        Expression::Cast(_) => Err(unsupported_plan_expression("Cast")),
    }
}

fn resolve_predicate_node(
    source: &PredicateRef,
    input_schema: &StructType,
    inverted: bool,
) -> DeltaResult<(PlanPredicateNode, bool)> {
    match source.as_ref() {
        Predicate::BooleanExpression(expression) => {
            let expression = PlanExpression::resolve(
                expression.clone(),
                input_schema,
                Some(&DataType::BOOLEAN),
            )?;
            let nullable = expression.is_nullable();
            let node = PlanPredicateNode::Boolean(expression);
            if inverted {
                Ok((
                    PlanPredicateNode::Not(Arc::new(PlanPredicate {
                        source: Arc::clone(source),
                        node,
                        nullable,
                    })),
                    nullable,
                ))
            } else {
                Ok((node, nullable))
            }
        }
        Predicate::Not(predicate) => resolve_predicate_node(
            &Arc::new(predicate.as_ref().clone()),
            input_schema,
            !inverted,
        ),
        Predicate::Unary(unary) if unary.op == UnaryPredicateOp::IsNull => {
            let expression =
                PlanExpression::resolve(unary.expr.as_ref().clone(), input_schema, None)?;
            Ok((
                PlanPredicateNode::IsNull {
                    expression,
                    inverted,
                },
                false,
            ))
        }
        Predicate::Binary(binary) => {
            let op = match binary.op {
                BinaryPredicateOp::LessThan => PlanComparison::LessThan,
                BinaryPredicateOp::GreaterThan => PlanComparison::GreaterThan,
                BinaryPredicateOp::Equal => PlanComparison::Equal,
                BinaryPredicateOp::Distinct => PlanComparison::Distinct,
                BinaryPredicateOp::In => {
                    return Err(Error::unsupported(
                        "IN is not part of the plan predicate dialect",
                    ))
                }
            };
            let left = PlanExpression::resolve(binary.left.as_ref().clone(), input_schema, None)?;
            let right = PlanExpression::resolve(
                binary.right.as_ref().clone(),
                input_schema,
                Some(left.data_type()),
            )?;
            require_comparable(left.data_type())?;
            let nullable =
                op != PlanComparison::Distinct && (left.is_nullable() || right.is_nullable());
            let node = PlanPredicateNode::Compare { op, left, right };
            if inverted {
                Ok((
                    PlanPredicateNode::Not(Arc::new(PlanPredicate {
                        source: Arc::clone(source),
                        node,
                        nullable,
                    })),
                    nullable,
                ))
            } else {
                Ok((node, nullable))
            }
        }
        Predicate::Junction(junction) => {
            if junction.preds.is_empty() {
                return Err(Error::generic(
                    "plan AND and OR predicates require at least one child",
                ));
            }
            let predicates = junction
                .preds
                .iter()
                .map(|predicate| {
                    let source = Arc::new(predicate.clone());
                    let (node, nullable) = resolve_predicate_node(&source, input_schema, inverted)?;
                    Ok(Arc::new(PlanPredicate {
                        source,
                        node,
                        nullable,
                    }))
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let nullable = predicates.iter().any(|predicate| predicate.is_nullable());
            let op = if inverted {
                junction.op.invert()
            } else {
                junction.op
            };
            let node = match op {
                JunctionPredicateOp::And => PlanPredicateNode::And(predicates),
                JunctionPredicateOp::Or => PlanPredicateNode::Or(predicates),
            };
            Ok((node, nullable))
        }
        Predicate::Opaque(_) => Err(Error::unsupported(
            "Opaque is not part of the plan predicate dialect",
        )),
        Predicate::Unknown(_) => Err(Error::unsupported(
            "Unknown is not part of the plan predicate dialect",
        )),
        Predicate::Unary(_) => Err(Error::internal_error("unknown unary predicate operation")),
    }
}

fn resolve_column(input_schema: &StructType, name: &ColumnName) -> DeltaResult<(DataType, bool)> {
    let mut nullable = false;
    let mut data_type = None;
    input_schema.visit_fields_of_path(name, |field| {
        nullable |= field.is_nullable();
        data_type = Some(field.data_type().clone());
    })?;
    let data_type = data_type.ok_or_else(|| Error::generic("plan column path cannot be empty"))?;
    Ok((data_type, nullable))
}

fn resolve_struct_patch(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
    output_schema: &StructType,
) -> DeltaResult<(PlanStructPatch, bool)> {
    let (source_schema, nullable) = match patch.input_path() {
        Some(path) => {
            let (data_type, nullable) = resolve_column(input_schema, path)?;
            let DataType::Struct(source) = data_type else {
                return Err(Error::generic(format!(
                    "plan StructPatch input `{path}` is not a struct"
                )));
            };
            (*source, nullable)
        }
        None => (input_schema.clone(), false),
    };

    let mut output_fields = output_schema.fields();
    let prepended_fields = patch
        .prepended_fields
        .iter()
        .map(|expression| resolve_patch_insertion(expression, input_schema, &mut output_fields))
        .collect::<DeltaResult<Vec<_>>>()?;

    let mut field_patches = HashMap::with_capacity(patch.field_patches.len());
    let mut used_required = 0usize;
    for input_field in source_schema.fields() {
        let field_patch = patch.field_patches.get(input_field.name());
        if field_patch.is_none_or(|field_patch| field_patch.keep_input) {
            let output_field = output_fields
                .next()
                .ok_or_else(|| Error::generic("plan StructPatch produces too many fields"))?;
            require_passthrough_field(input_field, output_field)?;
        }
        let Some(field_patch) = field_patch else {
            continue;
        };
        let insertions = field_patch
            .insertions
            .iter()
            .map(|expression| resolve_patch_insertion(expression, input_schema, &mut output_fields))
            .collect::<DeltaResult<Vec<_>>>()?;
        field_patches.insert(
            input_field.name().clone(),
            PlanFieldPatch {
                keep_input: field_patch.keep_input,
                insertions,
                optional: field_patch.optional,
            },
        );
        if !field_patch.optional {
            used_required += 1;
        }
    }

    let required = patch
        .field_patches
        .values()
        .filter(|field_patch| !field_patch.optional)
        .count();
    if used_required != required {
        return Err(Error::generic(
            "plan StructPatch has a required patch for a missing input field",
        ));
    }
    for (name, field_patch) in &patch.field_patches {
        if field_patch.optional && !field_patches.contains_key(name) {
            field_patches.insert(
                name.clone(),
                PlanFieldPatch {
                    keep_input: field_patch.keep_input,
                    insertions: Vec::new(),
                    optional: true,
                },
            );
        }
    }

    let appended_fields = patch
        .appended_fields
        .iter()
        .map(|expression| resolve_patch_insertion(expression, input_schema, &mut output_fields))
        .collect::<DeltaResult<Vec<_>>>()?;
    if output_fields.next().is_some() {
        return Err(Error::generic(
            "plan StructPatch produces fewer fields than its output schema",
        ));
    }

    Ok((
        PlanStructPatch {
            input_path: patch.input_path.clone(),
            field_patches,
            prepended_fields,
            appended_fields,
        },
        nullable,
    ))
}

fn resolve_patch_insertion<'a>(
    expression: &ExpressionRef,
    input_schema: &StructType,
    output_fields: &mut impl Iterator<Item = &'a StructField>,
) -> DeltaResult<PlanExpressionRef> {
    let field = output_fields
        .next()
        .ok_or_else(|| Error::generic("plan StructPatch produces too many fields"))?;
    let expression = PlanExpression::resolve(
        Arc::clone(expression),
        input_schema,
        Some(field.data_type()),
    )?;
    require_field_assignment(&expression, field, None, input_schema)?;
    Ok(expression)
}

fn require_expected_type(
    actual: &DataType,
    expected: Option<&DataType>,
    context: &str,
) -> DeltaResult<()> {
    if let Some(expected) = expected {
        if actual != expected {
            return Err(Error::generic(format!(
                "plan {context} has type {actual}, expected {expected}"
            )));
        }
    }
    Ok(())
}

fn require_struct_type<'a>(
    expected: Option<&'a DataType>,
    context: &str,
) -> DeltaResult<&'a StructType> {
    match expected {
        Some(DataType::Struct(schema)) => Ok(schema),
        Some(other) => Err(Error::generic(format!(
            "plan {context} requires a struct output type, got {other}"
        ))),
        None => Err(Error::generic(format!(
            "plan {context} requires an expected output type"
        ))),
    }
}

fn require_field_assignment(
    expression: &PlanExpression,
    field: &StructField,
    struct_guard: Option<&PlanExpression>,
    input_schema: &StructType,
) -> DeltaResult<()> {
    let guarded_non_null = struct_guard
        .map(|guard| expression_is_non_null_when_guard_is_true(expression, guard, input_schema))
        .transpose()?
        .unwrap_or(false);
    if !field.is_nullable() && expression.is_nullable() && !guarded_non_null {
        return Err(Error::generic(format!(
            "plan expression `{}` for non-nullable field `{}` may produce null",
            expression.source_expression(),
            field.name()
        )));
    }
    Ok(())
}

/// Proves the narrow conditional-nullability pattern emitted by replay projections.
///
/// A nullable parent makes every descendant effectively nullable. A struct guard such as
/// `add.deletionVector.storageType IS NOT NULL` proves that the nullable `add` and
/// `deletionVector` ancestors exist, so non-null sibling leaves may safely populate non-null fields
/// inside that guarded struct. Unrelated guards and nullable leaves remain rejected.
fn expression_is_non_null_when_guard_is_true(
    expression: &PlanExpression,
    guard: &PlanExpression,
    input_schema: &StructType,
) -> DeltaResult<bool> {
    if !expression.is_nullable() {
        return Ok(true);
    }
    match guard.kind() {
        PlanExpressionKind::Predicate(predicate) => {
            predicate_proves_expression_non_null(predicate, expression, input_schema)
        }
        _ => expression_non_null_implies_target_non_null(guard, expression, input_schema),
    }
}

fn expression_non_null_implies_target_non_null(
    condition: &PlanExpression,
    target: &PlanExpression,
    input_schema: &StructType,
) -> DeltaResult<bool> {
    if condition.source_expression() == target.source_expression() {
        return Ok(true);
    }
    if let PlanExpressionKind::Coalesce(expressions) = condition.kind() {
        for expression in expressions {
            if !expression_non_null_implies_target_non_null(expression, target, input_schema)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    match target.kind() {
        PlanExpressionKind::Column(column) => {
            let PlanExpressionKind::Column(condition_column) = condition.kind() else {
                return Ok(false);
            };
            Ok(nullable_column_prefixes(input_schema, column.name())?
                .into_iter()
                .all(|prefix| condition_column.name().path().starts_with(prefix.path())))
        }
        PlanExpressionKind::Coalesce(expressions) => {
            for expression in expressions {
                if expression_non_null_implies_target_non_null(condition, expression, input_schema)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        PlanExpressionKind::MapToStruct { map, .. } => {
            expression_non_null_implies_target_non_null(condition, map, input_schema)
        }
        _ => Ok(false),
    }
}

fn nullable_column_prefixes(
    input_schema: &StructType,
    column: &ColumnName,
) -> DeltaResult<Vec<ColumnName>> {
    let mut prefixes = Vec::new();
    for end in 1..=column.path().len() {
        let prefix = ColumnName::new(column.path()[..end].iter().cloned());
        if input_schema.field_at(&prefix)?.is_nullable() {
            prefixes.push(prefix);
        }
    }
    Ok(prefixes)
}

fn predicate_proves_expression_non_null(
    predicate: &PlanPredicate,
    target: &PlanExpression,
    input_schema: &StructType,
) -> DeltaResult<bool> {
    match predicate.kind() {
        PlanPredicateKind::IsNull {
            expression,
            inverted: true,
        }
        | PlanPredicateKind::Boolean(expression) => {
            expression_non_null_implies_target_non_null(expression, target, input_schema)
        }
        PlanPredicateKind::And(predicates) => {
            for predicate in predicates {
                if predicate_proves_expression_non_null(predicate, target, input_schema)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn require_passthrough_field(input: &StructField, output: &StructField) -> DeltaResult<()> {
    if input.name() != output.name()
        || input.data_type() != output.data_type()
        || input.is_nullable() != output.is_nullable()
    {
        return Err(Error::generic(format!(
            "plan StructPatch passthrough field `{}` does not exactly match output field `{}`",
            input.name(),
            output.name()
        )));
    }
    Ok(())
}

fn require_string_map(data_type: &DataType) -> DeltaResult<()> {
    let DataType::Map(map) = data_type else {
        return Err(Error::generic(format!(
            "plan MapToStruct input must be a map, got {data_type}"
        )));
    };
    if map.key_type() != &DataType::STRING || map.value_type() != &DataType::STRING {
        return Err(Error::generic(format!(
            "plan MapToStruct input must be MAP<STRING, STRING>, got {data_type}"
        )));
    }
    Ok(())
}

fn require_comparable(data_type: &DataType) -> DeltaResult<()> {
    if matches!(data_type, DataType::Primitive(_)) {
        Ok(())
    } else {
        Err(Error::unsupported(format!(
            "plan comparisons require primitive operands, got {data_type}"
        )))
    }
}

fn unsupported_plan_expression(name: &str) -> Error {
    Error::unsupported(format!(
        "{name} is not part of the mandatory plan expression dialect"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit, null_lit, BinaryExpressionOp, Expression};
    use crate::schema::{schema, schema_ref};

    fn input_schema() -> StructType {
        schema! {
            not_null "id": LONG,
            nullable "maybe": LONG,
            nullable "parent": {
                not_null "child": STRING,
            },
        }
    }

    #[test]
    fn resolves_literal_type_and_nullability() {
        let input = input_schema();
        let value = PlanExpression::resolve(lit(1i64), &input, None).unwrap();
        assert_eq!(value.data_type(), &DataType::LONG);
        assert!(!value.is_nullable());

        let null = PlanExpression::resolve(null_lit(DataType::LONG), &input, None).unwrap();
        assert_eq!(null.data_type(), &DataType::LONG);
        assert!(null.is_nullable());
    }

    #[test]
    fn column_inherits_nullable_parent() {
        let expression =
            PlanExpression::resolve(col!("parent.child"), &input_schema(), None).unwrap();
        assert_eq!(expression.data_type(), &DataType::STRING);
        assert!(expression.is_nullable());
    }

    #[test]
    fn rejects_non_boolean_filter_expression() {
        let error =
            PlanPredicate::resolve(Predicate::BooleanExpression(lit(1i64)), &input_schema())
                .unwrap_err();
        assert!(error.to_string().contains("expected boolean"));
    }

    #[test]
    fn rejects_arithmetic_from_mandatory_dialect() {
        let expression = Expression::binary(BinaryExpressionOp::Divide, col!("id"), lit(2i64));
        let error = PlanExpression::resolve(expression, &input_schema(), None).unwrap_err();
        assert!(error
            .to_string()
            .contains("not part of the mandatory plan expression dialect"));
    }

    #[test]
    fn resolves_struct_children_from_output_schema() {
        let output = schema_ref! {
            not_null "id": LONG,
            nullable "maybe": LONG,
        };
        let expression = Expression::struct_from([col!("id"), col!("maybe")]);
        let resolved = PlanExpression::resolve(
            expression,
            &input_schema(),
            Some(&DataType::from(output.as_ref().clone())),
        )
        .unwrap();
        let PlanExpressionKind::Struct { fields, .. } = resolved.kind() else {
            panic!("expected Struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::LONG);
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn rejects_nullable_expression_for_non_nullable_field() {
        let output = schema! { not_null "maybe": LONG };
        let expression = Expression::struct_from([col!("maybe")]);
        let error =
            PlanExpression::resolve(expression, &input_schema(), Some(&DataType::from(output)))
                .unwrap_err();
        assert!(error.to_string().contains("may produce null"));
    }

    #[test]
    fn struct_guard_proves_non_null_sibling_leaves() {
        let input = schema! {
            nullable "add": {
                nullable "dv": {
                    not_null "storage": STRING,
                    not_null "path": STRING,
                    nullable "offset": INTEGER,
                },
            },
        };
        let output = schema! {
            nullable "dv": {
                not_null "storage": STRING,
                not_null "path": STRING,
                nullable "offset": INTEGER,
            },
        };
        let dv = Expression::struct_with_nullability_from(
            [
                col!("add.dv.storage"),
                col!("add.dv.path"),
                col!("add.dv.offset"),
            ],
            Expression::from_pred(col!("add.dv.storage").is_not_null()),
        );
        let expression = Expression::struct_from([dv]);

        PlanExpression::resolve(expression, &input, Some(&DataType::from(output))).unwrap();
    }

    #[test]
    fn unrelated_struct_guard_does_not_hide_nullable_assignment() {
        let input = schema! {
            nullable "value": STRING,
            not_null "guard": BOOLEAN,
        };
        let output = schema! { nullable "record": { not_null "value": STRING } };
        let record = Expression::struct_with_nullability_from([col!("value")], col!("guard"));
        let expression = Expression::struct_from([record]);

        let error =
            PlanExpression::resolve(expression, &input, Some(&DataType::from(output))).unwrap_err();
        assert!(error.to_string().contains("may produce null"));
    }

    #[test]
    fn coalesced_guard_proves_corresponding_coalesced_sibling() {
        let action = schema! {
            nullable "dv": {
                not_null "storage": STRING,
                not_null "path": STRING,
            },
        };
        let input = schema! {
            nullable "add": (action.clone()),
            nullable "remove": (action),
        };
        let output = schema! {
            nullable "dv": {
                not_null "storage": STRING,
                not_null "path": STRING,
            },
        };
        let storage = Expression::coalesce([col!("add.dv.storage"), col!("remove.dv.storage")]);
        let path = Expression::coalesce([col!("add.dv.path"), col!("remove.dv.path")]);
        let dv = Expression::struct_with_nullability_from(
            [storage.clone(), path],
            Expression::from_pred(storage.is_not_null()),
        );

        PlanExpression::resolve(
            Expression::struct_from([dv]),
            &input,
            Some(&DataType::from(output)),
        )
        .unwrap();
    }

    #[test]
    fn rejects_empty_junction() {
        let predicate = Predicate::Junction(crate::expressions::JunctionPredicate {
            op: JunctionPredicateOp::And,
            preds: Vec::new(),
        });
        let error = PlanPredicate::resolve(predicate, &input_schema()).unwrap_err();
        assert!(error.to_string().contains("at least one child"));
    }
}
