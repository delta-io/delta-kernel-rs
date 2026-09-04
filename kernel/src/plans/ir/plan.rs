//! Validated plan containers ([`Plan`], [`PlanNode`]).

use std::collections::HashSet;
use std::sync::Arc;

pub use super::nodes::Operator;
use super::nodes::{Filter, Project, ScanFile, Values};
use crate::schema::{SchemaRef, StructType};
use crate::{DeltaResult, Error};

// ============================================================================
// Plan nodes
// ============================================================================

/// One node in a plan: an [`Operator`] and the indices of its input nodes.
///
/// A node is identified by its position in [`Plan::nodes`]; `inputs` lists those indices for
/// the upstream nodes this operator reads from. `inputs` order is interpreted per [`Operator`]
/// (e.g. for `Operator::SemiJoin` the convention is `[probe, build]`). `Operator::UnionAll`
/// emits the rows of all inputs regardless of input order.
#[derive(Debug, Clone)]
pub struct PlanNode {
    op: Operator,
    inputs: Vec<usize>,
}

impl PlanNode {
    /// A node applying `op` over the nodes at `inputs` (indices into [`Plan::nodes`]).
    pub(crate) fn new(op: impl Into<Operator>, inputs: Vec<usize>) -> Self {
        Self {
            op: op.into(),
            inputs,
        }
    }

    /// Returns this node's relational operator.
    pub fn operator(&self) -> &Operator {
        &self.op
    }

    /// Returns the indices of this node's inputs, in operator-defined order.
    pub fn inputs(&self) -> &[usize] {
        &self.inputs
    }

    /// Consumes this node and returns its operator and input indices.
    pub fn into_parts(self) -> (Operator, Vec<usize>) {
        (self.op, self.inputs)
    }
}

// ============================================================================
// Plans
// ============================================================================

/// A plan: an ordered sequence of [`PlanNode`]s forming a dataflow DAG.
///
/// A node is identified by its index in `nodes`. Each [`PlanNode`] pairs an operator with the
/// indices of its inputs:
///
/// - `op` ([`Operator`]) is the operator: a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<usize>` naming the indices of the upstream nodes the operator reads from.
///
/// A node depends on another when one of its `inputs` is that node's index. `nodes` is stored in
/// topological order: every node appears after the nodes it consumes (each input index is
/// strictly less than the node's own index), so an engine can evaluate `nodes` in slice order;
/// each node's inputs are guaranteed bound by the time the node is reached.
///
/// A well-formed `Plan` has at least one node. The **terminal node** is always the last entry in
/// [`Self::nodes`]: no other node lists its index in `inputs`, and its rows are the value the engine
/// streams to the caller.
///
/// The terminal node describes the rows produced by the plan. The calling API determines who
/// consumes them: kernel may use [`PlanExecutor`](crate::plans::PlanExecutor), or the connector may
/// consume them directly when kernel returns the plan.
///
/// # Optimization
///
/// For the best performance, connectors are encouraged to run kernel-produced
/// plans through their query optimizer before execution (e.g. to fold adjacent
/// filters, merge scans over the same files, or choose physical join and scan
/// strategies).
///
/// # Example
///
/// A five-node plan: two independent scans, each filtered, then unioned. The `nodes`
/// `Vec<PlanNode>`:
///
/// ```text
/// Plan {
///     nodes: vec![
///         PlanNode { op: ScanParquet(..), inputs: vec![]     },  // node 0
///         PlanNode { op: ScanParquet(..), inputs: vec![]     },  // node 1
///         PlanNode { op: Filter(..),      inputs: vec![0]    },  // node 2
///         PlanNode { op: Filter(..),      inputs: vec![1]    },  // node 3
///         PlanNode { op: UnionAll(..),    inputs: vec![2, 3] },  // node 4
///     ],
/// }
/// ```
///
/// The dataflow DAG this encodes:
///
/// ```text
///    ScanParquet [0]     ScanParquet [1]
///           |                    |
///           v                    v
///       Filter [2]           Filter [3]
///           |                    |
///           +---------+----------+
///                     v
///               UnionAll [4]   <-- terminal (last node)
/// ```
///
/// The engine evaluates the nodes in the order of the `nodes` vector: nodes `0` and `1` first
/// (sources), then `2` and `3`, then `4`. The engine streams the rows produced at the terminal
/// node to the caller.
#[derive(Debug, Clone)]
pub struct Plan {
    nodes: Vec<PlanNode>,
}

impl Plan {
    /// Constructs a plan after validating its topology, operator inputs, and propagated schemas.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty plan, a non-topological input reference, an invalid operator
    /// arity, or an operator whose values or schema do not match its inputs.
    pub(crate) fn try_new(nodes: Vec<PlanNode>) -> DeltaResult<Self> {
        let plan = Self { nodes };
        plan.validate()?;
        Ok(plan)
    }

    /// Returns the plan nodes in topological order, with the terminal node last.
    pub fn nodes(&self) -> &[PlanNode] {
        &self.nodes
    }

    /// Consumes this plan and returns its nodes in topological order.
    pub fn into_nodes(self) -> Vec<PlanNode> {
        self.nodes
    }

    /// Revalidates this plan's topology, operator inputs, and propagated schemas.
    ///
    /// Plan construction performs the same validation. Serialization calls this method again as
    /// defense in depth at the process boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty plan, a non-topological input reference, an invalid operator
    /// arity, or an operator whose values or schema do not match its inputs.
    pub fn validate(&self) -> DeltaResult<()> {
        if self.nodes.is_empty() {
            return Err(Error::generic("plan must contain at least one node"));
        }

        let mut schemas = Vec::with_capacity(self.nodes.len());
        for (node_index, node) in self.nodes.iter().enumerate() {
            u32::try_from(node_index)
                .map_err(|_| Error::generic("plan has more nodes than the wire format supports"))?;
            for &input in &node.inputs {
                if input >= node_index {
                    return Err(node_error(
                        node_index,
                        &node.op,
                        format!(
                            "input {input} must reference one of the {node_index} prior node(s)"
                        ),
                    ));
                }
                u32::try_from(input).map_err(|_| {
                    node_error(
                        node_index,
                        &node.op,
                        format!("input {input} cannot be represented by the wire format"),
                    )
                })?;
            }

            let input_schemas = node
                .inputs
                .iter()
                .map(|&input| Arc::clone(&schemas[input]))
                .collect::<Vec<_>>();
            let output_schema = validate_operator(node_index, &node.op, &input_schemas)?;
            schemas.push(output_schema);
        }
        Ok(())
    }
}

fn validate_operator(
    node_index: usize,
    op: &Operator,
    inputs: &[SchemaRef],
) -> DeltaResult<SchemaRef> {
    let result = (|| -> DeltaResult<SchemaRef> {
        Ok(match op {
            Operator::ScanParquet(scan) => {
                require_arity(inputs, 0)?;
                validate_scan(&scan.schema, &scan.file_constant_columns, &scan.files)?;
                Arc::clone(&scan.schema)
            }
            Operator::ScanJson(scan) => {
                require_arity(inputs, 0)?;
                validate_scan(&scan.schema, &scan.file_constant_columns, &scan.files)?;
                Arc::clone(&scan.schema)
            }
            Operator::Values(values) => {
                require_arity(inputs, 0)?;
                validate_values(values)?;
                Arc::clone(&values.schema)
            }
            Operator::Project(project) => {
                require_arity(inputs, 1)?;
                Project::try_new(
                    inputs[0].as_ref(),
                    Arc::clone(project.expression().source_expression()),
                    Arc::clone(project.schema()),
                )?;
                Arc::clone(project.schema())
            }
            Operator::Filter(filter) => {
                require_arity(inputs, 1)?;
                Filter::try_new(
                    inputs[0].as_ref(),
                    Arc::clone(filter.predicate().source_predicate()),
                )?;
                Arc::clone(&inputs[0])
            }
            Operator::DataSkipping(site) => {
                require_arity(inputs, 1)?;
                site.validate_input(inputs[0].as_ref())?;
                Arc::clone(&inputs[0])
            }
            Operator::DynamicScan(scan) => {
                require_arity(inputs, 1)?;
                scan.validate_input(&inputs[0])?;
                Arc::clone(&scan.schema)
            }
            Operator::Aggregate(aggregate) => {
                require_arity(inputs, 1)?;
                aggregate.validate_input(inputs[0].as_ref())?;
                Arc::clone(&aggregate.schema)
            }
            Operator::SemiJoin(join) => {
                require_arity(inputs, 2)?;
                if join.probe_keys.len() != join.build_keys.len() {
                    return Err(Error::generic(format!(
                        "join has {} probe key(s) but {} build key(s)",
                        join.probe_keys.len(),
                        join.build_keys.len()
                    )));
                }
                for (probe, build) in join.probe_keys.iter().zip(&join.build_keys) {
                    let probe_field = inputs[0].field_at(probe)?;
                    let build_field = inputs[1].field_at(build)?;
                    if probe_field.data_type() != build_field.data_type() {
                        return Err(Error::generic(format!(
                            "join keys `{probe}` and `{build}` have different types: {} and {}",
                            probe_field.data_type(),
                            build_field.data_type()
                        )));
                    }
                }
                Arc::clone(&inputs[0])
            }
            Operator::UnionAll(_) => {
                if inputs.len() < 2 {
                    return Err(Error::generic("union_all requires at least two inputs"));
                }
                if let Some(index) = inputs.iter().position(|schema| schema != &inputs[0]) {
                    return Err(Error::generic(format!(
                        "union_all input {index} has a different schema from input 0"
                    )));
                }
                Arc::clone(&inputs[0])
            }
        })
    })();
    result.map_err(|error| node_error(node_index, op, error.to_string()))
}

fn require_arity(inputs: &[SchemaRef], expected: usize) -> DeltaResult<()> {
    if inputs.len() != expected {
        return Err(Error::generic(format!(
            "expected {expected} input(s), found {}",
            inputs.len()
        )));
    }
    Ok(())
}

fn validate_scan(
    schema: &StructType,
    file_constant_columns: &[String],
    files: &[ScanFile],
) -> DeltaResult<()> {
    let mut seen = HashSet::new();
    let mut fields = Vec::with_capacity(file_constant_columns.len());
    for name in file_constant_columns {
        if !seen.insert(name) {
            return Err(Error::generic(format!(
                "file-constant column `{name}` is listed more than once"
            )));
        }
        let field = schema.field(name).ok_or_else(|| {
            Error::generic(format!(
                "file-constant column `{name}` is absent from scan schema"
            ))
        })?;
        if field.is_metadata_column() {
            return Err(Error::generic(format!(
                "file-constant column `{name}` is a metadata column"
            )));
        }
        fields.push(field);
    }

    for (file_index, file) in files.iter().enumerate() {
        if file.file_constants.len() != fields.len() {
            return Err(Error::generic(format!(
                "file {file_index} has {} constant value(s), expected {}",
                file.file_constants.len(),
                fields.len()
            )));
        }
        for (value_index, (value, field)) in file.file_constants.iter().zip(&fields).enumerate() {
            if value.data_type() != *field.data_type() {
                return Err(Error::generic(format!(
                    "file {file_index} constant {value_index} for `{}` has type {}, expected {}",
                    field.name(),
                    value.data_type(),
                    field.data_type()
                )));
            }
            if value.is_null() && !field.is_nullable() {
                return Err(Error::generic(format!(
                    "file {file_index} constant {value_index} for non-nullable column `{}` is null",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn validate_values(values: &Values) -> DeltaResult<()> {
    let fields = values.schema.fields().collect::<Vec<_>>();
    for (row_index, row) in values.rows.iter().enumerate() {
        if row.len() != fields.len() {
            return Err(Error::generic(format!(
                "values row {row_index} has {} value(s), expected {}",
                row.len(),
                fields.len()
            )));
        }
        for (column_index, (value, field)) in row.iter().zip(&fields).enumerate() {
            if value.data_type() != *field.data_type() {
                return Err(Error::generic(format!(
                    "values row {row_index} column {column_index} `{}` has type {}, expected {}",
                    field.name(),
                    value.data_type(),
                    field.data_type()
                )));
            }
            if value.is_null() && !field.is_nullable() {
                return Err(Error::generic(format!(
                    "values row {row_index} column {column_index} `{}` is null but not nullable",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn node_error(node_index: usize, op: &Operator, message: impl std::fmt::Display) -> Error {
    Error::generic(format!("plan node {node_index} ({op}): {message}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::expressions::{col, column_name, lit, Predicate, Scalar};
    use crate::plans::ir::nodes::{Filter, SemiJoin, UnionAll};
    use crate::schema::{DataType, StructField, StructType};

    fn schema(name: &str, data_type: DataType) -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            name, data_type,
        )]))
    }

    fn values(schema: SchemaRef, value: Scalar) -> PlanNode {
        PlanNode::new(Values::new(schema, vec![vec![value]]), vec![])
    }

    #[test]
    fn empty_plan_is_rejected() {
        assert_result_error_with_message(Plan::try_new(vec![]), "at least one node");
    }

    #[test]
    fn forward_input_reference_is_rejected() {
        let node = PlanNode::new(Values::new(schema("id", DataType::LONG), vec![]), vec![0]);
        assert_result_error_with_message(Plan::try_new(vec![node]), "must reference");
    }

    #[test]
    fn values_must_match_their_schema() {
        let node = values(schema("id", DataType::LONG), Scalar::String("wrong".into()));
        assert_result_error_with_message(Plan::try_new(vec![node]), "has type string");
    }

    #[test]
    fn filter_is_rechecked_against_its_actual_input() {
        let string_schema = schema("id", DataType::STRING);
        let filter = Filter::try_new(
            &string_schema,
            Predicate::eq(col!("id"), lit("expected-string")),
        )
        .unwrap();
        let nodes = vec![
            values(schema("id", DataType::LONG), Scalar::Long(1)),
            PlanNode::new(filter, vec![0]),
        ];
        assert_result_error_with_message(Plan::try_new(nodes), "has type string, expected long");
    }

    #[test]
    fn joins_require_pairwise_compatible_key_types() {
        let join = SemiJoin {
            inverted: false,
            probe_keys: vec![column_name!("id")],
            build_keys: vec![column_name!("id")],
        };
        let nodes = vec![
            values(schema("id", DataType::LONG), Scalar::Long(1)),
            values(schema("id", DataType::STRING), Scalar::String("1".into())),
            PlanNode::new(join, vec![0, 1]),
        ];
        assert_result_error_with_message(Plan::try_new(nodes), "different types");
    }

    #[test]
    fn union_requires_identical_input_schemas() {
        let nodes = vec![
            values(schema("id", DataType::LONG), Scalar::Long(1)),
            values(schema("id", DataType::STRING), Scalar::String("1".into())),
            PlanNode::new(UnionAll, vec![0, 1]),
        ];
        assert_result_error_with_message(Plan::try_new(nodes), "different schema");
    }
}
