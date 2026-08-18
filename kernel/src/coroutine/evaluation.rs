//! Expression-evaluation requests.

use std::any::Any;
use std::sync::Arc;

use derive_more::Constructor;

use crate::expressions::{ExpressionRef, Scalar};
use crate::schema::{DataType, SchemaRef};
use crate::{EngineData, FilteredEngineData};

/// Values to materialize as one [`EngineData`] batch.
#[derive(Constructor)]
pub struct CreateEngineData {
    /// Schema describing each row.
    pub schema: SchemaRef,
    /// Owned rows with one scalar per top-level schema field.
    pub rows: Vec<Vec<Scalar>>,
}

/// Opaque, shared connector state for a prepared expression evaluator.
#[derive(Clone)]
pub enum EvaluatorHandle {
    /// Scalar handle, commonly for FFI.
    Id(i64),
    /// Shared in-process evaluator state.
    Arc(Arc<dyn Any + Send + Sync>),
}

/// Parameters for preparing an expression evaluator.
#[derive(Constructor)]
pub struct CreateExpressionEvaluator {
    /// Schema of each input batch.
    pub input_schema: SchemaRef,
    /// Expression to evaluate.
    pub expression: ExpressionRef,
    /// Type of each evaluation result.
    pub output_type: DataType,
}

/// A prepared evaluator and input batch to evaluate.
#[derive(Constructor)]
pub struct EvaluateExpression {
    /// Connector-provided prepared evaluator.
    pub evaluator: EvaluatorHandle,
    /// Input batch.
    pub input: Arc<dyn EngineData>,
}

/// A prepared evaluator and filtered input batch to evaluate while preserving row selection.
#[derive(Constructor)]
pub struct EvaluateFilteredExpression {
    /// Connector-provided prepared evaluator.
    pub evaluator: EvaluatorHandle,
    /// Filtered input batch.
    pub input: Arc<FilteredEngineData>,
}
