use std::collections::HashSet;
use std::io::BufReader;
use std::sync::Arc;

use arrow_arith::boolean::{is_not_null, not};
use arrow_array::{
    new_null_array,
    array::PrimitiveArray,
    types::{Int32Type, Int64Type},
    Array,
    BooleanArray,
    Datum,
    RecordBatch,
    StringArray,
    StructArray,
};
use arrow_ord::cmp::{gt, gt_eq, lt, lt_eq};
use arrow_json::ReaderBuilder;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::nullif::nullif;
use tracing::debug;

use crate::expressions::scalars::Scalar;
use crate::expressions::BinaryOperator;

use crate::error::{DeltaResult, Error};
use crate::scan::Expression;
use crate::schema::SchemaRef;

/// Data skipping predicates produce boolean arrays as output, where true means the predicate was
/// satisfied, false means the predicate was not satisfied, and null means the predicate could not
/// be evaluated.
type MetadataFilterResult = Result<BooleanArray, ArrowError>;

/// Trait representing a data skipping predicate, which can convert a record batch of stats into a
/// data skipping filter result.
// TODO: This whole trait hierarchy should really just be a bunch of closures, but it's ~impossible
// to specify correct lifetimes when a closure outlives the state it captures. You can use move ||
// syntax to make it compile, but the resulting closure is FnOnce because the compiler (incorrectly)
// assumes that the closure taking ownership means that invoking it consumes the captured state.
trait MetadataFilterFn {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult;
}

/// Helper method for boxed data skipping predicates.
impl MetadataFilterFn for Box<dyn MetadataFilterFn> {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult {
        self.as_ref().invoke(&stats)
    }
}

/// Every data skipping predicate has at least one leaf node that references a stats column.
// TODO: This shouldn't be public, but rust weird about visiblity and impl
struct MetadataFilterColumnFn {
    stat_name: &'static str,
    nested_names: Vec<String>,
    col_name: String
}

impl MetadataFilterColumnFn {
    /// Helper method for drilling down into a (possibly nested) stats column.
    /// A column such as minValues.a.b.c would be expressed as minValues [a, b] c.
    fn column_as_struct<'a>(name: &str, column: &Option<&'a Arc<dyn Array>>)
                            -> Result<&'a StructArray, ArrowError> {
        column
            .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(ArrowError::SchemaError(format!("{} is not a struct", name)))
    }

    /// Given a record batch of stats, extracts the requested stats column.
    fn invoke<'a>(&self, stats: &'a RecordBatch) -> Result<&'a Arc<dyn Array>, ArrowError> {
        let mut col = Self::column_as_struct(&self.stat_name, &stats.column_by_name(&self.stat_name));
        for col_name in &self.nested_names {
            col = Self::column_as_struct(&col_name, &col?.column_by_name(&col_name));
        }
        col?.column_by_name(&self.col_name)
            .ok_or(ArrowError::SchemaError(format!("No such column: {}", self.col_name)))
    }
}

/// <left> AND <right>
struct MetadataFilterAndFn {
    left: Box<dyn MetadataFilterFn>,
    right: Box<dyn MetadataFilterFn>,
}

impl MetadataFilterFn for MetadataFilterAndFn {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult {
        arrow_arith::boolean::and(&self.left.invoke(&stats)?, &self.right.invoke(&stats)?)
    }
}

/// <left> OR <right>
struct MetadataFilterOrFn {
    left: Box<dyn MetadataFilterFn>,
    right: Box<dyn MetadataFilterFn>,
}

impl MetadataFilterFn for MetadataFilterOrFn {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult {
        arrow_arith::boolean::or(&self.left.invoke(&stats)?, &self.right.invoke(&stats)?)
    }
}

/// <column> <op> <literal>, for open-ended comparisons such as < or >=.
struct MetadataFilterComparisonFn {
    op: fn(&dyn Datum, &dyn Datum) -> MetadataFilterResult,
    column: MetadataFilterColumnFn,
    literal: Box<dyn Datum>,
}

impl MetadataFilterFn for MetadataFilterComparisonFn {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult {
        (self.op)(&self.column.invoke(&stats)?, self.literal.as_ref())
    }
}

/// <column> = <literal> -- equality is a special case, requiring two column comparisons instead of one.
struct MetadataFilterEqComparisonFn {
    min_column: MetadataFilterColumnFn,
    max_column: MetadataFilterColumnFn,
    literal: Box<dyn Datum>,
}

impl MetadataFilterFn for MetadataFilterEqComparisonFn {
    fn invoke(&self, stats: &RecordBatch) -> MetadataFilterResult {
        arrow_arith::boolean::and(
            &lt_eq(&self.min_column.invoke(&stats)?, self.literal.as_ref())?,
            &lt_eq(self.literal.as_ref(), &self.max_column.invoke(&stats)?)?)
    }
}

trait ProvidesMetadataFilter {
    fn extract_metadata_filters(&self) -> Option<Box<dyn MetadataFilterFn>>;

    // TODO: These aren't really part of the interface, but rust does not allow an impl to define
    // any helper methods (private or otherwise).
    fn extract_stats_column(&self, stat_name: &'static str) -> Option<MetadataFilterColumnFn>;
    fn extract_literal(&self) -> Option<Box<dyn Datum>>;
}

impl ProvidesMetadataFilter for Expression {

    fn extract_stats_column(&self, stat_name: &'static str) -> Option<MetadataFilterColumnFn> {
        match self {
            // TODO: split names like a.b.c into [a, b], c below
            Expression::Column(name) => {
                let f = MetadataFilterColumnFn {
                    stat_name,
                    nested_names: vec![], // TODO: Actually handle nested columns...
                    col_name: name.to_string(),
                };
                Some(f)
            }
            _ => None,
        }
    }

    fn extract_literal(&self) -> Option<Box<dyn Datum>> {
        match self {
            Expression::Literal(Scalar::Long(v)) =>
                Some(Box::new(PrimitiveArray::<Int64Type>::new_scalar(*v))),
            Expression::Literal(Scalar::Integer(v)) =>
                Some(Box::new(PrimitiveArray::<Int32Type>::new_scalar(*v))),
            _ => None
        }
    }

    /// Converts this expression to Some data skipping predicate over the given record batch, if the
    /// expression is eligible for data skipping. Otherwise None. The predicate is callable,
    /// converting a record batch to a boolean array.
    fn extract_metadata_filters(&self) -> Option<Box<dyn MetadataFilterFn>> {
        match self {
            // <expr> AND <expr>
            Expression::BinaryOperation { op: BinaryOperator::And, left, right } => {
                println!("AND got left {} and right {}", left, right);
                let left = left.extract_metadata_filters();
                let right = right.extract_metadata_filters();
                // If one leg of the AND is missing, it just degenerates to the other leg.
                match (left, right) {
                    (Some(left), Some(right)) => {
                        let f = MetadataFilterAndFn { left, right };
                        Some(Box::new(f))
                    }
                    (left, right) => left.or(right),
                }
            }

            // <expr> OR <expr>
            Expression::BinaryOperation { op: BinaryOperator::Or, left, right } => {
                let left = left.extract_metadata_filters();
                let right = right.extract_metadata_filters();
                // OR is valid only if both legs are valid.
                left.zip(right).map(|(left, right)| -> Box<dyn MetadataFilterFn> {
                    let f = MetadataFilterOrFn { left, right };
                    Box::new(f)
                })
            }

            // col <compare> value
            Expression::BinaryOperation { op, left, right } => {
                let min_column = left.extract_stats_column("minValues");
                let max_column = left.extract_stats_column("maxValues");
                let literal = right.extract_literal();
                let (op, column): (fn(&dyn Datum, &dyn Datum) -> MetadataFilterResult, _) = match op {
                    BinaryOperator::Equal => {
                        // Equality filter compares the literal against both min and max stat columns
                        println!("Got an equality filter");
                        return min_column.zip(max_column).zip(literal)
                            .map(|((min_column, max_column), literal)| -> Box<dyn MetadataFilterFn> {
                                let f = MetadataFilterEqComparisonFn { min_column, max_column, literal };
                                Box::new(f)
                            });
                    }

                    BinaryOperator::LessThan => (lt, min_column),
                    BinaryOperator::LessThanOrEqual => (lt_eq, min_column),
                    BinaryOperator::GreaterThan => (gt, max_column),
                    BinaryOperator::GreaterThanOrEqual => (gt_eq, max_column),

                    _ => return None, // Incompatible operator
                };
                column.zip(literal).map(|(column, literal)| -> Box<dyn MetadataFilterFn> {
                    let f = MetadataFilterComparisonFn { op, column, literal };
                    Box::new(f)
                })
            }
            _ => None,
        }
    }
}

pub(crate) struct DataSkippingFilter {
    stats_schema: Arc<Schema>,
    predicate: Box<dyn MetadataFilterFn>,
}

impl DataSkippingFilter {
    pub(crate) fn try_new(table_schema: &SchemaRef, predicate: &Option<Expression>) -> Option<DataSkippingFilter> {
        let predicate = match predicate {
            Some(predicate) => predicate,
            _ => return None,
        };
        println!("Creating a data skipping filter for {}", &predicate);
        let field_names: HashSet<_> = predicate.references();

        // Build the stats read schema by extracting the column names referenced by the predicate,
        // extracting the corresponding field from the table schema, and inserting that field.
        let data_fields: Vec<_> = table_schema.fields.iter()
            .filter(|field| field_names.contains(&field.name.as_str()))
            .filter_map(|field| Field::try_from(field).ok())
            .collect::<Vec<_>>();

        let stats_schema = Schema::new(vec![
            Field::new("minValues", DataType::Struct(data_fields.clone().into()), true),
            Field::new("maxValues", DataType::Struct(data_fields.into()), true),
        ]);
        let stats_schema = stats_schema.into();

        predicate.extract_metadata_filters()
            .map(|predicate| DataSkippingFilter { stats_schema, predicate } )
    }

    pub(crate) fn apply(&self, actions: &RecordBatch) -> DeltaResult<RecordBatch> {
        let adds = actions
            .column_by_name("add")
            .ok_or(Error::MissingColumn("Column 'add' not found.".into()))?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(Error::UnexpectedColumnType(
                "Expected type 'StructArray'.".into(),
            ))?;
        let stats = adds
            .column_by_name("stats")
            .ok_or(Error::MissingColumn("Column 'stats' not found.".into()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::UnexpectedColumnType(
                "Expected type 'StringArray'.".into(),
            ))?;

        let parsed_stats = concat_batches(
            &self.stats_schema,
            stats
                .iter()
                .map(|json_string| Self::hack_parse(&self.stats_schema, json_string))
                .collect::<Result<Vec<_>, _>>()?
                .iter(),
        )?;

        let skipping_vector = self.predicate.invoke(&parsed_stats)?;
        let skipping_vector = &is_not_null(&nullif(&skipping_vector, &not(&skipping_vector)?)?)?;

        let before_count = actions.num_rows();
        let after = filter_record_batch(&actions, skipping_vector)?;
        debug!(
            "number of actions before/after data skipping: {before_count} / {}",
            after.num_rows()
        );
        Ok(after)
    }

    fn hack_parse(stats_schema: &Arc<Schema>, json_string: Option<&str>) -> DeltaResult<RecordBatch> {
        match json_string {
            Some(s) => Ok(ReaderBuilder::new(stats_schema.clone())
                          .build(BufReader::new(s.as_bytes()))?
                          .collect::<Vec<_>>()
                          .into_iter()
                          .next()
                          .transpose()?
                          .ok_or(Error::MissingData("Expected data".into()))?),
            None => Ok(RecordBatch::try_new(
                stats_schema.clone().into(),
                stats_schema.fields.iter()
                    .map(|field| new_null_array(&field.data_type(), 1))
                    .collect()
            )?),
        }
    }
}
