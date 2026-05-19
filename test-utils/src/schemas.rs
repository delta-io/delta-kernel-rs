//! Tiny kernel [`StructType`] schema builders used across engine integration tests.

use std::sync::Arc;

use delta_kernel::arrow::array::{Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

/// Schema with a single non-null `LONG` field named `x`.
///
/// Used as the canonical one-column LONG schema for scan/parity/window/state-machine tests.
pub fn single_long_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "x",
        DataType::LONG,
    )]))
}

/// Build a schema where every column has `LONG` type. Each `(name, nullable)` entry produces a
/// `StructField` with the given nullability.
pub fn long_schema(cols: &[(&str, bool)]) -> SchemaRef {
    let fields: Vec<StructField> = cols
        .iter()
        .map(|(name, nullable)| {
            if *nullable {
                StructField::nullable(*name, DataType::LONG)
            } else {
                StructField::not_null(*name, DataType::LONG)
            }
        })
        .collect();
    Arc::new(StructType::new_unchecked(fields))
}

/// Build a `Vec<Vec<Scalar>>` of [`Scalar::Long`] rows from nested `i64` iterators.
///
/// Each outer item is a row; each inner item is a column value. Equivalent to writing the
/// nested `vec![vec![Scalar::Long(..), ..], ..]` literal by hand.
pub fn long_rows<I, R>(rows: I) -> Vec<Vec<Scalar>>
where
    I: IntoIterator<Item = R>,
    R: IntoIterator<Item = i64>,
{
    rows.into_iter()
        .map(|row| row.into_iter().map(Scalar::Long).collect())
        .collect()
}

/// Build a single-column `Int64` [`RecordBatch`] with the given column name (non-null).
pub fn int64_batch(col: &str, values: &[i64]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        col,
        ArrowDataType::Int64,
        false,
    )]));
    let array = Arc::new(Int64Array::from_iter_values(values.iter().copied()));
    RecordBatch::try_new(schema, vec![array]).expect("int64_batch")
}
