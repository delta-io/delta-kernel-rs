use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;

use crate::scan::ColumnType;
use crate::schema::SchemaRef;
use crate::{DeltaResult, ExpressionRef};

use super::{TableChanges, CDF_FIELDS};

/// The result of building a [`TableChanges`] scan over a table. This can be used to get a change
/// data feed from the table
#[allow(unused)]
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
}

/// Builder to read the `TableChanges` of a table.
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}

impl TableChangesScanBuilder {
    /// Create a new [`TableChangesScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`TableChanges`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`TableChanges`]. See
    /// [`TableChangesScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    pub fn with_schema_opt(self, schema_opt: Option<SchemaRef>) -> Self {
        match schema_opt {
            Some(schema) => self.with_schema(schema),
            None => self,
        }
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self
            .schema
            .unwrap_or(self.table_changes.schema.clone().into());
        let mut have_partition_cols = false;
        let mut read_fields = Vec::with_capacity(logical_schema.fields.len());

        // Loop over all selected fields and note if they are columns that will be read from the
        // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
        // be filled in by evaluating an expression ([`ColumnType::Partition`])
        println!("Logical schema: {:?}", logical_schema);
        let column_types = logical_schema
            .fields()
            .enumerate()
            .map(|(index, logical_field)| -> DeltaResult<_> {
                if self
                    .table_changes
                    .partition_columns()
                    .contains(logical_field.name())
                {
                    // Store the index into the schema for this field. When we turn it into an
                    // expression in the inner loop, we will index into the schema and get the name and
                    // data type, which we need to properly materialize the column.
                    have_partition_cols = true;
                    Ok(ColumnType::Partition(index))
                } else if CDF_FIELDS
                    .iter()
                    .any(|field| field.name() == logical_field.name())
                {
                    // CDF Columns are generated, so they do not have a column mapping.
                    Ok(ColumnType::Selected(logical_field.name().to_string()))
                } else {
                    // Add to read schema, store field so we can build a `Column` expression later
                    // if needed (i.e. if we have partition columns)
                    let physical_field =
                        logical_field.make_physical(*self.table_changes.column_mapping_mode())?;
                    debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                    let physical_name = physical_field.name.clone();
                    read_fields.push(physical_field);
                    Ok(ColumnType::Selected(physical_name))
                }
            })
            .try_collect()?;
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            predicate: self.predicate,
            all_fields: column_types,
            have_partition_cols,
        })
    }
}
#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr, Scalar};
    use crate::scan::ColumnType;
    use crate::schema::{DataType, StructField, StructType};
    use crate::{Expression, Table};

    #[test]
    fn simple_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = table.table_changes(engine.as_ref(), 0, 1).unwrap();

        let scan = table_changes.into_scan_builder().build().unwrap();
        assert_eq!(
            scan.all_fields,
            vec![
                ColumnType::Selected("part".to_string()),
                ColumnType::Selected("id".to_string()),
                ColumnType::Selected("_change_type".to_string()),
                ColumnType::Selected("_commit_version".to_string()),
                ColumnType::Selected("_commit_timestamp".to_string()),
            ]
        );
        assert_eq!(scan.predicate, None);
        assert!(!scan.have_partition_cols);
    }

    #[test]
    fn projected_and_filtered_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = table.table_changes(engine.as_ref(), 0, 1).unwrap();

        let schema = table_changes
            .schema()
            .project(&["id", "_commit_version"])
            .unwrap();
        let predicate = Arc::new(Expression::gt(column_expr!("id"), Scalar::from(10)));
        let scan = table_changes
            .into_scan_builder()
            .with_schema(schema)
            .with_predicate(predicate.clone())
            .build()
            .unwrap();
        assert_eq!(
            scan.all_fields,
            vec![
                ColumnType::Selected("id".to_string()),
                ColumnType::Selected("_commit_version".to_string()),
            ]
        );
        assert_eq!(
            scan.logical_schema,
            StructType::new([
                StructField::new("id", DataType::INTEGER, true),
                StructField::new("_commit_version", DataType::LONG, false),
            ])
            .into()
        );
        assert!(!scan.have_partition_cols);
        assert_eq!(scan.predicate, Some(predicate));
    }
}
