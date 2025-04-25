use std::collections::HashMap;

use itertools::Itertools;

use crate::expressions::Scalar;
use crate::scan::{parse_partition_value, ColumnType};
use crate::schema::{ColumnName, DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, Expression};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{
    ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    REMOVE_CHANGE_TYPE,
};

/// Returns a map from change data feed column name to an expression that generates the row data.
fn get_cdf_columns(scan_file: &CdfScanFile) -> DeltaResult<HashMap<&str, Expression>> {
    let timestamp = Scalar::timestamp_ntz_from_millis(scan_file.commit_timestamp)?;
    let version = scan_file.commit_version;
    let change_type: Expression = match scan_file.scan_type {
        CdfScanFileType::Cdc => Expression::column([CHANGE_TYPE_COL_NAME]),
        CdfScanFileType::Add => Expression::literal(ADD_CHANGE_TYPE),
        CdfScanFileType::Remove => Expression::literal(REMOVE_CHANGE_TYPE),
    };
    let expressions = [
        (CHANGE_TYPE_COL_NAME, change_type),
        (COMMIT_VERSION_COL_NAME, Expression::literal(version)),
        (COMMIT_TIMESTAMP_COL_NAME, timestamp.into()),
    ];
    Ok(expressions.into_iter().collect())
}

/// Generates the expression used to convert physical data from the `scan_file` path into logical
/// data matching the `logical_schema`
pub(crate) fn physical_to_logical_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    all_fields: &[ColumnType],
) -> DeltaResult<Expression> {
    let mut cdf_columns = get_cdf_columns(scan_file)?;
    let all_fields = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name();
                let value_expression =
                    parse_partition_value(scan_file.partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) => {
                // Remove to take ownership
                let generated_column = cdf_columns.remove(field_name.as_str());
                Ok(generated_column.unwrap_or_else(|| ColumnName::new([field_name]).into()))
            }
        })
        .try_collect()?;
    Ok(Expression::Struct(all_fields))
}

/// Gets the physical schema that will be used to read data in the `scan_file` path.
pub(crate) fn scan_file_physical_schema(
    scan_file: &CdfScanFile,
    physical_schema: &StructType,
) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING);
        let fields = physical_schema.fields().cloned().chain(Some(change_type));
        StructType::new(fields).into()
    } else {
        physical_schema.clone().into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::expressions::{column_expr, Expression as Expr, Scalar};
    use crate::scan::ColumnType;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::physical_to_logical::physical_to_logical_expr;
    use crate::table_changes::scan_file::{CdfScanFile, CdfScanFileType};
    use crate::table_changes::{
        ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
        REMOVE_CHANGE_TYPE,
    };

    #[test]
    fn verify_physical_to_logical_expression() {
        let test = |scan_type, expected_expr| {
            let scan_file = CdfScanFile {
                scan_type,
                path: "fake_path".to_string(),
                dv_info: Default::default(),
                remove_dv: None,
                partition_values: HashMap::from([("age".to_string(), "20".to_string())]),
                commit_version: 42,
                commit_timestamp: 1234,
            };
            let logical_schema = StructType::new([
                StructField::nullable("id", DataType::STRING),
                StructField::not_null("age", DataType::LONG),
                StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING),
                StructField::not_null(COMMIT_VERSION_COL_NAME, DataType::LONG),
                StructField::not_null(COMMIT_TIMESTAMP_COL_NAME, DataType::TIMESTAMP),
            ]);
            let all_fields = vec![
                ColumnType::Selected("id".to_string()),
                ColumnType::Partition(1),
                ColumnType::Selected(CHANGE_TYPE_COL_NAME.to_string()),
                ColumnType::Selected(COMMIT_VERSION_COL_NAME.to_string()),
                ColumnType::Selected(COMMIT_TIMESTAMP_COL_NAME.to_string()),
            ];
            let phys_to_logical_expr =
                physical_to_logical_expr(&scan_file, &logical_schema, &all_fields).unwrap();
            let expected_expr = Expr::struct_from([
                column_expr!("id"),
                Scalar::Long(20).into(),
                expected_expr,
                Expr::literal(42i64),
                Scalar::TimestampNtz(1234000).into(), // Microsecond is 1000x millisecond
            ]);

            assert_eq!(phys_to_logical_expr, expected_expr)
        };

        let cdc_change_type = Expr::column([CHANGE_TYPE_COL_NAME]);
        test(CdfScanFileType::Add, Expr::literal(ADD_CHANGE_TYPE));
        test(CdfScanFileType::Remove, Expr::literal(REMOVE_CHANGE_TYPE));
        test(CdfScanFileType::Cdc, cdc_change_type);
    }
}
