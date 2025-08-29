use std::collections::HashMap;
use std::sync::Arc;

use crate::expressions::Scalar;
use crate::scan::{log_replay::create_transform_expr, ColumnType, Scan};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, Expression, ExpressionRef};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{
    ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    REMOVE_CHANGE_TYPE,
};

/// Creates a transform expression that converts physical CDF file data to logical CDF output.
///
/// This combines table data (using regular scan transform logic) with computed CDF metadata
/// to produce the complete logical Change Data Feed row structure.
pub(crate) fn get_cdf_transform_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    all_fields: &[ColumnType],
) -> DeltaResult<ExpressionRef> {
    // Separate table fields from CDF metadata fields
    // Table fields (regular columns + partitions) will be read from the physical file
    // CDF metadata fields will be computed based on scan context
    let table_fields: Vec<_> = all_fields
        .iter()
        .filter_map(|field| match field {
            ColumnType::Selected(col_name) => {
                if col_name != CHANGE_TYPE_COL_NAME
                    && col_name != COMMIT_VERSION_COL_NAME
                    && col_name != COMMIT_TIMESTAMP_COL_NAME
                {
                    Some(ColumnType::Selected(col_name.clone()))
                } else {
                    None
                }
            }
            ColumnType::Partition(idx) => Some(ColumnType::Partition(*idx)),
        })
        .collect();

    // Create transform expression for table data (reuses regular scan logic)
    let table_transform_spec = Scan::get_transform_spec(&table_fields);
    let table_expr = create_transform_expr(
        &table_transform_spec,
        &scan_file.partition_values,
        logical_schema,
    )?;

    // Append CDF metadata expressions to complete the logical schema
    let cdf_columns = get_cdf_columns(scan_file)?;
    let table_exprs = if let Expression::Struct(exprs) = table_expr.as_ref() {
        exprs.clone()
    } else {
        return Err(Error::InternalError(
            "Expected struct expression from table transform".to_string(),
        ));
    };

    let mut all_exprs = table_exprs;

    // Add CDF metadata expressions in the same order as they appear in the logical schema
    for field in all_fields {
        if let ColumnType::Selected(col_name) = field {
            if let Some(cdf_expr) = cdf_columns.get(col_name.as_str()) {
                all_exprs.push(Arc::new(cdf_expr.clone()));
            }
        }
    }

    Ok(Arc::new(Expression::Struct(all_exprs)))
}

/// Returns a map from change data feed column name to an expression that generates the row data.
fn get_cdf_columns(scan_file: &CdfScanFile) -> DeltaResult<HashMap<&str, Expression>> {
    let timestamp = Scalar::timestamp_from_millis(scan_file.commit_timestamp)?;
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
    use crate::table_changes::physical_to_logical::get_cdf_transform_expr;
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
                get_cdf_transform_expr(&scan_file, &logical_schema, &all_fields)
                    .unwrap()
                    .as_ref()
                    .clone();
            let expected_expr = Expr::struct_from([
                column_expr!("id"),
                Scalar::Long(20).into(),
                expected_expr,
                Expr::literal(42i64),
                Scalar::Timestamp(1234000).into(), // Microsecond is 1000x millisecond
            ]);

            assert_eq!(phys_to_logical_expr, expected_expr)
        };

        let cdc_change_type = Expr::column([CHANGE_TYPE_COL_NAME]);
        test(CdfScanFileType::Add, Expr::literal(ADD_CHANGE_TYPE));
        test(CdfScanFileType::Remove, Expr::literal(REMOVE_CHANGE_TYPE));
        test(CdfScanFileType::Cdc, cdc_change_type);
    }
}
