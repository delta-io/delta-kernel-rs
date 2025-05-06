pub(crate) use self::delta_to_df::*;
pub(crate) use self::df_to_delta::*;

mod delta_to_df;
mod df_to_delta;

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};
    use delta_kernel::expressions::{BinaryExpression, BinaryOperator, Expression};

    #[test]
    fn test_roundtrip_simple_and() {
        let df_expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_nested_and() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_mixed_and_or() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)).and(col("d").eq(lit(4))));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_unary() {
        let df_expr = !col("a").eq(lit(1));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_is_null() {
        let df_expr = col("a").is_null();
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_binary_ops() {
        let df_expr = col("a") + col("b") * col("c");
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_comparison_ops() {
        let df_expr = col("a").gt(col("b")).and(col("c").lt_eq(col("d")));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_unsupported_operators() {
        // Test that unsupported operators return appropriate errors
        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::Distinct,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());

        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::In,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());

        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::NotIn,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());
    }

    #[test]
    fn test_unsupported_struct() {
        let delta_expr = Expression::Struct(vec![
            Expression::Column("a".parse().unwrap()),
            Expression::Column("b".parse().unwrap()),
        ]);
        assert!(to_df_expr(&delta_expr).is_err());
    }
}
