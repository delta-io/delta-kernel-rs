#![allow(dead_code)]

use delta_kernel::expressions::{Expression, Predicate, Scalar, lit, null_lit};
use delta_kernel::schema::{DataType, StructField};

pub fn direct_calls() {
    let _ = StructField::new("a", DataType::INTEGER, true); // expect: kernel_named_nullability
    let _ = StructField::new("a", DataType::INTEGER, false); // expect: kernel_named_nullability
    let _ = Expression::literal(42); // expect: kernel_expression_literal
    let _ = lit(Scalar::Null(DataType::INTEGER)); // expect: kernel_null_literal
    let _ = Predicate::literal(true); // expect: kernel_predicate_constant
    let _ = Predicate::literal(false); // expect: kernel_predicate_constant
}

pub fn aliases() {
    use delta_kernel::expressions::{
        Expression as Expr, Predicate as Pred, Scalar as Value, lit as literal,
    };
    use delta_kernel::schema::StructField as Field;

    let _ = Field::new("a", DataType::INTEGER, true); // expect: kernel_named_nullability
    let _ = Expr::literal(42); // expect: kernel_expression_literal
    let _ = literal(Value::Null(DataType::INTEGER)); // expect: kernel_null_literal
    let _ = Pred::literal(true); // expect: kernel_predicate_constant
}

pub fn preferred_and_runtime(nullable: bool, value: bool) {
    let _ = StructField::new("a", DataType::INTEGER, nullable);
    let _ = StructField::nullable("a", DataType::INTEGER);
    let _ = StructField::not_null("a", DataType::INTEGER);
    let _ = lit(42);
    let _ = null_lit(DataType::INTEGER);
    let _ = Predicate::literal(value);
    let _ = Predicate::TRUE;
    let _ = Predicate::FALSE;
}

#[allow(kernel_expression_literal)]
pub fn intentional_constructor_test() {
    let _ = Expression::literal(42);
}

mod lookalikes {
    use delta_kernel::expressions::{Scalar as KernelScalar, lit as kernel_lit};
    use delta_kernel::schema::DataType;

    struct StructField;
    impl StructField {
        fn new(_: &str, _: DataType, _: bool) -> Self {
            Self
        }
    }

    struct Expression;
    impl Expression {
        fn literal(_: i32) -> Self {
            Self
        }
    }

    struct Predicate;
    impl Predicate {
        fn literal(_: bool) -> Self {
            Self
        }
    }

    enum Scalar {
        Null(DataType),
    }
    impl From<Scalar> for KernelScalar {
        fn from(value: Scalar) -> Self {
            match value {
                Scalar::Null(data_type) => Self::Null(data_type),
            }
        }
    }

    fn lit(_: KernelScalar) {}

    fn calls() {
        let _ = StructField::new("a", DataType::INTEGER, true);
        let _ = Expression::literal(42);
        let _ = Predicate::literal(true);
        lit(KernelScalar::Null(DataType::INTEGER));
        let _ = kernel_lit(Scalar::Null(DataType::INTEGER));
    }
}

macro_rules! generated {
    () => {
        let _ = Expression::literal(42);
    };
}

pub fn generated_code() {
    generated!();
}
