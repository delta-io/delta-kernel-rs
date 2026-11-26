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

mod parameters {
    use delta_kernel::schema::{ArrayType, MapType};
    use itertools::Itertools;

    use super::*;

    fn vec_sum(values: Vec<i32>) -> i32 {
        // expect: kernel_vec_parameter_into_iterator
        values.into_iter().map(|value| value + 1).sum()
    }

    type Items<T> = std::vec::Vec<T>;
    fn vec_alias(values: Items<i32>) -> Vec<i32> {
        // expect: kernel_vec_parameter_into_iterator
        IntoIterator::into_iter(values)
            .filter(|value| *value > 0)
            .collect()
    }

    fn vec_for(values: Vec<i32>) -> i32 {
        // expect: kernel_vec_parameter_into_iterator
        let mut sum = 0;
        for value in values {
            sum += value;
        }
        sum
    }

    fn vec_explicit_for(values: Vec<i32>) -> i32 {
        // expect: kernel_vec_parameter_into_iterator
        let mut sum = 0;
        for value in values.into_iter().map(|value| value + 1) {
            sum += value;
        }
        sum
    }

    fn vec_join(values: Vec<String>) -> String {
        // expect: kernel_vec_parameter_into_iterator
        values
            .into_iter()
            .map(|value| value.to_uppercase())
            .join(",")
    }

    fn data_type_field(data_type: DataType) -> StructField {
        // expect: kernel_data_type_parameter_into
        StructField::nullable("a", data_type)
    }

    type Kind = delta_kernel::schema::DataType;
    fn data_type_alias(data_type: Kind) -> Expression {
        // expect: kernel_data_type_parameter_into
        null_lit(data_type)
    }

    fn data_type_array(data_type: DataType) -> ArrayType {
        // expect: kernel_data_type_parameter_into
        ArrayType::new(data_type, true)
    }

    fn data_type_map(data_type: DataType) -> MapType {
        // expect: kernel_data_type_parameter_into
        MapType::new(DataType::STRING, data_type, true)
    }

    fn data_type_scalar(data_type: DataType) -> Scalar {
        // expect: kernel_data_type_parameter_into
        Scalar::null(data_type)
    }

    struct Builder;
    impl Builder {
        fn consume(&self, values: Vec<i32>) -> usize {
            values.into_iter().count()
        }

        fn field(&self, data_type: DataType) -> StructField {
            // expect: kernel_data_type_parameter_into
            StructField::not_null("a", data_type)
        }
    }

    fn vec_borrowed(values: Vec<i32>) -> i32 {
        values.iter().sum()
    }

    fn vec_multiple_uses(values: Vec<i32>) -> usize {
        let length = values.len();
        length + values.into_iter().count()
    }

    fn vec_slice(values: Vec<i32>) -> i32 {
        values[0]
    }

    fn vec_reverse(values: Vec<i32>) -> i32 {
        values.into_iter().rev().sum()
    }

    fn vec_iterator_length(values: Vec<i32>) -> usize {
        values.into_iter().len()
    }

    fn filtered_count(values: Vec<i32>) -> usize {
        // expect: kernel_vec_parameter_into_iterator
        values.into_iter().filter(|value| *value > 0).count()
    }

    fn plain_count(values: Vec<i32>) -> usize {
        values.into_iter().count()
    }

    fn mapped_count(values: Vec<i32>) -> usize {
        values.into_iter().map(|value| value + 1).count()
    }

    fn filtered_then_mapped_count(values: Vec<i32>) -> usize {
        values
            .into_iter()
            .filter(|value| *value > 0)
            .map(|value| value + 1)
            .count()
    }

    fn last_value(values: Vec<i32>) -> Option<i32> {
        values.into_iter().last()
    }

    fn vec_iterator_escapes(values: Vec<i32>) -> impl Iterator<Item = i32> + Send {
        values.into_iter().map(|value| value + 1)
    }

    fn takes_vec_iterator(values: Vec<i32>) -> usize {
        fn requires_concrete(iterator: std::vec::IntoIter<i32>) -> usize {
            iterator.len()
        }
        requires_concrete(values.into_iter())
    }

    fn vec_capture(values: Vec<i32>) -> impl FnOnce() -> i32 + Send {
        move || values.into_iter().sum()
    }

    fn vec_reference(values: &Vec<i32>) -> i32 {
        values.into_iter().sum()
    }

    fn vec_generic(values: impl IntoIterator<Item = i32>) -> i32 {
        values.into_iter().sum()
    }

    fn data_type_generic(data_type: impl Into<DataType>) -> StructField {
        StructField::nullable("a", data_type)
    }

    fn data_type_clone(data_type: DataType) -> StructField {
        StructField::nullable("a", data_type.clone())
    }

    fn data_type_multiple(data_type: DataType) -> (StructField, StructField) {
        (
            StructField::nullable("a", data_type.clone()),
            StructField::nullable("b", data_type),
        )
    }

    fn data_type_borrow(data_type: DataType) -> String {
        format!("{data_type:?}")
    }

    fn data_type_match(data_type: DataType) -> bool {
        matches!(data_type, DataType::Primitive(_))
    }

    fn data_type_reference(data_type: &DataType) -> StructField {
        StructField::nullable("a", data_type.clone())
    }

    fn data_type_capture(data_type: DataType) -> impl FnOnce() -> StructField + Send {
        move || StructField::nullable("a", data_type)
    }

    fn data_type_concrete_sink(data_type: DataType) -> Expression {
        Expression::cast(lit(42), data_type)
    }

    macro_rules! count_values {
        ($values:expr) => {
            $values.into_iter().count()
        };
    }
    fn vec_macro(values: Vec<i32>) -> usize {
        count_values!(values)
    }

    macro_rules! field {
        ($data_type:expr) => {
            StructField::nullable("a", $data_type)
        };
    }
    fn data_type_macro(data_type: DataType) -> StructField {
        field!(data_type)
    }

    // These compile the proposed signatures against the same consuming operations.
    mod generalized {
        use super::*;

        fn sum(values: impl IntoIterator<Item = i32>) -> i32 {
            values.into_iter().map(|value| value + 1).sum()
        }

        fn filter(values: impl IntoIterator<Item = i32>) -> Vec<i32> {
            IntoIterator::into_iter(values)
                .filter(|value| *value > 0)
                .collect()
        }

        fn for_loop(values: impl IntoIterator<Item = i32>) -> i32 {
            let mut sum = 0;
            for value in values {
                sum += value;
            }
            sum
        }

        fn join(values: impl IntoIterator<Item = String>) -> String {
            values
                .into_iter()
                .map(|value| value.to_uppercase())
                .join(",")
        }

        fn array(data_type: impl Into<DataType>) -> ArrayType {
            ArrayType::new(data_type, true)
        }

        fn map(data_type: impl Into<DataType>) -> MapType {
            MapType::new(DataType::STRING, data_type, true)
        }

        fn scalar(data_type: impl Into<DataType>) -> Scalar {
            Scalar::null(data_type)
        }

        fn accepted_inputs() {
            let _ = sum([1, 2]);
            let _ = filter([1, 2]);
            let _ = for_loop([1, 2]);
            let _ = join(["a".to_owned()]);
            let _ = array(ArrayType::new(DataType::INTEGER, true));
            let _ = map(ArrayType::new(DataType::INTEGER, true));
            let _ = scalar(ArrayType::new(DataType::INTEGER, true));
        }
    }

    trait ObjectSafe {
        fn consume(&self, values: Vec<i32>) -> i32 {
            values.into_iter().sum()
        }
        fn field(&self, data_type: DataType) -> StructField;
    }

    impl ObjectSafe for Builder {
        fn field(&self, data_type: DataType) -> StructField {
            StructField::nullable("a", data_type)
        }
    }

    fn dyn_boundary(builder: &dyn ObjectSafe) {
        let _ = builder.consume(vec![1, 2]);
        let _ = builder.field(DataType::INTEGER);
    }

    #[allow(improper_ctypes_definitions)]
    extern "C" fn foreign_vec(values: Vec<i32>) -> i32 {
        values.into_iter().sum()
    }

    #[allow(improper_ctypes_definitions)]
    extern "C" fn foreign_data_type(data_type: DataType) -> StructField {
        StructField::nullable("a", data_type)
    }

    mod lookalikes {
        struct Vec<T>(std::vec::Vec<T>);
        impl<T> IntoIterator for Vec<T> {
            type Item = T;
            type IntoIter = std::vec::IntoIter<T>;
            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        }
        fn local_vec(values: Vec<i32>) -> i32 {
            values.into_iter().sum()
        }

        enum DataType {
            Integer,
        }
        struct StructField;
        impl StructField {
            fn nullable(_: &str, _: DataType) -> Self {
                Self
            }
        }
        fn local_data_type(data_type: DataType) -> StructField {
            StructField::nullable("a", data_type)
        }

        struct OtherField;
        impl OtherField {
            fn nullable(_: &str, _: impl Into<super::DataType>) -> Self {
                Self
            }
        }
        fn local_constructor(data_type: super::DataType) -> OtherField {
            OtherField::nullable("a", data_type)
        }
    }
}

mod guard_clauses {
    fn function_return(input: Option<i32>, output: &mut Vec<i32>) -> i32 {
        if let Some(value) = input {
            output.push(value);
            value + 1
        } else {
            return 0;
        }
    }

    fn function_statement(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else {
            return;
        };
    }

    fn success_also_returns(input: Option<i32>, output: &mut Vec<i32>) -> i32 {
        if let Some(value) = input {
            output.push(value);
            return value + 1;
        } else {
            return 0;
        }
    }

    fn for_tail(values: &[Option<i32>], output: &mut Vec<i32>, skipped: &mut usize) {
        for input in values {
            if let Some(value) = *input {
                // expect: kernel_prefer_guard_clause
                output.push(value);
                output.push(value + 1);
            } else {
                *skipped += 1;
            }
        }
    }

    fn while_tail(
        input: Option<i32>,
        remaining: &mut usize,
        output: &mut Vec<i32>,
        skipped: &mut usize,
    ) {
        while *remaining > 0 {
            *remaining -= 1;
            if let Some(value) = input {
                // expect: kernel_prefer_guard_clause
                output.push(value);
                output.push(value + 1);
            } else {
                *skipped += 1;
            }
        }
    }

    fn explicit_continue(values: &[Option<i32>], output: &mut Vec<i32>) {
        for input in values {
            if let Some(value) = *input {
                output.push(value);
                output.push(value + 1);
            } else {
                continue;
            }
        }
    }

    fn explicit_break(input: Option<i32>, output: &mut Vec<i32>) {
        loop {
            if let Some(value) = input {
                output.push(value);
                output.push(value + 1);
                return;
            } else {
                break;
            }
        }
    }

    fn returning_success_with_skip(input: Option<i32>, output: &mut Vec<i32>, skipped: &mut usize) {
        loop {
            if let Some(value) = input {
                output.push(value);
                return;
            } else {
                *skipped += 1;
            }
        }
    }

    fn nested_loop(values: &[Option<i32>], output: &mut Vec<i32>, skipped: &mut usize) {
        for _ in 0..2 {
            for input in values {
                if let Some(value) = *input {
                    // expect: kernel_prefer_guard_clause
                    output.push(value);
                    output.push(value + 1);
                } else {
                    *skipped += 1;
                }
            }
        }
    }

    fn retained_postlude(values: &[Option<i32>], output: &mut Vec<i32>, skipped: &mut usize) {
        for input in values {
            if let Some(value) = *input {
                output.push(value);
                output.push(value + 1);
            } else {
                *skipped += 1;
            }
            output.push(99);
        }
    }

    fn function_postlude(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else {
            return;
        }
        output.push(99);
    }

    fn no_exit_outside_loop(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else {
            output.push(0);
        }
    }

    fn initialized(input: Option<i32>) -> i32 {
        let result = if let Some(value) = input {
            let doubled = value * 2;
            doubled + 1
        } else {
            return 0;
        };
        result
    }

    fn nested_initializer(input: Option<i32>) -> i32 {
        let result = {
            if let Some(value) = input {
                let doubled = value * 2;
                doubled + 1
            } else {
                return 0;
            }
        };
        result
    }

    fn argument(input: Option<i32>) -> i32 {
        std::hint::black_box(if let Some(value) = input {
            let doubled = value * 2;
            doubled + 1
        } else {
            return 0;
        })
    }

    fn discarded_value(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            value + 1
        } else {
            return;
        };
    }

    fn nested_closure(input: Option<i32>, output: &mut Vec<i32>) {
        let mut work = || {
            if let Some(value) = input {
                output.push(value);
                output.push(value + 1);
            } else {
                return;
            }
        };
        work();
    }

    fn exit_in_closure(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else {
            (|| return)();
        }
    }

    fn exit_in_nested_loop(values: &[Option<i32>], output: &mut Vec<i32>) {
        for input in values {
            if let Some(value) = *input {
                output.push(value);
                output.push(value + 1);
            } else {
                for _ in 0..2 {
                    break;
                }
            }
        }
    }

    fn else_if(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else if let Some(value) = input {
            output.push(value);
            output.push(value + 1);
        } else {
            return;
        }
    }

    fn condition_guard(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(value) = input
            && value > 0
        {
            output.push(value);
            output.push(value + 1);
        } else {
            return;
        }
    }

    fn reference_binding(input: Option<i32>, output: &mut Vec<i32>) {
        if let Some(ref value) = input {
            output.push(*value);
            output.push(*value + 1);
        } else {
            return;
        }
    }

    fn already_guarded(input: Option<i32>, output: &mut Vec<i32>) {
        let Some(value) = input else { return };
        output.push(value);
        output.push(value + 1);
    }

    fn loop_already_guarded(values: &[Option<i32>], output: &mut Vec<i32>, skipped: &mut usize) {
        for input in values {
            let Some(value) = *input else {
                *skipped += 1;
                continue;
            };
            output.push(value);
            output.push(value + 1);
        }
    }

    macro_rules! generated_guard {
        ($input:expr, $output:expr) => {
            if let Some(value) = $input {
                $output.push(value);
                $output.push(value + 1);
            } else {
                return;
            }
        };
    }

    fn generated(input: Option<i32>, output: &mut Vec<i32>) {
        generated_guard!(input, output);
    }
}

mod nested_calls {
    use delta_kernel::expressions::StructData;

    use super::*;

    fn identity(value: usize) -> usize {
        value
    }
    fn combine_numbers(left: usize, right: usize) -> usize {
        left + right
    }
    fn combine_text(left: &str, right: &str) -> usize {
        left.len() + right.len()
    }
    fn owned_text(left: &str, right: &str) -> String {
        [left, right].concat()
    }
    fn borrowed_len(value: &String) -> usize {
        value.len()
    }
    fn try_text(left: &str, right: &str) -> Result<usize, ()> {
        Ok(combine_text(left, right))
    }
    fn is_positive(value: usize) -> bool {
        value > 0
    }
    fn boolean_identity(value: bool) -> bool {
        value
    }
    fn use_closure(value: impl FnOnce() -> usize) -> usize {
        value()
    }
    fn outer_with_a_name_long_enough_that_combining_a_nested_call_wraps_the_line(
        value: usize,
    ) -> usize {
        value
    }

    fn outermost_only() {
        // expect-next: kernel_multiline_nested_calls
        let _ = identity(identity(combine_text(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        )));
    }

    fn one_line_child() {
        // expect-next: kernel_multiline_nested_calls
        let _ = outer_with_a_name_long_enough_that_combining_a_nested_call_wraps_the_line(
            combine_numbers(123_456, 654_321),
        );
    }

    fn borrowed_argument() {
        // expect-next: kernel_multiline_nested_calls
        let _ = borrowed_len(&owned_text(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        ));
    }

    fn question_mark() -> Result<(), ()> {
        // expect-next: kernel_multiline_nested_calls
        let _ = identity(try_text(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        )?);
        Ok(())
    }

    struct Receiver;
    impl Receiver {
        fn accept(&self, value: usize) -> usize {
            value
        }
        fn next(self) -> Self {
            self
        }
    }

    fn method_argument() -> Result<(), ()> {
        // expect-next: kernel_multiline_nested_calls
        let _ = Receiver.accept(try_text(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        )?);
        Ok(())
    }

    fn aliases_and_unrelated_names() {
        use combine_text as unrelated_constructor;
        // expect-next: kernel_multiline_nested_calls
        let _ = identity(unrelated_constructor(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        ));
    }

    fn kernel_enum_constructor() -> delta_kernel::KernelResult<Scalar> {
        // expect-next: kernel_multiline_nested_calls
        let value = Scalar::Struct(StructData::try_new(
            std::sync::Arc::new(delta_kernel::schema::StructType::try_new([
                StructField::nullable(
                    "a_long_field_name_to_keep_the_constructor_wrapped",
                    DataType::INTEGER,
                ),
            ])?),
            vec![Scalar::Integer(42)],
        )?);
        Ok(value)
    }

    fn compact() {
        let _ = identity(identity(combine_numbers(1, 2)));
    }

    fn one_call_simple_arguments() {
        let _ = combine_text(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        );
    }

    fn fluent_receiver_chain() {
        let _ = Receiver.next().next().next();
    }

    fn short_argument_in_wrapped_receiver_chain() {
        let _ = Receiver
            .next()
            .next()
            .next()
            .next()
            .next()
            .next()
            .next()
            .next()
            .accept(combine_numbers(1, 2));
    }

    fn select_callback(_: &str, _: &str) -> fn(usize) -> usize {
        identity
    }

    fn wrapped_callee_with_compact_invocation() {
        let _ = select_callback(
            "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
            "second_argument_long_enough_to_keep_the_combined_call_multiline",
        )(combine_numbers(1, 2));
    }

    fn comment_forced_wrapping() {
        let _ = identity(
            // Keep this explanation next to the argument.
            combine_numbers(1, 2),
        );
    }

    fn conditional_argument(branch: bool) {
        let _ = identity(if branch {
            combine_text(
                "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
                "second_argument_long_enough_to_keep_the_combined_call_multiline",
            )
        } else {
            0
        });
    }

    fn matched_argument(branch: bool) {
        let _ = identity(match branch {
            true => identity(combine_text(
                "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
                "second_argument_long_enough_to_keep_the_combined_call_multiline",
            )),
            false => 0,
        });
    }

    fn short_circuit(branch: bool) {
        let _ = boolean_identity(
            branch
                && is_positive(combine_text(
                    "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
                    "second_argument_long_enough_to_keep_the_combined_call_multiline",
                )),
        );
    }

    fn closure_argument() {
        let _ = use_closure(|| {
            identity(combine_text(
                "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
                "second_argument_long_enough_to_keep_the_combined_call_multiline",
            ))
        });
    }

    const fn constant_function_with_a_name_long_enough_to_force_nested_wrapping(
        value: usize,
    ) -> usize {
        value
    }
    const fn constant_context() -> usize {
        constant_function_with_a_name_long_enough_to_force_nested_wrapping(
            constant_function_with_a_name_long_enough_to_force_nested_wrapping(1),
        )
    }
    static CONSTANT_CONTEXT: usize =
        constant_function_with_a_name_long_enough_to_force_nested_wrapping(
            constant_function_with_a_name_long_enough_to_force_nested_wrapping(1),
        );

    #[rustfmt::skip]
    fn formatting_skipped() -> usize {
        identity(
            combine_numbers(
                1,
                2,
            ),
        )
    }

    macro_rules! generated_nested {
        () => {
            identity(combine_text(
                "first_argument_long_enough_to_force_the_nested_expression_to_wrap",
                "second_argument_long_enough_to_keep_the_combined_call_multiline",
            ))
        };
    }
    fn generated() {
        let _ = generated_nested!();
    }
}
