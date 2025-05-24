//! Defines [`EngineExpressionVisitor`]. This is a visitor that can be used to convert the kernel's
//! [`Expression`] or [`Predicate`] to an engine's native expression format.
use std::ffi::c_void;

use delta_kernel::expressions::{
    ArrayData, BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp,
    Expression, JunctionPredicate, JunctionPredicateOp, MapData, Predicate, Scalar, StructData,
    UnaryPredicate, UnaryPredicateOp,
};

use crate::expressions::{SharedExpression, SharedPredicate};
use crate::{handle::Handle, kernel_string_slice, KernelStringSlice};

type VisitLiteralFn<T> = extern "C" fn(data: *mut c_void, sibling_list_id: usize, value: T);
type VisitUnaryFn = extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitBinaryFn = extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitJunctionFn =
    extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);

/// The [`EngineExpressionVisitor`] defines a visitor system to allow engines to build their own
/// representation of a kernel expression or predicate.
///
/// The model is list based. When the kernel needs a list, it will ask engine to allocate one of a
/// particular size. Once allocated the engine returns an `id`, which can be any integer identifier
/// ([`usize`]) the engine wants, and will be passed back to the engine to identify the list in the
/// future.
///
/// Every expression the kernel visits belongs to some list of "sibling" elements. The schema
/// itself is a list of schema elements, and every complex type (struct expression, array, junction, etc)
/// contains a list of "child" elements.
///  1. Before visiting any complex expression type, the kernel asks the engine to allocate a list to
///     hold its children
///  2. When visiting any expression element, the kernel passes its parent's "child list" as the
///     "sibling list" the element should be appended to:
///      - For a struct literal, first visit each struct field and visit each value
///      - For a struct expression, visit each sub expression.
///      - For an array literal, visit each of the elements.
///      - For a junction `and` or `or` expression, visit each sub-expression.
///      - For a binary operator expression, visit the left and right operands.
///      - For a unary `is null` or `not` expression, visit the sub-expression.
///  3. When visiting a complex expression, the kernel also passes the "child list" containing
///     that element's (already-visited) children.
///  4. The [`visit_expression`] method returns the id of the list of top-level columns
///
/// WARNING: The visitor MUST NOT retain internal references to string slices or binary data passed
/// to visitor methods
/// TODO: Visit type information in struct field and null. This will likely involve using the schema
/// visitor. Note that struct literals are currently in flux, and may change significantly. Here is
/// the relevant issue: <https://github.com/delta-io/delta-kernel-rs/issues/412>
#[repr(C)]
pub struct EngineExpressionVisitor {
    /// An opaque engine state pointer
    pub data: *mut c_void,
    /// Creates a new expression list, optionally reserving capacity up front
    pub make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,
    /// Visit a 32bit `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_int: VisitLiteralFn<i32>,
    /// Visit a 64bit `long`  belonging to the list identified by `sibling_list_id`.
    pub visit_literal_long: VisitLiteralFn<i64>,
    /// Visit a 16bit `short` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_short: VisitLiteralFn<i16>,
    /// Visit an 8bit `byte` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_byte: VisitLiteralFn<i8>,
    /// Visit a 32bit `float` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_float: VisitLiteralFn<f32>,
    /// Visit a 64bit `double` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_double: VisitLiteralFn<f64>,
    /// Visit a `string` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_string: VisitLiteralFn<KernelStringSlice>,
    /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
    pub visit_literal_bool: VisitLiteralFn<bool>,
    /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
    /// The timestamp is microsecond precision and adjusted to UTC.
    pub visit_literal_timestamp: VisitLiteralFn<i64>,
    /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
    /// The timestamp is microsecond precision with no timezone.
    pub visit_literal_timestamp_ntz: VisitLiteralFn<i64>,
    /// Visit a 32bit integer `date` representing days since UNIX epoch 1970-01-01.  The `date` belongs
    /// to the list identified by `sibling_list_id`.
    pub visit_literal_date: VisitLiteralFn<i32>,
    /// Visit binary data at the `buffer` with length `len` belonging to the list identified by
    /// `sibling_list_id`.
    pub visit_literal_binary:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, buffer: *const u8, len: usize),
    /// Visit a 128bit `decimal` value with the given precision and scale. The 128bit integer
    /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
    /// bits in `value_ls`. The `decimal` belongs to the list identified by `sibling_list_id`.
    pub visit_literal_decimal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        value_ms: i64,
        value_ls: u64,
        precision: u8,
        scale: u8,
    ),
    /// Visit a struct literal belonging to the list identified by `sibling_list_id`.
    /// The field names of the struct are in a list identified by `child_field_list_id`.
    /// The values of the struct are in a list identified by `child_value_list_id`.
    pub visit_literal_struct: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        child_field_list_id: usize,
        child_value_list_id: usize,
    ),
    /// Visit an array literal belonging to the list identified by `sibling_list_id`.
    /// The values of the array are in a list identified by `child_list_id`.
    pub visit_literal_array:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
    /// Visit a map literal belonging to the list identified by `sibling_list_id`.
    /// The keys of the map are in order in a list identified by `key_list_id`. The values of the
    /// map are in order in a list identified by `value_list_id`.
    pub visit_literal_map: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        key_list_id: usize,
        value_list_id: usize,
    ),
    /// Visits a null value belonging to the list identified by `sibling_list_id.
    pub visit_literal_null: extern "C" fn(data: *mut c_void, sibling_list_id: usize),
    /// Visits an `and` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the array are in a list identified by `child_list_id`
    pub visit_and: VisitJunctionFn,
    /// Visits an `or` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the array are in a list identified by `child_list_id`
    pub visit_or: VisitJunctionFn,
    /// Visits a `not` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression will be in a _one_ item list identified by `child_list_id`
    pub visit_not: VisitUnaryFn,
    /// Visits a `is_null` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression will be in a _one_ item list identified by `child_list_id`
    pub visit_is_null: VisitUnaryFn,
    /// Visits the `LessThan` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_lt: VisitBinaryFn,
    /// Visits the `GreaterThan` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_gt: VisitBinaryFn,
    /// Visits the `Equal` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_eq: VisitBinaryFn,
    /// Visits the `Distinct` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_distinct: VisitBinaryFn,
    /// Visits the `In` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_in: VisitBinaryFn,
    /// Visits the `Add` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_add: VisitBinaryFn,
    /// Visits the `Minus` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_minus: VisitBinaryFn,
    /// Visits the `Multiply` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_multiply: VisitBinaryFn,
    /// Visits the `Divide` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_divide: VisitBinaryFn,
    /// Visits the `column` belonging to the list identified by `sibling_list_id`.
    pub visit_column:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
    /// Visits a `StructExpression` belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the `StructExpression` are in a list identified by `child_list_id`
    pub visit_struct_expr:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
}

/// Visit the expression of the passed [`SharedExpression`] Handle using the provided `visitor`.
/// See the documentation of [`EngineExpressionVisitor`] for a description of how this visitor
/// works.
///
/// This method returns the id that the engine generated for the top level expression
///
/// # Safety
///
/// The caller must pass a valid SharedExpression Handle and expression visitor
#[no_mangle]
pub unsafe extern "C" fn visit_expression(
    expression: &Handle<SharedExpression>,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    visit_expression_internal(expression.as_ref(), visitor)
}

/// Visit the expression of the passed [`Expression`] pointer using the provided `visitor`.  See the
/// documentation of [`EngineExpressionVisitor`] for a description of how this visitor works.
///
/// This method returns the id that the engine generated for the top level expression
///
/// # Safety
///
/// The caller must pass a valid Expression pointer and expression visitor
#[no_mangle]
pub unsafe extern "C" fn visit_expression_ref(
    expression: &Expression,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    visit_expression_internal(expression, visitor)
}

/// Visit the predicate of the passed [`SharedPredicate`] Handle using the provided `visitor`.
/// See the documentation of [`EngineExpressionVisitor`] for a description of how this visitor
/// works.
///
/// This method returns the id that the engine generated for the top level predicate
///
/// # Safety
///
/// The caller must pass a valid SharedPredicate Handle and expression visitor
#[no_mangle]
pub unsafe extern "C" fn visit_predicate(
    predicate: &Handle<SharedPredicate>,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    visit_predicate_internal(predicate.as_ref(), visitor)
}

/// Visit the predicate of the passed [`Predicate`] pointer using the provided `visitor`.  See the
/// documentation of [`EngineExpressionVisitor`] for a description of how this visitor works.
///
/// This method returns the id that the engine generated for the top level predicate
///
/// # Safety
///
/// The caller must pass a valid Predicate pointer and expression visitor
#[no_mangle]
pub unsafe extern "C" fn visit_predicate_ref(
    predicate: &Predicate,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    visit_predicate_internal(predicate, visitor)
}

macro_rules! call {
    ( $visitor:ident, $visitor_fn:ident $(, $extra_args:expr) *) => {
        ($visitor.$visitor_fn)($visitor.data $(, $extra_args) *)
    };
}

fn visit_expression_array(
    visitor: &mut EngineExpressionVisitor,
    array: &ArrayData,
    sibling_list_id: usize,
) {
    #[allow(deprecated)]
    let elements = array.array_elements();
    let child_list_id = call!(visitor, make_field_list, elements.len());
    for scalar in elements {
        visit_expression_scalar(visitor, scalar, child_list_id);
    }
    call!(visitor, visit_literal_array, sibling_list_id, child_list_id);
}

fn visit_expression_map(
    visitor: &mut EngineExpressionVisitor,
    map_data: &MapData,
    sibling_list_id: usize,
) {
    let pairs = map_data.pairs();
    let key_list_id = call!(visitor, make_field_list, pairs.len());
    let value_list_id = call!(visitor, make_field_list, pairs.len());
    for (key, val) in pairs {
        visit_expression_scalar(visitor, key, key_list_id);
        visit_expression_scalar(visitor, val, value_list_id);
    }
    call!(
        visitor,
        visit_literal_map,
        sibling_list_id,
        key_list_id,
        value_list_id
    );
}

fn visit_expression_struct_literal(
    visitor: &mut EngineExpressionVisitor,
    struct_data: &StructData,
    sibling_list_id: usize,
) {
    let child_value_list_id = call!(visitor, make_field_list, struct_data.fields().len());
    let child_field_list_id = call!(visitor, make_field_list, struct_data.fields().len());
    for (field, value) in struct_data.fields().iter().zip(struct_data.values()) {
        let field_name = field.name();
        call!(
            visitor,
            visit_literal_string,
            child_field_list_id,
            kernel_string_slice!(field_name)
        );
        visit_expression_scalar(visitor, value, child_value_list_id);
    }
    call!(
        visitor,
        visit_literal_struct,
        sibling_list_id,
        child_field_list_id,
        child_value_list_id
    )
}

fn visit_expression_struct(
    visitor: &mut EngineExpressionVisitor,
    exprs: &[Expression],
    sibling_list_id: usize,
) {
    let child_list_id = call!(visitor, make_field_list, exprs.len());
    for expr in exprs {
        visit_expression_impl(visitor, expr, child_list_id);
    }
    call!(visitor, visit_struct_expr, sibling_list_id, child_list_id)
}

fn visit_predicate_junction(
    visitor: &mut EngineExpressionVisitor,
    op: &JunctionPredicateOp,
    preds: &[Predicate],
    sibling_list_id: usize,
) {
    let child_list_id = call!(visitor, make_field_list, preds.len());
    for pred in preds {
        visit_predicate_impl(visitor, pred, child_list_id);
    }

    let visit_fn = match op {
        JunctionPredicateOp::And => &visitor.visit_and,
        JunctionPredicateOp::Or => &visitor.visit_or,
    };
    visit_fn(visitor.data, sibling_list_id, child_list_id);
}

fn visit_expression_scalar(
    visitor: &mut EngineExpressionVisitor,
    scalar: &Scalar,
    sibling_list_id: usize,
) {
    match scalar {
        Scalar::Integer(val) => call!(visitor, visit_literal_int, sibling_list_id, *val),
        Scalar::Long(val) => call!(visitor, visit_literal_long, sibling_list_id, *val),
        Scalar::Short(val) => call!(visitor, visit_literal_short, sibling_list_id, *val),
        Scalar::Byte(val) => call!(visitor, visit_literal_byte, sibling_list_id, *val),
        Scalar::Float(val) => call!(visitor, visit_literal_float, sibling_list_id, *val),
        Scalar::Double(val) => {
            call!(visitor, visit_literal_double, sibling_list_id, *val)
        }
        Scalar::String(val) => {
            let val = kernel_string_slice!(val);
            call!(visitor, visit_literal_string, sibling_list_id, val)
        }
        Scalar::Boolean(val) => call!(visitor, visit_literal_bool, sibling_list_id, *val),
        Scalar::Timestamp(val) => {
            call!(visitor, visit_literal_timestamp, sibling_list_id, *val)
        }
        Scalar::TimestampNtz(val) => {
            call!(visitor, visit_literal_timestamp_ntz, sibling_list_id, *val)
        }
        Scalar::Date(val) => call!(visitor, visit_literal_date, sibling_list_id, *val),
        Scalar::Binary(buf) => call!(
            visitor,
            visit_literal_binary,
            sibling_list_id,
            buf.as_ptr(),
            buf.len()
        ),
        Scalar::Decimal(v) => {
            call!(
                visitor,
                visit_literal_decimal,
                sibling_list_id,
                (v.bits() >> 64) as i64,
                v.bits() as u64,
                v.precision(),
                v.scale()
            )
        }
        Scalar::Null(_) => call!(visitor, visit_literal_null, sibling_list_id),
        Scalar::Struct(struct_data) => {
            visit_expression_struct_literal(visitor, struct_data, sibling_list_id)
        }
        Scalar::Array(array) => visit_expression_array(visitor, array, sibling_list_id),
        Scalar::Map(map_data) => visit_expression_map(visitor, map_data, sibling_list_id),
    }
}

fn visit_expression_impl(
    visitor: &mut EngineExpressionVisitor,
    expression: &Expression,
    sibling_list_id: usize,
) {
    match expression {
        Expression::Literal(scalar) => visit_expression_scalar(visitor, scalar, sibling_list_id),
        Expression::Column(name) => {
            let name = name.to_string();
            let name = kernel_string_slice!(name);
            call!(visitor, visit_column, sibling_list_id, name);
        }
        Expression::Struct(exprs) => visit_expression_struct(visitor, exprs, sibling_list_id),
        Expression::Predicate(pred) => visit_predicate_impl(visitor, pred, sibling_list_id),
        Expression::Binary(BinaryExpression { op, left, right }) => {
            let child_list_id = call!(visitor, make_field_list, 2);
            visit_expression_impl(visitor, left, child_list_id);
            visit_expression_impl(visitor, right, child_list_id);
            let visit_fn = match op {
                BinaryExpressionOp::Plus => visitor.visit_add,
                BinaryExpressionOp::Minus => visitor.visit_minus,
                BinaryExpressionOp::Multiply => visitor.visit_multiply,
                BinaryExpressionOp::Divide => visitor.visit_divide,
            };
            visit_fn(visitor.data, sibling_list_id, child_list_id);
        }
    }
}

fn visit_predicate_impl(
    visitor: &mut EngineExpressionVisitor,
    predicate: &Predicate,
    sibling_list_id: usize,
) {
    match predicate {
        Predicate::BooleanExpression(expr) => visit_expression_impl(visitor, expr, sibling_list_id),
        Predicate::Not(pred) => {
            let child_list_id = call!(visitor, make_field_list, 1);
            visit_predicate_impl(visitor, pred, child_list_id);
            call!(visitor, visit_not, sibling_list_id, child_list_id);
        }
        Predicate::Unary(UnaryPredicate { op, expr }) => {
            let child_list_id = call!(visitor, make_field_list, 1);
            visit_expression_impl(visitor, expr, child_list_id);
            let visit_fn = match op {
                UnaryPredicateOp::IsNull => visitor.visit_is_null,
            };
            visit_fn(visitor.data, sibling_list_id, child_list_id);
        }
        Predicate::Binary(BinaryPredicate { op, left, right }) => {
            let child_list_id = call!(visitor, make_field_list, 2);
            visit_expression_impl(visitor, left, child_list_id);
            visit_expression_impl(visitor, right, child_list_id);
            let visit_fn = match op {
                BinaryPredicateOp::LessThan => visitor.visit_lt,
                BinaryPredicateOp::GreaterThan => visitor.visit_gt,
                BinaryPredicateOp::Equal => visitor.visit_eq,
                BinaryPredicateOp::Distinct => visitor.visit_distinct,
                BinaryPredicateOp::In => visitor.visit_in,
            };
            visit_fn(visitor.data, sibling_list_id, child_list_id);
        }
        Predicate::Junction(JunctionPredicate { op, preds }) => {
            visit_predicate_junction(visitor, op, preds, sibling_list_id)
        }
    }
}

fn visit_expression_internal(
    expression: &Expression,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    let top_level = call!(visitor, make_field_list, 1);
    visit_expression_impl(visitor, expression, top_level);
    top_level
}

fn visit_predicate_internal(predicate: &Predicate, visitor: &mut EngineExpressionVisitor) -> usize {
    let top_level = call!(visitor, make_field_list, 1);
    visit_predicate_impl(visitor, predicate, top_level);
    top_level
}
