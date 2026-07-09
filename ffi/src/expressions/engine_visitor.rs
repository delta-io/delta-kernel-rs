//! Defines [`EngineExpressionVisitor`]. This is a visitor that can be used to convert the kernel's
//! [`Expression`] or [`Predicate`] to an engine's native expression format.
use std::ffi::c_void;

use delta_kernel::expressions::{
    ArrayData, BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp,
    ColumnName, Expression, ExpressionRef, ExpressionStructPatch, JunctionPredicate,
    JunctionPredicateOp, MapData, MapToStructExpression, OpaqueExpression, OpaqueExpressionOpRef,
    OpaquePredicate, OpaquePredicateOpRef, ParseJsonExpression, Predicate, Scalar, StructData,
    UnaryExpression, UnaryExpressionOp, UnaryPredicate, UnaryPredicateOp, VariadicExpression,
    VariadicExpressionOp,
};

use super::kernel_visitor::NullTypeTag;
use crate::expressions::{
    SharedExpression, SharedOpaqueExpressionOp, SharedOpaquePredicateOp, SharedPredicate,
};
use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice, SharedSchema};

type VisitLiteralFn<T> = extern "C" fn(data: *mut c_void, sibling_list_id: usize, value: T);
type VisitUnaryFn = extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitBinaryFn = extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitVariadicFn =
    extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitJunctionFn =
    extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitParseJsonFn = extern "C" fn(
    data: *mut c_void,
    sibling_list_id: usize,
    child_list_id: usize,
    output_schema: Handle<SharedSchema>,
);

/// The [`EngineExpressionVisitor`] defines a visitor system to allow engines to build their own
/// representation of a kernel expression or predicate.
///
/// The model is list based. When the kernel needs a list, it will ask engine to allocate one of a
/// particular size. Once allocated the engine returns an `id`, which can be any integer identifier
/// ([`usize`]) the engine wants, and will be passed back to the engine to identify the list in the
/// future.
///
/// Every expression the kernel visits belongs to some list of "sibling" elements. The schema
/// itself is a list of schema elements, and every complex type (struct expression, array, junction,
/// etc) contains a list of "child" elements.
///  1. Before visiting any complex expression type, the kernel asks the engine to allocate a list
///     to hold its children
///  2. When visiting any expression element, the kernel passes its parent's "child list" as the
///     "sibling list" the element should be appended to:
///      - For a struct literal, first visit each struct field and visit each value
///      - For a struct expression, visit each sub expression.
///      - For an array literal, visit each of the elements.
///      - For a junction `and` or `or` expression, visit each sub-expression.
///      - For a binary operator expression, visit the left and right operands.
///      - For a unary `is null` or `not` expression, visit the sub-expression.
///  3. When visiting a complex expression, the kernel also passes the "child list" containing that
///     element's (already-visited) children.
///  4. The [`visit_expression`] method returns the id of the list of top-level columns
///
/// WARNING: The visitor MUST NOT retain internal references to string slices or binary data passed
/// to visitor methods
/// TODO: Visit type information in struct field. This will likely involve using the schema visitor.
/// Note that struct literals are currently in flux, and may change significantly. Here is the
/// relevant issue: <https://github.com/delta-io/delta-kernel-rs/issues/412>
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
    /// Visit a 32bit integer `date` representing days since UNIX epoch 1970-01-01.  The `date`
    /// belongs to the list identified by `sibling_list_id`.
    pub visit_literal_date: VisitLiteralFn<i32>,
    /// Visit an `interval year to month` literal as a signed month count.
    pub visit_literal_interval_year_month: VisitLiteralFn<i32>,
    /// Visit an `interval day to second` literal as a signed microsecond count.
    pub visit_literal_interval_day_time: VisitLiteralFn<i64>,
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
    /// Visits a typed null value belonging to the list identified by `sibling_list_id`.
    ///
    /// The `type_tag` identifies the data type using the `NullTypeTag` encoding. For decimal
    /// nulls (`type_tag == 12`), `precision` and `scale` carry the decimal type parameters;
    /// for all other types, they are zero. Non-primitive types (struct, array, map, variant)
    /// use `type_tag == 255`.
    pub visit_literal_null: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        type_tag: u8,
        precision: u8,
        scale: u8,
    ),
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
    /// Visits the `ToJson` unary operator belonging to the list identified by `sibling_list_id`.
    /// The sub-expression will be in a _one_ item list identified by `child_list_id`
    pub visit_to_json: VisitUnaryFn,
    /// Visits the `ParseJson` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression (JSON string) will be in a _one_ item list identified by
    /// `child_list_id`. The `output_schema` handle specifies the schema to parse the JSON
    /// into.
    pub visit_parse_json: VisitParseJsonFn,
    /// Visits the `MapToStruct` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression (map column) will be in a _one_ item list identified by `child_list_id`.
    /// The output struct schema is determined by the evaluator's result type.
    pub visit_map_to_struct: VisitUnaryFn,
    /// Visits the `LessThan` binary operator belonging to the list identified by
    /// `sibling_list_id`. The operands will be in a _two_ item list identified by
    /// `child_list_id`
    pub visit_lt: VisitBinaryFn,
    /// Visits the `GreaterThan` binary operator belonging to the list identified by
    /// `sibling_list_id`. The operands will be in a _two_ item list identified by
    /// `child_list_id`
    pub visit_gt: VisitBinaryFn,
    /// Visits the `Equal` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_eq: VisitBinaryFn,
    /// Visits the `Distinct` binary operator belonging to the list identified by
    /// `sibling_list_id`. The operands will be in a _two_ item list identified by
    /// `child_list_id`
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
    /// Visits the `Multiply` binary operator belonging to the list identified by
    /// `sibling_list_id`. The operands will be in a _two_ item list identified by
    /// `child_list_id`
    pub visit_multiply: VisitBinaryFn,
    /// Visits the `Divide` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_divide: VisitBinaryFn,
    /// Visits the `Coalesce` variadic operator belonging to the list identified by
    /// `sibling_list_id`. The operands will be in a list identified by `child_list_id`
    pub visit_coalesce: VisitVariadicFn,
    /// Visits the `Array` variadic constructor belonging to the list identified by
    /// `sibling_list_id`. The element expressions will be in a list identified by
    /// `child_list_id`.
    pub visit_array: VisitVariadicFn,
    /// Visits the `column` belonging to the list identified by `sibling_list_id`.
    pub visit_column:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
    /// Visits a `Struct` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions (fields) of the struct are in a list identified by `child_list_id`
    pub visit_struct_expr:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
    /// Visits a `StructPatch` expression belonging to the list identified by `sibling_list_id`.
    /// The `input_path_list_id` is a zero-or-one item list containing the patch's input path as a
    /// column reference. The `prepended_field_list_id` and `appended_field_list_id` identify
    /// expression lists to emit before and after the named input fields. The
    /// `field_patch_list_id` identifies the list of named field patches to apply. See also
    /// [`Self::visit_field_patch`].
    pub visit_struct_patch_expr: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        input_path_list_id: usize,
        prepended_field_list_id: usize,
        field_patch_list_id: usize,
        appended_field_list_id: usize,
    ),
    /// Visits one named field patch of a `StructPatch` expression that owns the list identified by
    /// `sibling_list_id`.
    ///
    /// The `insertion_expr_list_id` identifies expressions to emit after this field's output
    /// position. If `keep_input` is true, the original input field is emitted before these
    /// insertions. If `keep_input` is false, the original input field is omitted and the first
    /// insertion, if present, occupies the input field's output position. The `optional` flag
    /// indicates that the patch is silently ignored when the input field does not exist.
    pub visit_field_patch: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        field_name: KernelStringSlice,
        insertion_expr_list_id: usize,
        keep_input: bool,
        optional: bool,
    ),
    /// Visits the operator (`op`) and children (`child_list_id`) of an opaque expression belonging
    /// to the list identified by `sibling_list_id`.
    pub visit_opaque_expr: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        op: Handle<SharedOpaqueExpressionOp>,
        child_list_id: usize,
    ),
    /// Visits the operator (`op`) and children (`child_list_id`) of an opaque predicate belonging
    /// to the list identified by `sibling_list_id`.
    pub visit_opaque_pred: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        op: Handle<SharedOpaquePredicateOp>,
        child_list_id: usize,
    ),
    /// Visits the name of an `Expression::Unknown` or `Predicate::Unknown` belonging to the
    /// list identified by `sibling_list_id`.
    pub visit_unknown:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
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

fn visit_expression_column(
    visitor: &mut EngineExpressionVisitor,
    name: &ColumnName,
    sibling_list_id: usize,
) {
    let name = name.to_string();
    let name = kernel_string_slice!(name);
    call!(visitor, visit_column, sibling_list_id, name);
}

fn visit_expression_struct(
    visitor: &mut EngineExpressionVisitor,
    exprs: &[ExpressionRef],
    sibling_list_id: usize,
) {
    let child_list_id = visit_expression_list(visitor, exprs);
    call!(visitor, visit_struct_expr, sibling_list_id, child_list_id)
}

fn visit_expression_list(visitor: &mut EngineExpressionVisitor, exprs: &[ExpressionRef]) -> usize {
    let child_list_id = call!(visitor, make_field_list, exprs.len());
    for expr in exprs {
        visit_expression_impl(visitor, expr, child_list_id);
    }
    child_list_id
}

fn visit_expression_struct_patch(
    visitor: &mut EngineExpressionVisitor,
    patch: &ExpressionStructPatch,
    sibling_list_id: usize,
) {
    // Treat the input path like a zero-or-one column expression list.
    let path_len = usize::from(patch.input_path.is_some());
    let path_list_id = call!(visitor, make_field_list, path_len);
    if let Some(ref column_name) = patch.input_path {
        visit_expression_column(visitor, column_name, path_list_id);
    };

    let prepended_field_list_id = visit_expression_list(visitor, &patch.prepended_fields);
    let appended_field_list_id = visit_expression_list(visitor, &patch.appended_fields);

    // Process each named field patch in turn. Field patch order is not semantically meaningful;
    // engines should apply field patches according to input schema order.
    let field_patch_list_id = call!(visitor, make_field_list, patch.field_patches.len());
    for (field_name, field_patch) in &patch.field_patches {
        let insertion_expr_list_id = visit_expression_list(visitor, &field_patch.insertions);
        call!(
            visitor,
            visit_field_patch,
            field_patch_list_id,
            kernel_string_slice!(field_name),
            insertion_expr_list_id,
            field_patch.keep_input,
            field_patch.optional
        );
    }

    // Attach the field patches to the parent struct patch.
    call!(
        visitor,
        visit_struct_patch_expr,
        sibling_list_id,
        path_list_id,
        prepended_field_list_id,
        field_patch_list_id,
        appended_field_list_id
    );
}

fn visit_expression_opaque(
    visitor: &mut EngineExpressionVisitor,
    op: &OpaqueExpressionOpRef,
    exprs: &[Expression],
    sibling_list_id: usize,
) {
    let child_list_id = call!(visitor, make_field_list, exprs.len());
    for expr in exprs {
        visit_expression_impl(visitor, expr, child_list_id);
    }
    let op = Handle::from(op.clone());
    call!(
        visitor,
        visit_opaque_expr,
        sibling_list_id,
        op,
        child_list_id
    );
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

fn visit_predicate_opaque(
    visitor: &mut EngineExpressionVisitor,
    op: &OpaquePredicateOpRef,
    exprs: &[Expression],
    sibling_list_id: usize,
) {
    let child_list_id = call!(visitor, make_field_list, exprs.len());
    for expr in exprs {
        visit_expression_impl(visitor, expr, child_list_id);
    }
    let op = Handle::from(op.clone());
    call!(
        visitor,
        visit_opaque_pred,
        sibling_list_id,
        op,
        child_list_id
    );
}

fn visit_unknown(visitor: &mut EngineExpressionVisitor, sibling_list_id: usize, name: &str) {
    call!(
        visitor,
        visit_unknown,
        sibling_list_id,
        kernel_string_slice!(name)
    );
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
        Scalar::Null(data_type) => {
            let (tag, precision, scale) = NullTypeTag::from_data_type(data_type);
            call!(
                visitor,
                visit_literal_null,
                sibling_list_id,
                tag as u8,
                precision,
                scale
            )
        }
        Scalar::IntervalYearMonth(val) => {
            call!(
                visitor,
                visit_literal_interval_year_month,
                sibling_list_id,
                *val
            )
        }
        Scalar::IntervalDayTime(val) => {
            call!(
                visitor,
                visit_literal_interval_day_time,
                sibling_list_id,
                *val
            )
        }
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
        Expression::Column(name) => visit_expression_column(visitor, name, sibling_list_id),
        Expression::Struct(exprs, _) => visit_expression_struct(visitor, exprs, sibling_list_id),
        Expression::StructPatch(patch) => {
            visit_expression_struct_patch(visitor, patch, sibling_list_id)
        }
        Expression::Predicate(pred) => visit_predicate_impl(visitor, pred, sibling_list_id),
        Expression::Unary(UnaryExpression { op, expr }) => {
            let child_list_id = call!(visitor, make_field_list, 1);
            visit_expression_impl(visitor, expr, child_list_id);
            let visit_fn = match op {
                UnaryExpressionOp::ToJson => visitor.visit_to_json,
            };
            visit_fn(visitor.data, sibling_list_id, child_list_id);
        }
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
        Expression::Variadic(VariadicExpression { op, exprs }) => {
            let child_list_id = call!(visitor, make_field_list, exprs.len());
            for expr in exprs {
                visit_expression_impl(visitor, expr, child_list_id);
            }
            let visit_fn = match op {
                VariadicExpressionOp::Coalesce => visitor.visit_coalesce,
                VariadicExpressionOp::Array => visitor.visit_array,
            };
            visit_fn(visitor.data, sibling_list_id, child_list_id);
        }
        Expression::Opaque(OpaqueExpression { op, exprs }) => {
            visit_expression_opaque(visitor, op, exprs, sibling_list_id)
        }
        Expression::ParseJson(ParseJsonExpression {
            json_expr,
            output_schema,
        }) => {
            let child_list_id = call!(visitor, make_field_list, 1);
            visit_expression_impl(visitor, json_expr, child_list_id);
            let schema_handle = Handle::from(output_schema.clone());
            call!(
                visitor,
                visit_parse_json,
                sibling_list_id,
                child_list_id,
                schema_handle
            );
        }
        Expression::MapToStruct(MapToStructExpression { map_expr }) => {
            let child_list_id = call!(visitor, make_field_list, 1);
            visit_expression_impl(visitor, map_expr, child_list_id);
            call!(visitor, visit_map_to_struct, sibling_list_id, child_list_id);
        }
        // TODO(#2975): Add a dedicated visitor callback for cast expressions.
        Expression::Cast(cast) => visit_unknown(
            visitor,
            sibling_list_id,
            &format!("cast_to_{}", cast.target),
        ),
        Expression::Unknown(name) => visit_unknown(visitor, sibling_list_id, name),
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
        Predicate::Opaque(OpaquePredicate { op, exprs }) => {
            visit_predicate_opaque(visitor, op, exprs, sibling_list_id)
        }
        Predicate::Unknown(name) => visit_unknown(visitor, sibling_list_id, name),
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

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{Expression, Scalar};
    use rstest::rstest;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum LiteralEvent {
        IntervalYearMonth { sibling_list_id: usize, value: i32 },
        IntervalDayTime { sibling_list_id: usize, value: i64 },
    }

    #[derive(Default)]
    struct TestExpressionBuilder {
        next_list_id: usize,
        events: Vec<LiteralEvent>,
    }

    extern "C" fn make_field_list(data: *mut c_void, _reserve: usize) -> usize {
        let builder = unsafe { &mut *(data as *mut TestExpressionBuilder) };
        let list_id = builder.next_list_id;
        builder.next_list_id += 1;
        list_id
    }

    extern "C" fn visit_literal_interval_year_month(
        data: *mut c_void,
        sibling_list_id: usize,
        value: i32,
    ) {
        let builder = unsafe { &mut *(data as *mut TestExpressionBuilder) };
        builder.events.push(LiteralEvent::IntervalYearMonth {
            sibling_list_id,
            value,
        });
    }

    extern "C" fn visit_literal_interval_day_time(
        data: *mut c_void,
        sibling_list_id: usize,
        value: i64,
    ) {
        let builder = unsafe { &mut *(data as *mut TestExpressionBuilder) };
        builder.events.push(LiteralEvent::IntervalDayTime {
            sibling_list_id,
            value,
        });
    }

    macro_rules! ignored_literal_fn {
        ($fn_name:ident, $value_type:ty) => {
            extern "C" fn $fn_name(
                _data: *mut c_void,
                _sibling_list_id: usize,
                _value: $value_type,
            ) {
            }
        };
    }

    ignored_literal_fn!(ignore_i32, i32);
    ignored_literal_fn!(ignore_i64, i64);
    ignored_literal_fn!(ignore_i16, i16);
    ignored_literal_fn!(ignore_i8, i8);
    ignored_literal_fn!(ignore_f32, f32);
    ignored_literal_fn!(ignore_f64, f64);
    ignored_literal_fn!(ignore_bool, bool);
    ignored_literal_fn!(ignore_string_slice, KernelStringSlice);

    extern "C" fn ignore_binary(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _buffer: *const u8,
        _len: usize,
    ) {
    }

    extern "C" fn ignore_decimal(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _value_ms: i64,
        _value_ls: u64,
        _precision: u8,
        _scale: u8,
    ) {
    }

    extern "C" fn ignore_struct_literal(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _child_field_list_id: usize,
        _child_value_list_id: usize,
    ) {
    }

    extern "C" fn ignore_child_list(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _child_list_id: usize,
    ) {
    }

    extern "C" fn ignore_map_literal(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _key_list_id: usize,
        _value_list_id: usize,
    ) {
    }

    extern "C" fn ignore_null(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _type_tag: u8,
        _precision: u8,
        _scale: u8,
    ) {
    }

    extern "C" fn ignore_parse_json(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _child_list_id: usize,
        _output_schema: Handle<SharedSchema>,
    ) {
    }

    extern "C" fn ignore_column(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _name: KernelStringSlice,
    ) {
    }

    extern "C" fn ignore_struct_patch(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _input_path_list_id: usize,
        _prepended_field_list_id: usize,
        _field_patch_list_id: usize,
        _appended_field_list_id: usize,
    ) {
    }

    extern "C" fn ignore_field_patch(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _field_name: KernelStringSlice,
        _insertion_expr_list_id: usize,
        _keep_input: bool,
        _optional: bool,
    ) {
    }

    extern "C" fn ignore_opaque_expr(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _op: Handle<SharedOpaqueExpressionOp>,
        _child_list_id: usize,
    ) {
    }

    extern "C" fn ignore_opaque_pred(
        _data: *mut c_void,
        _sibling_list_id: usize,
        _op: Handle<SharedOpaquePredicateOp>,
        _child_list_id: usize,
    ) {
    }

    fn test_visitor(builder: &mut TestExpressionBuilder) -> EngineExpressionVisitor {
        EngineExpressionVisitor {
            data: builder as *mut _ as *mut c_void,
            make_field_list,
            visit_literal_int: ignore_i32,
            visit_literal_long: ignore_i64,
            visit_literal_short: ignore_i16,
            visit_literal_byte: ignore_i8,
            visit_literal_float: ignore_f32,
            visit_literal_double: ignore_f64,
            visit_literal_string: ignore_string_slice,
            visit_literal_bool: ignore_bool,
            visit_literal_timestamp: ignore_i64,
            visit_literal_timestamp_ntz: ignore_i64,
            visit_literal_date: ignore_i32,
            visit_literal_interval_year_month,
            visit_literal_interval_day_time,
            visit_literal_binary: ignore_binary,
            visit_literal_decimal: ignore_decimal,
            visit_literal_struct: ignore_struct_literal,
            visit_literal_array: ignore_child_list,
            visit_literal_map: ignore_map_literal,
            visit_literal_null: ignore_null,
            visit_and: ignore_child_list,
            visit_or: ignore_child_list,
            visit_not: ignore_child_list,
            visit_is_null: ignore_child_list,
            visit_to_json: ignore_child_list,
            visit_parse_json: ignore_parse_json,
            visit_map_to_struct: ignore_child_list,
            visit_lt: ignore_child_list,
            visit_gt: ignore_child_list,
            visit_eq: ignore_child_list,
            visit_distinct: ignore_child_list,
            visit_in: ignore_child_list,
            visit_add: ignore_child_list,
            visit_minus: ignore_child_list,
            visit_multiply: ignore_child_list,
            visit_divide: ignore_child_list,
            visit_coalesce: ignore_child_list,
            visit_array: ignore_child_list,
            visit_column: ignore_column,
            visit_struct_expr: ignore_child_list,
            visit_struct_patch_expr: ignore_struct_patch,
            visit_field_patch: ignore_field_patch,
            visit_opaque_expr: ignore_opaque_expr,
            visit_opaque_pred: ignore_opaque_pred,
            visit_unknown: ignore_column,
        }
    }

    #[rstest]
    #[case(
        Expression::literal(Scalar::IntervalYearMonth(26)),
        LiteralEvent::IntervalYearMonth { sibling_list_id: 0, value: 26 }
    )]
    #[case(
        Expression::literal(Scalar::IntervalDayTime(987_654)),
        LiteralEvent::IntervalDayTime { sibling_list_id: 0, value: 987_654 }
    )]
    fn visit_expression_uses_interval_literal_callbacks(
        #[case] expression: Expression,
        #[case] expected: LiteralEvent,
    ) {
        let mut builder = TestExpressionBuilder::default();
        let mut visitor = test_visitor(&mut builder);

        let top_level_id = visit_expression_internal(&expression, &mut visitor);

        assert_eq!(top_level_id, 0);
        assert_eq!(builder.events, vec![expected]);
    }
}
