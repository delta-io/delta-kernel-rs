#pragma once

#include "delta_kernel_ffi.h"
#include "expression.h"
#include <assert.h>

/**
 * This module converts an engine expression (ExpressionItemList) back into
 * a kernel expression (SharedExpression) by calling the appropriate visit_*
 * functions from KernelExpressionVisitorState.
 */

// Forward declarations
uintptr_t reconstruct_expression_item(
    KernelExpressionVisitorState* state,
    ExpressionItem item);

uintptr_t reconstruct_literal(
    KernelExpressionVisitorState* state,
    struct Literal* lit) {
  switch (lit->type) {
    case Integer:
      return visit_expression_literal_int(state, lit->value.integer_data);
    case Long:
      return visit_expression_literal_long(state, lit->value.long_data);
    case Short:
      return visit_expression_literal_short(state, lit->value.short_data);
    case Byte:
      return visit_expression_literal_byte(state, lit->value.byte_data);
    case Float:
      return visit_expression_literal_float(state, lit->value.float_data);
    case Double:
      return visit_expression_literal_double(state, lit->value.double_data);
    case String: {
      KernelStringSlice str_slice = {
        .ptr = lit->value.string_data,
        .len = strlen(lit->value.string_data)
      };
      ExternResultusize result = visit_expression_literal_string(
          state, str_slice, NULL);
      return result.ok;
    }
    case Boolean:
      return visit_expression_literal_bool(state, lit->value.boolean_data);
    case Timestamp:
      return visit_expression_literal_timestamp(state, lit->value.long_data);
    case TimestampNtz:
      return visit_expression_literal_timestamp_ntz(state,
          lit->value.long_data);
    case Date:
      return visit_expression_literal_date(state, lit->value.integer_data);
    case Binary: {
      return visit_expression_literal_binary(
          state, lit->value.binary.buf, lit->value.binary.len);
    }
    case Decimal: {
      struct Decimal* dec = &lit->value.decimal;
      return visit_expression_literal_decimal(
          state, dec->hi, dec->lo, dec->precision, dec->scale);
    }
    case Null:
      return visit_expression_literal_null(state);
    case Struct: {
      fprintf(stderr,
          "Warning: Struct literal reconstruction not fully implemented\n");
      return visit_expression_literal_int(state, 0);
    }
    case Array: {
      fprintf(stderr,
          "Warning: Array literal reconstruction not fully implemented\n");
      return visit_expression_literal_int(state, 0);
    }
    case Map: {
      fprintf(stderr,
          "Warning: Map literal reconstruction not fully implemented\n");
      return visit_expression_literal_int(state, 0);
    }
  }
  assert(0 && "Unknown literal type");
  return 0;
}

uintptr_t reconstruct_binop(
    KernelExpressionVisitorState* state,
    struct BinOp* binop) {
  assert(binop->exprs.len == 2);
  uintptr_t left = reconstruct_expression_item(state, binop->exprs.list[0]);
  uintptr_t right = reconstruct_expression_item(state, binop->exprs.list[1]);
  
  switch (binop->op) {
    case Add:
      return visit_expression_plus(state, left, right);
    case Minus:
      return visit_expression_minus(state, left, right);
    case Divide:
      return visit_expression_divide(state, left, right);
    case Multiply:
      return visit_expression_multiply(state, left, right);
    case LessThan:
      return visit_predicate_lt(state, left, right);
    case GreaterThan:
      return visit_predicate_gt(state, left, right);
    case Equal:
      return visit_predicate_eq(state, left, right);
    case Distinct:
      return visit_predicate_distinct(state, left, right);
    case In:
      return visit_predicate_in(state, left, right);
  }
  assert(0 && "Unknown binary op");
  return 0;
}

// Helper to create an iterator from ExpressionItemList
typedef struct {
  ExpressionItemList* list;
  size_t current_index;
  KernelExpressionVisitorState* state;
} ReconstructIteratorState;

const void* reconstruct_next_fn(void* data) {
  ReconstructIteratorState* iter_state = (ReconstructIteratorState*)data;
  if (iter_state->current_index >= iter_state->list->len) {
    // Return NULL to signal end of iteration
    return NULL;
  }
  
  ExpressionItem item = iter_state->list->list[iter_state->current_index];
  iter_state->current_index++;
  
  uintptr_t result = reconstruct_expression_item(iter_state->state, item);
  // Return the result as a pointer (cast uintptr_t to void*)
  return (const void*)result;
}

uintptr_t reconstruct_variadic(
    KernelExpressionVisitorState* state,
    struct Variadic* variadic) {
  ReconstructIteratorState iter_state = {
    .list = &variadic->exprs,
    .current_index = 0,
    .state = state
  };
  
  EngineIterator iterator = {
    .data = &iter_state,
    .get_next = reconstruct_next_fn
  };
  
  switch (variadic->op) {
    case And:
      return visit_predicate_and(state, &iterator);
    case Or:
      return visit_predicate_or(state, &iterator);
    case StructExpression:
      return visit_expression_struct(state, &iterator);
  }
  assert(0 && "Unknown variadic op");
  return 0;
}

uintptr_t reconstruct_unary(
    KernelExpressionVisitorState* state,
    struct Unary* unary) {
  assert(unary->sub_expr.len == 1);
  uintptr_t inner = reconstruct_expression_item(state, unary->sub_expr.list[0]);
  
  switch (unary->type) {
    case Not:
      return visit_predicate_not(state, inner);
    case IsNull:
      return visit_predicate_is_null(state, inner);
  }
  assert(0 && "Unknown unary op");
  return 0;
}

uintptr_t reconstruct_expression_item(
    KernelExpressionVisitorState* state,
    ExpressionItem item) {
  switch (item.type) {
    case Literal:
      return reconstruct_literal(state, (struct Literal*)item.ref);
    case BinOp:
      return reconstruct_binop(state, (struct BinOp*)item.ref);
    case Variadic:
      return reconstruct_variadic(state, (struct Variadic*)item.ref);
    case Unary:
      return reconstruct_unary(state, (struct Unary*)item.ref);
    case Column: {
      char* column_name = (char*)item.ref;
      KernelStringSlice str_slice = {
        .ptr = column_name,
        .len = strlen(column_name)
      };
      ExternResultusize result = visit_expression_column(state, str_slice, NULL);
      return result.ok;
    }
    case Unknown: {
      struct Unknown* unknown = (struct Unknown*)item.ref;
      KernelStringSlice str_slice = {
        .ptr = unknown->name,
        .len = strlen(unknown->name)
      };
      return visit_expression_unknown(state, str_slice);
    }
    case Transform:
    case FieldTransform:
    case OpaqueExpression:
    case OpaquePredicate:
      fprintf(stderr,
          "Warning: Complex expression type not yet supported "
          "for reconstruction\n");
      return visit_expression_literal_int(state, 0);
  }
  assert(0 && "Unknown expression type");
  return 0;
}

/**
 * Reconstruct a kernel expression from an engine expression
 * representation. Returns a SharedExpression handle that must be freed with
 * free_kernel_expression.
 */
SharedExpression* reconstruct_kernel_expression(
    ExpressionItemList expr_list) {
  KernelExpressionVisitorState* state = visit_expression_state_new();
  
  assert(expr_list.len > 0);
  uintptr_t expr_id = reconstruct_expression_item(state, expr_list.list[0]);
  
  // Convert the expression ID to a SharedExpression handle
  SharedExpression* result = visit_expression_state_to_expression(
      state, expr_id);
  
  visit_expression_state_free(state);
  return result;
}

/**
 * Reconstruct a kernel predicate from an engine predicate representation.
 * Returns a SharedPredicate handle that must be freed with
 * free_kernel_predicate.
 */
SharedPredicate* reconstruct_kernel_predicate(
    ExpressionItemList pred_list) {
  KernelExpressionVisitorState* state = visit_expression_state_new();
  
  assert(pred_list.len > 0);
  uintptr_t pred_id = reconstruct_expression_item(state, pred_list.list[0]);
  
  // Convert the predicate ID to a SharedPredicate handle
  SharedPredicate* result = visit_expression_state_to_predicate(
      state, pred_id);
  
  visit_expression_state_free(state);
  return result;
}

