#pragma once

#include "delta_kernel_ffi.h"
#include "expression.h"
#include "../common/kernel_utils.h"
#include <assert.h>
#include <stdlib.h>

/**
 * This module converts an engine expression (ExpressionItemList) back into
 * a kernel expression (SharedExpression) by calling the appropriate visit_*
 * functions from KernelExpressionVisitorState.
 */

// Forward declarations
uintptr_t convert_engine_to_kernel_expression_item(
    KernelExpressionVisitorState* state,
    ExpressionItem item);

uintptr_t convert_engine_to_kernel_literal(
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
          state, str_slice, allocate_error);
      if (result.tag == Errusize) {
        print_error("visit_expression_literal_string failed", (Error*)result.err);
        free_error((Error*)result.err);
        abort();
      }
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
      ExternResultusize result = visit_expression_literal_decimal(
          state, dec->hi, dec->lo, dec->precision, dec->scale, allocate_error);
      if (result.tag == Errusize) {
        print_error("visit_expression_literal_decimal failed", (Error*)result.err);
        free_error((Error*)result.err);
        abort();
      }
      return result.ok;
    }
    case Null: {
      ExternResultusize result = visit_expression_literal_null(state, allocate_error);
      if (result.tag == Errusize) {
        print_error("visit_expression_literal_null failed", (Error*)result.err);
        free_error((Error*)result.err);
        abort();
      }
      return result.ok;
    }
    case Struct:
      fprintf(stderr, "Error: Struct literal type not supported\n");
      assert(0 && "Struct literal type not supported");
      abort(); // Explicitly abort even if assertions are disabled
    case Array:
      fprintf(stderr, "Error: Array literal type not supported\n");
      assert(0 && "Array literal type not supported");
      abort(); // Explicitly abort even if assertions are disabled
    case Map:
      fprintf(stderr, "Error: Map literal type not supported\n");
      assert(0 && "Map literal type not supported");
      abort(); // Explicitly abort even if assertions are disabled
    default:
      fprintf(stderr,
          "Error: Unknown literal type in convert_engine_to_kernel_literal\n");
      assert(0 && "Unknown literal type in convert_engine_to_kernel_literal");
      abort(); // Explicitly abort even if assertions are disabled
  }
}

uintptr_t convert_engine_to_kernel_binop(
    KernelExpressionVisitorState* state,
    struct BinOp* binop) {
  assert(binop->exprs.len == 2);
  uintptr_t left = convert_engine_to_kernel_expression_item(
      state, binop->exprs.list[0]);
  uintptr_t right = convert_engine_to_kernel_expression_item(
      state, binop->exprs.list[1]);
  
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
    default:
      fprintf(stderr, "Error: Unknown binary op in convert_engine_to_kernel_binop\n");
      assert(0 && "Unknown binary op in convert_engine_to_kernel_binop");
      abort(); // Explicitly abort even if assertions are disabled
  }
}

// Helper to create an iterator from ExpressionItemList
typedef struct {
  ExpressionItemList* list;
  size_t current_index;
  KernelExpressionVisitorState* state;
} EngineToKernelIteratorState;

const void* convert_engine_to_kernel_next_fn(void* data) {
  EngineToKernelIteratorState* iter_state = (EngineToKernelIteratorState*)data;
  if (iter_state->current_index >= iter_state->list->len) {
    // Return NULL to signal end of iteration
    return NULL;
  }
  
  ExpressionItem item = iter_state->list->list[iter_state->current_index];
  iter_state->current_index++;
  
  uintptr_t result = convert_engine_to_kernel_expression_item(iter_state->state, item);
  // Return the result as a pointer (cast uintptr_t to void*)
  return (const void*)result;
}

uintptr_t convert_engine_to_kernel_variadic(
    KernelExpressionVisitorState* state,
    struct Variadic* variadic) {
  EngineToKernelIteratorState iter_state = {
    .list = &variadic->exprs,
    .current_index = 0,
    .state = state
  };
  
  EngineIterator iterator = {
    .data = &iter_state,
    .get_next = convert_engine_to_kernel_next_fn
  };
  
  switch (variadic->op) {
    case And:
      return visit_predicate_and(state, &iterator);
    case Or:
      return visit_predicate_or(state, &iterator);
    case StructExpression:
      return visit_expression_struct(state, &iterator);
    default:
      fprintf(stderr,
          "Error: Unknown variadic op in convert_engine_to_kernel_variadic\n");
      assert(0 && "Unknown variadic op in convert_engine_to_kernel_variadic");
      abort(); // Explicitly abort even if assertions are disabled
  }
}

uintptr_t convert_engine_to_kernel_unary(
    KernelExpressionVisitorState* state,
    struct Unary* unary) {
  assert(unary->sub_expr.len == 1);
  uintptr_t inner = convert_engine_to_kernel_expression_item(
      state, unary->sub_expr.list[0]);
  
  switch (unary->type) {
    case Not:
      return visit_predicate_not(state, inner);
    case IsNull:
      return visit_predicate_is_null(state, inner);
    default:
      fprintf(stderr, "Error: Unknown unary op in convert_engine_to_kernel_unary\n");
      assert(0 && "Unknown unary op in convert_engine_to_kernel_unary");
      abort(); // Explicitly abort even if assertions are disabled
  }
}

uintptr_t convert_engine_to_kernel_expression_item(
    KernelExpressionVisitorState* state,
    ExpressionItem item) {
  switch (item.type) {
    case Literal:
      return convert_engine_to_kernel_literal(state, (struct Literal*)item.ref);
    case BinOp:
      return convert_engine_to_kernel_binop(state, (struct BinOp*)item.ref);
    case Variadic:
      return convert_engine_to_kernel_variadic(state, (struct Variadic*)item.ref);
    case Unary:
      return convert_engine_to_kernel_unary(state, (struct Unary*)item.ref);
    case Column: {
      char* column_name = (char*)item.ref;
      KernelStringSlice str_slice = {
        .ptr = column_name,
        .len = strlen(column_name)
      };
      ExternResultusize result = visit_expression_column(
          state, str_slice, allocate_error);
      if (result.tag == Errusize) {
        print_error("visit_expression_column failed", (Error*)result.err);
        free_error((Error*)result.err);
        abort();
      }
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
    default:
      fprintf(stderr,
          "Error: Unknown expression type in "
          "convert_engine_to_kernel_expression_item\n");
      assert(0 &&
          "Unknown expression type in convert_engine_to_kernel_expression_item");
      abort(); // Explicitly abort even if assertions are disabled
  }
}

// Visitor function for converting ExpressionItemList to kernel expression
static inline uintptr_t expression_item_list_visitor(
    void* expr_list_ptr,
    KernelExpressionVisitorState* state) {
  ExpressionItemList* expr_list = (ExpressionItemList*)expr_list_ptr;
  assert(expr_list->len > 0);
  return convert_engine_to_kernel_expression_item(state, expr_list->list[0]);
}

/**
 * Convert an engine expression to a kernel expression using the visitor
 * pattern. Returns a SharedExpression handle that must be freed with
 * free_kernel_expression.
 * 
 * This function uses the EngineExpression visitor pattern, completely
 * hiding KernelExpressionVisitorState management from the caller.
 */
SharedExpression* convert_engine_to_kernel_expression(
    ExpressionItemList expr_list) {
  EngineExpression engine_expr = {
    .expression = (void*)&expr_list,
    .visitor = expression_item_list_visitor
  };
  
  ExternResultHandleSharedExpression result = visit_engine_expression(
      &engine_expr, allocate_error);
  
  if (result.tag == OkHandleSharedExpression) {
    return result.ok;
  } else {
    print_error("Failed to convert engine expression to kernel expression",
        (Error*)result.err);
    free_error((Error*)result.err);
    abort();
  }
}

// Visitor function for converting ExpressionItemList to kernel predicate
static inline uintptr_t predicate_item_list_visitor(
    void* pred_list_ptr,
    KernelExpressionVisitorState* state) {
  ExpressionItemList* pred_list = (ExpressionItemList*)pred_list_ptr;
  assert(pred_list->len > 0);
  return convert_engine_to_kernel_expression_item(state, pred_list->list[0]);
}

/**
 * Convert an engine predicate to a kernel predicate using the visitor
 * pattern. Returns a SharedPredicate handle that must be freed with
 * free_kernel_predicate.
 * 
 * This function uses the EnginePredicate visitor pattern, completely
 * hiding KernelExpressionVisitorState management from the caller.
 */
SharedPredicate* convert_engine_to_kernel_predicate(
    ExpressionItemList pred_list) {
  EnginePredicate engine_pred = {
    .predicate = (void*)&pred_list,
    .visitor = predicate_item_list_visitor
  };
  
  ExternResultHandleSharedPredicate result = visit_engine_predicate(
      &engine_pred, allocate_error);
  
  if (result.tag == OkHandleSharedPredicate) {
    return result.ok;
  } else {
    print_error("Failed to convert engine predicate to kernel predicate",
        (Error*)result.err);
    free_error((Error*)result.err);
    abort();
  }
}


