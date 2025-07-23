#pragma once

#include "expression.h"

/**
 * This module defines a function `print_tree` to recursively print an ExpressionItem.
 */

void print_tree_helper(ExpressionItem ref, int depth);
void print_n_spaces(int n) {
  if (n == 0)
    return;
  printf("  ");
  print_n_spaces(n - 1);
}
void print_expression_item_list(ExpressionItemList list, int depth) {
  for (size_t i = 0; i < list.len; i++) {
    print_tree_helper(list.list[i], depth);
  }
}
void print_opaque_op_name(void* op_type, KernelStringSlice name) {
  int len = name.len & 0x7fffffff; // truncate to 31 bits to ensure a positive value
  printf("%s(%.*s)\n", (char*) op_type, len, name.ptr);
}

void print_tree_helper(ExpressionItem ref, int depth) {
  print_n_spaces(depth);
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      switch (op->op) {
        case Add: {
          printf("Add\n");
          break;
        }
        case Minus: {
          printf("Minus\n");
          break;
        };
        case Divide: {
          printf("Divide\n");
          break;
        };
        case Multiply: {
          printf("Multiply\n");
          break;
        };
        case LessThan: {
          printf("LessThan\n");
          break;
        };
        case GreaterThan: {
          printf("GreaterThan\n");
          break;
        };
        case Equal: {
          printf("Equal\n");
          break;
        };
        case In: {
          printf("In\n");
          break;
        };
        case Distinct:
          printf("Distinct\n");
          break;
      }
      print_expression_item_list(op->exprs, depth + 1);
      break;
    }
    case Variadic: {
      struct Variadic* var = ref.ref;
      switch (var->op) {
        case And:
          printf("And\n");
          break;
        case Or:
          printf("Or\n");
          break;
        case StructExpression:
          printf("StructExpression\n");
          break;
      }
      print_expression_item_list(var->exprs, depth + 1);
      break;
    }
    case OpaqueExpression: {
      struct OpaqueExpression* opaque = ref.ref;
      visit_kernel_opaque_expression_op_name(opaque->op, "OpaqueExpression", print_opaque_op_name);
      print_expression_item_list(opaque->exprs, depth + 1);
      break;
    }
    case OpaquePredicate: {
      struct OpaquePredicate* opaque = ref.ref;
      visit_kernel_opaque_predicate_op_name(opaque->op, "OpaquePredicate", print_opaque_op_name);
      print_expression_item_list(opaque->exprs, depth + 1);
      break;
    }
    case Unknown: {
      struct Unknown* unknown = ref.ref;
      printf("Unknown(%s)\n", unknown->name);
      break;
    }
    case Literal: {
      struct Literal* lit = ref.ref;
      switch (lit->type) {
        case Integer:
          printf("Integer(%d)\n", lit->value.integer_data);
          break;
        case Long:
          printf("Long(%lld)\n", (long long)lit->value.long_data);
          break;
        case Short:
          printf("Short(%hd)\n", lit->value.short_data);
          break;
        case Byte:
          printf("Byte(%hhd)\n", lit->value.byte_data);
          break;
        case Float:
          printf("Float(%f)\n", (float)lit->value.float_data);
          break;
        case Double:
          printf("Double(%f)\n", lit->value.double_data);
          break;
        case String: {
          printf("String(%s)\n", lit->value.string_data);
          break;
        }
        case Boolean:
          printf("Boolean(%d)\n", lit->value.boolean_data);
          break;
        case Timestamp:
          printf("Timestamp(%lld)\n", (long long)lit->value.long_data);
          break;
        case TimestampNtz:
          printf("TimestampNtz(%lld)\n", (long long)lit->value.long_data);
          break;
        case Date:
          printf("Date(%d)\n", lit->value.integer_data);
          break;
        case Binary: {
          printf("Binary(");
          for (size_t i = 0; i < lit->value.binary.len; i++) {
            printf("%02x", lit->value.binary.buf[i]);
          }
          printf(")\n");
          break;
        }
        case Decimal: {
          struct Decimal* dec = &lit->value.decimal;
          printf("Decimal(%lld,%llu,%d,%d)\n",
                 (long long)dec->hi,
                 (unsigned long long)dec->lo,
                 dec->precision,
                 dec->scale);
          break;
        }
        case Null:
          printf("Null\n");
          break;
        case Struct:
          printf("Struct\n");
          struct Struct* struct_data = &lit->value.struct_data;
          for (size_t i = 0; i < struct_data->values.len; i++) {
            print_n_spaces(depth + 1);

            // Extract field name from field
            ExpressionItem item = struct_data->fields.list[i];
            assert(item.type == Literal);
            struct Literal* lit = item.ref;
            assert(lit->type == String);

            printf("Field: %s\n", lit->value.string_data);
            print_tree_helper(struct_data->values.list[i], depth + 2);
          }
          break;
        case Array:
          printf("Array\n");
          struct ArrayData* array = &lit->value.array_data;
          print_expression_item_list(array->exprs, depth + 1);
          break;
        case Map:
          printf("Map\n");
          struct MapData* map_data = &lit->value.map_data;
          for (size_t i = 0; i < map_data->keys.len; i++) {
            print_n_spaces(depth + 1);

            // Extract key
            ExpressionItem key = map_data->keys.list[i];
            assert(key.type == Literal);
            struct Literal* key_lit = key.ref;
            assert(key_lit->type == String);
            // Extract val
            ExpressionItem val = map_data->vals.list[i];
            assert(val.type == Literal);
            struct Literal* val_lit = val.ref;
            assert(val_lit->type == String);

            // instead of recursing (which forces newlines) we just directly print strings here
            printf("String(%s): String(%s)\n", key_lit->value.string_data, val_lit->value.string_data);
          }
          break;
      }
      break;
    }
    case Unary: {
      struct Unary* unary = ref.ref;
      switch (unary->type) {
        case Not:
          printf("Not\n");
          break;
        case IsNull:
          printf("IsNull\n");
          break;
      }

      print_expression_item_list(unary->sub_expr, depth + 1);
      break;
    }
    case Column: {
      char* column_name = ref.ref;
      printf("Column(%s)\n", column_name);
      break;
    }
  }
}

void print_expression(ExpressionItemList expression) {
  print_expression_item_list(expression, 0);
}
