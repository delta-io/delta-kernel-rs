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
void print_tree_helper(ExpressionItem ref, int depth) {
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      print_n_spaces(depth);
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
        case LessThanOrEqual: {
          printf("LessThanOrEqual\n");
          break;
        }
        case GreaterThan: {
          printf("GreaterThan\n");
          break;
        };
        case GreaterThaneOrEqual: {
          printf("GreaterThanOrEqual\n");
          break;
        };
        case Equal: {
          printf("Equal\n");
          break;
        };
        case NotEqual: {
          printf("NotEqual\n");
          break;
        };
        case In: {
          printf("In\n");
          break;
        };
        case NotIn: {
          printf("NotIn\n");
          break;
        }; break;
        case Distinct:
          printf("Distinct\n");
          break;
      }
      print_expression_item_list(op->exprs, depth + 1);
      break;
    }
    case Variadic: {
      struct Variadic* var = ref.ref;
      print_n_spaces(depth);
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
    case Literal: {
      struct Literal* lit = ref.ref;
      print_n_spaces(depth);
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
      }
      break;
    }
    case Unary: {
      print_n_spaces(depth);
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
    case Column:
      print_n_spaces(depth);
      char* column_name = ref.ref;
      printf("Column(%s)\n", column_name);
      break;
  }
}

void print_expression(ExpressionItemList expression) {
  print_expression_item_list(expression, 0);
}
