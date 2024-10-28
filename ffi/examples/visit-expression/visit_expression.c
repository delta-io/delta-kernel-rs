#include "delta_kernel_ffi.h"
#include "expression.h"
#include "expression_print.h"

int main() {
  SharedExpression* pred = get_testing_kernel_expression();
  ExpressionItemList expr = construct_predicate(pred);
  print_expression(expr);
  free_expression_list(expr);
  free_kernel_predicate(pred);
  return 0;
}
