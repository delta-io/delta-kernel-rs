#include "delta_kernel_ffi.h"
#include "expression.h"
#include "expression_print.h"

int main() {
  SharedExpression* expr = get_testing_kernel_expression();
  ExpressionItemList expr_list = construct_expression(expr);
  print_expression(expr_list);
  free_expression_list(expr_list);
  free_kernel_expression(expr);

  SharedPredicate* pred = get_testing_kernel_predicate();
  ExpressionItemList pred_list = construct_predicate(pred);
  print_expression(pred_list);
  free_expression_list(pred_list);
  free_kernel_predicate(pred);
  return 0;
}
