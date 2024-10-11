#include "delta_kernel_ffi.h"
#include "expression.h"
int main()
{
  SharedExpression* pred = get_testing_kernel_expression();
  ExpressionItemList list = construct_predicate(pred);
  ExpressionItem ref = list.exprList[0];
  print_tree(ref, 0);
  free_expression_item_list(list);
  free_kernel_predicate(pred);
  return 0;
}
