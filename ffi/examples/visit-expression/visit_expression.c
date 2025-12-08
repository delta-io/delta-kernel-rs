#include "delta_kernel_ffi.h"
#include "expression.h"
#include "expression_print.h"
#include "engine_to_kernel_expression.h"
#include <stdbool.h>

// Test case structure for organizing test runs
typedef struct {
  const char* name;
  SharedExpression* (*get_expression_fn)(void);
  SharedPredicate* (*get_predicate_fn)(void);
  bool validate_roundtrip;
  const char* description;
} TestCase;

// Run a single test case for expressions and predicates
// The C side owns the memory for the expressions and predicates
// and needs to be freed from the C side while emulates the engine side.
bool run_test_case(const TestCase* test) {
  bool all_passed = true;
  
  printf("=== %s ===\n", test->name);
  printf("%s\n\n", test->description);
  
  // Test expressions
  SharedExpression* expr = test->get_expression_fn();
  ExpressionItemList expr_list = construct_expression(expr);
  print_expression(expr_list);
 
  // The round-trip test for complex expressions is not supported yet.
  // We need to add kernel visitor functions for complex expressions
  // and enable the test for complex expressions.
  if (test->validate_roundtrip) {
    SharedExpression* expr_rekernel = 
        convert_engine_to_kernel_expression(expr_list);
    bool expr_equal = expressions_are_equal(&expr, &expr_rekernel);
    
    printf("\n=== Expression Round-trip Test ===\n");
    if (expr_equal) {
      printf("SUCCESS: Round-trip expression matches original!\n");
    } else {
      printf("FAILURE: Round-trip expression does NOT match original!\n");
      all_passed = false;
    }
    free_kernel_expression(expr_rekernel);
  }
  
  free_expression_list(expr_list);
  free_kernel_expression(expr);
  
  // Test predicates
  SharedPredicate* pred = test->get_predicate_fn();
  ExpressionItemList pred_list = construct_predicate(pred);
  print_expression(pred_list);
  
  // The round-trip test for complex expressions is not supported yet.
  // We need to add kernel visitor functions for complex expressions
  // and enable the test for complex expressions.
  if (test->validate_roundtrip) {
    SharedPredicate* pred_rekernel = 
        convert_engine_to_kernel_predicate(pred_list);
    bool pred_equal = predicates_are_equal(&pred, &pred_rekernel);
    
    printf("\n=== Predicate Round-trip Test ===\n");
    if (pred_equal) {
      printf("SUCCESS: Round-trip predicate matches original!\n");
    } else {
      printf("FAILURE: Round-trip predicate does NOT match original!\n");
      all_passed = false;
    }
    free_kernel_predicate(pred_rekernel);
  }
  
  free_expression_list(pred_list);
  free_kernel_predicate(pred);
  
  return all_passed;
}

int main() {
  // Define test cases
  // We use an iterator pattern to add tests
  TestCase test_cases[] = {
    {
      .name = "Complex Expression Test",
      .get_expression_fn = get_testing_kernel_expression,
      .get_predicate_fn = get_testing_kernel_predicate,
      // TODO: Enable this once #1471 (i.e complex expressions are supported)
      .validate_roundtrip = false,
      .description = 
        "This test demonstrates the full range of expression types.\n"
        "Some types are not yet supported in round-trip reconstruction:\n"
        "  - Struct/Array/Map literals (nested data structures)\n"
        "  - Transform expressions (schema evolution operations)\n"
        "  - Opaque expressions (custom user-defined expressions)"
    },
    {
      .name = "Simple Round-trip Test",
      .get_expression_fn = get_simple_testing_kernel_expression,
      .get_predicate_fn = get_simple_testing_kernel_predicate,
      .validate_roundtrip = true,
      .description = 
        "This test validates expressions/predicates with full support.\n"
        "Supported types: primitives (int, long, float, double, bool, "
        "string),\n  temporal (date, timestamp, timestamp_ntz), binary, "
        "decimal, null,\n  binary operations (+, -, *, /), struct "
        "expressions, predicates (eq, ne, lt, le,\n  gt, ge, distinct, "
        "is_null, is_not_null, not, and, or)"
    }
  };
  
  bool all_tests_passed = true;
  size_t num_tests = sizeof(test_cases) / sizeof(test_cases[0]);
  
  // Run all test cases
  for (size_t i = 0; i < num_tests; i++) {
    if (!run_test_case(&test_cases[i])) {
      all_tests_passed = false;
    }
    if (i < num_tests - 1) {
      printf("\n");  // Separator between test cases
    }
  }
  
  return all_tests_passed ? 0 : 1;
}
