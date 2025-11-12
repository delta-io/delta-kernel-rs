cmake_minimum_required(VERSION 3.12)
project(visit_expressions)

add_executable(visit_expression visit_expression.c ../common/kernel_utils.c)
target_compile_definitions(visit_expression PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(visit_expression PUBLIC 
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(visit_expression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(visit_expression PUBLIC delta_kernel_ffi)
target_compile_options(visit_expression PUBLIC)

if(MSVC)
  target_compile_options(read_table PRIVATE /W4 /WX)
else()
  # no-strict-prototypes because arrow headers have fn defs without prototypes
  target_compile_options(visit_expression PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes -g -fsanitize=address)
  target_link_options(visit_expression PRIVATE -g -fsanitize=address)
endif()

# Add the kernel expresion -> engine expression test
include(CTest)
set(ExprTestRunner "../../../tests/test-expression-visitor/run_test.sh")
set(ExprExpectedPath "../../../tests/test-expression-visitor/expected.txt")
add_test(NAME test_expression_visitor COMMAND ${ExprTestRunner} ${ExprExpectedPath})
