cmake_minimum_required(VERSION 3.12)
project(visit_expressions)

add_executable(visit_expression visit_expression.c)
target_compile_definitions(visit_expression PUBLIC DEFINE_DEFAULT_ENGINE)
target_include_directories(visit_expression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers")
target_link_directories(visit_expression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(visit_expression PUBLIC delta_kernel_ffi)
target_compile_options(visit_expression PUBLIC)

target_compile_options(visit_expression PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes)

# Get info on the OS and platform
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(MACOSX TRUE)
endif()
if(UNIX AND NOT APPLE)
    set(LINUX TRUE)
endif()

# Add the kernel expresion -> engine expression test
include(CTest)
set(ExprTestRunner "../../../tests/test-expression-visitor/run_test.sh")
set(ExprExpectedPath "../../../tests/test-expression-visitor/expected.txt")
add_test(NAME test_expression_visitor COMMAND ${ExprTestRunner} ${ExprExpectedPath})
if(LINUX)
  add_test(NAME test_expression_visitor_leaks COMMAND valgrind
    --error-exitcode=1
    --tool=memcheck
    --leak-check=full
    --errors-for-leak-kinds=definite
    --show-leak-kinds=definite ./visit_expression)
endif()
