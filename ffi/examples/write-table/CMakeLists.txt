cmake_minimum_required(VERSION 3.12)
project(write_table)

add_executable(write_table write_table.c ../common/kernel_utils.c ../common/column_defaults.c)
target_compile_definitions(write_table PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(write_table PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(write_table PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(write_table PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(write_table PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(write_table PRIVATE /W4 /WX)
else()
  target_compile_options(write_table PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(write_table PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(TestRunner "../../../tests/write-table-testing/run_test.sh")
set(ExpectedPath "../../../tests/write-table-testing/expected-data")
set(CreateTableBinary "${CMAKE_CURRENT_SOURCE_DIR}/../create-table/build/create_table")
set(KernelTestPath "${CMAKE_CURRENT_SOURCE_DIR}/../../../kernel/tests/data")
add_test(NAME write_table_empty_commit
         COMMAND ${TestRunner} ${CreateTableBinary} ${ExpectedPath}/empty-commit.expected)
# Exercises the column-default write flow (-d) against a fixture that enables allowColumnDefaults
# and declares three top-level defaults. Seeded by copy rather than create-table, which cannot
# declare column metadata. Without the acknowledgement this commit fails, so the test covers the
# gate as much as the reporting.
add_test(NAME write_table_column_defaults
         COMMAND ${TestRunner} fixture:${KernelTestPath}/table-with-column-defaults
                 ${ExpectedPath}/column-defaults-commit.expected -d)
