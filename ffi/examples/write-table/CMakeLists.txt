cmake_minimum_required(VERSION 3.12)
project(write_table)

add_executable(write_table write_table.c ../common/kernel_utils.c)
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
add_test(NAME write_table_empty_commit
         COMMAND ${TestRunner} ${CreateTableBinary} ${ExpectedPath}/empty-commit.expected)
