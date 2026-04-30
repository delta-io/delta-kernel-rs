cmake_minimum_required(VERSION 3.12)
project(read_table_changes)

add_executable(read_table_changes read_table_changes.c ../common/kernel_utils.c)
target_compile_definitions(read_table_changes PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(read_table_changes PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(read_table_changes PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(read_table_changes PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(read_table_changes PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(read_table_changes PRIVATE /W4 /WX)
else()
  target_compile_options(read_table_changes PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(read_table_changes PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(TestRunner "../../../tests/read-table-changes-testing/run_test.sh")
set(DatPath "../../../../acceptance/tests/dat/out/reader_tests/generated")
set(ExpectedPath "../../../tests/read-table-changes-testing/expected-data")
add_test(NAME read_cdf_full COMMAND ${TestRunner} ${DatPath}/cdf/delta/ ${ExpectedPath}/cdf.expected)
add_test(NAME read_cdf_from_v1 COMMAND ${TestRunner} ${DatPath}/cdf/delta/ ${ExpectedPath}/cdf-from-v1.expected -s 1)
add_test(NAME read_cdf_v1_only COMMAND ${TestRunner} ${DatPath}/cdf/delta/ ${ExpectedPath}/cdf-v1-only.expected -s 1 -e 1)
# v2 is the DELETE commit (4 rows of CDC data); exercises the CDC-file-only path.
add_test(NAME read_cdf_v2_only COMMAND ${TestRunner} ${DatPath}/cdf/delta/ ${ExpectedPath}/cdf-v2-only.expected -s 2 -e 2)
