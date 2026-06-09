cmake_minimum_required(VERSION 3.12)
project(checkpoint_table)

add_executable(checkpoint_table checkpoint_table.c ../common/kernel_utils.c)
target_compile_definitions(checkpoint_table PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(checkpoint_table PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(checkpoint_table PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(checkpoint_table PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(checkpoint_table PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(checkpoint_table PRIVATE /W4 /WX)
else()
  target_compile_options(checkpoint_table PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(checkpoint_table PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(TestRunner "../../../tests/checkpoint-table-testing/run_test.sh")
set(ExpectedPath "../../../tests/checkpoint-table-testing/expected-data")
add_test(NAME checkpoint_table_inline
         COMMAND ${TestRunner} inline ${ExpectedPath}/inline.expected)
add_test(NAME checkpoint_table_v2_no_sidecar
         COMMAND ${TestRunner} v2_no_sidecar ${ExpectedPath}/v2_no_sidecar.expected)
add_test(NAME checkpoint_table_v2_with_sidecars
         COMMAND ${TestRunner} v2_with_sidecars ${ExpectedPath}/v2_with_sidecars.expected)
