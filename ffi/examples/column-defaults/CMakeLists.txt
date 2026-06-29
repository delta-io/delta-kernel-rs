cmake_minimum_required(VERSION 3.12)
project(column_defaults)

add_executable(column_defaults column_defaults.c ../common/kernel_utils.c)
# DEFINE_COLUMN_DEFAULTS_IN_DEV exposes the column-defaults symbols, which only exist when
# libdelta_kernel_ffi is built with the `column-defaults-in-dev` feature.
target_compile_definitions(column_defaults PUBLIC DEFINE_DEFAULT_ENGINE_BASE DEFINE_COLUMN_DEFAULTS_IN_DEV)
target_include_directories(column_defaults PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(column_defaults PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(column_defaults PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(column_defaults PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(column_defaults PRIVATE /W4 /WX)
else()
  target_compile_options(column_defaults PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(column_defaults PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(TestRunner "../../../tests/column-defaults-testing/run_test.sh")
set(TablePath "../../../tests/column-defaults-testing/table")
set(ExpectedPath "../../../tests/column-defaults-testing/expected-data")
add_test(NAME column_defaults_discovery
         COMMAND ${TestRunner} ${TablePath} ${ExpectedPath}/column-defaults.expected)
