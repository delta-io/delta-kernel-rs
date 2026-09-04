cmake_minimum_required(VERSION 3.12)
project(create_table)

add_executable(create_table create_table.c ../common/kernel_utils.c)
target_compile_definitions(create_table PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(create_table PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(create_table PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(create_table PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(create_table PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(create_table PRIVATE /W4 /WX)
else()
  target_compile_options(create_table PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(create_table PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(TestRunner "../../../tests/create-table-testing/run_test.sh")
set(ExpectedPath "../../../tests/create-table-testing/expected-data")
add_test(NAME create_table_basic COMMAND ${TestRunner} ${ExpectedPath}/basic.expected)
