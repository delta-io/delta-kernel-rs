cmake_minimum_required(VERSION 3.12)
project(uc_catalog_example)

add_executable(uc_catalog_example uc_catalog_example.c ../common/kernel_utils.c)
target_compile_definitions(uc_catalog_example PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(uc_catalog_example PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(uc_catalog_example PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(uc_catalog_example PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(uc_catalog_example PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(uc_catalog_example PRIVATE /W3 /WX)
else()
  target_compile_options(uc_catalog_example PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(uc_catalog_example PRIVATE -g -fsanitize=address)
endif()

# Testing
include(CTest)
set(ExprTestRunner "../../../tests/test-uc-catalog-ffi/run_test.sh")
set(ExprExpectedPath "../../../tests/test-uc-catalog-ffi/expected.txt")
add_test(NAME test_uc_catalog_ffi COMMAND ${ExprTestRunner} ${ExprExpectedPath})
