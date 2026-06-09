cmake_minimum_required(VERSION 3.12)
project(custom_io_engine)
add_executable(custom_io_engine custom_io_engine.c ../common/kernel_utils.c)
target_compile_definitions(custom_io_engine PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(custom_io_engine PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(custom_io_engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(custom_io_engine PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(custom_io_engine PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif()

if(MSVC)
  target_compile_options(custom_io_engine PRIVATE /W3 /WX)
else()
  target_compile_options(custom_io_engine PRIVATE -Wall -Wextra -Wpedantic -Werror -g)
endif()
