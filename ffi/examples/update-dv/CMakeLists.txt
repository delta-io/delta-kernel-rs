cmake_minimum_required(VERSION 3.12)
project(update_dv)

add_executable(update_dv update_dv.c ../common/kernel_utils.c)
target_compile_definitions(update_dv PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(update_dv PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(update_dv PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(update_dv PUBLIC delta_kernel_ffi)

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(update_dv PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(update_dv PRIVATE /W4 /WX)
else()
  target_compile_options(update_dv PRIVATE -Wall -Wextra -Wpedantic -Werror -g -fsanitize=address)
  target_link_options(update_dv PRIVATE -g -fsanitize=address)
endif()
