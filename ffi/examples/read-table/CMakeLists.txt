cmake_minimum_required(VERSION 3.12)
project(read_table)

# Options
option(PRINT_DATA "Print out the table data. Requires arrow-glib" ON)
option(VERBOSE "Enable for more diagnostic messages." OFF)

# Paths to directories
set(FFI_HEADERS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers")
set(TARGET_DEBUG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
set(COMMON_DIR "${CMAKE_CURRENT_SOURCE_DIR}/common")

# Include the pkg-config module if needed
if(PRINT_DATA)
    include(FindPkgConfig)
    pkg_check_modules(GLIB REQUIRED glib-2.0)
    pkg_check_modules(ARROW_GLIB REQUIRED arrow-glib)
endif()

# Create the common static library
add_library(common_lib STATIC
    ${COMMON_DIR}/arrow.c
    ${COMMON_DIR}/kernel_utils.c
)

# Include directories for common_lib
target_include_directories(common_lib PUBLIC
    ${COMMON_DIR}
    ${FFI_HEADERS_DIR}
    $<IF:$<BOOL:${PRINT_DATA}>,${ARROW_GLIB_INCLUDE_DIRS},>
    $<IF:$<BOOL:${PRINT_DATA}>,${CMAKE_CURRENT_SOURCE_DIR},>
)

# Link directories for common_lib
target_link_directories(common_lib PUBLIC
    ${TARGET_DEBUG_DIR}
    $<IF:$<BOOL:${PRINT_DATA}>,${ARROW_GLIB_LIBRARY_DIRS},>
)

# Link libraries for common_lib
target_link_libraries(common_lib PUBLIC
    delta_kernel_ffi
    $<IF:$<BOOL:${PRINT_DATA}>,${ARROW_GLIB_LIBRARIES},>
)

# Compile definitions for common_lib
target_compile_definitions(common_lib PUBLIC DEFINE_DEFAULT_ENGINE)

if(VERBOSE)
    target_compile_definitions(common_lib PUBLIC VERBOSE)
endif()

if(PRINT_DATA)
    target_compile_definitions(common_lib PUBLIC PRINT_ARROW_DATA)
    target_compile_options(common_lib PUBLIC ${ARROW_GLIB_CFLAGS_OTHER})
endif()

# List of executables
set(EXECUTABLES read_table write_table)

foreach(EXE ${EXECUTABLES})
    add_executable(${EXE} ${EXE}.c)

    # Include directories for the executable
    target_include_directories(${EXE} PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        ${FFI_HEADERS_DIR}
    )

    # Link directories for the executable
    target_link_directories(${EXE} PUBLIC ${TARGET_DEBUG_DIR})

    # Link libraries
    target_link_libraries(${EXE} PUBLIC common_lib)

    # Compile definitions for the executable (if needed)
    if(VERBOSE)
        target_compile_definitions(${EXE} PUBLIC VERBOSE)
    endif()

    if(PRINT_DATA)
        target_compile_definitions(${EXE} PUBLIC PRINT_ARROW_DATA)
        target_compile_options(${EXE} PUBLIC ${ARROW_GLIB_CFLAGS_OTHER})
    endif()

    # Compiler options
    if(MSVC)
        target_compile_options(${EXE} PRIVATE /W3 /WX)
    else()
        # Suppress warnings about function prototypes due to arrow headers
        target_compile_options(${EXE} PRIVATE
            -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes -g -fsanitize=address
        )
        target_link_options(${EXE} PRIVATE -g -fsanitize=address)
    endif()

    # Platform-specific libraries
    if(WIN32)
        set(CMAKE_C_FLAGS_DEBUG "/MT")
        target_link_libraries(${EXE} PUBLIC
            ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject
        )
    endif()
endforeach()

# Add the tests
include(CTest)
set(TestRunner "../../../tests/read-table-testing/run_test.sh")
set(DatPath "../../../../acceptance/tests/dat/out/reader_tests/generated")
set(ExpectedPath "../../../tests/read-table-testing/expected-data")

add_test(NAME read_and_print_all_prim
    COMMAND ${TestRunner} ${DatPath}/all_primitive_types/delta/ ${ExpectedPath}/all-prim-types.expected
)
add_test(NAME read_and_print_basic_partitioned
    COMMAND ${TestRunner} ${DatPath}/basic_partitioned/delta/ ${ExpectedPath}/basic-partitioned.expected
)
