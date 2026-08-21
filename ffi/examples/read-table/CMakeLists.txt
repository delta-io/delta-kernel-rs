cmake_minimum_required(VERSION 3.12)
project(read_table)
option(PRINT_DATA "Print out the table data. Requires arrow-glib" ON)
option(VERBOSE "Enable for more diagnostics messages." OFF)
option(
  GEO_TYPE_IN_DEV
  "Register the geometry/geography schema test. Requires delta_kernel_ffi built with --features geo-type-in-dev."
  OFF)
add_executable(read_table read_table.c arrow.c ../common/kernel_utils.c)
target_compile_definitions(read_table PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(read_table PUBLIC 
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common")
target_link_directories(read_table PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(read_table PUBLIC delta_kernel_ffi)
target_compile_options(read_table PUBLIC)

# Add the test
include(CTest)
set(TestRunner "../../../tests/read-table-testing/run_test.sh")
set(DatPath "../../../../acceptance/tests/dat/out/reader_tests/generated")
set(ExpectedPath "../../../tests/read-table-testing/expected-data")
set(KernelTestPath "../../../../kernel/tests/data")
add_test(NAME read_and_print_all_prim COMMAND ${TestRunner} ${DatPath}/all_primitive_types/delta/ ${ExpectedPath}/all-prim-types.expected)
add_test(NAME read_and_print_all_prim_cols COMMAND ${TestRunner} ${DatPath}/all_primitive_types/delta/ ${ExpectedPath}/all-prim-types-cols.expected -cdecimal,float64)
add_test(NAME read_and_print_nested COMMAND ${TestRunner} ${DatPath}/nested_types/delta/ ${ExpectedPath}/nested-types.expected)
add_test(NAME read_and_print_nested_cols COMMAND ${TestRunner} ${DatPath}/nested_types/delta/ ${ExpectedPath}/nested-types-cols.expected -cmap,struct,array)
add_test(NAME read_and_print_basic_partitioned COMMAND ${TestRunner} ${DatPath}/basic_partitioned/delta/ ${ExpectedPath}/basic-partitioned.expected)
add_test(NAME read_and_print_with_dv_small COMMAND ${TestRunner} ${KernelTestPath}/table-with-dv-small/ ${ExpectedPath}/table-with-dv-small.expected)
add_test(NAME read_and_print_with_short_dv COMMAND ${TestRunner} ${KernelTestPath}/with-short-dv/ ${ExpectedPath}/with-short-dv.expected)
# Exercises the column-default FFI (-d) against a log-only fixture that declares three top-level
# defaults. The table has no add files, so the data section is empty; the point is the printed
# defaults block.
add_test(NAME read_and_print_column_defaults COMMAND ${TestRunner} ${KernelTestPath}/table-with-column-defaults/ ${ExpectedPath}/table-with-column-defaults.expected -d)
# Exercises the Arrow batch-mode scan metadata path (scan_metadata_next_arrow). The arrow path
# does not call read_parquet_file, so the data section is "[No data]"; this test still
# requires PRINT_DATA=ON because iterate_scan_metadata_arrow is gated behind it.
add_test(NAME read_arrow_metadata_basic COMMAND ${TestRunner} ${DatPath}/basic_partitioned/delta/ ${ExpectedPath}/basic-partitioned-arrow-metadata.expected -a)
# Exercises typed field metadata across the FFI: the column_mapping table's fields carry
# `delta.columnMapping.id`, which the schema visitor reads back via get_from_metadata_map and
# prints per field.
add_test(NAME read_column_mapping_metadata COMMAND ${TestRunner} ${DatPath}/column_mapping/delta/ ${ExpectedPath}/column-mapping-arrow-metadata.expected -a)
if(GEO_TYPE_IN_DEV)
  add_test(NAME read_and_print_geo COMMAND ${TestRunner} ${KernelTestPath}/table-with-geo/ ${ExpectedPath}/table-with-geo.expected)
endif()

if(WIN32)
  set(CMAKE_C_FLAGS_DEBUG "/MT")
  target_link_libraries(read_table PUBLIC ws2_32 userenv bcrypt ncrypt crypt32 secur32 ntdll RuntimeObject)
endif(WIN32)

if(MSVC)
  target_compile_options(read_table PRIVATE /W3 /WX)
else()
  # no-strict-prototypes because arrow headers have fn defs without prototypes
  target_compile_options(read_table PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes -g -fsanitize=address)
  target_link_options(read_table PRIVATE -g -fsanitize=address)
endif()

if(VERBOSE)
  target_compile_definitions(read_table PUBLIC VERBOSE)
endif(VERBOSE)

if(PRINT_DATA)
  include(FindPkgConfig)
  pkg_check_modules(GLIB REQUIRED glib-2.0)
  pkg_check_modules(ARROW_GLIB REQUIRED arrow-glib)
  target_include_directories(read_table PUBLIC ${ARROW_GLIB_INCLUDE_DIRS})
  target_link_directories(read_table PUBLIC ${ARROW_GLIB_LIBRARY_DIRS})
  target_link_libraries(read_table PUBLIC ${ARROW_GLIB_LIBRARIES})
  # arrow-glib's .pc file (as of v19) leaks `-std=c++11` into CFLAGS; that's a C++ flag
  # which the C compiler rejects under -Werror. Drop any -std= flags before forwarding.
  set(ARROW_GLIB_CFLAGS_FILTERED "")
  foreach(flag ${ARROW_GLIB_CFLAGS_OTHER})
    if(NOT flag MATCHES "^-std=")
      list(APPEND ARROW_GLIB_CFLAGS_FILTERED ${flag})
    endif()
  endforeach()
  target_compile_options(read_table PUBLIC ${ARROW_GLIB_CFLAGS_FILTERED})
  target_compile_definitions(read_table PUBLIC PRINT_ARROW_DATA)
endif(PRINT_DATA)
