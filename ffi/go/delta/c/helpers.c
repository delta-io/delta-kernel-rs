#include "helpers.h"
#include <stdlib.h>
#include <string.h>

// Helper function to allocate a string from KernelStringSlice
// This is used as a callback for FFI functions that need an allocator
// Returns void* to match AllocateStringFn signature
void* allocate_string_helper(struct KernelStringSlice slice) {
    char* str = (char*)malloc(slice.len + 1);
    if (str) {
        memcpy(str, slice.ptr, slice.len);
        str[slice.len] = '\0';
    }
    return str;
}

// Helper to extract ok value from snapshot result union
HandleSharedSnapshot get_ok_snapshot(struct ExternResultHandleSharedSnapshot result) {
    return result.ok;
}

// Helper to extract err value from snapshot result union
struct EngineError* get_err_snapshot(struct ExternResultHandleSharedSnapshot result) {
    return result.err;
}

// Helper to get engine from result union
HandleSharedExternEngine get_ok_engine(struct ExternResultHandleSharedExternEngine result) {
    return result.ok;
}

// Helper to get error from engine result union
struct EngineError* get_err_engine(struct ExternResultHandleSharedExternEngine result) {
    return result.err;
}

// Helper to get builder from result union
struct EngineBuilder* get_ok_builder(struct ExternResultEngineBuilder result) {
    return result.ok;
}

// Helper to get error from builder result union
struct EngineError* get_err_builder(struct ExternResultEngineBuilder result) {
    return result.err;
}

// Helper to extract ok value from scan result union
HandleSharedScan get_ok_scan(struct ExternResultHandleSharedScan result) {
    return result.ok;
}

// Helper to extract error from scan result union
struct EngineError* get_err_scan(struct ExternResultHandleSharedScan result) {
    return result.err;
}

// Wrapper function to get snapshot table root using our allocator
char* get_snapshot_table_root(HandleSharedSnapshot snapshot) {
    return (char*)snapshot_table_root(snapshot, allocate_string_helper);
}

// Wrapper function to get scan table root using our allocator
char* get_scan_table_root(HandleSharedScan scan) {
    return (char*)scan_table_root(scan, allocate_string_helper);
}

// Visitor callback for collecting partition column names
void collect_string_visitor(void* context, struct KernelStringSlice slice) {
    StringArray* arr = (StringArray*)context;
    if (arr->len < arr->capacity) {
        arr->strings[arr->len] = allocate_string_helper(slice);
        arr->len++;
    }
}

// Helper to iterate partition columns and collect them into an array
StringArray* get_partition_columns_helper(HandleSharedSnapshot snapshot) {
    uintptr_t count = get_partition_column_count(snapshot);

    StringArray* arr = (StringArray*)malloc(sizeof(StringArray));
    arr->len = 0;
    arr->capacity = count;
    arr->strings = (char**)malloc(sizeof(char*) * count);

    HandleStringSliceIterator iter = get_partition_columns(snapshot);

    // Iterate through all partition columns
    for (;;) {
        bool has_next = string_slice_next(iter, arr, collect_string_visitor);
        if (!has_next) {
            break;
        }
    }

    free_string_slice_data(iter);
    return arr;
}

// Helper to free string array
void free_string_array(StringArray* arr) {
    if (arr) {
        for (uintptr_t i = 0; i < arr->len; i++) {
            free(arr->strings[i]);
        }
        free(arr->strings);
        free(arr);
    }
}

// Schema visitor C wrappers - extract handle and call Go callbacks

uintptr_t c_make_field_list(void* data, uintptr_t reserve) {
    uintptr_t handle = *(uintptr_t*)data;
    return goMakeFieldList(handle, reserve);
}

void c_visit_struct(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                    bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitStruct(handle, sibling_list_id, name, is_nullable, child_list_id);
}

void c_visit_string(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                    bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitString(handle, sibling_list_id, name, is_nullable);
}

void c_visit_long(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                  bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitLong(handle, sibling_list_id, name, is_nullable);
}

void c_visit_integer(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                     bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitInteger(handle, sibling_list_id, name, is_nullable);
}

void c_visit_boolean(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                     bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitBoolean(handle, sibling_list_id, name, is_nullable);
}

void c_visit_double(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                    bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitDouble(handle, sibling_list_id, name, is_nullable);
}

void c_visit_short(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                   bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitShort(handle, sibling_list_id, name, is_nullable);
}

void c_visit_byte(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                  bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitByte(handle, sibling_list_id, name, is_nullable);
}

void c_visit_float(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                   bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitFloat(handle, sibling_list_id, name, is_nullable);
}

void c_visit_binary(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                    bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitBinary(handle, sibling_list_id, name, is_nullable);
}

void c_visit_date(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                  bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitDate(handle, sibling_list_id, name, is_nullable);
}

void c_visit_timestamp(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                       bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitTimestamp(handle, sibling_list_id, name, is_nullable);
}

void c_visit_timestamp_ntz(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                           bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitTimestampNtz(handle, sibling_list_id, name, is_nullable);
}

void c_visit_array(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                   bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitArray(handle, sibling_list_id, name, is_nullable, child_list_id);
}

void c_visit_map(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                 bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitMap(handle, sibling_list_id, name, is_nullable, child_list_id);
}

void c_visit_decimal(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                     bool is_nullable, const struct CStringMap* metadata, uint8_t precision, uint8_t scale) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goVisitDecimal(handle, sibling_list_id, name, is_nullable, precision, scale);
}

// Scan metadata iterator helpers

struct EngineError* get_err_scan_metadata_iter(struct ExternResultHandleSharedScanMetadataIterator result) {
    return result.err;
}

HandleSharedScanMetadataIterator get_ok_scan_metadata_iter(struct ExternResultHandleSharedScanMetadataIterator result) {
    return result.ok;
}

struct EngineError* get_err_bool(struct ExternResultbool result) {
    return result.err;
}

bool get_ok_bool(struct ExternResultbool result) {
    return result.ok;
}

struct EngineError* get_err_file_read_result_iter(struct ExternResultHandleExclusiveFileReadResultIterator result) {
    return result.err;
}

HandleExclusiveFileReadResultIterator get_ok_file_read_result_iter(struct ExternResultHandleExclusiveFileReadResultIterator result) {
    return result.ok;
}

// Scan metadata visitor C wrappers are in scan_helpers.c
// Engine data visitor C wrappers are in read_data_helpers.c
