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
