#pragma once

#include "delta_kernel_ffi.h"

// String allocation helper
// Allocates a C string from a KernelStringSlice
// Returns void* to match AllocateStringFn signature
void* allocate_string_helper(struct KernelStringSlice slice);

// Union accessors for snapshot results
HandleSharedSnapshot get_ok_snapshot(struct ExternResultHandleSharedSnapshot result);
struct EngineError* get_err_snapshot(struct ExternResultHandleSharedSnapshot result);

// Union accessors for engine results
HandleSharedExternEngine get_ok_engine(struct ExternResultHandleSharedExternEngine result);
struct EngineError* get_err_engine(struct ExternResultHandleSharedExternEngine result);

// Union accessors for builder results
struct EngineBuilder* get_ok_builder(struct ExternResultEngineBuilder result);
struct EngineError* get_err_builder(struct ExternResultEngineBuilder result);

// Union accessors for scan results
HandleSharedScan get_ok_scan(struct ExternResultHandleSharedScan result);
struct EngineError* get_err_scan(struct ExternResultHandleSharedScan result);

// Wrapper functions that use allocator
char* get_snapshot_table_root(HandleSharedSnapshot snapshot);
char* get_scan_table_root(HandleSharedScan scan);

// Partition column helpers
typedef struct {
    char** strings;
    uintptr_t len;
    uintptr_t capacity;
} StringArray;

void collect_string_visitor(void* context, struct KernelStringSlice slice);
StringArray* get_partition_columns_helper(HandleSharedSnapshot snapshot);
void free_string_array(StringArray* arr);
