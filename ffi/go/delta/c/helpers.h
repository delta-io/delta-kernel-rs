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

// Schema visitor C wrappers (call back to Go)
extern uintptr_t goMakeFieldList(uintptr_t handle, uintptr_t reserve);
extern void goVisitStruct(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable, uintptr_t childListID);
extern void goVisitArray(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable, uintptr_t childListID);
extern void goVisitMap(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable, uintptr_t childListID);
extern void goVisitDecimal(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable, uint8_t precision, uint8_t scale);
extern void goVisitString(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitLong(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitInteger(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitShort(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitByte(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitFloat(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitDouble(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitBoolean(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitBinary(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitDate(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitTimestamp(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);
extern void goVisitTimestampNtz(uintptr_t handle, uintptr_t siblingListID, struct KernelStringSlice name, bool nullable);

uintptr_t c_make_field_list(void* data, uintptr_t reserve);
void c_visit_struct(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id);
void c_visit_array(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id);
void c_visit_map(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata, uintptr_t child_list_id);
void c_visit_decimal(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata, uint8_t precision, uint8_t scale);
void c_visit_string(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_long(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_integer(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_short(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_byte(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_float(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_double(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_boolean(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_binary(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_date(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_timestamp(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);
void c_visit_timestamp_ntz(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const struct CStringMap* metadata);

// Scan metadata iterator helpers
struct EngineError* get_err_scan_metadata_iter(struct ExternResultHandleSharedScanMetadataIterator result);
HandleSharedScanMetadataIterator get_ok_scan_metadata_iter(struct ExternResultHandleSharedScanMetadataIterator result);
struct EngineError* get_err_bool(struct ExternResultbool result);
bool get_ok_bool(struct ExternResultbool result);
