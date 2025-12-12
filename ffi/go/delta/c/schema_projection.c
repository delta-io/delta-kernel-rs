#include "schema_projection.h"
#include <stdlib.h>
#include <string.h>

// C wrapper functions that extract handle and call Go callbacks
uintptr_t c_projection_make_field_list(void* data, uintptr_t reserve) {
    uintptr_t handle = *(uintptr_t*)data;
    return goProjectionMakeFieldList(handle, reserve);
}

void c_projection_visit_string(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitString(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_long(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitLong(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_integer(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                 bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitInteger(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_short(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                               bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitShort(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_byte(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitByte(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_float(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                               bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitFloat(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_double(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitDouble(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_boolean(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                 bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitBoolean(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_binary(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitBinary(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_date(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitDate(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_timestamp(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                   bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitTimestamp(handle, sibling_list_id, name, is_nullable);
}

void c_projection_visit_timestamp_ntz(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                       bool is_nullable, const struct CStringMap* metadata) {
    (void)metadata; // unused for now
    uintptr_t handle = *(uintptr_t*)data;
    goProjectionVisitTimestampNtz(handle, sibling_list_id, name, is_nullable);
}

// Main callback from kernel to build projected schema
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state) {
    ColumnProjection* projection = (ColumnProjection*)data;

    // TODO: Implement actual schema projection filtering
    // For now, just visit the original schema as-is
    // The proper implementation requires:
    // 1. Visit original schema to get all fields
    // 2. Filter fields based on projection->columns list
    // 3. Build new schema with only matching fields using state callbacks
    //
    // This is complex because we need to call state's visitor methods
    // to build the filtered schema, which requires understanding the
    // exact field types and structure.

    (void)state;
    (void)projection;

    // Return 0 to indicate we're using the original schema for now
    return 0;
}
