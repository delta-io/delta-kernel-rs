#ifndef SCHEMA_PROJECTION_H
#define SCHEMA_PROJECTION_H

#include "delta_kernel_ffi.h"

// schema projection data - stores column names and original schema
typedef struct ColumnProjection {
    const char** columns;
    int count;
    HandleSharedSchema original_schema;
} ColumnProjection;

// projection filter context - passed to filtering visitor
typedef struct ProjectionFilterContext {
    ColumnProjection* projection;
    struct KernelSchemaVisitorState* target_state;
    uintptr_t target_list_id;
} ProjectionFilterContext;

// visitor callback for schema projection
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state);

// go callbacks - implemented in Go for filtering
extern uintptr_t goProjectionMakeFieldList(uintptr_t handle, uintptr_t reserve);
extern void goProjectionVisitString(uintptr_t handle, uintptr_t sibling_list_id,
                                     struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitLong(uintptr_t handle, uintptr_t sibling_list_id,
                                   struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitInteger(uintptr_t handle, uintptr_t sibling_list_id,
                                      struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitShort(uintptr_t handle, uintptr_t sibling_list_id,
                                    struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitByte(uintptr_t handle, uintptr_t sibling_list_id,
                                   struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitFloat(uintptr_t handle, uintptr_t sibling_list_id,
                                    struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitDouble(uintptr_t handle, uintptr_t sibling_list_id,
                                     struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitBoolean(uintptr_t handle, uintptr_t sibling_list_id,
                                      struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitBinary(uintptr_t handle, uintptr_t sibling_list_id,
                                     struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitDate(uintptr_t handle, uintptr_t sibling_list_id,
                                   struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitTimestamp(uintptr_t handle, uintptr_t sibling_list_id,
                                        struct KernelStringSlice name, bool nullable);
extern void goProjectionVisitTimestampNtz(uintptr_t handle, uintptr_t sibling_list_id,
                                           struct KernelStringSlice name, bool nullable);

// C wrapper functions for filtering visitor
uintptr_t c_projection_make_field_list(void* data, uintptr_t reserve);
void c_projection_visit_string(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_long(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_integer(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                 bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_short(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                               bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_byte(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_float(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                               bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_double(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_boolean(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                 bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_binary(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_date(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                              bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_timestamp(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                   bool is_nullable, const struct CStringMap* metadata);
void c_projection_visit_timestamp_ntz(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name,
                                       bool is_nullable, const struct CStringMap* metadata);

#endif
