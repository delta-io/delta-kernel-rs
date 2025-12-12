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

// Error allocator helper for FFI calls
void* allocate_error_helper(struct KernelStringSlice slice);

// Accessor functions for ExternResultusize union
static inline uintptr_t get_ok_field_id(struct ExternResultusize result) {
    return result.ok;
}

static inline struct EngineError* get_err_field_id(struct ExternResultusize result) {
    return result.err;
}

// FFI functions for building schema fields (from delta_kernel_ffi)
// These are not in the generated header but exist in the library
extern struct ExternResultusize visit_field_string(struct KernelSchemaVisitorState *state,
                                                   struct KernelStringSlice name,
                                                   bool nullable,
                                                   AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_long(struct KernelSchemaVisitorState *state,
                                                 struct KernelStringSlice name,
                                                 bool nullable,
                                                 AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_integer(struct KernelSchemaVisitorState *state,
                                                    struct KernelStringSlice name,
                                                    bool nullable,
                                                    AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_short(struct KernelSchemaVisitorState *state,
                                                  struct KernelStringSlice name,
                                                  bool nullable,
                                                  AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_byte(struct KernelSchemaVisitorState *state,
                                                 struct KernelStringSlice name,
                                                 bool nullable,
                                                 AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_float(struct KernelSchemaVisitorState *state,
                                                  struct KernelStringSlice name,
                                                  bool nullable,
                                                  AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_double(struct KernelSchemaVisitorState *state,
                                                   struct KernelStringSlice name,
                                                   bool nullable,
                                                   AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_boolean(struct KernelSchemaVisitorState *state,
                                                    struct KernelStringSlice name,
                                                    bool nullable,
                                                    AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_binary(struct KernelSchemaVisitorState *state,
                                                   struct KernelStringSlice name,
                                                   bool nullable,
                                                   AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_date(struct KernelSchemaVisitorState *state,
                                                 struct KernelStringSlice name,
                                                 bool nullable,
                                                 AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_timestamp(struct KernelSchemaVisitorState *state,
                                                      struct KernelStringSlice name,
                                                      bool nullable,
                                                      AllocateErrorFn allocate_error);
extern struct ExternResultusize visit_field_timestamp_ntz(struct KernelSchemaVisitorState *state,
                                                          struct KernelStringSlice name,
                                                          bool nullable,
                                                          AllocateErrorFn allocate_error);

// visitor callback for schema projection
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state);

// Go callback - implemented in Go for building projected schema
extern uintptr_t goBuildProjectedSchema(uintptr_t projection, uintptr_t state, HandleSharedSchema original_schema);

#endif
