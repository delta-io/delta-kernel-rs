#ifndef SCHEMA_PROJECTION_H
#define SCHEMA_PROJECTION_H

#include "delta_kernel_ffi.h"

// simple schema projection - just column names
typedef struct ColumnProjection {
    const char** columns;
    int count;
} ColumnProjection;

// visitor callback for schema projection
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state);

#endif
