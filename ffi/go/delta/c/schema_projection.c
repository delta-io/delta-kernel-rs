#include "schema_projection.h"
#include <stdlib.h>
#include <string.h>

// Error allocator helper for FFI calls
void* allocate_error_helper(struct KernelStringSlice slice) {
    char* str = (char*)malloc(slice.len + 1);
    if (str) {
        memcpy(str, slice.ptr, slice.len);
        str[slice.len] = '\0';
    }
    return str;
}

// Main callback from kernel to build projected schema
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state) {
    ColumnProjection* projection = (ColumnProjection*)data;

    // Call Go function to build projected schema
    // It will visit the original schema, filter fields, and build the result
    return goBuildProjectedSchema(
        (uintptr_t)projection,
        (uintptr_t)state,
        projection->original_schema
    );
}
