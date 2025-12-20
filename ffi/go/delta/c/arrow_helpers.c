#include "arrow_helpers.h"
#include <stdlib.h>

// Helper to access Arrow array buffers
void* get_arrow_buffer(struct FFI_ArrowArray* array, int64_t index) {
    if (array == NULL || array->buffers == NULL || index >= array->n_buffers) {
        return NULL;
    }
    return (void*)array->buffers[index];
}

// Helper to get Arrow schema format string
const char* get_arrow_format(struct FFI_ArrowSchema* schema) {
    if (schema == NULL) {
        return NULL;
    }
    return schema->format;
}

// Helper to get Arrow schema name
const char* get_arrow_name(struct FFI_ArrowSchema* schema) {
    if (schema == NULL) {
        return NULL;
    }
    return schema->name;
}

// Helper to get child schema
struct FFI_ArrowSchema* get_arrow_child_schema(struct FFI_ArrowSchema* schema, int64_t index) {
    if (schema == NULL || schema->children == NULL || index >= schema->n_children) {
        return NULL;
    }
    return schema->children[index];
}

// Helper to get child array
struct FFI_ArrowArray* get_arrow_child_array(struct FFI_ArrowArray* array, int64_t index) {
    if (array == NULL || array->children == NULL || index >= array->n_children) {
        return NULL;
    }
    return array->children[index];
}
