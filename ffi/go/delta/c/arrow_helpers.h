#pragma once

#include "delta_kernel_ffi.h"

// Helper to access Arrow array buffers
void* get_arrow_buffer(struct FFI_ArrowArray* array, int64_t index);

// Helper to get Arrow schema format string
const char* get_arrow_format(struct FFI_ArrowSchema* schema);

// Helper to get Arrow schema name
const char* get_arrow_name(struct FFI_ArrowSchema* schema);

// Helper to get child schema
struct FFI_ArrowSchema* get_arrow_child_schema(struct FFI_ArrowSchema* schema, int64_t index);

// Helper to get child array
struct FFI_ArrowArray* get_arrow_child_array(struct FFI_ArrowArray* array, int64_t index);
