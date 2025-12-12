#include "schema_projection.h"
#include <string.h>

// visit schema and only include requested columns
uintptr_t visit_projection_schema(void* data, struct KernelSchemaVisitorState* state) {
    // for now, just return 0 - minimal stub
    // full implementation would:
    // 1. use make_field_list(state, count) to create list
    // 2. visit original schema
    // 3. filter by column names
    // 4. rebuild with only matching columns

    // TODO: implement actual filtering logic
    return 0;
}
