#include "read_data_helpers.h"

// Forward declare Go export
extern void goVisitEngineData(uintptr_t handle, HandleExclusiveEngineData engineData);

// C wrapper that receives handle value directly (not a pointer to it)
void c_visit_engine_data(void* data, HandleExclusiveEngineData engineData) {
    // data IS the handle value (cast from uintptr_t), not a pointer to it
    uintptr_t handle = (uintptr_t)data;
    goVisitEngineData(handle, engineData);
}
