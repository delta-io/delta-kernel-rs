#include "read_data_helpers.h"

// Forward declare Go export
extern void goVisitEngineData(uintptr_t handle, HandleExclusiveEngineData engineData);

// C wrapper that extracts handle and calls Go callback
void c_visit_engine_data(void* data, HandleExclusiveEngineData engineData) {
    uintptr_t handle = *(uintptr_t*)data;
    goVisitEngineData(handle, engineData);
}
