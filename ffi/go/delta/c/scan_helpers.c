#include "scan_helpers.h"

// C wrappers that extract handle and call Go callbacks

// Forward declare Go exports (will be available from CGO when this is included)
extern void goVisitScanMetadata(uintptr_t handle, HandleSharedScanMetadata scanMetadata);
extern void goVisitScanFile(uintptr_t handle, struct KernelStringSlice path, int64_t size,
                             const struct Stats* stats, const struct CDvInfo* dv_info,
                             const struct Expression* transform, const struct CStringMap* partition_map);

void c_visit_scan_metadata(void* data, HandleSharedScanMetadata scanMetadata) {
    uintptr_t handle = *(uintptr_t*)data;
    goVisitScanMetadata(handle, scanMetadata);
}

void c_visit_scan_file(void* data, struct KernelStringSlice path, int64_t size,
                       const struct Stats* stats, const struct CDvInfo* dv_info,
                       const struct Expression* transform, const struct CStringMap* partition_map) {
    uintptr_t handle = *(uintptr_t*)data;
    goVisitScanFile(handle, path, size, stats, dv_info, transform, partition_map);
}
