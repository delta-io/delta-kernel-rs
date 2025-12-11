package delta

// This file defines C wrapper functions that call the Go exports
// It's in a separate file so it's compiled after the Go exports are processed

/*
#include "delta_kernel_ffi.h"

// Forward declare Go exports (will be resolved by linker)
extern void goVisitScanMetadata(uintptr_t handle, HandleSharedScanMetadata scanMetadata);
extern void goVisitScanFile(uintptr_t handle, struct KernelStringSlice path, int64_t size,
                             const struct Stats* stats, const struct CDvInfo* dv_info,
                             const struct Expression* transform, const struct CStringMap* partition_map);

// C wrappers that extract handle and call Go callbacks
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
*/
import "C"
