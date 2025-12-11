#pragma once

#include "delta_kernel_ffi.h"

// C wrapper functions
// Note: Go exports (goVisitScanMetadata, goVisitScanFile) are automatically declared by CGO
void c_visit_scan_metadata(void* data, HandleSharedScanMetadata scanMetadata);
void c_visit_scan_file(void* data, struct KernelStringSlice path, int64_t size,
                       const struct Stats* stats, const struct CDvInfo* dv_info,
                       const struct Expression* transform, const struct CStringMap* partition_map);
