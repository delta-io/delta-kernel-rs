package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include <stdlib.h>
#include <string.h>

// Helper function to allocate a string from KernelStringSlice
// Returns void* to match AllocateStringFn signature
static void* allocate_string_helper(struct KernelStringSlice slice) {
    char* str = (char*)malloc(slice.len + 1);
    if (str) {
        memcpy(str, slice.ptr, slice.len);
        str[slice.len] = '\0';
    }
    return str;
}

// Helper to extract ok value from scan result
static inline HandleSharedScan get_ok_scan(struct ExternResultHandleSharedScan result) {
    return result.ok;
}

// Helper to extract error from scan result
static inline struct EngineError* get_err_scan(struct ExternResultHandleSharedScan result) {
    return result.err;
}

// Wrapper function to get scan table root using our allocator
static char* get_scan_table_root(HandleSharedScan scan) {
    return (char*)scan_table_root(scan, allocate_string_helper);
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Scan represents a Delta Lake table scan operation
type Scan struct {
	handle         C.HandleSharedScan
	logicalSchema  C.HandleSharedSchema
	physicalSchema C.HandleSharedSchema
}

// Scan creates a new scan of the snapshot
// This allows reading data from the table with optional predicates and schema projection
// TODO: Add predicate and schema parameters for filtering and projection
func (s *Snapshot) Scan() (*Scan, error) {
	// Pass nil for predicate and schema (untyped for now, will be properly typed later)
	result := C.scan(s.handle, s.engine, nil, nil)

	if result.tag == C.ErrHandleSharedScan {
		errPtr := C.get_err_scan(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error creating scan: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating scan")
	}

	handle := C.get_ok_scan(result)
	scan := &Scan{handle: handle}

	// Get schemas immediately
	scan.logicalSchema = C.scan_logical_schema(scan.handle)
	scan.physicalSchema = C.scan_physical_schema(scan.handle)

	return scan, nil
}

// LogicalSchema returns the logical schema of the scan
// The logical schema represents the user-facing schema with all transformations applied
// TODO: Implement proper schema extraction via visitor pattern
func (sc *Scan) LogicalSchema() (*Schema, error) {
	// For now, return placeholder
	// Eventually this will use the visitor pattern to extract fields from sc.logicalSchema
	return &Schema{
		Fields: []*Field{
			{Name: "logical_schema", DataType: "visitor_pattern_not_implemented", Nullable: true},
		},
	}, nil
}

// PhysicalSchema returns the physical schema of the scan
// The physical schema represents the actual schema in the data files
// TODO: Implement proper schema extraction via visitor pattern
func (sc *Scan) PhysicalSchema() (*Schema, error) {
	// For now, return placeholder
	// Eventually this will use the visitor pattern to extract fields from sc.physicalSchema
	return &Schema{
		Fields: []*Field{
			{Name: "physical_schema", DataType: "visitor_pattern_not_implemented", Nullable: true},
		},
	}, nil
}

// TableRoot returns the root path of the table for this scan
func (sc *Scan) TableRoot() (string, error) {
	cStr := C.get_scan_table_root(sc.handle)
	if cStr == nil {
		return "", fmt.Errorf("failed to get scan table root")
	}
	defer C.free(unsafe.Pointer(cStr))

	return C.GoString(cStr), nil
}

// Close releases the scan and schema resources
func (sc *Scan) Close() {
	if sc.logicalSchema != nil {
		C.free_schema(sc.logicalSchema)
		sc.logicalSchema = nil
	}
	if sc.physicalSchema != nil {
		C.free_schema(sc.physicalSchema)
		sc.physicalSchema = nil
	}
	if sc.handle != nil {
		C.free_scan(sc.handle)
		sc.handle = nil
	}
}
