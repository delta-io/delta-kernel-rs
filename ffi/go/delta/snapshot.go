package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
#include "c/helpers.c"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Snapshot represents a Delta Lake table snapshot
type Snapshot struct {
	handle C.HandleSharedSnapshot
	engine C.HandleSharedExternEngine
}

// getDefaultEngine creates a default engine for the given table path
func getDefaultEngine(tablePath string) (C.HandleSharedExternEngine, error) {
	cPath := C.CString(tablePath)
	defer C.free(unsafe.Pointer(cPath))

	pathSlice := C.struct_KernelStringSlice{
		ptr: cPath,
		len: C.uintptr_t(len(tablePath)),
	}

	// Get engine builder
	builderResult := C.get_engine_builder(pathSlice, nil)

	if builderResult.tag == C.ErrEngineBuilder {
		return nil, fmt.Errorf("failed to get engine builder")
	}

	builder := C.get_ok_builder(builderResult)

	// Build the engine
	engineResult := C.builder_build(builder)

	if engineResult.tag == C.ErrHandleSharedExternEngine {
		errPtr := C.get_err_engine(engineResult)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error creating engine: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating engine")
	}

	return C.get_ok_engine(engineResult), nil
}

// NewSnapshot creates a new snapshot from a table path
func NewSnapshot(tablePath string) (*Snapshot, error) {
	// Create default engine first
	engine, err := getDefaultEngine(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	// Create C string slice
	cPath := C.CString(tablePath)
	defer C.free(unsafe.Pointer(cPath))

	pathSlice := C.struct_KernelStringSlice{
		ptr: cPath,
		len: C.uintptr_t(len(tablePath)),
	}

	// Call snapshot function with engine
	result := C.snapshot(pathSlice, engine)

	// Check for error
	if result.tag == C.ErrHandleSharedSnapshot {
		// Extract error from union using helper
		errPtr := C.get_err_snapshot(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating snapshot")
	}

	// Extract handle from union using helper
	handle := C.get_ok_snapshot(result)

	return &Snapshot{handle: handle, engine: engine}, nil
}

// NewSnapshotAtVersion creates a snapshot at a specific version
func NewSnapshotAtVersion(tablePath string, version uint64) (*Snapshot, error) {
	// Create default engine first
	engine, err := getDefaultEngine(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	cPath := C.CString(tablePath)
	defer C.free(unsafe.Pointer(cPath))

	pathSlice := C.struct_KernelStringSlice{
		ptr: cPath,
		len: C.uintptr_t(len(tablePath)),
	}

	// Call with engine and version
	result := C.snapshot_at_version(pathSlice, engine, C.Version(version))

	if result.tag == C.ErrHandleSharedSnapshot {
		errPtr := C.get_err_snapshot(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating snapshot at version")
	}

	handle := C.get_ok_snapshot(result)

	return &Snapshot{handle: handle, engine: engine}, nil
}

// Version returns the version number of this snapshot
func (s *Snapshot) Version() uint64 {
	return uint64(C.version(s.handle))
}

// Engine returns the engine handle for this snapshot
func (s *Snapshot) Engine() C.HandleSharedExternEngine {
	return s.engine
}

// TableRoot returns the root path of the Delta table
func (s *Snapshot) TableRoot() (string, error) {
	// Call our C wrapper that uses the allocator
	cStr := C.get_snapshot_table_root(s.handle)
	if cStr == nil {
		return "", fmt.Errorf("failed to get table root")
	}
	defer C.free(unsafe.Pointer(cStr))

	// Convert C string to Go string
	return C.GoString(cStr), nil
}

// PartitionColumns returns the list of partition column names for this table
func (s *Snapshot) PartitionColumns() ([]string, error) {
	// Use C helper to get partition columns
	arr := C.get_partition_columns_helper(s.handle)
	if arr == nil {
		return nil, fmt.Errorf("failed to get partition columns")
	}
	defer C.free_string_array(arr)

	// Convert C string array to Go slice
	count := int(arr.len)
	if count == 0 {
		return []string{}, nil
	}

	result := make([]string, count)
	// Access the C array of strings
	cStrings := (*[1 << 30]*C.char)(unsafe.Pointer(arr.strings))[:count:count]
	for i := 0; i < count; i++ {
		result[i] = C.GoString(cStrings[i])
	}

	return result, nil
}

// Close releases the snapshot and engine resources
func (s *Snapshot) Close() {
	if s.handle != nil {
		C.free_snapshot(s.handle)
		s.handle = nil
	}
	if s.engine != nil {
		C.free_engine(s.engine)
		s.engine = nil
	}
}
