package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include <stdlib.h>

// Helper to extract ok value from union
static inline HandleSharedSnapshot get_ok_snapshot(struct ExternResultHandleSharedSnapshot result) {
    return result.ok;
}

// Helper to extract err value from union
static inline struct EngineError* get_err_snapshot(struct ExternResultHandleSharedSnapshot result) {
    return result.err;
}

// Helper to get engine from result
static inline HandleSharedExternEngine get_ok_engine(struct ExternResultHandleSharedExternEngine result) {
    return result.ok;
}

// Helper to get error from engine result
static inline struct EngineError* get_err_engine(struct ExternResultHandleSharedExternEngine result) {
    return result.err;
}

// Helper to get builder from result
static inline struct EngineBuilder* get_ok_builder(struct ExternResultEngineBuilder result) {
    return result.ok;
}

// Helper to get error from builder result
static inline struct EngineError* get_err_builder(struct ExternResultEngineBuilder result) {
    return result.err;
}
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
