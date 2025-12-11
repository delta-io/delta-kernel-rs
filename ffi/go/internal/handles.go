package internal

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// SnapshotHandle wraps a SharedSnapshot handle from FFI
type SnapshotHandle struct {
	handle C.HandleSharedSnapshot
}

// NewSnapshotHandle creates a new snapshot handle wrapper
func NewSnapshotHandle(handle C.HandleSharedSnapshot) *SnapshotHandle {
	return &SnapshotHandle{handle: handle}
}

// Handle returns the underlying C handle
func (s *SnapshotHandle) Handle() C.HandleSharedSnapshot {
	return s.handle
}

// Free releases the snapshot handle
func (s *SnapshotHandle) Free() {
	if s.handle != nil {
		C.free_snapshot(s.handle)
		s.handle = nil
	}
}

// SchemaHandle wraps a SharedSchema handle from FFI
type SchemaHandle struct {
	handle C.HandleSharedSchema
}

// NewSchemaHandle creates a new schema handle wrapper
func NewSchemaHandle(handle C.HandleSharedSchema) *SchemaHandle {
	return &SchemaHandle{handle: handle}
}

// Handle returns the underlying C handle
func (s *SchemaHandle) Handle() C.HandleSharedSchema {
	return s.handle
}

// Free releases the schema handle
func (s *SchemaHandle) Free() {
	if s.handle != nil {
		C.free_schema(s.handle)
		s.handle = nil
	}
}

// StringSlice creates a C.KernelStringSlice from a Go string
func StringSlice(s string) C.struct_KernelStringSlice {
	if s == "" {
		return C.struct_KernelStringSlice{
			ptr: nil,
			len: 0,
		}
	}
	return C.struct_KernelStringSlice{
		ptr: (*C.char)(unsafe.Pointer(unsafe.StringData(s))),
		len: C.uintptr_t(len(s)),
	}
}

// GoString converts a C string pointer and length to a Go string
func GoString(ptr *C.char, length C.uintptr_t) string {
	if ptr == nil || length == 0 {
		return ""
	}
	return C.GoStringN(ptr, C.int(length))
}
