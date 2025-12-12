package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
#include "schema_projection.h"
// Note: helpers.c and schema_projection.c are included in snapshot.go to avoid duplicate symbols
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
func (s *Snapshot) Scan() (*Scan, error) {
	return s.ScanWithOptions(nil)
}

// ScanOptions configures scan behavior
type ScanOptions struct {
	Columns []string // column projection - nil means all columns
	// TODO: add Predicate field
}

// ScanWithOptions creates a new scan with optional column projection and predicates
func (s *Snapshot) ScanWithOptions(opts *ScanOptions) (*Scan, error) {
	var engineSchema *C.struct_EngineSchema

	// TODO: Implement column projection
	// For now, column projection is not implemented - we always read all columns
	// The API is in place but the actual filtering logic needs to be completed
	if opts != nil && len(opts.Columns) > 0 {
		// Projection requested but not yet implemented - just use nil engineSchema
		engineSchema = nil
	}

	result := C.scan(s.handle, s.engine, nil, engineSchema)

	if result.tag == C.ErrHandleSharedScan {
		errPtr := C.get_err_scan(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error creating scan: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating scan")
	}

	handle := C.get_ok_scan(result)
	scan := &Scan{handle: handle}

	scan.logicalSchema = C.scan_logical_schema(scan.handle)
	scan.physicalSchema = C.scan_physical_schema(scan.handle)

	return scan, nil
}

// LogicalSchema returns the logical schema of the scan
// The logical schema represents the user-facing schema with all transformations applied
func (sc *Scan) LogicalSchema() (*Schema, error) {
	if sc.logicalSchema == nil {
		return nil, fmt.Errorf("logical schema not available")
	}

	// Create a schema builder visitor
	builder := NewSchemaBuilder()

	// Visit the schema to extract fields
	rootListID, err := visitSchemaWithVisitor(sc.logicalSchema, builder)
	if err != nil {
		return nil, fmt.Errorf("failed to visit logical schema: %w", err)
	}

	// Build and return the schema
	return builder.Build(rootListID), nil
}

// PhysicalSchema returns the physical schema of the scan
// The physical schema represents the actual schema in the data files
func (sc *Scan) PhysicalSchema() (*Schema, error) {
	if sc.physicalSchema == nil {
		return nil, fmt.Errorf("physical schema not available")
	}

	// Create a schema builder visitor
	builder := NewSchemaBuilder()

	// Visit the schema to extract fields
	rootListID, err := visitSchemaWithVisitor(sc.physicalSchema, builder)
	if err != nil {
		return nil, fmt.Errorf("failed to visit physical schema: %w", err)
	}

	// Build and return the schema
	return builder.Build(rootListID), nil
}

// PhysicalSchemaHandle returns the raw C handle for the physical schema
// This is useful when you need to pass the schema to FFI functions
func (sc *Scan) PhysicalSchemaHandle() C.HandleSharedSchema {
	return sc.physicalSchema
}

// ReadFile creates an iterator to read data from a parquet file
// The engine parameter should typically come from snapshot.Engine()
func (sc *Scan) ReadFile(engine C.HandleSharedExternEngine, file *FileMeta) (*FileReadResultIterator, error) {
	return readParquetFile(engine, file, sc.physicalSchema)
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

// Go callback stubs for projection filtering
// TODO: Implement actual filtering logic

//export goProjectionMakeFieldList
func goProjectionMakeFieldList(handleValue C.uintptr_t, reserve C.uintptr_t) C.uintptr_t {
	return 0
}

//export goProjectionVisitString
func goProjectionVisitString(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitLong
func goProjectionVisitLong(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitInteger
func goProjectionVisitInteger(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitShort
func goProjectionVisitShort(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitByte
func goProjectionVisitByte(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitFloat
func goProjectionVisitFloat(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitDouble
func goProjectionVisitDouble(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitBoolean
func goProjectionVisitBoolean(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitBinary
func goProjectionVisitBinary(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitDate
func goProjectionVisitDate(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitTimestamp
func goProjectionVisitTimestamp(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}

//export goProjectionVisitTimestampNtz
func goProjectionVisitTimestampNtz(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
}
