package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
// Note: helpers.c is included in snapshot.go to avoid duplicate symbols
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
