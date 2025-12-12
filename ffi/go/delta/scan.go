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

	// Build engine schema with column projection if requested
	if opts != nil && len(opts.Columns) > 0 {
		// Get the scan's logical schema as the original schema
		// We need to create a temporary scan first to get the full schema
		fullScanResult := C.scan(s.handle, s.engine, nil, nil)
		if fullScanResult.tag == C.ErrHandleSharedScan {
			return nil, fmt.Errorf("failed to get full schema for projection")
		}
		fullScan := C.get_ok_scan(fullScanResult)
		logicalSchema := C.scan_logical_schema(fullScan)

		if logicalSchema == nil {
			C.free_scan(fullScan)
			return nil, fmt.Errorf("failed to get logical schema for projection")
		}

		// Allocate and populate C string array for column names
		cColumns := C.malloc(C.size_t(len(opts.Columns)) * C.size_t(unsafe.Sizeof(uintptr(0))))

		columnArray := (*[1 << 28]*C.char)(cColumns)
		cStrings := make([]*C.char, len(opts.Columns))
		for i, col := range opts.Columns {
			cStrings[i] = C.CString(col)
			columnArray[i] = cStrings[i]
		}

		// Create ColumnProjection struct on heap (needs to outlive this scope)
		projection := (*C.struct_ColumnProjection)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ColumnProjection{}))))
		if projection == nil {
			C.free_scan(fullScan)
			return nil, fmt.Errorf("failed to allocate projection struct")
		}
		projection.columns = (**C.char)(cColumns)
		projection.count = C.int(len(opts.Columns))
		projection.original_schema = logicalSchema

		// Create EngineSchema using helper (handles function pointer correctly)
		engineSchema = C.create_projection_engine_schema(unsafe.Pointer(projection))
		if engineSchema == nil {
			C.free_scan(fullScan)
			return nil, fmt.Errorf("failed to create projection engine schema")
		}

		// Perform the scan with projection
		result := C.scan(s.handle, s.engine, nil, engineSchema)

		// Now cleanup everything (the scan has been created)
		C.free(unsafe.Pointer(engineSchema))
		C.free(unsafe.Pointer(projection))
		for _, cStr := range cStrings {
			C.free(unsafe.Pointer(cStr))
		}
		C.free(cColumns)
		C.free_scan(fullScan)

		// Check scan result
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

// ProjectionBuilder implements SchemaVisitor to build a projected schema
// It visits the original schema and adds only selected fields to the kernel state
type ProjectionBuilder struct {
	projection []string                              // column names to include (in requested order)
	state      *C.struct_KernelSchemaVisitorState    // target state to build schema
	fieldMap   map[string]C.uintptr_t                // maps field name to field ID
}

// NewProjectionBuilder creates a new projection builder
func NewProjectionBuilder(columns []string, state *C.struct_KernelSchemaVisitorState) *ProjectionBuilder {
	return &ProjectionBuilder{
		projection: columns,
		state:      state,
		fieldMap:   make(map[string]C.uintptr_t),
	}
}

// shouldInclude checks if a field name is in the projection
func (p *ProjectionBuilder) shouldInclude(name string) bool {
	for _, col := range p.projection {
		if col == name {
			return true
		}
	}
	return false
}

// MakeFieldList - we don't need this for projection, just return 0
func (p *ProjectionBuilder) MakeFieldList(reserve int) int {
	return 0
}

// Helper to add a field via FFI
func (p *ProjectionBuilder) addFieldViaFFI(name string, nullable bool, visitFn func(C.struct_KernelStringSlice, C.bool) C.struct_ExternResultusize) {
	if !p.shouldInclude(name) {
		return
	}

	// Convert name to C string slice
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	nameSlice := C.struct_KernelStringSlice{
		ptr: cName,
		len: C.uintptr_t(len(name)),
	}

	// Call the FFI function
	result := visitFn(nameSlice, C.bool(nullable))
	if result.tag == C.Okusize {
		fieldID := C.get_ok_field_id(result)
		p.fieldMap[name] = fieldID
	}
}

// Implement SchemaVisitor interface for primitive types
func (p *ProjectionBuilder) VisitString(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_string(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitLong(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_long(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitInteger(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_integer(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitShort(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_short(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitByte(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_byte(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitFloat(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_float(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitDouble(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_double(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitBoolean(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_boolean(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitBinary(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_binary(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitDate(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_date(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitTimestamp(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_timestamp(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

func (p *ProjectionBuilder) VisitTimestampNtz(siblingListID int, name string, nullable bool) {
	p.addFieldViaFFI(name, nullable, func(nameSlice C.struct_KernelStringSlice, n C.bool) C.struct_ExternResultusize {
		return C.visit_field_timestamp_ntz(p.state, nameSlice, n, C.AllocateErrorFn(C.allocate_error_helper))
	})
}

// Complex types - for now, not supported in projection
func (p *ProjectionBuilder) VisitStruct(siblingListID int, name string, nullable bool, childListID int) {
	// TODO: Handle struct projection
}

func (p *ProjectionBuilder) VisitArray(siblingListID int, name string, nullable bool, childListID int) {
	// TODO: Handle array projection
}

func (p *ProjectionBuilder) VisitMap(siblingListID int, name string, nullable bool, childListID int) {
	// TODO: Handle map projection
}

func (p *ProjectionBuilder) VisitDecimal(siblingListID int, name string, nullable bool, precision uint8, scale uint8) {
	// TODO: Handle decimal projection
}

// BuildFinalStruct builds the final projected struct from collected field IDs
// The fields are added in the order specified by the user's projection list
func (p *ProjectionBuilder) BuildFinalStruct() (C.uintptr_t, error) {
	if len(p.fieldMap) == 0 {
		return 0, fmt.Errorf("no fields in projection")
	}

	// Build field IDs array in the user's requested order
	fieldIDs := make([]C.uintptr_t, 0, len(p.projection))
	for _, colName := range p.projection {
		if fieldID, ok := p.fieldMap[colName]; ok {
			fieldIDs = append(fieldIDs, fieldID)
		}
	}

	if len(fieldIDs) == 0 {
		return 0, fmt.Errorf("no fields found in field map")
	}

	// Convert field IDs to C array
	fieldIDsPtr := (*C.uintptr_t)(C.malloc(C.size_t(len(fieldIDs)) * C.size_t(unsafe.Sizeof(C.uintptr_t(0)))))
	defer C.free(unsafe.Pointer(fieldIDsPtr))

	fieldIDsSlice := (*[1 << 30]C.uintptr_t)(unsafe.Pointer(fieldIDsPtr))[:len(fieldIDs):len(fieldIDs)]
	for i, id := range fieldIDs {
		fieldIDsSlice[i] = id
	}

	// Create empty name for root struct (name is ignored for root)
	emptyName := C.struct_KernelStringSlice{
		ptr: nil,
		len: 0,
	}

	// Call visit_field_struct to build the final schema
	result := C.visit_field_struct(
		p.state,
		emptyName,
		fieldIDsPtr,
		C.uintptr_t(len(fieldIDs)),
		C.bool(false), // not nullable
		C.AllocateErrorFn(C.allocate_error_helper),
	)

	if result.tag == C.Okusize {
		return C.get_ok_field_id(result), nil
	}

	return 0, fmt.Errorf("failed to build final struct")
}

//export goBuildProjectedSchema
func goBuildProjectedSchema(projectionPtr C.uintptr_t, statePtr C.uintptr_t, originalSchemaHandle C.HandleSharedSchema) C.uintptr_t {
	// Extract projection columns from C struct
	proj := (*C.struct_ColumnProjection)(unsafe.Pointer(uintptr(projectionPtr)))
	columns := make([]string, int(proj.count))
	colArray := (*[1 << 28]*C.char)(unsafe.Pointer(proj.columns))
	for i := 0; i < int(proj.count); i++ {
		columns[i] = C.GoString(colArray[i])
	}

	// Create projection builder
	state := (*C.struct_KernelSchemaVisitorState)(unsafe.Pointer(uintptr(statePtr)))
	builder := NewProjectionBuilder(columns, state)

	// Visit the original schema with our filtering builder
	_, err := visitSchemaWithVisitor(originalSchemaHandle, builder)
	if err != nil {
		return 0
	}

	// Build and return the final struct
	structID, err := builder.BuildFinalStruct()
	if err != nil {
		return 0
	}

	return structID
}
