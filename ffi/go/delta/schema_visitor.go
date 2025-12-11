package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
// Note: C wrapper functions are in helpers.c
*/
import "C"
import (
	"runtime/cgo"
)

// SchemaVisitor is the interface for visiting schema elements
// The kernel will call these methods as it traverses the schema tree
type SchemaVisitor interface {
	// MakeFieldList creates a new field list with optional capacity reservation
	// Returns the list ID
	MakeFieldList(reserve int) int

	// VisitStruct is called for struct types (including top-level schema)
	// childListID contains the fields of the struct
	VisitStruct(siblingListID int, name string, nullable bool, childListID int)

	// VisitString is called for string fields
	VisitString(siblingListID int, name string, nullable bool)

	// VisitLong is called for long (int64) fields
	VisitLong(siblingListID int, name string, nullable bool)

	// VisitInteger is called for integer (int32) fields
	VisitInteger(siblingListID int, name string, nullable bool)

	// VisitBoolean is called for boolean fields
	VisitBoolean(siblingListID int, name string, nullable bool)

	// VisitDouble is called for double (float64) fields
	VisitDouble(siblingListID int, name string, nullable bool)

	// TODO: Add more visit methods as needed (array, map, decimal, etc.)
}

// visitSchemaWithVisitor calls the FFI visit_schema with a Go visitor
func visitSchemaWithVisitor(schemaHandle C.HandleSharedSchema, visitor SchemaVisitor) (int, error) {
	// Create a cgo.Handle to pass the visitor safely to C
	handle := cgo.NewHandle(visitor)
	defer handle.Delete()

	// Allocate space for handle on C heap to avoid Go pointer issues
	handlePtr := C.malloc(C.sizeof_uintptr_t)
	defer C.free(handlePtr)
	*(*C.uintptr_t)(handlePtr) = C.uintptr_t(handle)

	// Create the C visitor struct
	// Cast function pointers to the correct types
	cVisitor := C.struct_EngineSchemaVisitor{
		data:            handlePtr,
		make_field_list: (*[0]byte)(C.c_make_field_list),
		visit_struct:    (*[0]byte)(C.c_visit_struct),
		visit_string:    (*[0]byte)(C.c_visit_string),
		visit_long:      (*[0]byte)(C.c_visit_long),
		visit_integer:   (*[0]byte)(C.c_visit_integer),
		visit_boolean:   (*[0]byte)(C.c_visit_boolean),
		visit_double:    (*[0]byte)(C.c_visit_double),
		// TODO: Set other visitor functions as we implement them
	}

	// Call the FFI function
	result := C.visit_schema(schemaHandle, &cVisitor)

	return int(result), nil
}

// Go callback implementations that are called from C
// These extract the handle and call the actual Go visitor methods

//export goMakeFieldList
func goMakeFieldList(handleValue C.uintptr_t, reserve C.uintptr_t) C.uintptr_t {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	listID := visitor.MakeFieldList(int(reserve))
	return C.uintptr_t(listID)
}

//export goVisitStruct
func goVisitStruct(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool, childListID C.uintptr_t) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitStruct(int(siblingListID), nameStr, bool(nullable), int(childListID))
}

//export goVisitString
func goVisitString(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitString(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitLong
func goVisitLong(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitLong(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitInteger
func goVisitInteger(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitInteger(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitBoolean
func goVisitBoolean(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitBoolean(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitDouble
func goVisitDouble(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitDouble(int(siblingListID), nameStr, bool(nullable))
}
