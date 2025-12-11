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

	// Complex type visitors (have children)
	VisitStruct(siblingListID int, name string, nullable bool, childListID int)
	VisitArray(siblingListID int, name string, nullable bool, childListID int)
	VisitMap(siblingListID int, name string, nullable bool, childListID int)

	// Decimal type with precision and scale
	VisitDecimal(siblingListID int, name string, nullable bool, precision uint8, scale uint8)

	// Simple type visitors
	VisitString(siblingListID int, name string, nullable bool)
	VisitLong(siblingListID int, name string, nullable bool)
	VisitInteger(siblingListID int, name string, nullable bool)
	VisitShort(siblingListID int, name string, nullable bool)
	VisitByte(siblingListID int, name string, nullable bool)
	VisitFloat(siblingListID int, name string, nullable bool)
	VisitDouble(siblingListID int, name string, nullable bool)
	VisitBoolean(siblingListID int, name string, nullable bool)
	VisitBinary(siblingListID int, name string, nullable bool)
	VisitDate(siblingListID int, name string, nullable bool)
	VisitTimestamp(siblingListID int, name string, nullable bool)
	VisitTimestampNtz(siblingListID int, name string, nullable bool)
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
		data:                handlePtr,
		make_field_list:     (*[0]byte)(C.c_make_field_list),
		visit_struct:        (*[0]byte)(C.c_visit_struct),
		visit_array:         (*[0]byte)(C.c_visit_array),
		visit_map:           (*[0]byte)(C.c_visit_map),
		visit_decimal:       (*[0]byte)(C.c_visit_decimal),
		visit_string:        (*[0]byte)(C.c_visit_string),
		visit_long:          (*[0]byte)(C.c_visit_long),
		visit_integer:       (*[0]byte)(C.c_visit_integer),
		visit_short:         (*[0]byte)(C.c_visit_short),
		visit_byte:          (*[0]byte)(C.c_visit_byte),
		visit_float:         (*[0]byte)(C.c_visit_float),
		visit_double:        (*[0]byte)(C.c_visit_double),
		visit_boolean:       (*[0]byte)(C.c_visit_boolean),
		visit_binary:        (*[0]byte)(C.c_visit_binary),
		visit_date:          (*[0]byte)(C.c_visit_date),
		visit_timestamp:     (*[0]byte)(C.c_visit_timestamp),
		visit_timestamp_ntz: (*[0]byte)(C.c_visit_timestamp_ntz),
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

//export goVisitShort
func goVisitShort(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitShort(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitByte
func goVisitByte(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitByte(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitFloat
func goVisitFloat(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitFloat(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitBinary
func goVisitBinary(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitBinary(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitDate
func goVisitDate(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitDate(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitTimestamp
func goVisitTimestamp(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitTimestamp(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitTimestampNtz
func goVisitTimestampNtz(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitTimestampNtz(int(siblingListID), nameStr, bool(nullable))
}

//export goVisitArray
func goVisitArray(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool, childListID C.uintptr_t) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitArray(int(siblingListID), nameStr, bool(nullable), int(childListID))
}

//export goVisitMap
func goVisitMap(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool, childListID C.uintptr_t) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitMap(int(siblingListID), nameStr, bool(nullable), int(childListID))
}

//export goVisitDecimal
func goVisitDecimal(handleValue C.uintptr_t, siblingListID C.uintptr_t, name C.struct_KernelStringSlice, nullable C.bool, precision C.uint8_t, scale C.uint8_t) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(SchemaVisitor)
	nameStr := C.GoStringN(name.ptr, C.int(name.len))
	visitor.VisitDecimal(int(siblingListID), nameStr, bool(nullable), uint8(precision), uint8(scale))
}
