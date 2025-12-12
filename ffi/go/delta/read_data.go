package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
#include "read_data_helpers.h"
*/
import "C"
import (
	"fmt"
	"runtime/cgo"
	"unsafe"
)

// FileReadResultIterator iterates over data batches read from a parquet file
type FileReadResultIterator struct {
	handle C.HandleExclusiveFileReadResultIterator
	engine C.HandleSharedExternEngine
}

// EngineData represents a batch of data from the engine
type EngineData struct {
	handle C.HandleExclusiveEngineData
}

// EngineDataVisitor is called for each batch of engine data
type EngineDataVisitor interface {
	// VisitEngineData is called for each data batch
	// Returns true to continue iteration, false to stop
	VisitEngineData(data *EngineData) bool
}

// FileMeta contains metadata about a file to read
type FileMeta struct {
	Path         string
	LastModified int64
	Size         uint64
}

// ReadParquetFile creates a new iterator for reading data from a parquet file
// This is an internal function - users should use Scan.ReadFile instead
func readParquetFile(engine C.HandleSharedExternEngine, file *FileMeta, physicalSchema C.HandleSharedSchema) (*FileReadResultIterator, error) {
	// Create C FileMeta struct
	cPath := C.CString(file.Path)
	defer C.free(unsafe.Pointer(cPath))

	cFileMeta := C.struct_FileMeta{
		path:          C.struct_KernelStringSlice{ptr: cPath, len: C.uintptr_t(len(file.Path))},
		last_modified: C.int64_t(file.LastModified),
		size:          C.uintptr_t(file.Size),
	}

	result := C.read_parquet_file(engine, &cFileMeta, physicalSchema)

	if result.tag == C.ErrHandleExclusiveFileReadResultIterator {
		errPtr := C.get_err_file_read_result_iter(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error reading parquet file: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error reading parquet file")
	}

	handle := C.get_ok_file_read_result_iter(result)
	return &FileReadResultIterator{
		handle: handle,
		engine: engine,
	}, nil
}

// Next advances the iterator and calls the visitor for the next batch of data
// Returns true if more data is available, false if done
func (it *FileReadResultIterator) Next(visitor EngineDataVisitor) (bool, error) {
	// Create a cgo.Handle to pass the visitor safely to C
	handle := cgo.NewHandle(visitor)
	defer handle.Delete()

	// Pass handle value directly as NullableCvoid (no C memory allocation needed)
	// The handle value itself is passed, not a pointer to it
	handleAsPtr := unsafe.Pointer(uintptr(handle))

	// Call the FFI function with our C callback
	result := C.read_result_next(it.handle, C.NullableCvoid(handleAsPtr), (*[0]byte)(C.c_visit_engine_data))

	if result.tag == C.Errbool {
		// Error occurred
		errPtr := C.get_err_bool(result)
		if errPtr != nil {
			return false, fmt.Errorf("kernel error iterating read result: %d", errPtr.etype)
		}
		return false, fmt.Errorf("unknown error iterating read result")
	}

	// Get the boolean result
	hasMore := bool(C.get_ok_bool(result))
	return hasMore, nil
}

// Length returns the length of the engine data batch
func (ed *EngineData) Length() uint64 {
	return uint64(C.engine_data_length(&ed.handle))
}

// GetRawData returns a pointer to the raw engine data
// This is typically used to convert to Arrow format
func (ed *EngineData) GetRawData() unsafe.Pointer {
	return C.get_raw_engine_data(ed.handle)
}

// TODO: ToArrowRecord - Convert engine data to Arrow Record
// This requires properly handling the Arrow C Data Interface
// For now, use GetRawData() if you need access to the underlying data pointer

// Close releases the iterator resources
func (it *FileReadResultIterator) Close() {
	if it.handle != nil {
		C.free_read_result_iter(it.handle)
		it.handle = nil
	}
}

// Close releases the engine data resources
func (ed *EngineData) Close() {
	if ed.handle != nil {
		C.free_engine_data(ed.handle)
		ed.handle = nil
	}
}

// Go callback implementations that are called from C

//export goVisitEngineData
func goVisitEngineData(handleValue C.uintptr_t, engineDataHandle C.HandleExclusiveEngineData) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(EngineDataVisitor)

	data := &EngineData{handle: engineDataHandle}

	// Call the visitor - if it returns false, we should stop iteration
	// For now, we always continue
	visitor.VisitEngineData(data)
}
