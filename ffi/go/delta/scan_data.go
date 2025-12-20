package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "helpers.h"
#include "scan_helpers.h"
*/
import "C"
import (
	"fmt"
	"runtime/cgo"
)

// ScanMetadataIterator iterates over scan metadata chunks
type ScanMetadataIterator struct {
	handle C.HandleSharedScanMetadataIterator
	engine C.HandleSharedExternEngine
}

// ScanMetadataVisitor is called for each chunk of scan metadata
type ScanMetadataVisitor interface {
	// VisitScanMetadata is called for each scan metadata chunk
	// Returns true to continue iteration, false to stop
	VisitScanMetadata(metadata *ScanMetadata) bool
}

// ScanMetadata represents a chunk of scan metadata
type ScanMetadata struct {
	handle C.HandleSharedScanMetadata
}

// FileVisitor is called for each file in the scan
type FileVisitor interface {
	// VisitFile is called for each file to scan
	VisitFile(path string, size int64, stats *Stats, partitionValues map[string]string)
}

// Stats represents file statistics
type Stats struct {
	NumRecords int64
}

// MetadataIterator creates a new scan metadata iterator
func (sc *Scan) MetadataIterator(engine C.HandleSharedExternEngine) (*ScanMetadataIterator, error) {
	result := C.scan_metadata_iter_init(engine, sc.handle)

	if result.tag == C.ErrHandleSharedScanMetadataIterator {
		errPtr := C.get_err_scan_metadata_iter(result)
		if errPtr != nil {
			return nil, fmt.Errorf("kernel error creating scan metadata iterator: %d", errPtr.etype)
		}
		return nil, fmt.Errorf("unknown error creating scan metadata iterator")
	}

	handle := C.get_ok_scan_metadata_iter(result)
	return &ScanMetadataIterator{
		handle: handle,
		engine: engine,
	}, nil
}

// Next advances the iterator and calls the visitor for the next chunk of scan metadata
// Returns true if more data is available, false if done
func (it *ScanMetadataIterator) Next(visitor ScanMetadataVisitor) (bool, error) {
	// Create a cgo.Handle to pass the visitor safely to C
	handle := cgo.NewHandle(visitor)
	defer handle.Delete()

	// Allocate space for handle on C heap
	handlePtr := C.malloc(C.sizeof_uintptr_t)
	defer C.free(handlePtr)
	*(*C.uintptr_t)(handlePtr) = C.uintptr_t(handle)

	// Call the FFI function
	result := C.scan_metadata_next(it.handle, C.NullableCvoid(handlePtr), (*[0]byte)(C.c_visit_scan_metadata))

	if result.tag == C.Errbool {
		// Error occurred
		errPtr := C.get_err_bool(result)
		if errPtr != nil {
			return false, fmt.Errorf("kernel error iterating scan metadata: %d", errPtr.etype)
		}
		return false, fmt.Errorf("unknown error iterating scan metadata")
	}

	// Get the boolean result
	hasMore := bool(C.get_ok_bool(result))
	return hasMore, nil
}

// VisitFiles iterates over all files in this scan metadata chunk
func (sm *ScanMetadata) VisitFiles(visitor FileVisitor) error {
	// Create a cgo.Handle to pass the visitor safely to C
	handle := cgo.NewHandle(visitor)
	defer handle.Delete()

	// Allocate space for handle on C heap
	handlePtr := C.malloc(C.sizeof_uintptr_t)
	defer C.free(handlePtr)
	*(*C.uintptr_t)(handlePtr) = C.uintptr_t(handle)

	// Call the FFI function to visit each file
	C.visit_scan_metadata(sm.handle, C.NullableCvoid(handlePtr), (*[0]byte)(C.c_visit_scan_file))

	return nil
}

// Close releases the iterator resources
func (it *ScanMetadataIterator) Close() {
	if it.handle != nil {
		C.free_scan_metadata_iter(it.handle)
		it.handle = nil
	}
}

// Close releases the scan metadata resources
func (sm *ScanMetadata) Close() {
	if sm.handle != nil {
		C.free_scan_metadata(sm.handle)
		sm.handle = nil
	}
}

// Go callback implementations that are called from C

//export goVisitScanMetadata
func goVisitScanMetadata(handleValue C.uintptr_t, scanMetadataHandle C.HandleSharedScanMetadata) {
	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(ScanMetadataVisitor)

	metadata := &ScanMetadata{handle: scanMetadataHandle}

	// Call the visitor - if it returns false, we should stop iteration
	// For now, we always continue
	visitor.VisitScanMetadata(metadata)
}

//export goVisitScanFile
func goVisitScanFile(
	handleValue C.uintptr_t,
	path C.struct_KernelStringSlice,
	size C.int64_t,
	stats *C.struct_Stats,
	dv_info *C.struct_CDvInfo,
	transform *C.struct_Expression,
	partition_map *C.struct_CStringMap,
) {
	// Unused parameters for now
	_ = dv_info
	_ = transform

	handle := cgo.Handle(handleValue)
	visitor := handle.Value().(FileVisitor)

	pathStr := C.GoStringN(path.ptr, C.int(path.len))

	// Convert stats if available
	var goStats *Stats
	if stats != nil {
		goStats = &Stats{
			NumRecords: int64(stats.num_records),
		}
	}

	// Convert partition values if available
	var goPartitionValues map[string]string
	if partition_map != nil {
		// TODO: Implement partition values conversion
		goPartitionValues = make(map[string]string)
	}

	visitor.VisitFile(pathStr, int64(size), goStats, goPartitionValues)
}

// C wrapper implementations are in scan_data_cgo.c
