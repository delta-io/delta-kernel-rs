package delta

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -I${SRCDIR}/c -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include "arrow_helpers.h"
#include "arrow_helpers.c"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// ArrowData wraps the Arrow C Data Interface structures
type ArrowData struct {
	array  *C.struct_FFI_ArrowArray
	schema *C.struct_FFI_ArrowSchema
}

// GetArrowData converts EngineData to ArrowData for manual parsing
func (ed *EngineData) GetArrowData(engine C.HandleSharedExternEngine) (*ArrowData, error) {
	result := C.get_raw_arrow_data(ed.handle, engine)

	// The result is a tagged union - we need to check if it succeeded
	// For now, let's try a simpler approach: cast the result directly
	// The OK variant should contain the ArrowFFIData pointer

	// Access the result as a byte array and extract the pointer
	// This is unsafe but necessary for FFI
	resultBytes := (*[unsafe.Sizeof(result)]byte)(unsafe.Pointer(&result))

	// Skip the tag (first few bytes) and get the pointer
	// The pointer should be after the tag in the union
	arrowDataPtr := *(**C.struct_ArrowFFIData)(unsafe.Pointer(&resultBytes[8]))

	if arrowDataPtr == nil {
		return nil, fmt.Errorf("failed to get arrow data")
	}

	return &ArrowData{
		array:  &arrowDataPtr.array,
		schema: &arrowDataPtr.schema,
	}, nil
}

// NumRows returns the number of rows in the Arrow array
func (ad *ArrowData) NumRows() int64 {
	if ad.array == nil {
		return 0
	}
	return int64(ad.array.length)
}

// NumColumns returns the number of columns in the Arrow schema
func (ad *ArrowData) NumColumns() int64 {
	if ad.schema == nil {
		return 0
	}
	return int64(ad.schema.n_children)
}

// ColumnName returns the name of a column
func (ad *ArrowData) ColumnName(index int) string {
	if ad.schema == nil {
		return ""
	}

	childSchema := C.get_arrow_child_schema(ad.schema, C.int64_t(index))
	if childSchema == nil {
		return ""
	}

	name := C.get_arrow_name(childSchema)
	if name == nil {
		return ""
	}

	return C.GoString(name)
}

// ColumnFormat returns the Arrow format string for a column
func (ad *ArrowData) ColumnFormat(index int) string {
	if ad.schema == nil {
		return ""
	}

	childSchema := C.get_arrow_child_schema(ad.schema, C.int64_t(index))
	if childSchema == nil {
		return ""
	}

	format := C.get_arrow_format(childSchema)
	if format == nil {
		return ""
	}

	return C.GoString(format)
}

// GetInt32Value reads an int32 value from a column
func (ad *ArrowData) GetInt32Value(colIndex int, rowIndex int64) (int32, bool) {
	if ad.array == nil {
		return 0, false
	}

	// Get the child array for this column
	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	// Check if value is null (buffer 0 is validity bitmap)
	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false // NULL value
		}
	}

	// Get the data buffer (buffer 1 for fixed-width types)
	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	// Read the int32 value
	dataArray := (*[1 << 30]int32)(dataBuf)
	return dataArray[rowIndex], true
}

// GetInt64Value reads an int64 value from a column
func (ad *ArrowData) GetInt64Value(colIndex int, rowIndex int64) (int64, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]int64)(dataBuf)
	return dataArray[rowIndex], true
}

// GetFloat64Value reads a float64 value from a column
func (ad *ArrowData) GetFloat64Value(colIndex int, rowIndex int64) (float64, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]float64)(dataBuf)
	return dataArray[rowIndex], true
}

// GetStringValue reads a string value from a column
func (ad *ArrowData) GetStringValue(colIndex int, rowIndex int64) (string, bool) {
	if ad.array == nil {
		return "", false
	}

	if colIndex >= int(ad.array.n_children) {
		return "", false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return "", false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return "", false
		}
	}

	// For strings, buffer 1 is offsets, buffer 2 is data
	offsetBuf := C.get_arrow_buffer(childArray, 1)
	dataBuf := C.get_arrow_buffer(childArray, 2)

	if offsetBuf == nil || dataBuf == nil {
		return "", false
	}

	// Read offsets (int32 for regular strings)
	offsetArray := (*[1 << 30]int32)(offsetBuf)
	start := offsetArray[rowIndex]
	end := offsetArray[rowIndex+1]

	// Read string data
	dataBytes := (*[1 << 30]byte)(dataBuf)
	strBytes := make([]byte, end-start)
	for i := int32(0); i < end-start; i++ {
		strBytes[i] = dataBytes[start+i]
	}

	return string(strBytes), true
}

// GetBooleanValue reads a boolean value from a column
func (ad *ArrowData) GetBooleanValue(colIndex int, rowIndex int64) (bool, bool) {
	if ad.array == nil {
		return false, false
	}

	if colIndex >= int(ad.array.n_children) {
		return false, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return false, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return false, false
		}
	}

	// For booleans, buffer 1 is a bitmap
	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return false, false
	}

	byteIndex := rowIndex / 8
	bitIndex := rowIndex % 8
	dataBytes := (*[1 << 30]byte)(dataBuf)
	value := (dataBytes[byteIndex] & (1 << bitIndex)) != 0

	return value, true
}

// GetInt8Value reads an int8 value from a column
func (ad *ArrowData) GetInt8Value(colIndex int, rowIndex int64) (int8, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]int8)(dataBuf)
	return dataArray[rowIndex], true
}

// GetInt16Value reads an int16 value from a column
func (ad *ArrowData) GetInt16Value(colIndex int, rowIndex int64) (int16, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]int16)(dataBuf)
	return dataArray[rowIndex], true
}

// GetFloat32Value reads a float32 value from a column
func (ad *ArrowData) GetFloat32Value(colIndex int, rowIndex int64) (float32, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]float32)(dataBuf)
	return dataArray[rowIndex], true
}

// GetBinaryValue reads a binary value from a column
func (ad *ArrowData) GetBinaryValue(colIndex int, rowIndex int64) ([]byte, bool) {
	if ad.array == nil {
		return nil, false
	}

	if colIndex >= int(ad.array.n_children) {
		return nil, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return nil, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return nil, false
		}
	}

	// For binary, buffer 1 is offsets, buffer 2 is data
	offsetBuf := C.get_arrow_buffer(childArray, 1)
	dataBuf := C.get_arrow_buffer(childArray, 2)

	if offsetBuf == nil || dataBuf == nil {
		return nil, false
	}

	// Read offsets (int32 for regular binary)
	offsetArray := (*[1 << 30]int32)(offsetBuf)
	start := offsetArray[rowIndex]
	end := offsetArray[rowIndex+1]

	// Read binary data
	dataBytes := (*[1 << 30]byte)(dataBuf)
	result := make([]byte, end-start)
	for i := int32(0); i < end-start; i++ {
		result[i] = dataBytes[start+i]
	}

	return result, true
}

// GetDate32Value reads a date32 value from a column (days since epoch)
func (ad *ArrowData) GetDate32Value(colIndex int, rowIndex int64) (int32, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]int32)(dataBuf)
	return dataArray[rowIndex], true
}

// GetTimestampValue reads a timestamp value from a column (microseconds since epoch)
func (ad *ArrowData) GetTimestampValue(colIndex int, rowIndex int64) (int64, bool) {
	if ad.array == nil {
		return 0, false
	}

	if colIndex >= int(ad.array.n_children) {
		return 0, false
	}

	childArray := C.get_arrow_child_array(ad.array, C.int64_t(colIndex))
	if childArray == nil {
		return 0, false
	}

	validityBuf := C.get_arrow_buffer(childArray, 0)
	if validityBuf != nil {
		byteIndex := rowIndex / 8
		bitIndex := rowIndex % 8
		validityBytes := (*[1 << 30]byte)(validityBuf)
		if (validityBytes[byteIndex] & (1 << bitIndex)) == 0 {
			return 0, false
		}
	}

	dataBuf := C.get_arrow_buffer(childArray, 1)
	if dataBuf == nil {
		return 0, false
	}

	dataArray := (*[1 << 30]int64)(dataBuf)
	return dataArray[rowIndex], true
}

// GetValue reads a value from any column, automatically detecting the type
func (ad *ArrowData) GetValue(colIndex int, rowIndex int64) (interface{}, bool) {
	format := ad.ColumnFormat(colIndex)

	switch {
	case format == "i": // int32
		return ad.GetInt32Value(colIndex, rowIndex)
	case format == "l": // int64
		return ad.GetInt64Value(colIndex, rowIndex)
	case format == "s": // int16 (short)
		return ad.GetInt16Value(colIndex, rowIndex)
	case format == "c": // int8 (byte)
		return ad.GetInt8Value(colIndex, rowIndex)
	case format == "g": // float64 (double)
		return ad.GetFloat64Value(colIndex, rowIndex)
	case format == "f": // float32
		return ad.GetFloat32Value(colIndex, rowIndex)
	case format == "u": // utf8 string
		return ad.GetStringValue(colIndex, rowIndex)
	case format == "b": // boolean
		return ad.GetBooleanValue(colIndex, rowIndex)
	case format == "z": // binary
		return ad.GetBinaryValue(colIndex, rowIndex)
	case format == "tdD": // date32 (days since epoch)
		return ad.GetDate32Value(colIndex, rowIndex)
	case len(format) >= 4 && format[:4] == "tss:": // timestamp seconds with timezone
		return ad.GetTimestampValue(colIndex, rowIndex)
	case len(format) >= 4 && format[:4] == "tsm:": // timestamp milliseconds with timezone
		return ad.GetTimestampValue(colIndex, rowIndex)
	case len(format) >= 4 && format[:4] == "tsu:": // timestamp microseconds with timezone
		return ad.GetTimestampValue(colIndex, rowIndex)
	case len(format) >= 4 && format[:4] == "tsn:": // timestamp nanoseconds with/without timezone
		return ad.GetTimestampValue(colIndex, rowIndex)
	default:
		return nil, false
	}
}
