package internal

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
)

// KernelError represents different error types from delta-kernel
type KernelError int

const (
	UnknownError                      KernelError = C.UnknownError
	FFIError                          KernelError = C.FFIError
	EngineDataTypeError               KernelError = C.EngineDataTypeError
	ExtractError                      KernelError = C.ExtractError
	GenericError                      KernelError = C.GenericError
	IOErrorError                      KernelError = C.IOErrorError
	FileNotFoundError                 KernelError = C.FileNotFoundError
	MissingColumnError                KernelError = C.MissingColumnError
	UnexpectedColumnTypeError         KernelError = C.UnexpectedColumnTypeError
	MissingDataError                  KernelError = C.MissingDataError
	MissingVersionError               KernelError = C.MissingVersionError
	DeletionVectorError               KernelError = C.DeletionVectorError
	InvalidUrlError                   KernelError = C.InvalidUrlError
	MalformedJsonError                KernelError = C.MalformedJsonError
	MissingMetadataError              KernelError = C.MissingMetadataError
	MissingProtocolError              KernelError = C.MissingProtocolError
	InvalidProtocolError              KernelError = C.InvalidProtocolError
	MissingMetadataAndProtocolError   KernelError = C.MissingMetadataAndProtocolError
	ParseError                        KernelError = C.ParseError
	JoinFailureError                  KernelError = C.JoinFailureError
	Utf8Error                         KernelError = C.Utf8Error
	ParseIntError                     KernelError = C.ParseIntError
	InvalidColumnMappingModeError     KernelError = C.InvalidColumnMappingModeError
	InvalidTableLocationError         KernelError = C.InvalidTableLocationError
	InvalidDecimalError               KernelError = C.InvalidDecimalError
	InvalidStructDataError            KernelError = C.InvalidStructDataError
	InternalError                     KernelError = C.InternalError
	InvalidExpression                 KernelError = C.InvalidExpression
	InvalidLogPath                    KernelError = C.InvalidLogPath
	FileAlreadyExists                 KernelError = C.FileAlreadyExists
	UnsupportedError                  KernelError = C.UnsupportedError
	ParseIntervalError                KernelError = C.ParseIntervalError
	ChangeDataFeedUnsupported         KernelError = C.ChangeDataFeedUnsupported
	ChangeDataFeedIncompatibleSchema  KernelError = C.ChangeDataFeedIncompatibleSchema
	InvalidCheckpoint                 KernelError = C.InvalidCheckpoint
	LiteralExpressionTransformError   KernelError = C.LiteralExpressionTransformError
	CheckpointWriteError              KernelError = C.CheckpointWriteError
	SchemaError                       KernelError = C.SchemaError
)

func (e KernelError) Error() string {
	switch e {
	case UnknownError:
		return "unknown error"
	case FFIError:
		return "FFI error"
	case FileNotFoundError:
		return "file not found"
	case InvalidTableLocationError:
		return "invalid table location"
	case SchemaError:
		return "schema error"
	default:
		return fmt.Sprintf("kernel error: %d", e)
	}
}

// Error represents a delta-kernel error with message
type Error struct {
	Code    KernelError
	Message string
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code.Error(), e.Message)
	}
	return e.Code.Error()
}

// AllocateError allocates and returns an error from an EngineError pointer
func AllocateError(cerr C.NullableCvoid) error {
	if cerr == nil {
		return nil
	}

	// Cast to EngineError to get error type
	engineErr := (*C.struct_EngineError)(cerr)
	errCode := KernelError(engineErr.etype)

	return &Error{Code: errCode, Message: ""}
}
