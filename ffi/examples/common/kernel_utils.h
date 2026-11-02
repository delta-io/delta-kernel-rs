#pragma once

#include <stdbool.h>
#include <delta_kernel_ffi.h>

// Error objects belong to the connector; Kernel only returns their pointers.
typedef struct ErrorParameter {
  char* key;
  char* value;
} ErrorParameter;

typedef struct Error {
  bool is_delta;
  FFIKernelError code;
  char* condition;
  char* sql_state;
  ErrorParameter* parameters;
  size_t parameter_count;
  char* msg;
} Error;

extern const ErrorAllocators error_allocators;

void print_diag(char* fmt, ...);
// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err);
// free an error
void free_error(Error* error);
// create a char* from a KernelStringSlice
void* allocate_string(const KernelStringSlice slice);
// utility function to convert key/val into slices and set them on a builder
// returns false on failure
bool set_builder_opt(HandleExclusiveEngineBuilder* engine_builder, char* key, char* val);
// Compile-time smoke test for snapshot-hint ABI types and entry-point signatures.
void compile_snapshot_hint_abi(void);
// print out metric info
void print_metric(MetricEvent event);
