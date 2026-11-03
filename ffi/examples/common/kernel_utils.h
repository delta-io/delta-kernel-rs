#pragma once

#include <stdbool.h>
#include <delta_kernel_ffi.h>

// Owned copies of one Delta error parameter's name and value.
typedef struct ErrorParameter {
  char* key;
  char* value;
} ErrorParameter;

// Both callbacks allocate this connector-owned object and copy the borrowed diagnostics into it.
// Kernel returns its pointer through ExternResult; the caller releases it with free_error.
typedef struct Error {
  bool is_delta;
  // Used only for Kernel errors; Delta errors use condition, sql_state, and parameters.
  FFIKernelError code;
  char* condition;
  char* sql_state;
  ErrorParameter* parameters;
  size_t parameter_count;
  char* msg;
} Error;

// Callbacks shared by the examples; neither callback needs connector context.
extern const ErrorAllocators error_allocators;

void print_diag(char* fmt, ...);
// Print Kernel or Delta diagnostics, or report allocation failure when err is NULL.
void print_error(const char* msg, Error* err);
// Free the error and its copied diagnostics. Accepts NULL.
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
