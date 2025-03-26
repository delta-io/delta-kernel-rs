#pragma once

#include <delta_kernel_ffi.h>

// This is how we represent our errors. The kernel will ask us to contruct this struct whenever it
// enounters an error, and then return the contructed EngineError to us
typedef struct Error
{
  struct EngineError etype;
  char* msg;
} Error;

void print_diag(char* fmt, ...);
// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err);
// free an error
void free_error(Error* error);
// create a char* from a KernelStringSlice
void* allocate_string(const KernelStringSlice slice);
// kernel will call this to allocate our errors. This can be used to create an "engine native" type
// error
EngineError* allocate_error(KernelError etype, const KernelStringSlice msg);
// utility function to convert key/val into slices and set them on a builder
void set_builder_opt(EngineBuilder* engine_builder, char* key, char* val);
