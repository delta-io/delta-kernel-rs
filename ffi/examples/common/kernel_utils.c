#include <delta_kernel_ffi.h>
#include <stdio.h>
#include <string.h>
#include "kernel_utils.h"

// some diagnostic functions
void print_diag(char* fmt, ...)
{
#ifdef VERBOSE
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
#else
  (void)(fmt);
#endif
}

// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err)
{
  printf("[ERROR] %s\n", msg);
  printf("  Kernel Code: %i\n", err->etype.etype);
  printf("  Kernel Msg: %s\n", err->msg);
}

// free an error
void free_error(Error* error)
{
  free(error->msg);
  free(error);
}

// kernel will call this to allocate our errors. This can be used to create an "engine native" type
// error
EngineError* allocate_error(KernelError etype, const KernelStringSlice msg)
{
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

#ifdef WIN32 // windows doesn't have strndup
char *strndup(const char *s, size_t n) {
  size_t len = strnlen(s, n);
  char *p = malloc(len + 1);
  if (p) {
    memcpy(p, s, len);
    p[len] = '\0';
  }
  return p;
}
#endif

// utility to turn a slice into a char*
void* allocate_string(const KernelStringSlice slice)
{
  return strndup(slice.ptr, slice.len);
}

// utility function to convert key/val into slices and set them on a builder
// returns false on failure
bool set_builder_opt(EngineBuilder* engine_builder, char* key, char* val)
{
  KernelStringSlice key_slice = { key, strlen(key) };
  KernelStringSlice val_slice = { val, strlen(val) };
  ExternResultbool res = set_builder_option(engine_builder, key_slice, val_slice);
  if (res.tag != Okbool) {
    print_error("Failed to set builder option.", (Error*)res.err);
    free_error((Error*)res.err);
    return false;
  }
  return true;
}

