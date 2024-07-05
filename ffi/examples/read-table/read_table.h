#pragma once

// uncomment below for more diagnotic messages
// #define VERBOSE

#include <delta_kernel_ffi.h>

// A list of partition column names
typedef struct PartitionList
{
  int len;
  char** cols;
} PartitionList;

// This is information we want to pass between callbacks. The kernel will generally take a `void*`
// "context" argument, and then pass it back when calling a callback.
struct EngineContext
{
  SharedGlobalScanState* global_state;
  SharedSchema* read_schema;
  char* table_root;
  SharedExternEngine* engine;
  PartitionList* partition_cols;
  const CStringMap* partition_values;
#ifdef PRINT_ARROW_DATA
  struct ArrowContext* arrow_context;
#endif
};

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
