#pragma once

#include <delta_kernel_ffi.h>

// A list of partition column names
typedef struct PartitionList
{
  uintptr_t len;
  char** cols;
} PartitionList;

// This is information we want to pass between callbacks. The kernel will generally take a `void*`
// "context" argument, and then pass it back when calling a callback.
struct EngineContext
{
  SharedGlobalScanState* global_state;
  SharedSchema* logical_schema;
  SharedSchema* read_schema;
  char* table_root;
  SharedExternEngine* engine;
  PartitionList* partition_cols;
  const CStringMap* partition_values;
#ifdef PRINT_ARROW_DATA
  struct ArrowContext* arrow_context;
#endif
};
