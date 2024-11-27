// This file contains code to work with arrow data. Used when we are actually reading and printing
// the content of the table
#pragma once

#ifdef PRINT_ARROW_DATA

#include "delta_kernel_ffi.h"
#include "read_table.h"

#include <glib.h>
#include <arrow-glib/arrow-glib.h>

typedef struct ArrowContext
{
  gsize num_batches;
  GList* batches;
  GArrowBooleanArray* cur_filter;
} ArrowContext;

ArrowContext* init_arrow_context(void);
void c_read_parquet_file(
  struct EngineContext* context,
  const KernelStringSlice path,
  const KernelBoolSlice selection_vector);
void print_arrow_context(ArrowContext* context);
void free_arrow_context(ArrowContext* context);

#endif // PRINT_ARROW_DATA
