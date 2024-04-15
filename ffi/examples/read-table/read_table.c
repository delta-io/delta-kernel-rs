#include <arrow-glib/arrow-glib.h>
#include <stdio.h>
#include <string.h>

#include "delta_kernel_ffi.h"

static void
print_array(GArrowArray *array)
{
  GArrowType value_type;
  gint64 i, n;

  value_type = garrow_array_get_value_type(array);

  g_print("[");
  n = garrow_array_get_length(array);

#define ARRAY_CASE(type, Type, TYPE, format)                                             \
  case GARROW_TYPE_##TYPE:                                                               \
    {                                                                                    \
      GArrow##Type##Array *real_array;                                                   \
      real_array = GARROW_##TYPE##_ARRAY(array);                                         \
      for (i = 0; i < n; i++) {                                                          \
        if (i > 0) {                                                                     \
          g_print(", ");                                                                 \
        }                                                                                \
        g_print(format, garrow_##type##_array_get_value(real_array, i));                 \
      }                                                                                  \
    }                                                                                    \
    break

  switch (value_type) {
    ARRAY_CASE(uint8, UInt8, UINT8, "%hhu");
    ARRAY_CASE(uint16, UInt16, UINT16, "%" G_GUINT16_FORMAT);
    ARRAY_CASE(uint32, UInt32, UINT32, "%" G_GUINT32_FORMAT);
    ARRAY_CASE(uint64, UInt64, UINT64, "%" G_GUINT64_FORMAT);
    ARRAY_CASE(int8, Int8, INT8, "%hhd");
    ARRAY_CASE(int16, Int16, INT16, "%" G_GINT16_FORMAT);
    ARRAY_CASE(int32, Int32, INT32, "%" G_GINT32_FORMAT);
    ARRAY_CASE(int64, Int64, INT64, "%" G_GINT64_FORMAT);
    ARRAY_CASE(float, Float, FLOAT, "%g");
    ARRAY_CASE(double, Double, DOUBLE, "%g");
  default:
    break;
  }
#undef ARRAY_CASE

  g_print("]\n");
}

static void
print_record_batch(GArrowRecordBatch *record_batch)
{
  guint nth_column, n_columns;

  n_columns = garrow_record_batch_get_n_columns(record_batch);
  for (nth_column = 0; nth_column < n_columns; nth_column++) {
    GArrowArray *array;

    g_print("columns[%u](%s): ",
            nth_column,
            garrow_record_batch_get_column_name(record_batch, nth_column));
    array = garrow_record_batch_get_column_data(record_batch, nth_column);
    print_array(array);
    g_object_unref(array);
  }
}


void visit_file(void *engine_context, struct KernelStringSlice file_name) {
    printf("file: %s\n", file_name.ptr);
}

void visit_data(void *engine_context, const void *engine_data, const struct KernelBoolSlice *selection_vec) {
  printf("Got some data\n");
  print_record_batch(engine_data);
  for (int i = 0; i < selection_vec->len; i++) {
    printf("sel[i] = %b\n", selection_vec->ptr[i]);
  }
}


int main(int argc, char* argv[]) {

  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = {table_path, strlen(table_path)};

  ExternResult______ExternEngineInterfaceHandle table_client_res =
    get_default_client(table_path_slice, NULL);
  if (table_client_res.tag != Ok______ExternEngineInterfaceHandle) {
    printf("Failed to get client\n");
    return -1;
  }

  const ExternEngineInterfaceHandle *table_client = table_client_res.ok;

  ExternResult______SnapshotHandle snapshot_handle_res = snapshot(table_path_slice, table_client);
  if (snapshot_handle_res.tag != Ok______SnapshotHandle) {
    printf("Failed to create snapshot\n");
    return -1;
  }

  const SnapshotHandle *snapshot_handle = snapshot_handle_res.ok;

  uint64_t v = version(snapshot_handle);
  printf("version: %llu\n", v);

  ExternResult_____KernelScanDataIterator data_iter_res =
    kernel_scan_data_init(snapshot_handle, table_client, NULL);
  if (data_iter_res.tag != Ok_____KernelScanDataIterator) {
    printf("Failed to construct scan dta iterator\n");
    return -1;
  }

  KernelScanDataIterator *data_iter = data_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResult_bool ok_res = kernel_scan_data_next(data_iter, NULL, visit_data);
    if (ok_res.tag != Ok_bool) {
      printf("Failed to iterate scan data\n");
      return -1;
    } else if (!ok_res.ok) {
      break;
    }
  }

  kernel_scan_data_free(data_iter);

  drop_snapshot(snapshot_handle);
  drop_table_client(table_client);

  return 0;
}
