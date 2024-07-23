#include "arrow.h"
#include <stdio.h>
#include <string.h>

#ifdef PRINT_ARROW_DATA

ArrowContext* init_arrow_context()
{
  ArrowContext* context = malloc(sizeof(ArrowContext));
  context->num_batches = 0;
  context->batches = NULL;
  context->cur_filter = NULL;
  return context;
}

// unref all the data in the context
void free_arrow_context(ArrowContext* context)
{
  g_list_free_full(g_steal_pointer(&context->batches), g_object_unref);
  free(context);
}

// report and free an error if it's not NULL. Return true if error was not null, false otherwise
static bool report_g_error(char* msg, GError* error)
{
  if (error != NULL) {
    printf("%s: %s\n", msg, error->message);
    g_error_free(error);
    return true;
  }
  return false;
}

// Turn ffi formatted schema data into a GArrowSchema
static GArrowSchema* get_schema(FFI_ArrowSchema* schema)
{
  GError* error = NULL;
  GArrowSchema* garrow_schema = garrow_schema_import((gpointer)schema, &error);
  report_g_error("Can't get schema", error);
  return garrow_schema;
}

// Turn ffi formatted record batch data into a GArrowRecordBatch
static GArrowRecordBatch* get_record_batch(FFI_ArrowArray* array, GArrowSchema* schema)
{
  GError* error = NULL;
  GArrowRecordBatch* record_batch = garrow_record_batch_import((gpointer)array, schema, &error);
  report_g_error("Can't get record batch", error);
  return record_batch;
}

// Add columns to a record batch for each partition. In a "real" engine we would want to parse the
// string values into the correct data type. This program just adds all partition columns as strings
// for simplicity
static GArrowRecordBatch* add_partition_columns(
  GArrowRecordBatch* record_batch,
  PartitionList* partition_cols,
  const CStringMap* partition_values)
{
  gint64 rows = garrow_record_batch_get_n_rows(record_batch);
  gint64 cols = garrow_record_batch_get_n_columns(record_batch);
  GArrowRecordBatch* cur_record_batch = record_batch;
  GError* error = NULL;
  for (int i = 0; i < partition_cols->len; i++) {
    char* col = partition_cols->cols[i];
    guint pos = cols + i;
    KernelStringSlice key = { col, strlen(col) };
    char* partition_val = get_from_map(partition_values, key, allocate_string);
    print_diag(
      "  Adding partition column '%s' with value '%s' at column %u\n",
      col,
      partition_val ? partition_val : "NULL",
      pos);
    GArrowStringArrayBuilder* builder = garrow_string_array_builder_new();
    for (gint64 i = 0; i < rows; i++) {
      if (partition_val) {
        garrow_string_array_builder_append_string(builder, partition_val, &error);
      } else {
        garrow_array_builder_append_null((GArrowArrayBuilder*)builder, &error);
      }
      if (report_g_error("Can't append to partition column builder", error)) {
        break;
      }
    }

    if (partition_val) {
      free(partition_val);
    }

    if (error != NULL) {
      printf("Giving up on column %s\n", col);
      g_object_unref(builder);
      error = NULL;
      continue;
    }

    GArrowArray* ret = garrow_array_builder_finish((GArrowArrayBuilder*)builder, &error);
    if (report_g_error("Can't build string array for parition column", error)) {
      printf("Giving up on column %s\n", col);
      g_object_unref(builder);
      error = NULL;
      continue;
    }
    g_object_unref(builder);

    GArrowField* field = garrow_field_new(col, (GArrowDataType*)garrow_string_data_type_new());
    GArrowRecordBatch* old_batch = cur_record_batch;
    cur_record_batch = garrow_record_batch_add_column(old_batch, pos, field, ret, &error);
    g_object_unref(old_batch);
    if (cur_record_batch == NULL) {
      if (error != NULL) {
        printf("Could not add column at %u: %s\n", pos, error->message);
        g_error_free(error);
      }
    }
  }
  return cur_record_batch;
}

// append a batch to our context
static void add_batch_to_context(
  ArrowContext* context,
  ArrowFFIData* arrow_data,
  PartitionList* partition_cols,
  const CStringMap* partition_values)
{
  GArrowSchema* schema = get_schema(&arrow_data->schema);
  GArrowRecordBatch* record_batch = get_record_batch(&arrow_data->array, schema);
  if (context->cur_filter != NULL) {
    GArrowRecordBatch* unfiltered = record_batch;
    record_batch = garrow_record_batch_filter(unfiltered, context->cur_filter, NULL, NULL);
    // unref the old batch and filter since we don't need them anymore
    g_object_unref(unfiltered);
    g_object_unref(context->cur_filter);
    context->cur_filter = NULL;
  }
  record_batch = add_partition_columns(record_batch, partition_cols, partition_values);
  if (record_batch == NULL) {
    printf("Failed to add parition columns, not adding batch\n");
    return;
  }
  context->batches = g_list_append(context->batches, record_batch);
  context->num_batches++;
  print_diag(
    "  Added batch to arrow context, have %i batches in context now\n", context->num_batches);
}

// convert to a garrow boolean array. can't use garrow_boolean_array_builder_append_values as that
// expects a gboolean*, which is actually an int* which is 4 bytes, but our slice is a C99 _Bool*
// which is 1 byte
static GArrowBooleanArray* slice_to_arrow_bool_array(const KernelBoolSlice slice)
{
  GArrowBooleanArrayBuilder* builder = garrow_boolean_array_builder_new();
  GError* error = NULL;
  for (uintptr_t i = 0; i < slice.len; i++) {
    gboolean val = slice.ptr[i] ? TRUE : FALSE;
    garrow_boolean_array_builder_append_value(builder, val, &error);
    if (report_g_error("Can't append to boolean builder", error)) {
      g_object_unref(builder);
      break;
    }
  }

  if (error != NULL) {
    return NULL;
  }

  GArrowArray* ret = garrow_array_builder_finish((GArrowArrayBuilder*)builder, &error);
  g_object_unref(builder);
  if (ret == NULL) {
    printf("Error in building boolean array");
    if (error != NULL) {
      printf(": %s\n", error->message);
      g_error_free(error);
    } else {
      printf(".\n");
    }
  }
  return (GArrowBooleanArray*)ret;
}

// This is the callback that will be called for each chunk of data read from the parquet file
static void visit_read_data(void* vcontext, ExclusiveEngineData* data)
{
  print_diag("  Converting read data to arrow\n");
  struct EngineContext* context = vcontext;
  ExternResultArrowFFIData arrow_res = get_raw_arrow_data(data, context->engine);
  if (arrow_res.tag != OkArrowFFIData) {
    print_error("Failed to get arrow data.", (Error*)arrow_res.err);
    free_error((Error*)arrow_res.err);
    exit(-1);
  }
  ArrowFFIData* arrow_data = arrow_res.ok;
  add_batch_to_context(
    context->arrow_context, arrow_data, context->partition_cols, context->partition_values);
}

// We call this for each file we get called back to read in read_table.c::visit_callback
void c_read_parquet_file(
  struct EngineContext* context,
  const KernelStringSlice path,
  const KernelBoolSlice selection_vector)
{
  int full_len = strlen(context->table_root) + path.len + 1;
  char* full_path = malloc(sizeof(char) * full_len);
  snprintf(full_path, full_len, "%s%.*s", context->table_root, (int)path.len, path.ptr);
  print_diag("  Reading parquet file at %s\n", full_path);
  KernelStringSlice path_slice = { full_path, full_len };
  FileMeta meta = {
    .path = path_slice,
  };
  ExternResultHandleExclusiveFileReadResultIterator read_res =
    read_parquet_file(context->engine, &meta, context->read_schema);
  if (read_res.tag != OkHandleExclusiveFileReadResultIterator) {
    printf("Couldn't read data\n");
    return;
  }
  if (selection_vector.len > 0) {
    GArrowBooleanArray* sel_array = slice_to_arrow_bool_array(selection_vector);
    if (sel_array == NULL) {
      printf("[WARN] Failed to get an arrow boolean array, selection vector will be ignored\n");
    }
    context->arrow_context->cur_filter = sel_array;
  }
  ExclusiveFileReadResultIterator* read_iter = read_res.ok;
  for (;;) {
    ExternResultbool ok_res = read_result_next(read_iter, context, visit_read_data);
    if (ok_res.tag != Okbool) {
      print_error("Failed to iterate read data.", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      exit(-1);
    } else if (!ok_res.ok) {
      print_diag("  Done reading parquet file\n");
      break;
    }
  }
  free_read_result_iter(read_iter);
}

struct extract_col_data {
  GList* list;
  guint col_idx;
};

void extract_col(GArrowRecordBatch* element, struct extract_col_data* data) {
  GArrowArray* array_data = garrow_record_batch_get_column_data(element, data->col_idx);
  data->list = g_list_append(data->list, array_data);
}

// Print the whole set of data. We iterate over each column, and concat each batch's data for that
// column together, then print the result.
void print_arrow_context(ArrowContext* context)
{
  if (context->num_batches > 0) {
    GError* error = NULL;
    guint cols = garrow_record_batch_get_n_columns(context->batches->data);
    for (guint c = 0; c < cols; c++) {
      // name owned by instance, so no need to free
      const gchar* name = garrow_record_batch_get_column_name(context->batches->data, c);
      printf("%s:  ", name);
      GArrowRecordBatch* batch = context->batches->data;
      GArrowArray* data = garrow_record_batch_get_column_data(batch, c);
      GList* remaining = g_list_nth(context->batches, 1);
      if (remaining != NULL) {
        struct extract_col_data remaining_data = {
          .list = NULL,
          .col_idx = c,
        };
        g_list_foreach(remaining, (GFunc)extract_col, &remaining_data);
        data = garrow_array_concatenate(data, remaining_data.list, &error);
        if (report_g_error("Can't concat array data", error)) {
          return;
        }
      }
      gchar* array_out = garrow_array_to_string(data, &error);
      if (report_g_error("Can't get array as string", error)) {
        g_object_unref(data);
        return;
      }
      printf("%s\n", array_out);
      g_free(array_out);
      g_object_unref(data);
    }
  } else {
    printf("[No data]\n");
  }
}

#endif // PRINT_ARROW_DATA
