#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <arrow/c/abi.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-writer.h>
#include <uuid/uuid.h>

#include "delta_kernel_ffi.h"
#include "arrow.h"
#include "schema.h"
#include "kernel_utils.h"

bool record_batch_to_ffi(GArrowRecordBatch *batch,
                         ArrowFFIData      *out_ffi,
                         GError           **error) {
  if (!batch || !out_ffi) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "record_batch_to_ffi: batch/out_ffi cannot be NULL");
    return false;
  }

  struct ArrowArray  *tmp_array  = NULL;
  struct ArrowSchema *tmp_schema = NULL;

  if (!garrow_record_batch_export(batch,
                                  (gpointer *)&tmp_array,
                                  (gpointer *)&tmp_schema,
                                  error)) {
    return false; // *error set by the export
  }

  // shallow copy into out_ffi->array / out_ffi->schema.
  memcpy(&out_ffi->array,  tmp_array,  sizeof(struct ArrowArray));
  memcpy(&out_ffi->schema, tmp_schema, sizeof(struct ArrowSchema));

  // don't double-free
  tmp_array->release  = NULL;
  tmp_schema->release = NULL;

  g_free(tmp_array);
  g_free(tmp_schema);

  // must eventually call:
  // - out_ffi->array.release(&out_ffi->array);
  // - out_ffi->schema.release(&out_ffi->schema);
  return true;
}

GArrowRecordBatch *create_write_metadata(const gchar *path,
                             gint64      size_value,
                             gint64      mod_time_value,
                             GError    **error)
{
  GArrowDataType *string_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
  GArrowDataType *key_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
  GArrowDataType *val_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
  GArrowMapDataType *map_type = garrow_map_data_type_new(key_type, val_type);
  GArrowDataType *int64_type = GARROW_DATA_TYPE(garrow_int64_data_type_new());
  GArrowDataType *bool_type = GARROW_DATA_TYPE(garrow_boolean_data_type_new());

  GArrowField *field_path = garrow_field_new("path", string_type);
  GArrowField *field_partition_values = garrow_field_new("partitionValues", GARROW_DATA_TYPE(map_type));
  GArrowField *field_size = garrow_field_new("size", int64_type);
  GArrowField *field_mod_time = garrow_field_new("modificationTime", int64_type);
  GArrowField *field_data_change = garrow_field_new("dataChange", bool_type);

  // unref the data types now that fields hold references
  g_object_unref(string_type);
  g_object_unref(key_type);
  g_object_unref(val_type);
  g_object_unref(map_type);
  g_object_unref(int64_type);
  g_object_unref(bool_type);

  // schema
  GList *fields = NULL;
  fields = g_list_append(fields, field_path);
  fields = g_list_append(fields, field_partition_values);
  fields = g_list_append(fields, field_size);
  fields = g_list_append(fields, field_mod_time);
  fields = g_list_append(fields, field_data_change);
  GArrowSchema *schema = garrow_schema_new(fields);
  g_list_free(fields);

  // schema now holds refs to the fields
  g_object_unref(field_path);
  g_object_unref(field_partition_values);
  g_object_unref(field_size);
  g_object_unref(field_mod_time);
  g_object_unref(field_data_change);

  if (!schema) {
    g_set_error_literal(error,
                        GARROW_ERROR,
                        GARROW_ERROR_INVALID,
                        "Failed to create schema");
    return NULL;
  }

  // path
  GArrowStringArrayBuilder *path_builder =
    GARROW_STRING_ARRAY_BUILDER(garrow_string_array_builder_new());
  if (!garrow_string_array_builder_append_string(path_builder, path, error)) {
    goto error_cleanup;
  }
  GArrowArray *path_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(path_builder), error);
  g_object_unref(path_builder);
  if (!path_array) {
    goto error_cleanup;
  }

  // partitionValues
  GArrowMapArrayBuilder *map_builder =
    garrow_map_array_builder_new(garrow_map_data_type_new(
                                   GARROW_DATA_TYPE(garrow_string_data_type_new()),
                                   GARROW_DATA_TYPE(garrow_string_data_type_new())),
                                 error);
  if (!map_builder) {
    g_object_unref(path_array);
    goto error_cleanup;
  }
  if (!garrow_map_array_builder_append_value(map_builder, error)) {
    g_object_unref(path_array);
    g_object_unref(map_builder);
    goto error_cleanup;
  }
  GArrowArray *partition_values_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(map_builder), error);
  g_object_unref(map_builder);
  if (!partition_values_array) {
    g_object_unref(path_array);
    goto error_cleanup;
  }

  // size
  GArrowInt64ArrayBuilder *size_builder =
    GARROW_INT64_ARRAY_BUILDER(garrow_int64_array_builder_new());
  if (!garrow_int64_array_builder_append_value(size_builder, size_value, error)) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    goto error_cleanup;
  }
  GArrowArray *size_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(size_builder), error);
  g_object_unref(size_builder);
  if (!size_array) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    goto error_cleanup;
  }

  // modificationTime
  GArrowInt64ArrayBuilder *mod_time_builder =
    GARROW_INT64_ARRAY_BUILDER(garrow_int64_array_builder_new());
  if (!garrow_int64_array_builder_append_value(mod_time_builder, mod_time_value, error)) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    g_object_unref(size_array);
    goto error_cleanup;
  }
  GArrowArray *mod_time_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(mod_time_builder), error);
  g_object_unref(mod_time_builder);
  if (!mod_time_array) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    g_object_unref(size_array);
    goto error_cleanup;
  }

  // dataChange
  GArrowBooleanArrayBuilder *bool_builder =
    GARROW_BOOLEAN_ARRAY_BUILDER(garrow_boolean_array_builder_new());
  if (!garrow_boolean_array_builder_append_value(bool_builder, TRUE, error)) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    g_object_unref(size_array);
    g_object_unref(mod_time_array);
    goto error_cleanup;
  }
  GArrowArray *data_change_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(bool_builder), error);
  g_object_unref(bool_builder);
  if (!data_change_array) {
    g_object_unref(path_array);
    g_object_unref(partition_values_array);
    g_object_unref(size_array);
    g_object_unref(mod_time_array);
    goto error_cleanup;
  }

  const guint32 n_rows = 1;
  GList *columns = NULL;
  columns = g_list_append(columns, path_array);
  columns = g_list_append(columns, partition_values_array);
  columns = g_list_append(columns, size_array);
  columns = g_list_append(columns, mod_time_array);
  columns = g_list_append(columns, data_change_array);
  GArrowRecordBatch *record_batch =
    garrow_record_batch_new(schema, n_rows, columns, error);
  g_list_free(columns);

  // record batch holds refs to arrays/schema
  g_object_unref(path_array);
  g_object_unref(partition_values_array);
  g_object_unref(size_array);
  g_object_unref(mod_time_array);
  g_object_unref(data_change_array);
  g_object_unref(schema);

  if (!record_batch) {
    // 'error' is set by garrow_record_batch_new() if it failed
    return NULL;
  }

  return record_batch;

error_cleanup:
  g_object_unref(schema);
  return NULL;
}

// create map type column "engineCommitInfo" with one row and one entry:
// {"engineInfo": "default engine"}
GArrowRecordBatch *create_commit_info(GError **error) {
  GArrowDataType *key_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
  GArrowDataType *val_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
  GArrowMapDataType *map_data_type =
    garrow_map_data_type_new(key_type, val_type);

  GArrowField *map_field =
    garrow_field_new("engineCommitInfo", GARROW_DATA_TYPE(map_data_type));

  GList *schema_fields = NULL;
  schema_fields = g_list_append(schema_fields, map_field);
  GArrowSchema *schema = garrow_schema_new(schema_fields);
  g_list_free(schema_fields);

  GArrowMapArrayBuilder *map_builder =
    garrow_map_array_builder_new(map_data_type, error);

  if (!map_builder) {
    g_object_unref(schema);
    g_object_unref(map_field);
    g_object_unref(key_type);
    g_object_unref(val_type);
    g_object_unref(map_data_type);
    return NULL;
  }

  GArrowArrayBuilder *key_builder =
    garrow_map_array_builder_get_key_builder(map_builder);
  GArrowArrayBuilder *item_builder =
    garrow_map_array_builder_get_item_builder(map_builder);

  if (!garrow_map_array_builder_append_value(map_builder, error)) {
    goto error_cleanup;
  }

  if (!garrow_string_array_builder_append_string(
        GARROW_STRING_ARRAY_BUILDER(key_builder),
        "engineInfo",
        error)) {
    goto error_cleanup;
  }
  if (!garrow_string_array_builder_append_string(
        GARROW_STRING_ARRAY_BUILDER(item_builder),
        "default engine",
        error)) {
    goto error_cleanup;
  }

  GArrowArray *map_array =
    garrow_array_builder_finish(GARROW_ARRAY_BUILDER(map_builder), error);
  if (!map_array) {
    goto error_cleanup;
  }

  guint32 n_rows = garrow_array_get_length(map_array);
  GList *columns = NULL;
  columns = g_list_append(columns, map_array);
  GArrowRecordBatch *record_batch =
    garrow_record_batch_new(schema, n_rows, columns, error);

  g_list_free(columns);
  g_object_unref(map_array);

  if (!record_batch) {
    // record_batch creation failed, error is set
    goto error_cleanup;
  }

  g_object_unref(map_builder);
  g_object_unref(schema);
  g_object_unref(map_field);
  g_object_unref(key_type);
  g_object_unref(val_type);
  g_object_unref(map_data_type);

  return record_batch;

error_cleanup:
  printf("Error creating commit info record batch: %s\n", (*error)->message);
  if (map_builder) {
    g_object_unref(map_builder);
  }
  if (schema) {
    g_object_unref(schema);
  }
  if (map_field) {
    g_object_unref(map_field);
  }
  if (key_type) {
    g_object_unref(key_type);
  }
  if (val_type) {
    g_object_unref(val_type);
  }
  if (map_data_type) {
    g_object_unref(map_data_type);
  }
  return NULL;
}

gboolean write_record_batch(gchar             *output_path,
                            GArrowRecordBatch *record_batch,
                            GError           **error) {
  // scrub prefix if it exists
  const char prefix[] = "file://";
  size_t prefix_len = strlen(prefix);
  if (strncmp(output_path, prefix, prefix_len) == 0) {
    memmove(output_path, output_path + prefix_len, strlen(output_path + prefix_len) + 1);
  }
  GArrowSchema *schema = garrow_record_batch_get_schema(record_batch);
  GParquetArrowFileWriter *writer = gparquet_arrow_file_writer_new_path (
    schema,
    output_path,
    NULL,
    error
  );

  if (writer == NULL) {
    printf("Error creating file writer: %s\n", (*error)->message);
    return FALSE;
  }

  g_object_unref(schema);

  if (!gparquet_arrow_file_writer_write_record_batch(writer, record_batch, error)) {
    g_object_unref(writer);
    return FALSE;
  }

  if (!gparquet_arrow_file_writer_close(writer, error)) {
    g_object_unref(writer);
    return FALSE;
  }

  g_object_unref(writer);
  return TRUE;
}

// (from build dir) run with ./write_table ../data/test_table/
// just appends (10, 11, 12) to the existing table
int main(int argc, char* argv[])
{
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Writing to table at %s\n", table_path);

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  ExternResultEngineBuilder engine_builder_res =
    get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkEngineBuilder) {
    print_error("Could not get engine builder.", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  ExternResultHandleSharedExternEngine engine_res =
    get_default_engine(table_path_slice, allocate_error);

  if (engine_res.tag != OkHandleSharedExternEngine) {
    print_error("Failed to get engine", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  SharedExternEngine* engine = engine_res.ok;

  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path_slice, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Failed to create transaction.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    return -1;
  }

  ExclusiveTransaction* txn = txn_res.ok;

  GError *error = NULL;
  GArrowRecordBatch *commit_info_batch = create_commit_info(&error);

  if (!commit_info_batch) {
    if (error) {
      fprintf(stderr, "Error: %s\n", error->message);
      g_error_free(error);
    }
    return 1;
  }

  ArrowFFIData commit_info_ffi;
  if (!record_batch_to_ffi(commit_info_batch, &commit_info_ffi, &error)) {
    if (error) {
      fprintf(stderr, "Error: %s\n", error->message);
      g_error_free(error);
    }
    // FIXME
    // handle error, cleanup
    return 1;
  }

  // FIXME
  // g_object_unref(commit_info);

  ExternResultHandleExclusiveEngineData commit_info_res =
    get_engine_data(&commit_info_ffi, engine);

  if (commit_info_res.tag != OkHandleExclusiveEngineData) {
    print_error("Failed to get commit info as engine data.", (Error*)commit_info_res.err);
    free_error((Error*)commit_info_res.err);
    return -1;
  }

  ExclusiveEngineData* commit_info = commit_info_res.ok;

  ExclusiveTransaction* txn_with_commit_info = with_commit_info(txn, commit_info);

  SharedWriteContext* write_ctx = get_write_context(txn_with_commit_info);

  // TODO
  // SharedSchema* write_schema = get_write_schema(write_ctx);

  char* table_root = get_write_path(write_ctx, allocate_string);

  uuid_t uuid;
  uuid_generate(uuid);

  char parquet_name[45];  // 36 characters + null terminator + ".parquet"
  uuid_unparse_lower(uuid, parquet_name);
  snprintf(parquet_name, sizeof(parquet_name), "%s.parquet", parquet_name);

  size_t path_size = strlen(table_root) + strlen(parquet_name) + 1;
  char *write_path = malloc(path_size);
  if (!write_path) {
    // TODO
    return 1;
  }

  snprintf(write_path, path_size, "%s%s", table_root, parquet_name);
  print_diag("writing to: %s\n", write_path);

  // build arrow array of data we want to append
  GArrowInt64ArrayBuilder *builder = garrow_int64_array_builder_new();
  if (!garrow_int64_array_builder_append_value(builder, 10, &error) ||
      !garrow_int64_array_builder_append_value(builder, 11, &error) ||
      !garrow_int64_array_builder_append_value(builder, 12, &error)) {
      g_print("Error appending value: %s\n", error->message);
      g_clear_error(&error);
      g_object_unref(builder);
      return EXIT_FAILURE;
  }

  GArrowArray *array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder), &error);
  if (!array) {
      g_print("Error building array: %s\n", error->message);
      g_clear_error(&error);
      g_object_unref(builder);
      return EXIT_FAILURE;
  }

  GArrowDataType *data_type = GARROW_DATA_TYPE(garrow_int64_data_type_new());
  GArrowField    *field     = garrow_field_new("value", data_type);
  g_object_unref(data_type);

  GList *fields = NULL;
  fields = g_list_append(fields, field);

  GArrowSchema *schema = garrow_schema_new(fields);
  g_list_free(fields);
  g_object_unref(field);

  if (!schema) {
    g_printerr("Error creating schema.\n");
    // 'field' is already unref
    g_object_unref(array);
    return 1;
  }

  guint32 n_rows = garrow_array_get_length(array);
  GList *columns = NULL;
  columns = g_list_append(columns, array);

  GArrowRecordBatch *record_batch =
    garrow_record_batch_new(schema, n_rows, columns, &error);

  g_list_free(columns);
  g_object_unref(schema);

  if (!record_batch) {
    g_printerr("Error creating RecordBatch: %s\n", error->message);
    g_clear_error(&error);
    g_object_unref(array);
    return 1;
  }
  g_object_unref(array);

  printf("writing %lld rows...\n", garrow_record_batch_get_n_rows(record_batch));

  write_record_batch(write_path, record_batch, &error);
  // FIXME
  GArrowRecordBatch *write_meta_batch = create_write_metadata(parquet_name, 1234, 5678, &error);


  ArrowFFIData write_meta_ffi;
  if (!record_batch_to_ffi(write_meta_batch, &write_meta_ffi, &error)) {
    if (error) {
      fprintf(stderr, "Error: %s\n", error->message);
      g_error_free(error);
    }
    // FIXME
    // handle error, cleanup
    return 1;
  }

  ExternResultHandleExclusiveEngineData write_meta_res =
    get_engine_data(&write_meta_ffi, engine);

  if (write_meta_res.tag != OkHandleExclusiveEngineData) {
    print_error("Failed to get commit info as engine data.", (Error*)commit_info_res.err);
    free_error((Error*)commit_info_res.err);
    return -1;
  }

  ExclusiveEngineData* write_meta = write_meta_res.ok;

  add_write_metadata(txn_with_commit_info, write_meta);

  // commit! WARN: txn is consumed by this call
  commit(txn_with_commit_info, engine);

  // Clean up
  g_object_unref(record_batch);
  g_object_unref(builder);

  free(write_path);

  // free_transaction(txn); // txn is consumed by commit
  free_engine(engine);

  return 0;
}
