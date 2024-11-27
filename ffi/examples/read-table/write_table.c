#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "arrow.h"
#include "schema.h"
#include "kernel_utils.h"

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

  // hack for now to generate commit info
  ExternResultc_void commit_info = new_commit_info(allocate_error);
  if (commit_info.tag != Okc_void) {
    print_error("Failed to create commit info.", (Error*)commit_info.err);
    free_error((Error*)commit_info.err);
    return -1;
  }

  void* commit_info_ptr = commit_info.ok;

  ExclusiveTransaction* txn_with_commit_info = with_commit_info(txn, commit_info_ptr);

  // build arrow array of data we want to append
  GError *error = NULL;
  GArrowInt64ArrayBuilder *builder = garrow_int64_array_builder_new();
  if (!garrow_int64_array_builder_append_value(builder, 10, &error) ||
      !garrow_int64_array_builder_append_value(builder, 11, &error) ||
      !garrow_int64_array_builder_append_value(builder, 12, &error)) {
      // Handle errors
      g_print("Error appending value: %s\n", error->message);
      g_clear_error(&error);
      g_object_unref(builder);
      return EXIT_FAILURE;
  }

  // Finish building the array
  GArrowArray *array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder), &error);
  if (!array) {
      g_print("Error building array: %s\n", error->message);
      g_clear_error(&error);
      g_object_unref(builder);
      return EXIT_FAILURE;
  }

  GArrowInt64Array *int64_array = GARROW_INT64_ARRAY(array);
  for (gint64 i = 0; i < garrow_array_get_length(array); i++) {
      if (garrow_array_is_valid(array, i)) {
          gint64 value = garrow_int64_array_get_value(int64_array, i);
          g_print("Value %" G_GINT64_FORMAT ": %lld\n", i, value);
      } else {
          g_print("Value %" G_GINT64_FORMAT ": NULL\n", i);
      }
  }

  // commit! WARN: txn is consumed by this call
  commit(txn_with_commit_info, engine);

  // Clean up
  g_object_unref(builder);
  g_object_unref(array);

  // uint64_t v = version(snapshot);
  // printf("read version: %" PRIu64 "\n\n", v);

  // create commit info
  // CommitInfo* = new_commit_info();

  // free_transaction(txn); // txn is consumed by commit
  free_engine(engine);

  return 0;
}
