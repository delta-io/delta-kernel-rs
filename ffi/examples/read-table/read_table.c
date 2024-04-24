#include <stdio.h>
#include <string.h>
#include "delta_kernel_ffi.h"

#ifdef PRINT_ARROW_DATA
#include "arrow.h"
#endif

struct EngineContext {
  GlobalScanState *global_state;
  const ExternEngineInterfaceHandle *engine_interface;
};

void print_selection_vector(char* indent, const struct KernelBoolSlice *selection_vec) {
  for (int i = 0; i < selection_vec->len; i++) {
    printf("%ssel[%i] = %b\n", indent, i, selection_vec->ptr[i]);
  }
}

void* allocate_string(const struct KernelStringSlice slice) {
  char* buf = malloc(sizeof(char) * (slice.len + 1)); // +1 for null
  snprintf(buf, slice.len + 1, "%s", slice.ptr);
  return buf;
}

void visit_callback(void* engine_context, const struct KernelStringSlice path, long size, struct CDvInfo *dv_info, struct CStringMap *partition_values) {
  printf("called back to actually read!\n  path: %.*s\n", path.len, path.ptr);
  struct EngineContext *context = engine_context;
  KernelBoolSlice *selection_vector = selection_vector_from_dv(dv_info, context->engine_interface, context->global_state);
  if (selection_vector) {
    printf("  Deletion vector selection vector:\n");
    print_selection_vector("    ", selection_vector);
    drop_bool_slice(selection_vector);
  } else {
    printf("  No deletion vector for this call\n");
  }
  // normally this would be picked out of the schema
  char* letter_key = "letter";
  KernelStringSlice key = {letter_key, strlen(letter_key)};
  char* partition_val = get_from_map(partition_values, key, allocate_string);
  if (partition_val) {
    printf("  letter partition here: %s\n", partition_val);
    free(partition_val);
  } else {
    printf("  no partition here\n");
  }
}

void visit_data(void *engine_context, struct EngineDataHandle *engine_data, const struct KernelBoolSlice selection_vec) {
  printf("Got some data\n");
  printf("  Of this data, here is a selection vector\n");
  print_selection_vector("    ", &selection_vec);
  visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = {table_path, strlen(table_path)};

  ExternResult_____EngineInterfaceBuilder interface_builder_res =
    get_engine_interface_builder(table_path_slice, NULL);
  if (interface_builder_res.tag != Ok_____EngineInterfaceBuilder) {
    printf("Could not get engine interface builder\n");
    return -1;
  }

  // an example of using a builder to set options when building a engine interface
  struct EngineInterfaceBuilder *interface_builder = interface_builder_res.ok;
  char* key_test = "key1";
  KernelStringSlice key_slice = {key_test, strlen(key_test)};
  char* val_test = "val1";
  KernelStringSlice val_slice = {val_test, strlen(val_test)};
  set_builder_option(interface_builder, key_slice, val_slice);
  ExternResult______ExternEngineInterfaceHandle engine_interface_res =
    builder_build(interface_builder);

  // alternately if we don't care to set any options on the builder:
  // ExternResult______ExternEngineInterfaceHandle engine_interface_res =
  //   get_default_client(table_path_slice, NULL);

  if (engine_interface_res.tag != Ok______ExternEngineInterfaceHandle) {
    printf("Failed to get client\n");
    return -1;
  }

  const ExternEngineInterfaceHandle *engine_interface = engine_interface_res.ok;

  ExternResult______SnapshotHandle snapshot_handle_res = snapshot(table_path_slice, engine_interface);
  if (snapshot_handle_res.tag != Ok______SnapshotHandle) {
    printf("Failed to create snapshot\n");
    return -1;
  }

  const SnapshotHandle *snapshot_handle = snapshot_handle_res.ok;

  uint64_t v = version(snapshot_handle);
  printf("version: %llu\n", v);

  ExternResult_____Scan scan_res = scan(snapshot_handle, engine_interface, NULL);
  if (scan_res.tag != Ok_____Scan) {
    printf("Failed to create scan\n");
    return -1;
  }

  Scan *scan = scan_res.ok;
  GlobalScanState *global_state = get_global_scan_state(scan);
  struct EngineContext context = { global_state, engine_interface };

  ExternResult_____KernelScanDataIterator data_iter_res =
    kernel_scan_data_init(engine_interface, scan);
  if (data_iter_res.tag != Ok_____KernelScanDataIterator) {
    printf("Failed to construct scan data iterator\n");
    return -1;
  }

  KernelScanDataIterator *data_iter = data_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResult_bool ok_res = kernel_scan_data_next(data_iter, &context, visit_data);
    if (ok_res.tag != Ok_bool) {
      printf("Failed to iterate scan data\n");
      return -1;
    } else if (!ok_res.ok) {
      break;
    }
  }

  kernel_scan_data_free(data_iter);
  drop_global_scan_state(global_state);
  drop_snapshot(snapshot_handle);
  drop_table_client(engine_interface);

  return 0;
}
