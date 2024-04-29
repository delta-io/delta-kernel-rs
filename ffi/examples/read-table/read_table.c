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

// This is how we represent our errors. The kernel will ask us to contruct this struct whenever it
// enounters an error, and then return the contructed EngineError to us
typedef struct Error {
  struct EngineError etype;
  char* msg;
} Error;


// create a char* from a KernelStringSlice
void* allocate_string(const struct KernelStringSlice slice) {
  char* buf = malloc(sizeof(char) * (slice.len + 1)); // +1 for null
  snprintf(buf, slice.len + 1, "%s", slice.ptr);
  return buf;
}

EngineError* allocate_error(KernelError etype, const struct KernelStringSlice msg) {
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

void free_error(Error* error) {
  free(error->msg);
  free(error);
}

void print_selection_vector(const char* indent, const struct KernelBoolSlice *selection_vec) {
  for (int i = 0; i < selection_vec->len; i++) {
    printf("%ssel[%i] = %b\n", indent, i, selection_vec->ptr[i]);
  }
}

void print_error(const char* indent, Error* err) {
  printf("%sCode: %i\n", indent, err->etype);
  printf("%sMsg: %s\n", indent, err->msg);
}

void set_builder_opt(struct EngineInterfaceBuilder *interface_builder, char* key, char* val) {
  KernelStringSlice key_slice = {key, strlen(key)};
  KernelStringSlice val_slice = {val, strlen(val)};
  set_builder_option(interface_builder, key_slice, val_slice);
}

void visit_callback(void* engine_context, const struct KernelStringSlice path, long size, const DvInfo *dv_info, struct CStringMap *partition_values) {
  printf("called back to actually read!\n  path: %.*s\n", path.len, path.ptr);
  struct EngineContext *context = engine_context;
  ExternResultKernelBoolSlice selection_vector_res = selection_vector_from_dv(dv_info, context->engine_interface, context->global_state);
  if (selection_vector_res.tag != OkKernelBoolSlice) {
    printf("Could not get selection vector from kernel\n");
    return;
  }
  KernelBoolSlice *selection_vector = selection_vector_res.ok;
  if (selection_vector) {
    printf("  Selection vector:\n");
    print_selection_vector("    ", selection_vector);
    drop_bool_slice(selection_vector);
  } else {
    printf("  No selection vector for this call\n");
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

void visit_data(void *engine_context, struct EngineDataHandle *engine_data, struct KernelBoolSlice selection_vec) {
  printf("Got some data\n");
  printf("  Of this data, here is a selection vector\n");
  print_selection_vector("    ", &selection_vec);
  visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
  drop_bool_slice(&selection_vec);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = {table_path, strlen(table_path)};

  ExternResultEngineInterfaceBuilder interface_builder_res =
    get_engine_interface_builder(table_path_slice, allocate_error);
  if (interface_builder_res.tag != OkEngineInterfaceBuilder) {
    printf("Could not get engine interface builder.\n");
    print_error("  ", (Error*)interface_builder_res.err);
    free_error((Error*)interface_builder_res.err);
    return -1;
  }

  // an example of using a builder to set options when building a engine interface
  struct EngineInterfaceBuilder *interface_builder = interface_builder_res.ok;
  set_builder_opt(interface_builder, "aws_region", "us-west-2");
  // potentially set credentials here
  //set_builder_opt(interface_builder, "aws_access_key_id" , "[redacted]");
  //set_builder_opt(interface_builder, "aws_secret_access_key", "[redacted]");
  ExternResultExternEngineInterfaceHandle engine_interface_res =
    builder_build(interface_builder);

  // alternately if we don't care to set any options on the builder:
  // ExternResultExternEngineInterfaceHandle engine_interface_res =
  //   get_default_client(table_path_slice, NULL);

  if (engine_interface_res.tag != OkExternEngineInterfaceHandle) {
    printf("Failed to get client\n");
    print_error("  ", (Error*)interface_builder_res.err);
    free_error((Error*)interface_builder_res.err);
    return -1;
  }

  const ExternEngineInterfaceHandle *engine_interface = engine_interface_res.ok;

  ExternResultSnapshotHandle snapshot_handle_res = snapshot(table_path_slice, engine_interface);
  if (snapshot_handle_res.tag != OkSnapshotHandle) {
    printf("Failed to create snapshot\n");
    print_error("  ", (Error*)snapshot_handle_res.err);
    free_error((Error*)snapshot_handle_res.err);
    return -1;
  }

  const SnapshotHandle *snapshot_handle = snapshot_handle_res.ok;

  uint64_t v = version(snapshot_handle);
  printf("version: %llu\n", v);

  ExternResultScan scan_res = scan(snapshot_handle, engine_interface, NULL);
  if (scan_res.tag != OkScan) {
    printf("Failed to create scan\n");
    print_error("  ", (Error*)scan_res.err);
    free_error((Error*)scan_res.err);
    return -1;
  }

  Scan *scan = scan_res.ok;
  GlobalScanState *global_state = get_global_scan_state(scan);
  struct EngineContext context = { global_state, engine_interface };

  ExternResultKernelScanDataIterator data_iter_res =
    kernel_scan_data_init(engine_interface, scan);
  if (data_iter_res.tag != OkKernelScanDataIterator) {
    printf("Failed to construct scan data iterator\n");
    print_error("  ", (Error*)data_iter_res.err);
    free_error((Error*)data_iter_res.err);
    return -1;
  }

  KernelScanDataIterator *data_iter = data_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res = kernel_scan_data_next(data_iter, &context, visit_data);
    if (ok_res.tag != Okbool) {
      printf("Failed to iterate scan data\n");
      print_error("  ", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      return -1;
    } else if (!ok_res.ok) {
      printf("Iterator done\n");
      break;
    }
  }

  printf("All done\n");

  kernel_scan_data_free(data_iter);
  drop_global_scan_state(global_state);
  drop_snapshot(snapshot_handle);
  drop_table_client(engine_interface);

  return 0;
}
