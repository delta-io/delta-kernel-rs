#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#include "delta_kernel_ffi.h"

void visit_callback(void* engine_context, const struct KernelStringSlice path, long size, struct CDvInfo *dv_info, struct CStringMap *partition_values) {
  printf("file: %.*s\n", (int)path.len, path.ptr);
}


void visit_data(void *engine_context, struct EngineDataHandle *engine_data, const struct KernelBoolSlice selection_vec) {
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

  ExternResult______ExternEngineInterfaceHandle engine_interface_res =
    get_default_client(table_path_slice, NULL);
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
  printf("version: %" PRIu64 "\n", v);
  ExternResult_____Scan scan_res = scan(snapshot_handle, engine_interface, NULL);
  if (scan_res.tag != Ok_____Scan) {
    printf("Failed to create scan\n");
    return -1;
  }

  Scan *scan = scan_res.ok;

  ExternResult_____KernelScanDataIterator data_iter_res =
    kernel_scan_data_init(engine_interface, scan);
  if (data_iter_res.tag != Ok_____KernelScanDataIterator) {
    printf("Failed to construct scan data iterator\n");
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

  drop_snapshot(snapshot_handle);
  drop_table_client(engine_interface);

  return 0;
}
