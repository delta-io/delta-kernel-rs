#include <stdio.h>
#include <string.h>

#include "delta_kernel_ffi.h"

void visit_file(void *engine_context, struct KernelStringSlice file_name) {
    int i;
    printf("file: ");
    for (i = 0; i < file_name.len; i++) {
        printf("%c", file_name.ptr[i]);
    }
    printf("\n");
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

  ExternResult_____KernelScanFileIterator file_iter_res =
    kernel_scan_files_init(snapshot_handle, table_client, NULL);
  if (file_iter_res.tag != Ok_____KernelScanFileIterator) {
    printf("Failed to construct scan file iterator\n");
    return -1;
  }

  KernelScanFileIterator *file_iter = file_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResult_bool ok_res = kernel_scan_files_next(file_iter, NULL, visit_file);
    if (ok_res.tag != Ok_bool) {
      printf("Failed to iterate scan file\n");
      return -1;
    } else if (!ok_res.ok) {
      break;
    }
  }

  kernel_scan_files_free(file_iter);
  drop_snapshot(snapshot_handle);
  drop_table_client(table_client);

  return 0;
}
