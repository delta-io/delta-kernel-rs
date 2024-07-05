#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "delta_kernel_ffi.h"

void visit_callback(void* engine_context,
                    KernelStringSlice path,
                    int64_t size,
                    const Stats* stats,
                    const DvInfo* dv_info,
                    const CStringMap* partition_values) {
  printf("file: %.*s (size: %" PRId64 ", num_records:", (int)path.len, path.ptr, size);
  if (stats) {
    printf("%" PRId64 ")\n", stats->num_records);
  } else {
    printf(" [no stats])\n");
  }
}

void visit_data(void* engine_context,
                ExclusiveEngineData* engine_data,
                const KernelBoolSlice selection_vec) {
  visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
}

int test_engine(KernelStringSlice table_path_slice,
                ExternResultHandleSharedExternEngine engine_res) {
  if (engine_res.tag != OkHandleSharedExternEngine) {
    printf("Failed to get engine\n");
    return -1;
  }

  SharedExternEngine* engine = engine_res.ok;

  ExternResultHandleSharedSnapshot snapshot_res = snapshot(table_path_slice, engine);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    printf("Failed to create snapshot\n");
    return -1;
  }

  SharedSnapshot* snapshot = snapshot_res.ok;

  uint64_t v = version(snapshot);
  printf("version: %" PRIu64 "\n", v);
  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL);
  if (scan_res.tag != OkHandleSharedScan) {
    printf("Failed to create scan\n");
    return -1;
  }

  SharedScan* scan = scan_res.ok;

  ExternResultHandleSharedScanDataIterator data_iter_res = kernel_scan_data_init(engine, scan);
  if (data_iter_res.tag != OkHandleSharedScanDataIterator) {
    printf("Failed to construct scan data iterator\n");
    return -1;
  }

  SharedScanDataIterator* data_iter = data_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res = kernel_scan_data_next(data_iter, NULL, visit_data);
    if (ok_res.tag != Okbool) {
      printf("Failed to iterate scan data\n");
      return -1;
    } else if (!ok_res.ok) {
      break;
    }
  }

  free_scan(scan);
  free_snapshot(snapshot);
  free_engine(engine);
  return 0;
}

int main(int argc, char* argv[]) {

  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  ExternResultHandleSharedExternEngine default_engine_res =
    get_default_engine(table_path_slice, NULL);
  ExternResultHandleSharedExternEngine sync_engine_res = get_sync_engine(NULL);

  printf("Executing with default engine\n");
  int default_test_res = test_engine(table_path_slice, default_engine_res);
  printf("Executing with sync engine\n");
  int sync_test_res = test_engine(table_path_slice, sync_engine_res);

  // return 0 iff neither test passes
  return default_test_res | sync_test_res;
}
