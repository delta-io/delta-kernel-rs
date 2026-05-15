#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// update_dv -- C FFI example for connector-authored deletion vector updates
// ============================================================================
//
// Usage:
//   ./update_dv /path/to/table data-file.parquet p file:///tmp/table/dv.bin 1 36 2
//
// The target table must already exist, have the deletionVectors reader/writer feature,
// and set delta.enableDeletionVectors=true. The DV file/bytes must already exist; this
// example only installs the descriptor into the Delta log.

static int storage_type_code(const char* storage_type) {
  if (strcmp(storage_type, "u") == 0) {
    return 0; // PersistedRelative
  }
  if (strcmp(storage_type, "i") == 0) {
    return 1; // Inline
  }
  if (strcmp(storage_type, "p") == 0) {
    return 2; // PersistedAbsolute
  }
  return -1;
}

static bool parse_i32(const char* text, int32_t* out) {
  char* end = NULL;
  long value = strtol(text, &end, 10);
  if (end == text || *end != '\0' || value < INT32_MIN || value > INT32_MAX) {
    return false;
  }
  *out = (int32_t)value;
  return true;
}

static bool parse_i64(const char* text, int64_t* out) {
  char* end = NULL;
  long long value = strtoll(text, &end, 10);
  if (end == text || *end != '\0') {
    return false;
  }
  *out = (int64_t)value;
  return true;
}

static void usage(const char* program) {
  fprintf(stderr,
          "Usage: %s /path/to/table data-file-path storage-type path-or-inline-dv "
          "offset-or-- size-in-bytes cardinality\n\n"
          "storage-type: u (persisted relative), i (inline), or p (persisted absolute)\n"
          "offset-or--: byte offset for persisted DVs, or '-' to omit it\n",
          program);
}

int main(int argc, char* argv[]) {
  if (argc != 8) {
    usage(argv[0]);
    return 1;
  }

  char* table_path = argv[1];
  char* data_file_path = argv[2];
  int storage_type = storage_type_code(argv[3]);
  char* path_or_inline_dv = argv[4];
  bool has_offset = strcmp(argv[5], "-") != 0;
  int32_t offset = 0;
  int32_t size_in_bytes = 0;
  int64_t cardinality = 0;

  if (storage_type < 0) {
    fprintf(stderr, "Invalid storage type: %s\n", argv[3]);
    usage(argv[0]);
    return 1;
  }
  if (has_offset && !parse_i32(argv[5], &offset)) {
    fprintf(stderr, "Invalid offset: %s\n", argv[5]);
    return 1;
  }
  if (!parse_i32(argv[6], &size_in_bytes)) {
    fprintf(stderr, "Invalid size_in_bytes: %s\n", argv[6]);
    return 1;
  }
  if (!parse_i64(argv[7], &cardinality)) {
    fprintf(stderr, "Invalid cardinality: %s\n", argv[7]);
    return 1;
  }

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };
  KernelStringSlice data_file_path_slice = { data_file_path, strlen(data_file_path) };
  KernelStringSlice dv_slice = { path_or_inline_dv, strlen(path_or_inline_dv) };

  // === Build engine ===
  ExternResultEngineBuilder engine_builder_res =
      get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkEngineBuilder) {
    print_error("Could not get engine builder.", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return 1;
  }
  ExternResultHandleSharedExternEngine engine_res = builder_build(engine_builder_res.ok);
  if (engine_res.tag != OkHandleSharedExternEngine) {
    print_error("Failed to build engine.", (Error*)engine_res.err);
    free_error((Error*)engine_res.err);
    return 1;
  }
  HandleSharedExternEngine engine = engine_res.ok;

  // === Start transaction ===
  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path_slice, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Failed to start transaction.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    free_engine(engine);
    return 1;
  }
  HandleExclusiveTransaction txn = txn_res.ok;

  const char* engine_info = "update_dv_example";
  KernelStringSlice engine_info_slice = { engine_info, strlen(engine_info) };
  ExternResultHandleExclusiveTransaction with_info_res =
      with_engine_info(txn, engine_info_slice, engine);
  if (with_info_res.tag != OkHandleExclusiveTransaction) {
    print_error("with_engine_info failed.", (Error*)with_info_res.err);
    free_error((Error*)with_info_res.err);
    free_engine(engine);
    return 1;
  }
  txn = with_info_res.ok;

  // === Build descriptor map ===
  HandleExclusiveDvDescriptorMap map = dv_descriptor_map_new();
  ExternResultHandleExclusiveDvDescriptor descriptor_res = dv_descriptor_new(
      storage_type, dv_slice, has_offset, offset, size_in_bytes, cardinality, engine);
  if (descriptor_res.tag != OkHandleExclusiveDvDescriptor) {
    print_error("dv_descriptor_new failed.", (Error*)descriptor_res.err);
    free_error((Error*)descriptor_res.err);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  HandleExclusiveDvDescriptor descriptor = descriptor_res.ok;

  ExternResultbool insert_res =
      dv_descriptor_map_insert(map, data_file_path_slice, descriptor, engine);
  if (insert_res.tag != Okbool) {
    print_error("dv_descriptor_map_insert failed.", (Error*)insert_res.err);
    free_error((Error*)insert_res.err);
    free_dv_descriptor(descriptor);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }

  // === Build a fresh scan metadata iterator for the update call ===
  ExternResultHandleMutableFfiSnapshotBuilder snapshot_builder_res =
      get_snapshot_builder(table_path_slice, engine);
  if (snapshot_builder_res.tag != OkHandleMutableFfiSnapshotBuilder) {
    print_error("Failed to get snapshot builder.", (Error*)snapshot_builder_res.err);
    free_error((Error*)snapshot_builder_res.err);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  ExternResultHandleSharedSnapshot snapshot_res =
      snapshot_builder_build(snapshot_builder_res.ok);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to load snapshot.", (Error*)snapshot_res.err);
    free_error((Error*)snapshot_res.err);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  HandleSharedSnapshot snapshot = snapshot_res.ok;

  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL, NULL);
  if (scan_res.tag != OkHandleSharedScan) {
    print_error("Failed to build scan.", (Error*)scan_res.err);
    free_error((Error*)scan_res.err);
    free_snapshot(snapshot);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  HandleSharedScan scan_handle = scan_res.ok;

  ExternResultHandleSharedScanMetadataIterator iter_res =
      scan_metadata_iter_init(engine, scan_handle);
  if (iter_res.tag != OkHandleSharedScanMetadataIterator) {
    print_error("scan_metadata_iter_init failed.", (Error*)iter_res.err);
    free_error((Error*)iter_res.err);
    free_scan(scan_handle);
    free_snapshot(snapshot);
    free_dv_descriptor_map(map);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  HandleSharedScanMetadataIterator scan_iter = iter_res.ok;

  ExternResultbool update_res =
      transaction_update_deletion_vectors(txn, map, scan_iter, engine);
  // map and scan_iter are consumed by transaction_update_deletion_vectors even on error.
  if (update_res.tag != Okbool) {
    print_error("transaction_update_deletion_vectors failed.", (Error*)update_res.err);
    free_error((Error*)update_res.err);
    free_scan(scan_handle);
    free_snapshot(snapshot);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }

  free_scan(scan_handle);
  free_snapshot(snapshot);

  // === Commit ===
  ExternResultHandleExclusiveCommittedTransaction commit_res = commit(txn, engine);
  if (commit_res.tag != OkHandleExclusiveCommittedTransaction) {
    print_error("commit failed.", (Error*)commit_res.err);
    free_error((Error*)commit_res.err);
    free_engine(engine);
    return 1;
  }
  HandleExclusiveCommittedTransaction committed = commit_res.ok;
  printf("Committed DV update at version: %" PRIu64 "\n",
         committed_transaction_version(&committed));
  free_committed_transaction(committed);
  free_engine(engine);
  return 0;
}
