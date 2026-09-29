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

  // Owned handles start NULL and are released by the cleanup ladder below. Each FFI call
  // that consumes a handle (e.g. `dv_descriptor_map_insert`, `snapshot_builder_build`,
  // `update_table_txn_update_deletion_vectors`, `update_table_txn_commit`) is paired with an explicit NULL
  // assignment so the cleanup block does not double-free.
  int rc = 1;
  Error* err = NULL;
  HandleSharedExternEngine engine = NULL;
  HandleExclusiveUpdateTableTransaction txn = NULL;
  HandleExclusiveDvDescriptorMap map = NULL;
  HandleExclusiveDvDescriptor descriptor = NULL;
  HandleExclusiveSnapshotBuilder snapshot_builder = NULL;
  HandleSharedSnapshot snapshot = NULL;
  HandleSharedScan scan_handle = NULL;
  HandleSharedScanMetadataIterator scan_iter = NULL;
  HandleExclusiveCommittedTransaction committed = NULL;

  // === Build engine ===
  ExternResultHandleExclusiveEngineBuilder engine_builder_res =
      get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkHandleExclusiveEngineBuilder) {
    err = (Error*)engine_builder_res.err;
    print_error("Could not get engine builder.", err);
    goto cleanup;
  }
  ExternResultHandleSharedExternEngine engine_res = builder_build(engine_builder_res.ok);
  if (engine_res.tag != OkHandleSharedExternEngine) {
    err = (Error*)engine_res.err;
    print_error("Failed to build engine.", err);
    goto cleanup;
  }
  engine = engine_res.ok;

  // === Build descriptor map ===
  map = dv_descriptor_map_new();
  ExternResultHandleExclusiveDvDescriptor descriptor_res = dv_descriptor_new(
      storage_type, dv_slice, has_offset, offset, size_in_bytes, cardinality, engine);
  if (descriptor_res.tag != OkHandleExclusiveDvDescriptor) {
    err = (Error*)descriptor_res.err;
    print_error("dv_descriptor_new failed.", err);
    goto cleanup;
  }
  descriptor = descriptor_res.ok;

  ExternResultbool insert_res =
      dv_descriptor_map_insert(map, data_file_path_slice, descriptor, engine);
  descriptor = NULL; // consumed by dv_descriptor_map_insert regardless of result
  if (insert_res.tag != Okbool) {
    err = (Error*)insert_res.err;
    print_error("dv_descriptor_map_insert failed.", err);
    goto cleanup;
  }

  // === Build a fresh scan metadata iterator for the update call ===
  ExternResultHandleExclusiveSnapshotBuilder snapshot_builder_res =
      get_snapshot_builder(table_path_slice, engine);
  if (snapshot_builder_res.tag != OkHandleExclusiveSnapshotBuilder) {
    err = (Error*)snapshot_builder_res.err;
    print_error("Failed to get snapshot builder.", err);
    goto cleanup;
  }
  snapshot_builder = snapshot_builder_res.ok;

  ExternResultHandleSharedSnapshot snapshot_res = snapshot_builder_build(snapshot_builder);
  snapshot_builder = NULL; // consumed by snapshot_builder_build
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    err = (Error*)snapshot_res.err;
    print_error("Failed to load snapshot.", err);
    goto cleanup;
  }
  snapshot = snapshot_res.ok;

  // === Configure and build transaction intent ===
  ExclusiveUpdateTableTransactionBuilder* txn_builder = get_update_table_txn_builder(snapshot);
  const char* engine_info = "update_dv_example";
  KernelStringSlice engine_info_slice = { engine_info, strlen(engine_info) };
  ExternResultHandleExclusiveUpdateTableTransactionBuilder with_info_res =
      update_table_txn_builder_with_engine_info(txn_builder, engine_info_slice, engine);
  if (with_info_res.tag != OkHandleExclusiveUpdateTableTransactionBuilder) {
    err = (Error*)with_info_res.err;
    print_error("setting builder engine info failed.", err);
    goto cleanup;
  }
  txn_builder = update_table_txn_builder_with_operation(
      with_info_res.ok, KernelUpdateTableOperationUpdate);
  ExternResultHandleExclusiveUpdateTableTransaction txn_res =
      update_table_txn_builder_build(txn_builder, engine);
  if (txn_res.tag != OkHandleExclusiveUpdateTableTransaction) {
    err = (Error*)txn_res.err;
    print_error("Failed to build transaction.", err);
    goto cleanup;
  }
  txn = txn_res.ok;

  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL, NULL);
  if (scan_res.tag != OkHandleSharedScan) {
    err = (Error*)scan_res.err;
    print_error("Failed to build scan.", err);
    goto cleanup;
  }
  scan_handle = scan_res.ok;

  ExternResultHandleSharedScanMetadataIterator iter_res =
      scan_metadata_iter_init(engine, scan_handle);
  if (iter_res.tag != OkHandleSharedScanMetadataIterator) {
    err = (Error*)iter_res.err;
    print_error("scan_metadata_iter_init failed.", err);
    goto cleanup;
  }
  scan_iter = iter_res.ok;

  ExternResultbool update_res =
      update_table_txn_update_deletion_vectors(txn, map, scan_iter, engine);
  // map and scan_iter are consumed by update_table_txn_update_deletion_vectors even on error.
  map = NULL;
  scan_iter = NULL;
  if (update_res.tag != Okbool) {
    err = (Error*)update_res.err;
    print_error("update_table_txn_update_deletion_vectors failed.", err);
    goto cleanup;
  }

  // === Commit ===
  ExternResultHandleExclusiveCommittedTransaction commit_res = update_table_txn_commit(txn, engine);
  txn = NULL; // consumed by commit
  if (commit_res.tag != OkHandleExclusiveCommittedTransaction) {
    err = (Error*)commit_res.err;
    print_error("commit failed.", err);
    goto cleanup;
  }
  committed = commit_res.ok;
  printf("Committed DV update at version: %" PRIu64 "\n",
         committed_transaction_version(&committed));
  rc = 0;

cleanup:
  if (err) free_error(err);
  if (committed) free_committed_transaction(committed);
  if (scan_iter) free_scan_metadata_iter(scan_iter);
  if (scan_handle) free_scan(scan_handle);
  if (snapshot) free_snapshot(snapshot);
  if (snapshot_builder) free_snapshot_builder(snapshot_builder);
  if (descriptor) free_dv_descriptor(descriptor);
  if (map) free_dv_descriptor_map(map);
  if (txn) free_update_table_txn(txn);
  if (engine) free_engine(engine);
  return rc;
}
