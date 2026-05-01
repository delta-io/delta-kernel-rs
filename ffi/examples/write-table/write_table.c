#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// write_table -- C FFI example for the write transaction surface
// ============================================================================
//
// Usage:
//   ./write_table /path/to/existing/table
//
// The target table must already exist (e.g. created by the `create-table` example).
//
// Demonstrates the write-path FFI surface:
//   - transaction(path, engine) to start an existing-table transaction
//   - with_engine_info(txn, "...", engine) to set commitInfo.engineInfo
//   - get_unpartitioned_write_context(txn, engine) + get_write_schema/get_write_path
//     (the values an engine needs when writing parquet files itself)
//   - set_data_change(txn, false) because this empty commit does not add data
//   - commit(txn, engine) to produce an empty commit, returning a CommittedTransaction handle
//   - committed_transaction_version + committed_transaction_post_commit_snapshot to read the
//     version and (when available) the post-commit snapshot directly from the result, avoiding
//     a fresh snapshot load
//   - free_committed_transaction to release the result handle
//
// NOTE: This example does NOT call add_files. Staging new files requires building an Arrow
// RecordBatch that matches Transaction::add_files_schema (path, partitionValues, size,
// modificationTime, stats), which needs arrow-glib (or equivalent) on the C side to
// construct. That flow is tracked as a follow-up; once the shared arrow-glib writer helper
// lands in ffi/examples/common/, this example should be extended to stage a real parquet
// file and exercise the full add_files -> commit path.

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s /path/to/existing/table\n", argv[0]);
    return 1;
  }
  char* table_path = argv[1];
  printf("Writing empty commit to %s\n", table_path);
  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

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
  SharedExternEngine* engine = engine_res.ok;

  // === Start a transaction on the latest snapshot ===
  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path_slice, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Failed to start transaction.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    free_engine(engine);
    return 1;
  }
  ExclusiveTransaction* txn = txn_res.ok;

  // set_data_change does not consume the handle. This commit has no data, so dataChange=false.
  // Setting it here (before with_engine_info) is fine: the consume-and-return chain below
  // preserves staged transaction state across handle handoffs.
  set_data_change(txn, false);

  // Attach engine_info. CONSUMES and returns the transaction handle.
  const char* engine_info = "write_table_example";
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

  // === Inspect the unpartitioned write context ===
  //
  // The WriteContext tells an engine what schema its parquet writer should use and where to
  // put the files. This example does not actually write any files, but we print these so
  // users see the shape of the information they'd consume in a real engine.
  ExternResultHandleSharedWriteContext wc_res = get_unpartitioned_write_context(txn, engine);
  if (wc_res.tag != OkHandleSharedWriteContext) {
    print_error("get_unpartitioned_write_context failed.", (Error*)wc_res.err);
    free_error((Error*)wc_res.err);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  SharedWriteContext* write_context = wc_res.ok;

  // SharedSchema is opaque in the C API; engines typically walk it with visit_schema (see
  // read-table/schema.h). We just confirm we got a valid handle and then free it.
  SharedSchema* write_schema = get_write_schema(write_context);
  printf("Write context:\n");
  printf("  schema_handle: %s\n", write_schema ? "<obtained>" : "<null>");
  char* write_path = get_write_path(write_context, allocate_string);
  if (write_path) {
    printf("  write_path:    %s\n", write_path);
    free(write_path);
  } else {
    printf("  write_path:    <none>\n");
  }
  free_schema(write_schema);
  free_write_context(write_context);

  // === Commit ===
  ExternResultHandleExclusiveCommittedTransaction commit_res = commit(txn, engine);
  if (commit_res.tag != OkHandleExclusiveCommittedTransaction) {
    print_error("commit failed.", (Error*)commit_res.err);
    free_error((Error*)commit_res.err);
    free_engine(engine);
    return 1;
  }
  HandleExclusiveCommittedTransaction committed = commit_res.ok;
  uint64_t committed_version = committed_transaction_version(committed);
  printf("Committed version: %" PRIu64 "\n", committed_version);

  // === Read post-commit snapshot directly from the CommittedTransaction ===
  // This avoids a fresh snapshot load: the kernel handed us an already-built snapshot for
  // the post-commit version. If the handle does not carry one (e.g. some catalog-managed or
  // retried-commit paths), fall back to loading via snapshot_builder_build.
  HandleSharedSnapshot snap;
  bool has_post_commit = committed_transaction_post_commit_snapshot(committed, &snap);
  if (has_post_commit) {
    printf("Post-commit snapshot version: %" PRIu64 "\n", version(snap));
    free_snapshot(snap);
  } else {
    printf("No post-commit snapshot available; loading via snapshot builder.\n");
    ExternResultHandleMutableFfiSnapshotBuilder snapshot_builder_res =
        get_snapshot_builder(table_path_slice, engine);
    if (snapshot_builder_res.tag != OkHandleMutableFfiSnapshotBuilder) {
      print_error("Failed to get snapshot builder.", (Error*)snapshot_builder_res.err);
      free_error((Error*)snapshot_builder_res.err);
      free_committed_transaction(committed);
      free_engine(engine);
      return 1;
    }
    ExternResultHandleSharedSnapshot snap_res =
        snapshot_builder_build(snapshot_builder_res.ok);
    if (snap_res.tag != OkHandleSharedSnapshot) {
      print_error("Failed to load snapshot after commit.", (Error*)snap_res.err);
      free_error((Error*)snap_res.err);
      free_committed_transaction(committed);
      free_engine(engine);
      return 1;
    }
    HandleSharedSnapshot loaded = snap_res.ok;
    printf("Snapshot version after commit: %" PRIu64 "\n", version(loaded));
    free_snapshot(loaded);
  }

  free_committed_transaction(committed);
  free_engine(engine);
  return 0;
}
