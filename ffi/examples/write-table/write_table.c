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
//   - get_unpartitioned_write_context(txn, engine) plus the four
//     write-context accessors an engine needs when writing parquet files itself:
//       - get_write_schema           -- logical (user-facing) schema
//       - get_physical_write_schema  -- on-disk parquet schema (carries
//                                       parquet.field.id under column mapping)
//       - get_logical_to_physical    -- transform to apply per batch
//       - get_write_path             -- table root URL (partitioned write
//                                       directory support tracked by #2355)
//   - set_data_change(txn, false) because this empty commit does not add data
//   - commit(txn, engine) to produce an empty commit
//   - Reading the committed version back via a new snapshot to confirm
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
  // The WriteContext carries the schema an engine's parquet writer should use plus the table
  // root URL it should write under. This example does not actually write any files, but we
  // print these so users see the shape of the information they'd consume in a real engine.
  ExternResultHandleSharedWriteContext wc_res = get_unpartitioned_write_context(txn, engine);
  if (wc_res.tag != OkHandleSharedWriteContext) {
    print_error("get_unpartitioned_write_context failed.", (Error*)wc_res.err);
    free_error((Error*)wc_res.err);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  SharedWriteContext* write_context = wc_res.ok;

  // SharedSchema and SharedExpression are opaque in the C API. Schemas are walked with
  // visit_schema (see read-table/schema.h); expressions are consumed by passing them to
  // new_expression_evaluator. A real engine feeds (logical_schema, logical_to_physical,
  // physical_schema) into new_expression_evaluator and applies the resulting evaluator to
  // each batch before handing the rewritten data to its parquet writer.
  SharedSchema* logical_schema = get_write_schema(write_context);
  SharedSchema* physical_schema = get_physical_write_schema(write_context);
  SharedExpression* logical_to_physical = get_logical_to_physical(write_context);
  printf("Write context:\n");
  printf("  logical_schema:      %s\n", logical_schema ? "<obtained>" : "<null>");
  printf("  physical_schema:     %s\n", physical_schema ? "<obtained>" : "<null>");
  printf("  logical_to_physical: %s\n", logical_to_physical ? "<obtained>" : "<null>");
  char* write_path = get_write_path(write_context, allocate_string);
  if (write_path) {
    printf("  write_path:          %s\n", write_path);
    free(write_path);
  } else {
    printf("  write_path:          <none>\n");
  }
  free_kernel_expression(logical_to_physical);
  free_schema(physical_schema);
  free_schema(logical_schema);
  free_write_context(write_context);

  // === Commit ===
  ExternResultu64 commit_res = commit(txn, engine);
  if (commit_res.tag != Oku64) {
    print_error("commit failed.", (Error*)commit_res.err);
    free_error((Error*)commit_res.err);
    free_engine(engine);
    return 1;
  }
  printf("Committed version: %" PRIu64 "\n", commit_res.ok);

  // === Read back via snapshot to confirm ===
  ExternResultHandleMutableFfiSnapshotBuilder snapshot_builder_res =
      get_snapshot_builder(table_path_slice, engine);
  if (snapshot_builder_res.tag != OkHandleMutableFfiSnapshotBuilder) {
    print_error("Failed to get snapshot builder.", (Error*)snapshot_builder_res.err);
    free_error((Error*)snapshot_builder_res.err);
    free_engine(engine);
    return 1;
  }
  ExternResultHandleSharedSnapshot snap_res =
      snapshot_builder_build(snapshot_builder_res.ok);
  if (snap_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to load snapshot after commit.", (Error*)snap_res.err);
    free_error((Error*)snap_res.err);
    free_engine(engine);
    return 1;
  }
  SharedSnapshot* snap = snap_res.ok;
  printf("Snapshot version after commit: %" PRIu64 "\n", version(snap));

  free_snapshot(snap);
  free_engine(engine);
  return 0;
}
