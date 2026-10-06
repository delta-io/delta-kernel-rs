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
// Commits an empty transaction against an existing table and prints the committed version.
// Adding files is omitted because constructing the Arrow batch needs an Arrow C builder.

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s /path/to/existing/table\n", argv[0]);
    return 1;
  }
  char* table_path = argv[1];
  printf("Writing empty commit to %s\n", table_path);
  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  // === Build engine ===
  ExternResultHandleExclusiveEngineBuilder engine_builder_res =
      get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkHandleExclusiveEngineBuilder) {
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

  // === Build transaction intent on the latest snapshot ===
  ExternResultHandleExclusiveSnapshotBuilder snapshot_builder_res =
      get_snapshot_builder(table_path_slice, engine);
  if (snapshot_builder_res.tag != OkHandleExclusiveSnapshotBuilder) {
    print_error("Failed to create snapshot builder.", (Error*)snapshot_builder_res.err);
    free_error((Error*)snapshot_builder_res.err);
    free_engine(engine);
    return 1;
  }
  ExternResultHandleSharedSnapshot snapshot_res =
      snapshot_builder_build(snapshot_builder_res.ok);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to create snapshot.", (Error*)snapshot_res.err);
    free_error((Error*)snapshot_res.err);
    free_engine(engine);
    return 1;
  }
  SharedSnapshot* snapshot = snapshot_res.ok;
  ExclusiveUpdateTableTransactionBuilder* txn_builder = new_update_table_txn_builder(snapshot);

  // This empty commit does not add data.
  txn_builder = update_table_txn_builder_with_data_change(txn_builder, false);

  const char* engine_info = "write_table_example";
  KernelStringSlice engine_info_slice = { engine_info, strlen(engine_info) };
  ExternResultHandleExclusiveUpdateTableTransactionBuilder with_info_res =
      update_table_txn_builder_with_engine_info(txn_builder, engine_info_slice, engine);
  if (with_info_res.tag != OkHandleExclusiveUpdateTableTransactionBuilder) {
    print_error("setting builder engine info failed.", (Error*)with_info_res.err);
    free_error((Error*)with_info_res.err);
    free_snapshot(snapshot);
    free_engine(engine);
    return 1;
  }
  txn_builder = with_info_res.ok;
  ExternResultHandleExclusiveUpdateTableTransaction txn_res =
      update_table_txn_builder_build(txn_builder, engine);
  if (txn_res.tag != OkHandleExclusiveUpdateTableTransaction) {
    print_error("Failed to build transaction.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    free_snapshot(snapshot);
    free_engine(engine);
    return 1;
  }
  ExclusiveUpdateTableTransaction* txn = txn_res.ok;
  free_snapshot(snapshot);

  // === Inspect the unpartitioned write context ===
  //
  // The WriteContext carries the schema an engine's parquet writer should use plus the table
  // root URL it should write under. This example does not actually write any files, but we
  // print these so users see the shape of the information they'd consume in a real engine.
  ExternResultHandleSharedWriteContext wc_res = update_table_txn_get_unpartitioned_write_context(txn, engine);
  if (wc_res.tag != OkHandleSharedWriteContext) {
    print_error("update_table_txn_get_unpartitioned_write_context failed.", (Error*)wc_res.err);
    free_error((Error*)wc_res.err);
    free_update_table_txn(txn);
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
  ExternResultHandleExclusiveCommittedTransaction commit_res = update_table_txn_commit(txn, engine);
  if (commit_res.tag != OkHandleExclusiveCommittedTransaction) {
    print_error("commit failed.", (Error*)commit_res.err);
    free_error((Error*)commit_res.err);
    free_engine(engine);
    return 1;
  }
  HandleExclusiveCommittedTransaction committed = commit_res.ok;
  printf("Committed version: %" PRIu64 "\n", committed_transaction_version(&committed));

  // === Read post-commit snapshot directly from the CommittedTransaction ===
  // Avoids a fresh snapshot load: the kernel hands back the already-built snapshot
  // for the post-commit version.
  struct OptionalValueHandleSharedSnapshot post_commit =
      committed_transaction_post_commit_snapshot(&committed);
  if (post_commit.tag == SomeHandleSharedSnapshot) {
    HandleSharedSnapshot snap = post_commit.some;
    printf("Post-commit snapshot version: %" PRIu64 "\n", version(snap));
    free_snapshot(snap);
  } else {
    printf("No post-commit snapshot available; loading via snapshot builder.\n");
    ExternResultHandleExclusiveSnapshotBuilder snapshot_builder_res =
        get_snapshot_builder(table_path_slice, engine);
    if (snapshot_builder_res.tag != OkHandleExclusiveSnapshotBuilder) {
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
