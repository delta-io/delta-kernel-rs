#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// checkpoint_table -- C FFI example for the checkpoint write surface
// ============================================================================
//
// Usage:
//   ./checkpoint_table /path/to/existing/table <sub-flow>
//
// where <sub-flow> is one of:
//   inline            -- no setter; kernel auto-picks V1/V2 from table protocol.
//   v2_no_sidecar     -- checkpoint_builder_set_v2_no_sidecar; V2 manifest, no sidecars.
//   v2_with_sidecars  -- checkpoint_builder_set_v2_with_sidecars(2); V2 manifest + sidecars.
//
// Demonstrates the opaque FfiCheckpointBuilder family:
//   - checkpoint_builder_for(snapshot, engine)
//   - checkpoint_builder_set_v2_no_sidecar(builder)
//   - checkpoint_builder_set_v2_with_sidecars(builder, hint)
//   - checkpoint_builder_build(builder) -> ExternResultFfiCheckpointWriteResult
//   - free_checkpoint_builder(builder) for the discard path (not exercised here)
//   - FfiCheckpointWriteResult discriminator inspection (Written vs AlreadyExists)
//   - free_snapshot on the returned snapshot handle from either variant

static int run_sub_flow(HandleSharedSnapshot snapshot,
                        SharedExternEngine* engine,
                        const char* sub_flow) {
  HandleMutableFfiCheckpointBuilder builder = checkpoint_builder_for(snapshot, engine);

  if (strcmp(sub_flow, "inline") == 0) {
    // No setter -- kernel auto-picks V1 or V2 based on table protocol features.
  } else if (strcmp(sub_flow, "v2_no_sidecar") == 0) {
    checkpoint_builder_set_v2_no_sidecar(&builder);
  } else if (strcmp(sub_flow, "v2_with_sidecars") == 0) {
    // Pass a small hint (2) so the fixture's handful of file actions splits across
    // multiple sidecars, making the on-disk shape observably different from the
    // V2-no-sidecar sub-flow.
    checkpoint_builder_set_v2_with_sidecars(&builder, 2);
  } else {
    fprintf(stderr, "Unknown sub-flow: %s\n", sub_flow);
    free_checkpoint_builder(builder);
    return 1;
  }

  ExternResultFfiCheckpointWriteResult build_res = checkpoint_builder_build(builder);
  if (build_res.tag != OkFfiCheckpointWriteResult) {
    print_error("checkpoint_builder_build failed.", (Error*)build_res.err);
    free_error((Error*)build_res.err);
    return 1;
  }

  FfiCheckpointWriteResult result = build_res.ok;
  HandleSharedSnapshot returned;
  const char* discrim_str;
  switch (result.tag) {
    case FfiCheckpointWriteResultWritten:
      returned = result.written;
      discrim_str = "Written";
      break;
    case FfiCheckpointWriteResultAlreadyExists:
      returned = result.already_exists;
      discrim_str = "AlreadyExists";
      break;
    default:
      fprintf(stderr, "Unexpected FfiCheckpointWriteResult tag: %d\n", result.tag);
      return 1;
  }

  uint64_t returned_version = version(returned);
  printf("sub_flow=%s result=%s version=%" PRIu64 "\n",
         sub_flow, discrim_str, returned_version);
  free_snapshot(returned);
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    fprintf(stderr,
            "Usage: %s /path/to/existing/table <sub-flow>\n"
            "  <sub-flow>: inline | v2_no_sidecar | v2_with_sidecars\n",
            argv[0]);
    return 1;
  }
  char* table_path = argv[1];
  char* sub_flow = argv[2];
  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  // === Build engine ===
  ExternResultEngineBuilder engine_builder_res =
      get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkEngineBuilder) {
    print_error("Could not get engine builder.", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return 1;
  }
  // Snapshot::checkpoint performs async I/O (read commit JSONs + write parquet checkpoint /
  // sidecars). Calling it from sync C without an explicit executor would hang waiting for one,
  // because DefaultEngineBuilder's lazy-executor path needs an outer tokio runtime to capture
  // and there isn't one here. Set a small multithreaded executor (2 workers, default blocking
  // threads) to drive the engine's async work.
  set_builder_with_multithreaded_executor(engine_builder_res.ok,
                                          /*worker_threads*/ 2,
                                          /*max_blocking_threads*/ 0);
  ExternResultHandleSharedExternEngine engine_res = builder_build(engine_builder_res.ok);
  if (engine_res.tag != OkHandleSharedExternEngine) {
    print_error("Failed to build engine.", (Error*)engine_res.err);
    free_error((Error*)engine_res.err);
    return 1;
  }
  SharedExternEngine* engine = engine_res.ok;

  // === Build snapshot ===
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
    print_error("Failed to load snapshot.", (Error*)snap_res.err);
    free_error((Error*)snap_res.err);
    free_engine(engine);
    return 1;
  }
  HandleSharedSnapshot snapshot = snap_res.ok;

  int rc = run_sub_flow(snapshot, engine, sub_flow);

  free_snapshot(snapshot);
  free_engine(engine);
  return rc;
}
