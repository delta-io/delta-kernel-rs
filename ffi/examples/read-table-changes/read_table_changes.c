#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// read_table_changes -- C FFI example for the Change Data Feed (CDF) surface
// ============================================================================
//
// Usage:
//   ./read_table_changes [-s start_version] [-e end_version] table/path
//
// Exercises the TableChanges FFI surface:
//   - table_changes_from_version / table_changes_between_versions
//   - table_changes_{schema,table_root,start_version,end_version}
//   - table_changes_scan + table_changes_scan_execute
//   - scan_table_changes_next iteration -> free_arrow_ffi_data
//
// For each returned Arrow batch we read the row count directly off the
// FFI_ArrowArray and print a summary, then release the batch via
// free_arrow_ffi_data. A real engine would instead hand the inner
// FFI_ArrowArray + FFI_ArrowSchema to its own arrow layer (e.g. arrow-glib's
// garrow_record_batch_import, mirroring read-table/arrow.c) before freeing.

// Parse a non-negative integer from optarg into *out. Returns true on success.
static bool parse_version(const char* arg, uint64_t* out) {
  errno = 0;
  char* end = NULL;
  unsigned long long v = strtoull(arg, &end, 10);
  if (errno != 0 || end == arg || *end != '\0') {
    return false;
  }
  *out = (uint64_t)v;
  return true;
}

int main(int argc, char* argv[]) {
  uint64_t start_version = 0;
  bool have_end_version = false;
  uint64_t end_version = 0;

  int c;
  while ((c = getopt(argc, argv, "s:e:")) != -1) {
    switch (c) {
      case 's':
        if (!parse_version(optarg, &start_version)) {
          fprintf(stderr, "Invalid -s value: %s\n", optarg);
          return 1;
        }
        break;
      case 'e':
        if (!parse_version(optarg, &end_version)) {
          fprintf(stderr, "Invalid -e value: %s\n", optarg);
          return 1;
        }
        have_end_version = true;
        break;
      default:
        fprintf(stderr,
                "Usage: %s [-s start_version] [-e end_version] table/path\n",
                argv[0]);
        return 1;
    }
  }
  if (optind != argc - 1) {
    fprintf(stderr,
            "Usage: %s [-s start_version] [-e end_version] table/path\n",
            argv[0]);
    return 1;
  }

  char* table_path = argv[optind];
  printf("Reading change data feed from %s\n", table_path);
  printf("  requested start_version: %" PRIu64 "\n", start_version);
  if (have_end_version) {
    printf("  requested end_version:   %" PRIu64 "\n", end_version);
  } else {
    printf("  requested end_version:   <latest>\n");
  }

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

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

  // === Create a TableChanges ===
  ExternResultHandleExclusiveTableChanges tc_res;
  if (have_end_version) {
    tc_res = table_changes_between_versions(table_path_slice, engine, start_version, end_version);
  } else {
    tc_res = table_changes_from_version(table_path_slice, engine, start_version);
  }
  if (tc_res.tag != OkHandleExclusiveTableChanges) {
    print_error("Failed to construct TableChanges.", (Error*)tc_res.err);
    free_error((Error*)tc_res.err);
    free_engine(engine);
    return 1;
  }
  ExclusiveTableChanges* table_changes = tc_res.ok;

  // === Inspect metadata accessors (these do not consume the handle) ===
  char* tc_table_root = table_changes_table_root(table_changes, allocate_string);
  uint64_t tc_start = table_changes_start_version(table_changes);
  uint64_t tc_end = table_changes_end_version(table_changes);
  printf("TableChanges:\n");
  printf("  table_root:    %s\n", tc_table_root);
  printf("  start_version: %" PRIu64 "\n", tc_start);
  printf("  end_version:   %" PRIu64 "\n", tc_end);
  free(tc_table_root);

  // table_changes_schema() clones the schema; we free it immediately since this example
  // does not pretty-print schemas (read-table's schema.h already demonstrates that).
  SharedSchema* cdf_schema = table_changes_schema(table_changes);
  free_schema(cdf_schema);

  // === Build and execute a scan ===
  //
  // table_changes_scan() consumes the ExclusiveTableChanges handle regardless of success or
  // failure, so we must not free table_changes after this call.
  ExternResultHandleSharedTableChangesScan scan_res =
      table_changes_scan(table_changes, engine, NULL);
  if (scan_res.tag != OkHandleSharedTableChangesScan) {
    print_error("Failed to build table_changes scan.", (Error*)scan_res.err);
    free_error((Error*)scan_res.err);
    free_engine(engine);
    return 1;
  }
  SharedTableChangesScan* scan = scan_res.ok;

  char* scan_root = table_changes_scan_table_root(scan, allocate_string);
  printf("Scan:\n");
  printf("  table_root: %s\n", scan_root);
  free(scan_root);

  // Logical vs physical schema: for CDF, the logical schema is the user-facing schema plus
  // the system columns _change_type, _commit_version, _commit_timestamp.
  SharedSchema* logical = table_changes_scan_logical_schema(scan);
  SharedSchema* physical = table_changes_scan_physical_schema(scan);
  free_schema(logical);
  free_schema(physical);

  ExternResultHandleSharedScanTableChangesIterator iter_res =
      table_changes_scan_execute(scan, engine);
  if (iter_res.tag != OkHandleSharedScanTableChangesIterator) {
    print_error("Failed to execute table_changes scan.", (Error*)iter_res.err);
    free_error((Error*)iter_res.err);
    free_table_changes_scan(scan);
    free_engine(engine);
    return 1;
  }
  SharedScanTableChangesIterator* iter = iter_res.ok;

  // === Iterate batches ===
  //
  // scan_table_changes_next returns Ok(non-null) for a batch, Ok(null) when the iterator
  // is exhausted, or Err. The handle is borrowed (not consumed), so we keep using `iter`
  // across calls. Each non-null batch must be released via free_arrow_ffi_data exactly
  // once.
  printf("CDF batches:\n");
  int exit_code = 0;
  int batch_idx = 0;
  int64_t total_rows = 0;
  for (;;) {
    ExternResultArrowFFIData next_res = scan_table_changes_next(iter);
    if (next_res.tag != OkArrowFFIData) {
      print_error("scan_table_changes_next failed.", (Error*)next_res.err);
      free_error((Error*)next_res.err);
      exit_code = 1;
      break;
    }
    if (next_res.ok == NULL) {
      break;
    }
    ArrowFFIData* batch = next_res.ok;
    printf("  batch %d: %" PRId64 " rows\n", batch_idx, batch->array.length);
    batch_idx++;
    total_rows += batch->array.length;
    free_arrow_ffi_data(batch);
  }
  printf("Total: %d batches, %" PRId64 " rows\n", batch_idx, total_rows);

  free_scan_table_changes_iter(iter);
  free_table_changes_scan(scan);
  free_engine(engine);
  return exit_code;
}
