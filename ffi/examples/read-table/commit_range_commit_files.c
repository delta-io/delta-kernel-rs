// Prints the commit files of a commit range through the FFI commit-file iterator.

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kernel_utils.h"

static void print_and_free_error(const char* msg, EngineError* err)
{
  Error* error = (Error*)err;
  print_error(msg, error);
  free_error(error);
}

// The commit file and its location are borrowed for this call only, so nothing is retained.
static void print_commit_file(NullableCvoid engine_context, const FfiCommitFile* commit_file)
{
  (void)engine_context;
  printf("commit file:\n");
  printf("  version: %" PRIu64 "\n", commit_file->version);
  printf(
    "  location: %.*s\n", (int)commit_file->file.location.len, commit_file->file.location.ptr);
  printf("  last_modified: %" PRId64 "\n", commit_file->file.last_modified);
  printf("  size: %" PRIu64 "\n", commit_file->file.size);
}

static bool print_commit_files(const char* table_path)
{
  bool success = false;
  SharedExternEngine* engine = NULL;
  SharedCommitRange* commit_range = NULL;
  SharedCommitFilesIterator* iter = NULL;
  KernelStringSlice path = { table_path, strlen(table_path) };

  ExternResultHandleSharedExternEngine engine_result = get_default_engine(path, allocate_error);
  if (engine_result.tag != OkHandleSharedExternEngine) {
    print_and_free_error("Could not build engine", engine_result.err);
    goto cleanup;
  }
  engine = engine_result.ok;

  ExternResultHandleExclusiveCommitRangeBuilder range_builder =
    commit_range_builder_for(path, 0, engine);
  if (range_builder.tag != OkHandleExclusiveCommitRangeBuilder) {
    print_and_free_error("Could not get commit range builder", range_builder.err);
    goto cleanup;
  }
  // Both calls consume the builder, so it is never freed here.
  HandleExclusiveCommitRangeBuilder pinned =
    commit_range_builder_with_end_version(range_builder.ok, 1);
  ExternResultHandleSharedCommitRange range_result = commit_range_builder_build(pinned);
  if (range_result.tag != OkHandleSharedCommitRange) {
    print_and_free_error("Could not build commit range", range_result.err);
    goto cleanup;
  }
  commit_range = range_result.ok;

  ExternResultHandleSharedCommitFilesIterator iter_result =
    commit_range_commit_files(commit_range, engine);
  if (iter_result.tag != OkHandleSharedCommitFilesIterator) {
    print_and_free_error("Could not get commit files iterator", iter_result.err);
    goto cleanup;
  }
  iter = iter_result.ok;

  for (;;) {
    ExternResultbool next = commit_range_commit_files_next(iter, NULL, print_commit_file);
    if (next.tag != Okbool) {
      print_and_free_error("Failed to advance commit files iterator", next.err);
      goto cleanup;
    }
    if (!next.ok) {
      break;
    }
  }
  success = true;

cleanup:
  if (iter) free_commit_files_iter(iter);
  if (commit_range) free_commit_range(commit_range);
  if (engine) free_engine(engine);
  return success;
}

int main(int argc, char** argv)
{
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <table-path>\n", argv[0]);
    return EXIT_FAILURE;
  }
  return print_commit_files(argv[1]) ? EXIT_SUCCESS : EXIT_FAILURE;
}
