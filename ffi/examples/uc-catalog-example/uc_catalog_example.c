#include <delta_kernel_ffi.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "kernel_utils.h"

// Context struct to hold any state needed by our client
// This can hold connection info, auth tokens, etc.
typedef struct UCContext {
    int call_count;
    const char* base_url;
} UCContext;


// Check that a staging file matches what the commit info says, then remove it
void validate_and_clean_staging_file(char* table_uri, char* file_name, Commit *commit_info) {
  char* uri = table_uri;

  // strip 'file://' if it's present
  if(strncmp(table_uri, "file://", 7) == 0) {
    uri = uri + 7;
  }

  int path_len = strlen(uri) + strlen(file_name) + 28;
  char path[path_len];
  snprintf(path, path_len, "%s_delta_log/_staged_commits/%s", uri, file_name);
  printf("Checking that staging file at %s is valid\n", path);
  struct stat buf;
  if (stat(path, &buf)) {
    // stat returned an error
    perror("Could not stat the staging file!");
    exit(-1);
  } else {
    if (buf.st_size != commit_info->file_size) {
      printf("staged has size: %9jd, but commit_info says something else\n", (intmax_t)buf.st_size);
      exit(-1);
    }
#if defined(__APPLE__)
    time_t mt = buf.st_mtimespec.tv_sec;
#else
    time_t mt = buf.st_mtim.tv_sec;
#endif
    time_t expected_mt = commit_info->file_modification_timestamp / 1000;
    if (mt != expected_mt) {
      printf("staged has modification time: %ld, but commit_info has %ld\n", mt, expected_mt);
      exit(-1);
    }
    printf("Staged file looks good\n");
    if (unlink(path)) {
      perror("Couldn't removed staged file");
    } else {
      printf("Removed staged file\n\n");
    }
  }
}

// our implementation of `commit`
OptionalValueHandleExclusiveRustString commit_callback(
    NullableCvoid context_ptr,
    CommitRequest request)
{
    UCContext* context = NULL;
    if (context_ptr != NULL) {
        context = (UCContext*)context_ptr;
        context->call_count++;
        printf("commit called (call #%d)\n", context->call_count);
        printf("committing to catalog at: %s\n", context->base_url);
    } else {
        printf("commit called\n");
    }

    // Extract request information
    char table_id[256];
    char table_uri[1024];
    snprintf(table_id, sizeof(table_id), "%.*s", (int)request.table_id.len, request.table_id.ptr);
    snprintf(table_uri, sizeof(table_uri), "%.*s", (int)request.table_uri.len, request.table_uri.ptr);

    printf("Committing to table ID: %s\n", table_id);
    printf("Table URI: %s\n", table_uri);

    if (request.commit_info.tag == SomeCommit) {
        Commit commit_info = request.commit_info.some;
        char* file_name = allocate_string(commit_info.file_name);

        printf("Commit info:\n");
        printf("  Version: %" PRId64 "\n", commit_info.version);
        printf("  Timestamp: %" PRId64 "\n", commit_info.timestamp);
        printf("  File name: %s\n", file_name);
        printf("  File size: %" PRId64 "\n", commit_info.file_size);
        printf("  File mod time: %" PRId64 "\n\n", commit_info.file_modification_timestamp);

        validate_and_clean_staging_file(table_uri, file_name, &commit_info);
        free(file_name);
    }

    if (request.latest_backfilled_version.tag == Somei64) {
        printf("Latest backfilled version: %" PRId64 "\n",
               request.latest_backfilled_version.some);
    }

    // Return None to indicate success
    OptionalValueHandleExclusiveRustString result;
    result.tag = NoneHandleExclusiveRustString;
    return result;
}

int main(int argc, char* argv[])
{
    if (argc != 2) {
        printf("Usage: %s <table_path>\n", argv[0]);
        return -1;
    }

    char* table_path = argv[1];

    // Initialize our UC context
    UCContext uc_context = {
        .call_count = 0,
        .base_url = "https://uc-catalog.example.com/api/v1"
    };

    // Create a UC commit client
    NullableCvoid context = (void*)&uc_context;
    HandleSharedFfiUCCommitClient uc_client = get_uc_commit_client(context, commit_callback);

    // Create a UC committer for a specific table
    const char* table_id = "my_catalog.my_schema.my_table";
    KernelStringSlice table_id_slice = { .ptr = table_id, .len = strlen(table_id) };

    ExternResultHandleMutableCommitter committer_res =
        get_uc_committer(uc_client, table_id_slice, allocate_error);

    if (committer_res.tag != OkHandleMutableCommitter) {
        print_error("Failed to create UC committer", (Error*)committer_res.err);
        free_error((Error*)committer_res.err);
        free_uc_commit_client(uc_client);
        return -1;
    }

    HandleMutableCommitter uc_committer = committer_res.ok;

    // Get the default engine
    KernelStringSlice table_path_slice = { .ptr = table_path, .len = strlen(table_path) };
    ExternResultEngineBuilder engine_builder_res =
        get_engine_builder(table_path_slice, allocate_error);

    if (engine_builder_res.tag != OkEngineBuilder) {
        print_error("Could not get engine builder", (Error*)engine_builder_res.err);
        free_error((Error*)engine_builder_res.err);
        free_uc_commit_client(uc_client);
        return -1;
    }

    EngineBuilder* engine_builder = engine_builder_res.ok;
    ExternResultHandleSharedExternEngine engine_res = builder_build(engine_builder);

    if (engine_res.tag != OkHandleSharedExternEngine) {
        print_error("Failed to build engine", (Error*)engine_res.err);
        free_error((Error*)engine_res.err);
        free_uc_commit_client(uc_client);
        return -1;
    }

    SharedExternEngine* engine = engine_res.ok;

    ExternResultHandleSharedSnapshot snapshot_res = snapshot(table_path_slice, engine);
    if (snapshot_res.tag != OkHandleSharedSnapshot) {
      print_error("Failed to create snapshot.", (Error*)snapshot_res.err);
      free_error((Error*)snapshot_res.err);
      return -1;
    }

    SharedSnapshot* snapshot = snapshot_res.ok;

    // Create a transaction with the UC committer
    ExternResultHandleExclusiveTransaction txn_res =
      transaction_with_committer(snapshot, engine, uc_committer);

    if (txn_res.tag != OkHandleExclusiveTransaction) {
        print_error("Failed to create transaction with UC committer", (Error*)txn_res.err);
        free_error((Error*)txn_res.err);
        free_engine(engine);
        free_uc_commit_client(uc_client);
        return -1;
    }

    HandleExclusiveTransaction txn = txn_res.ok;

    // In a real txn we could now add files using add_files()

    // Add engine info to the transaction
    const char* engine_info = "uc_example_engine";
    KernelStringSlice engine_info_slice = { .ptr = engine_info, .len = strlen(engine_info) };

    ExternResultHandleExclusiveTransaction txn_with_info_res =
        with_engine_info(txn, engine_info_slice, engine);

    if (txn_with_info_res.tag != OkHandleExclusiveTransaction) {
        print_error("Failed to set engine info", (Error*)txn_with_info_res.err);
        free_error((Error*)txn_with_info_res.err);
        free_engine(engine);
        free_uc_commit_client(uc_client);
        return -1;
    }

    HandleExclusiveTransaction txn_with_info = txn_with_info_res.ok;
    // calling commit here will end up calling our callback
    ExternResultu64 commit_res = commit(txn_with_info, engine);

    if (commit_res.tag != Oku64) {
        print_error("Commit failed", (Error*)commit_res.err);
        free_error((Error*)commit_res.err);
        free_engine(engine);
        free_uc_commit_client(uc_client);
        return -1;
    }

    printf("\nCommitted version: %lu\n", (unsigned long)commit_res.ok);

    // Cleanup
    // Note: txn_with_info was consumed by commit(), so we don't free it
    free_engine(engine);
    free_uc_commit_client(uc_client);
    free_snapshot(snapshot);

    printf("Total UC API calls: %d\n", uc_context.call_count);

    return 0;
}
