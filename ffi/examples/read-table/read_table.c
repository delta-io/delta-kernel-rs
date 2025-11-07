#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "arrow.h"
#include "read_table.h"
#include "schema.h"
#include "kernel_utils.h"

// Print the content of a selection vector if `VERBOSE` is defined in read_table.h
void print_selection_vector(const char* indent, const KernelBoolSlice* selection_vec)
{
#ifdef VERBOSE
  for (uintptr_t i = 0; i < selection_vec->len; i++) {
    printf("%ssel[%" PRIxPTR "] = %u\n", indent, i, selection_vec->ptr[i]);
  }
#else
  (void)indent;
  (void)selection_vec;
#endif
}

// Print info about table partitions if `VERBOSE` is defined in read_table.h
void print_partition_info(struct EngineContext* context, const CStringMap* partition_values)
{
#ifdef VERBOSE
  for (uintptr_t i = 0; i < context->partition_cols->len; i++) {
    char* col = context->partition_cols->cols[i];
    KernelStringSlice key = { col, strlen(col) };
    char* partition_val = get_from_string_map(partition_values, key, allocate_string);
    if (partition_val) {
      print_diag("  partition '%s' here: %s\n", col, partition_val);
      free(partition_val);
    } else {
      print_diag("  no partition here\n");
    }
  }
#else
  (void)context;
  (void)partition_values;
#endif
}

// Kernel will call this function for each file that should be scanned. The arguments include enough
// context to construct the correct logical data from the physically read parquet
void scan_row_callback(
  void* engine_context,
  KernelStringSlice path,
  int64_t size,
  const Stats* stats,
  const CDvInfo* cdv_info,
  const Expression* transform,
  const CStringMap* partition_values)
{
  (void)size; // not using this at the moment
  struct EngineContext* context = engine_context;
  print_diag("Called back to read file: %.*s. (size: %" PRIu64 ", num records: ", (int)path.len, path.ptr, size);
  if (stats) {
    print_diag("%" PRId64 ")\n", stats->num_records);
  } else {
    print_diag(" [no stats])\n");
  }
  KernelStringSlice table_root_slice = { context->table_root, strlen(context->table_root) };
  KernelBoolSlice selection_vector;

  if (cdv_info->has_vector) {
    ExternResultKernelBoolSlice selection_vector_res =
      selection_vector_from_dv(cdv_info->info, context->engine, table_root_slice);
    if (selection_vector_res.tag != OkKernelBoolSlice) {
      printf("Could not get selection vector from kernel\n");
      exit(-1);
    }
    selection_vector = selection_vector_res.ok;
    if (selection_vector.len > 0) {
      print_diag("  Selection vector for this file:\n");
      print_selection_vector("    ", &selection_vector);
    } else {
      print_diag("  No selection vector for this file\n");
    }
  } else {
    print_diag("  No selection vector for this file\n");
    selection_vector.len = 0;
  }
  context->partition_values = partition_values;
  print_partition_info(context, partition_values);
#ifdef PRINT_ARROW_DATA
  c_read_parquet_file(context, path, selection_vector, transform);
#endif
  free_bool_slice(selection_vector);
  context->partition_values = NULL;
}

// For each chunk of scan metadata (which may contain multiple files to scan), kernel will call this
// function (named do_visit_scan_metadata to avoid conflict with visit_scan_metadata exported by
// kernel)
void do_visit_scan_metadata(void* engine_context, HandleSharedScanMetadata scan_metadata) {
  print_diag("\nScan iterator found some data to read\n  Of this data, here is "
             "a selection vector\n");
  struct EngineContext* context = engine_context;

  ExternResultKernelBoolSlice selection_vector_res =
    selection_vector_from_scan_metadata(scan_metadata, context->engine);
  if (selection_vector_res.tag != OkKernelBoolSlice) {
    printf("Could not get selection vector from kernel\n");
    exit(-1);
  }
  KernelBoolSlice selection_vector = selection_vector_res.ok;
  print_selection_vector("    ", &selection_vector);

  // Ask kernel to iterate each individual file and call us back with extracted metadata
  print_diag("Asking kernel to call us back for each scan row (file to read)\n");
  visit_scan_metadata(scan_metadata, engine_context, scan_row_callback);
  free_bool_slice(selection_vector);
  free_scan_metadata(scan_metadata);
}

// Called for each element of the partition StringSliceIterator. We just turn the slice into a
// `char*` and append it to our list. We knew the total number of partitions up front, so this
// assumes that `list->cols` has been allocated with enough space to store the pointer.
void visit_partition(void* context, const KernelStringSlice partition)
{
  PartitionList* list = context;
  char* col = allocate_string(partition);
  list->cols[list->len] = col;
  list->len++;
}

// Build a list of partition column names.
PartitionList* get_partition_list(SharedSnapshot* snapshot)
{
  print_diag("Building list of partition columns\n");
  uintptr_t count = get_partition_column_count(snapshot);
  PartitionList* list = malloc(sizeof(PartitionList));
  // We set the `len` to 0 here and use it to track how many items we've added to the list
  list->len = 0;
  list->cols = malloc(sizeof(char*) * count);
  StringSliceIterator* part_iter = get_partition_columns(snapshot);
  for (;;) {
    bool has_next = string_slice_next(part_iter, list, visit_partition);
    if (!has_next) {
      print_diag("Done iterating partition columns\n");
      break;
    }
  }
  if (list->len != count) {
    printf("Error, partition iterator did not return get_partition_column_count columns\n");
    exit(-1);
  }
  if (list->len > 0) {
    print_diag("Partition columns are:\n");
    for (uintptr_t i = 0; i < list->len; i++) {
      print_diag("  - %s\n", list->cols[i]);
    }
  } else {
    print_diag("Table has no partition columns\n");
  }
  free_string_slice_data(part_iter);
  return list;
}

void free_partition_list(PartitionList* list) {
  for (uintptr_t i = 0; i < list->len; i++) {
    free(list->cols[i]);
  }
  free(list->cols);
  free(list);
}

static const char *LEVEL_STRING[] = {
  "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
};

// define some ansi color escapes so we can have nice colored output in our logs
#define RED   "\x1b[31m"
#define BLUE  "\x1b[34m"
#define DIM   "\x1b[2m"
#define RESET "\x1b[0m"

void tracing_callback(struct Event event) {
  struct timeval tv;
  char buffer[32];
  gettimeofday(&tv, NULL);
  struct tm *tm_info = gmtime(&tv.tv_sec);
  strftime(buffer, 26, "%Y-%m-%dT%H:%M:%S", tm_info);
  char* level_color = event.level < 3 ? RED : BLUE;
  printf(
    "%s%s.%06dZ%s [%sKernel %s%s] %s%.*s%s: %.*s\n",
    DIM,
    buffer,
    (int)tv.tv_usec, // safe, microseconds are in int range
    RESET,
    level_color,
    LEVEL_STRING[event.level],
    RESET,
    DIM,
    (int)event.target.len,
    event.target.ptr,
    RESET,
    (int)event.message.len,
    event.message.ptr);
  if (event.file.ptr) {
    printf(
      "  %sat%s %.*s:%i\n",
      DIM,
      RESET,
      (int)event.file.len,
      event.file.ptr,
      event.line);
  }
}

void log_line_callback(KernelStringSlice line) {
  printf("%.*s", (int)line.len, line.ptr);
}

int main(int argc, char* argv[])
{
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

#ifdef VERBOSE
  enable_event_tracing(tracing_callback, TRACE);
  // we could also do something like this if we want less control over formatting
  // enable_formatted_log_line_tracing(log_line_callback, TRACE, FULL, true, true, false, false);
#else
  enable_event_tracing(tracing_callback, INFO);
#endif

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  ExternResultEngineBuilder engine_builder_res =
    get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkEngineBuilder) {
    print_error("Could not get engine builder.", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  // an example of using a builder to set options when building an engine
  EngineBuilder* engine_builder = engine_builder_res.ok;
  set_builder_opt(engine_builder, "aws_region", "us-west-2");
  // potentially set credentials here
  // set_builder_opt(engine_builder, "aws_access_key_id" , "[redacted]");
  // set_builder_opt(engine_builder, "aws_secret_access_key", "[redacted]");
  ExternResultHandleSharedExternEngine engine_res = builder_build(engine_builder);

  // alternately if we don't care to set any options on the builder:
  // ExternResultExternEngineHandle engine_res =
  //   get_default_engine(table_path_slice, NULL);

  if (engine_res.tag != OkHandleSharedExternEngine) {
    print_error("File to get engine", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
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

  uint64_t v = version(snapshot);
  printf("version: %" PRIu64 "\n\n", v);
  print_schema(snapshot);

  char* table_root = snapshot_table_root(snapshot, allocate_string);
  print_diag("Table root: %s\n", table_root);

  PartitionList* partition_cols = get_partition_list(snapshot);

  print_diag("Starting table scan\n\n");

  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL);
  if (scan_res.tag != OkHandleSharedScan) {
    printf("Failed to create scan\n");
    return -1;
  }

  SharedScan* scan = scan_res.ok;

  char* scan_table_path = scan_table_root(scan, allocate_string);
  print_diag("Scan table root: %s\n", scan_table_path);

  SharedSchema* logical_schema = scan_logical_schema(scan);
  SharedSchema* physical_schema = scan_physical_schema(scan);
  struct EngineContext context = {
    logical_schema,
    physical_schema,
    table_root,
    engine,
    partition_cols,
    .partition_values = NULL,
#ifdef PRINT_ARROW_DATA
    .arrow_context = init_arrow_context(),
#endif
  };

  ExternResultHandleSharedScanMetadataIterator data_iter_res =
    scan_metadata_iter_init(engine, scan);
  if (data_iter_res.tag != OkHandleSharedScanMetadataIterator) {
    print_error("Failed to construct scan metadata iterator.", (Error*)data_iter_res.err);
    free_error((Error*)data_iter_res.err);
    return -1;
  }

  SharedScanMetadataIterator* data_iter = data_iter_res.ok;

  print_diag("\nIterating scan metadata\n");

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res =
      scan_metadata_next(data_iter, &context, do_visit_scan_metadata);
    if (ok_res.tag != Okbool) {
      print_error("Failed to iterate scan metadata.", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      return -1;
    } else if (!ok_res.ok) {
      print_diag("Scan metadata iterator done\n");
      break;
    }
  }

  print_diag("All done reading table data\n");

#ifdef PRINT_ARROW_DATA
  print_arrow_context(context.arrow_context);
  free_arrow_context(context.arrow_context);
  context.arrow_context = NULL;
#endif

  free_scan_metadata_iter(data_iter);
  free_scan(scan);
  free_schema(logical_schema);
  free_schema(physical_schema);
  free_snapshot(snapshot);
  free_engine(engine);
  free(context.table_root);
  free(scan_table_path);
  free_partition_list(context.partition_cols);

  return 0;
}
