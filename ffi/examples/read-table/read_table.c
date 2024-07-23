#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "arrow.h"
#include "read_table.h"
#include "schema.h"

// some diagnostic functions
void print_diag(char* fmt, ...)
{
#ifdef VERBOSE
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
#else
  (void)(fmt);
#endif
}

// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err)
{
  printf("[ERROR] %s\n", msg);
  printf("  Kernel Code: %i\n", err->etype.etype);
  printf("  Kernel Msg: %s\n", err->msg);
}

// free an error
void free_error(Error* error)
{
  free(error->msg);
  free(error);
}

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
  for (int i = 0; i < context->partition_cols->len; i++) {
    char* col = context->partition_cols->cols[i];
    KernelStringSlice key = { col, strlen(col) };
    char* partition_val = get_from_map(partition_values, key, allocate_string);
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

// kernel will call this to allocate our errors. This can be used to create an "engine native" type
// error
EngineError* allocate_error(KernelError etype, const KernelStringSlice msg)
{
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

// utility to turn a slice into a char*
void* allocate_string(const KernelStringSlice slice)
{
  return strndup(slice.ptr, slice.len);
}

// utility function to convert key/val into slices and set them on a builder
void set_builder_opt(EngineBuilder* engine_builder, char* key, char* val)
{
  KernelStringSlice key_slice = { key, strlen(key) };
  KernelStringSlice val_slice = { val, strlen(val) };
  set_builder_option(engine_builder, key_slice, val_slice);
}

// Kernel will call this function for each file that should be scanned. The arguments include enough
// context to constuct the correct logical data from the physically read parquet
void scan_row_callback(
  void* engine_context,
  KernelStringSlice path,
  int64_t size,
  const Stats* stats,
  const DvInfo* dv_info,
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
  ExternResultKernelBoolSlice selection_vector_res =
    selection_vector_from_dv(dv_info, context->engine, context->global_state);
  if (selection_vector_res.tag != OkKernelBoolSlice) {
    printf("Could not get selection vector from kernel\n");
    exit(-1);
  }
  KernelBoolSlice selection_vector = selection_vector_res.ok;
  if (selection_vector.len > 0) {
    print_diag("  Selection vector for this file:\n");
    print_selection_vector("    ", &selection_vector);
  } else {
    print_diag("  No selection vector for this file\n");
  }
  context->partition_values = partition_values;
  print_partition_info(context, partition_values);
#ifdef PRINT_ARROW_DATA
  c_read_parquet_file(context, path, selection_vector);
#endif
  free_bool_slice(selection_vector);
  context->partition_values = NULL;
}

// For each chunk of scan data (which may contain multiple files to scan), kernel will call this
// function (named do_visit_scan_data to avoid conflict with visit_scan_data exported by kernel)
void do_visit_scan_data(
  void* engine_context,
  ExclusiveEngineData* engine_data,
  KernelBoolSlice selection_vec)
{
  print_diag("\nScan iterator found some data to read\n  Of this data, here is "
             "a selection vector\n");
  print_selection_vector("    ", &selection_vec);
  // Ask kernel to iterate each individual file and call us back with extracted metadata
  print_diag("Asking kernel to call us back for each scan row (file to read)\n");
  visit_scan_data(engine_data, selection_vec, engine_context, scan_row_callback);
  free_bool_slice(selection_vec);
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
PartitionList* get_partition_list(SharedGlobalScanState* state)
{
  print_diag("Building list of partition columns\n");
  int count = get_partition_column_count(state);
  PartitionList* list = malloc(sizeof(PartitionList));
  // We set the `len` to 0 here and use it to track how many items we've added to the list
  list->len = 0;
  list->cols = malloc(sizeof(char*) * count);
  StringSliceIterator* part_iter = get_partition_columns(state);
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
    for (int i = 0; i < list->len; i++) {
      print_diag("  - %s\n", list->cols[i]);
    }
  } else {
    print_diag("Table has no partition columns\n");
  }
  free_string_slice_data(part_iter);
  return list;
}

int main(int argc, char* argv[])
{
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

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

  print_diag("Starting table scan\n\n");

  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL);
  if (scan_res.tag != OkHandleSharedScan) {
    printf("Failed to create scan\n");
    return -1;
  }

  SharedScan* scan = scan_res.ok;
  SharedGlobalScanState* global_state = get_global_scan_state(scan);
  SharedSchema* read_schema = get_global_read_schema(global_state);
  PartitionList* partition_cols = get_partition_list(global_state);
  struct EngineContext context = {
    global_state,
    read_schema,
    table_root,
    engine,
    partition_cols,
    .partition_values = NULL,
#ifdef PRINT_ARROW_DATA
    .arrow_context = init_arrow_context(),
#endif
  };

  ExternResultHandleSharedScanDataIterator data_iter_res = kernel_scan_data_init(engine, scan);
  if (data_iter_res.tag != OkHandleSharedScanDataIterator) {
    print_error("Failed to construct scan data iterator.", (Error*)data_iter_res.err);
    free_error((Error*)data_iter_res.err);
    return -1;
  }

  SharedScanDataIterator* data_iter = data_iter_res.ok;

  print_diag("\nIterating scan data\n");

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res = kernel_scan_data_next(data_iter, &context, do_visit_scan_data);
    if (ok_res.tag != Okbool) {
      print_error("Failed to iterate scan data.", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      return -1;
    } else if (!ok_res.ok) {
      print_diag("Scan data iterator done\n");
      break;
    }
  }

  print_diag("All done reading table data\n");

#ifdef PRINT_ARROW_DATA
  print_arrow_context(context.arrow_context);
  free_arrow_context(context.arrow_context);
  context.arrow_context = NULL;
#endif

  free_kernel_scan_data(data_iter);
  free_global_read_schema(read_schema);
  free_global_scan_state(global_state);
  free_snapshot(snapshot);
  free_engine(engine);
  free(context.table_root);

  return 0;
}
