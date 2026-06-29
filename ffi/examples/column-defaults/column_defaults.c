#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// column_defaults -- C FFI example for the column-defaults discovery surface
// ============================================================================
//
// Usage:
//   ./column_defaults /path/to/table
//
// Requires libdelta_kernel_ffi to be built with the `column-defaults-in-dev` feature (so the
// `DEFINE_COLUMN_DEFAULTS_IN_DEV` guarded symbols are present in the header and library).
//
// Demonstrates both column-defaults entry points sharing one `EngineColumnDefaultVisitor`:
//   - visit_schema_column_defaults(schema, engine, visitor) over a snapshot's logical schema
//     (no feature gate -- reports whatever defaults the fields declare)
//   - visit_column_defaults(txn, engine, visitor) over a transaction (requires the
//     `allowColumnDefaults` writer feature)
//
// For each defaulted top-level column the visitor receives the logical name, the raw
// CURRENT_DEFAULT SQL, whether the kernel parsed it, and -- when parsable -- an owned
// SharedExpression handle the engine must free with free_kernel_expression.

#define MAX_DEFAULTS 32

typedef struct
{
  char* name;
  char* raw_sql;
  bool is_kernel_parsable;
  bool has_parsed_expr;
} CollectedDefault;

typedef struct
{
  CollectedDefault items[MAX_DEFAULTS];
  size_t count;
} Collector;

// EngineColumnDefaultVisitor::visit_default. Records each column into the Collector behind
// `data` and frees the kernel-parsed expression handle (ownership transfers to the engine).
static void visit_default(
  void* data,
  KernelStringSlice name,
  KernelStringSlice raw_sql,
  bool is_kernel_parsable,
  struct OptionalValueHandleSharedExpression default_expr)
{
  Collector* collector = (Collector*)data;
  bool has_parsed_expr = (default_expr.tag == SomeHandleSharedExpression);
  // The kernel transfers ownership of the parsed-default expression to us; free it. A real
  // engine would first read the literal via visit_expression / EngineExpressionVisitor.
  if (has_parsed_expr) {
    free_kernel_expression(default_expr.some);
  }
  if (collector->count >= MAX_DEFAULTS) {
    printf("[WARN] too many column defaults; dropping %.*s\n", (int)name.len, name.ptr);
    return;
  }
  CollectedDefault* item = &collector->items[collector->count++];
  item->name = allocate_string(name);
  item->raw_sql = allocate_string(raw_sql);
  item->is_kernel_parsable = is_kernel_parsable;
  item->has_parsed_expr = has_parsed_expr;
}

static int cmp_by_name(const void* a, const void* b)
{
  return strcmp(((const CollectedDefault*)a)->name, ((const CollectedDefault*)b)->name);
}

// Free the heap strings the collector owns and reset it to empty. Call this on every exit path
// after one or more callbacks may have fired, including error returns -- otherwise the strings
// allocated for columns visited before an error leak.
static void reset_collector(Collector* collector)
{
  for (size_t i = 0; i < collector->count; i++) {
    free(collector->items[i].name);
    free(collector->items[i].raw_sql);
  }
  collector->count = 0;
}

// Print the collected defaults in a deterministic (name-sorted) order, then reset the collector
// and free the strings it owns.
static void print_and_reset(const char* header, Collector* collector)
{
  qsort(collector->items, collector->count, sizeof(CollectedDefault), cmp_by_name);
  printf("%s (%zu):\n", header, collector->count);
  for (size_t i = 0; i < collector->count; i++) {
    CollectedDefault* item = &collector->items[i];
    printf(
      "  %s: raw_sql=\"%s\" kernel_parsable=%s has_parsed_expr=%s\n",
      item->name,
      item->raw_sql,
      item->is_kernel_parsable ? "true" : "false",
      item->has_parsed_expr ? "true" : "false");
  }
  reset_collector(collector);
}

int main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "Usage: %s /path/to/table\n", argv[0]);
    return 1;
  }
  char* table_path = argv[1];
  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  // === Build engine ===
  ExternResultEngineBuilder engine_builder_res = get_engine_builder(table_path_slice, allocate_error);
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

  Collector collector = { 0 };
  EngineColumnDefaultVisitor visitor = { .data = &collector, .visit_default = visit_default };

  // === Schema-level discovery (no transaction, no feature gate) ===
  ExternResultHandleMutableFfiSnapshotBuilder snap_builder_res =
    get_snapshot_builder(table_path_slice, engine);
  if (snap_builder_res.tag != OkHandleMutableFfiSnapshotBuilder) {
    print_error("Failed to get snapshot builder.", (Error*)snap_builder_res.err);
    free_error((Error*)snap_builder_res.err);
    free_engine(engine);
    return 1;
  }
  ExternResultHandleSharedSnapshot snapshot_res = snapshot_builder_build(snap_builder_res.ok);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to build snapshot.", (Error*)snapshot_res.err);
    free_error((Error*)snapshot_res.err);
    free_engine(engine);
    return 1;
  }
  SharedSnapshot* snapshot = snapshot_res.ok;
  SharedSchema* schema = logical_schema(snapshot);

  ExternResultusize schema_count = visit_schema_column_defaults(&schema, engine, &visitor);
  if (schema_count.tag != Okusize) {
    print_error("visit_schema_column_defaults failed.", (Error*)schema_count.err);
    free_error((Error*)schema_count.err);
    reset_collector(&collector);
    free_schema(schema);
    free_snapshot(snapshot);
    free_engine(engine);
    return 1;
  }
  print_and_reset("Schema column defaults", &collector);
  free_schema(schema);
  free_snapshot(snapshot);

  // === Transaction-level discovery (enforces the allowColumnDefaults writer feature) ===
  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path_slice, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Failed to start transaction.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    free_engine(engine);
    return 1;
  }
  ExclusiveTransaction* txn = txn_res.ok;

  ExternResultusize txn_count = visit_column_defaults(&txn, engine, &visitor);
  if (txn_count.tag != Okusize) {
    print_error("visit_column_defaults failed.", (Error*)txn_count.err);
    free_error((Error*)txn_count.err);
    reset_collector(&collector);
    free_transaction(txn);
    free_engine(engine);
    return 1;
  }
  print_and_reset("Transaction column defaults", &collector);

  free_transaction(txn);
  free_engine(engine);
  return 0;
}
