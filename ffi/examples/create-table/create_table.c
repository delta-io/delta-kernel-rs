#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

// ============================================================================
// create_table -- C FFI example for CREATE TABLE
// ============================================================================
//
// Usage:
//   ./create_table /path/to/new/table
//
// Demonstrates:
//   - Building a schema via the KernelSchemaVisitorState API (engine-side -> kernel-side
//     schema conversion): visit_field_long, visit_field_string, visit_field_struct.
//   - get_create_table_builder with a static schema spec.
//   - create_table_builder_with_table_property to set `delta.enableChangeDataFeed`.
//   - create_table_builder_build -> create_table_commit.
//   - Opening a snapshot on the freshly-created table to confirm the commit landed.
//
// Note: get_create_table_builder takes engine_info and stores it on the builder, so the
// transaction is already labelled by the time we call create_table_commit. The example
// therefore does NOT call create_table_with_engine_info -- that function exists for
// engines that want to override the engine_info after building (and is exercised by the
// existing-table write path in write-table).
//
// The example does not stage any initial files. The
// add_files flow requires constructing an Arrow batch matching Transaction::add_files_schema,
// which would pull in arrow-glib (or a similar C-level Arrow builder).

// === Schema visitor ===
//
// The engine provides `struct EngineSchema { void* schema; uintptr_t (*visitor)(...); }`.
// Our schema description is a simple array of fields (no nesting). The visitor calls the
// kernel's visit_field_* functions to register each field, then a final visit_field_struct
// to register the top-level struct.

enum field_type {
  FIELD_LONG,
  FIELD_STRING,
};

struct field_spec {
  const char* name;
  enum field_type type;
  bool nullable;
};

struct schema_spec {
  const struct field_spec* fields;
  size_t field_count;
};

static uintptr_t build_schema(void* data, KernelSchemaVisitorState* state) {
  const struct schema_spec* spec = data;
  uintptr_t* child_ids = malloc(spec->field_count * sizeof(uintptr_t));
  for (size_t i = 0; i < spec->field_count; i++) {
    const struct field_spec* f = &spec->fields[i];
    KernelStringSlice name = { f->name, strlen(f->name) };
    ExternResultusize r;
    switch (f->type) {
      case FIELD_LONG:
        r = visit_field_long(state, name, f->nullable, allocate_error);
        break;
      case FIELD_STRING:
        r = visit_field_string(state, name, f->nullable, allocate_error);
        break;
      default:
        fprintf(stderr, "Unknown field type %d\n", f->type);
        free(child_ids);
        return 0;
    }
    if (r.tag != Okusize) {
      print_error("visit_field_* failed", (Error*)r.err);
      free_error((Error*)r.err);
      free(child_ids);
      return 0;
    }
    child_ids[i] = r.ok;
  }
  // Register the top-level struct. The name of the top-level struct is not used by kernel;
  // pass a stable placeholder.
  KernelStringSlice root_name = { "root", 4 };
  ExternResultusize root = visit_field_struct(
      state, root_name, child_ids, spec->field_count, /*nullable*/ false, allocate_error);
  free(child_ids);
  if (root.tag != Okusize) {
    print_error("visit_field_struct failed for top-level", (Error*)root.err);
    free_error((Error*)root.err);
    return 0;
  }
  return root.ok;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s /path/to/new/table\n", argv[0]);
    return 1;
  }
  char* table_path = argv[1];
  printf("Creating table at %s\n", table_path);
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

  // === Describe the schema ===
  // NOTE: Delta requires columns to be nullable unless the `invariants` writer feature is
  // enabled.
  static const struct field_spec fields[] = {
    { "id", FIELD_LONG, /*nullable*/ true },
    { "name", FIELD_STRING, /*nullable*/ true },
  };
  struct schema_spec spec = { fields, sizeof(fields) / sizeof(fields[0]) };
  EngineSchema engine_schema = { .schema = &spec, .visitor = build_schema };

  // === Get create-table builder ===
  const char* engine_info_str = "create_table_example";
  KernelStringSlice engine_info_slice = { engine_info_str, strlen(engine_info_str) };
  ExternResultHandleExclusiveCreateTableBuilder builder_res =
      get_create_table_builder(table_path_slice, &engine_schema, engine_info_slice, engine);
  if (builder_res.tag != OkHandleExclusiveCreateTableBuilder) {
    print_error("Failed to get create-table builder.", (Error*)builder_res.err);
    free_error((Error*)builder_res.err);
    free_engine(engine);
    return 1;
  }
  ExclusiveCreateTableBuilder* builder = builder_res.ok;

  // === Chain a table property (CONSUMES and RETURNS the builder handle) ===
  const char* prop_key = "delta.enableChangeDataFeed";
  const char* prop_val = "true";
  KernelStringSlice prop_key_slice = { prop_key, strlen(prop_key) };
  KernelStringSlice prop_val_slice = { prop_val, strlen(prop_val) };
  ExternResultHandleExclusiveCreateTableBuilder prop_res = create_table_builder_with_table_property(
      builder, prop_key_slice, prop_val_slice, engine);
  if (prop_res.tag != OkHandleExclusiveCreateTableBuilder) {
    // IMPORTANT: the old builder handle is consumed unconditionally, including on error.
    print_error("Failed to set table property.", (Error*)prop_res.err);
    free_error((Error*)prop_res.err);
    free_engine(engine);
    return 1;
  }
  builder = prop_res.ok;

  // === Build -> produces a create-table transaction ===
  ExternResultHandleExclusiveCreateTransaction txn_res =
      create_table_builder_build(builder, engine);
  if (txn_res.tag != OkHandleExclusiveCreateTransaction) {
    print_error("create_table_builder_build failed.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    free_engine(engine);
    return 1;
  }
  ExclusiveCreateTransaction* txn = txn_res.ok;

  // === Commit ===
  ExternResultu64 commit_res = create_table_commit(txn, engine);
  if (commit_res.tag != Oku64) {
    print_error("create_table_commit failed.", (Error*)commit_res.err);
    free_error((Error*)commit_res.err);
    free_engine(engine);
    return 1;
  }
  printf("Committed version: %" PRIu64 "\n", commit_res.ok);

  // === Open a snapshot on the new table to confirm it landed ===
  ExternResultHandleMutableFfiSnapshotBuilder snapshot_builder_res =
      get_snapshot_builder(table_path_slice, engine);
  if (snapshot_builder_res.tag != OkHandleMutableFfiSnapshotBuilder) {
    print_error("Failed to get snapshot builder.", (Error*)snapshot_builder_res.err);
    free_error((Error*)snapshot_builder_res.err);
    free_engine(engine);
    return 1;
  }
  ExternResultHandleSharedSnapshot snapshot_res =
      snapshot_builder_build(snapshot_builder_res.ok);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to load snapshot of created table.", (Error*)snapshot_res.err);
    free_error((Error*)snapshot_res.err);
    free_engine(engine);
    return 1;
  }
  SharedSnapshot* snapshot = snapshot_res.ok;
  printf("Snapshot version after create: %" PRIu64 "\n", version(snapshot));
  printf("Partition columns: %" PRIuPTR "\n", get_partition_column_count(snapshot));

  free_snapshot(snapshot);
  free_engine(engine);
  return 0;
}
