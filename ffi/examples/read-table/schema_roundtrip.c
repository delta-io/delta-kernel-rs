#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kernel_utils.h"
#include "read_table.h"
#include "schema.h"
#include "kernel_schema_visitor.h"

static void require(bool condition, const char* message)
{
  if (!condition) {
    fprintf(stderr, "[ERROR] %s\n", message);
    exit(1);
  }
}

static void require_field(
  const SchemaItem* item,
  const char* name,
  const char* type,
  bool is_nullable,
  const char* column_mapping_id,
  const char* physical_name)
{
  require(strcmp(item->name, name) == 0, "unexpected field name");
  require(strcmp(item->type, type) == 0, "unexpected field type");
  require(item->is_nullable == is_nullable, "unexpected field nullability");
  require(item->column_mapping_id != NULL, "missing column mapping id");
  require(strcmp(item->column_mapping_id, column_mapping_id) == 0, "wrong column mapping id");
  require(item->column_mapping_physical_name != NULL, "missing physical name");
  require(
    strcmp(item->column_mapping_physical_name, physical_name) == 0,
    "wrong physical name");
}

static void require_interval_schema(CSchema* schema)
{
  SchemaItemList* top_level = &schema->builder->lists[schema->list_id];
  require(top_level->len == 3, "expected exactly three fields");
  require_field(&top_level->list[0], "year", "interval year", false, "1", "year_phys");
  require_field(&top_level->list[1], "month", "interval month", true, "2", "month_phys");
  require_field(
    &top_level->list[2], "full", "interval year to month", true, "3", "full_phys");
}

int main(int argc, char* argv[])
{
  if (kernel_ffi_abi_version() != KERNEL_FFI_ABI_VERSION) {
    fprintf(stderr, "Unsupported delta-kernel FFI ABI version\n");
    return 1;
  }
  if (argc != 2) {
    fprintf(stderr, "Usage: %s table/path\n", argv[0]);
    return 1;
  }

  KernelStringSlice table_path = { argv[1], strlen(argv[1]) };
  ExternResultHandleSharedExternEngine engine_res =
    get_default_engine(table_path, allocate_error);
  if (engine_res.tag != OkHandleSharedExternEngine) {
    print_error("Failed to get engine", (Error*)engine_res.err);
    free_error((Error*)engine_res.err);
    return 1;
  }
  SharedExternEngine* engine = engine_res.ok;

  ExternResultHandleExclusiveSnapshotBuilder snapshot_builder_res =
    get_snapshot_builder(table_path, engine);
  if (snapshot_builder_res.tag != OkHandleExclusiveSnapshotBuilder) {
    print_error("Failed to get snapshot builder", (Error*)snapshot_builder_res.err);
    free_error((Error*)snapshot_builder_res.err);
    free_engine(engine);
    return 1;
  }

  ExternResultHandleSharedSnapshot snapshot_res =
    snapshot_builder_build(snapshot_builder_res.ok);
  if (snapshot_res.tag != OkHandleSharedSnapshot) {
    print_error("Failed to build snapshot", (Error*)snapshot_res.err);
    free_error((Error*)snapshot_res.err);
    free_engine(engine);
    return 1;
  }
  SharedSnapshot* snapshot = snapshot_res.ok;

  CSchema* schema = get_cschema(snapshot, engine);
  require_interval_schema(schema);

  char requested_cols[] = "year,month,full";
  RequestedSchemaSpec spec = {
    .cschema = schema,
    .requested_cols = requested_cols,
  };
  EngineSchema engine_schema = {
    .schema = &spec,
    .visitor = visit_requested_spec,
  };
  ExternResultHandleSharedScan scan_res = scan(snapshot, engine, NULL, &engine_schema);
  if (scan_res.tag != OkHandleSharedScan) {
    print_error("Failed to create projected scan", (Error*)scan_res.err);
    free_error((Error*)scan_res.err);
    free_cschema(schema);
    free_snapshot(snapshot);
    free_engine(engine);
    return 1;
  }
  SharedScan* scan = scan_res.ok;

  SharedSchema* projected_schema = scan_logical_schema(scan);
  CSchema* projected_cschema = build_cschema(projected_schema, engine);
  require_interval_schema(projected_cschema);

  print_cschema("Schema", schema);
  print_cschema("Projected schema", projected_cschema);

  free_cschema(projected_cschema);
  free_schema(projected_schema);
  free_scan(scan);
  free_cschema(schema);
  free_snapshot(snapshot);
  free_engine(engine);
  return 0;
}
