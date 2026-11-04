// Verifies native C annotation ownership and requested-schema reconstruction through the FFI.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "schema.h"
#include "kernel_schema_visitor.h"

// Keeps JSON-null values distinct from empty C strings across schema reconstruction.
static bool check_schema(CSchema* schema)
{
  const UdtAnnotationEntry expected[] = {
    { "class", "example.Value" },
    { "pyClass", NULL },
    { "", "" },
  };
  SchemaItemList fields = schema->builder->lists[schema->list_id];
  if (fields.len != 2) {
    return false;
  }
  for (size_t i = 0; i < fields.len; i++) {
    SchemaItem* field = &fields.list[i];
    if (strcmp(field->name, i == 0 ? "value" : "empty") != 0 ||
        strcmp(field->type, "udt") != 0 || !field->is_nullable ||
        field->children >= (uintptr_t)schema->builder->list_count) {
      return false;
    }
    SchemaItemList physical = schema->builder->lists[field->children];
    if (physical.len != 1 || strcmp(physical.list[0].type, "long") != 0) {
      return false;
    }
    size_t expected_len = i == 0 ? sizeof(expected) / sizeof(expected[0]) : 0;
    if (field->udt_annotation.len != expected_len) {
      return false;
    }
    for (size_t j = 0; j < expected_len; j++) {
      bool found = false;
      for (size_t k = 0; k < field->udt_annotation.len; k++) {
        const UdtAnnotationEntry* actual = &field->udt_annotation.entries[k];
        if (strcmp(actual->key, expected[j].key) == 0) {
          found = expected[j].value ?
                    actual->value && strcmp(actual->value, expected[j].value) == 0 :
                    actual->value == NULL;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
  }
  return true;
}

// Rebuilds the fixture schema through a requested scan and verifies that copied annotations remain
// valid after their source Rust schemas are released.
static bool check_round_trip(const char* table_path)
{
  bool success = false;
  SharedExternEngine* engine = NULL;
  SharedSnapshot* snapshot = NULL;
  SharedScan* requested_scan = NULL;
  CSchema* original = NULL;
  CSchema* rebuilt = NULL;
  KernelStringSlice path = { table_path, strlen(table_path) };
  ExternResultHandleExclusiveEngineBuilder builder = get_engine_builder(path, allocate_error);
  if (builder.tag != OkHandleExclusiveEngineBuilder) {
    free_error((Error*)builder.err);
    goto cleanup;
  }
  ExternResultHandleSharedExternEngine engine_result = builder_build(builder.ok);
  if (engine_result.tag != OkHandleSharedExternEngine) {
    free_error((Error*)engine_result.err);
    goto cleanup;
  }
  engine = engine_result.ok;
  ExternResultHandleExclusiveSnapshotBuilder snapshot_builder = get_snapshot_builder(path, engine);
  if (snapshot_builder.tag != OkHandleExclusiveSnapshotBuilder) {
    free_error((Error*)snapshot_builder.err);
    goto cleanup;
  }
  ExternResultHandleSharedSnapshot snapshot_result = snapshot_builder_build(snapshot_builder.ok);
  if (snapshot_result.tag != OkHandleSharedSnapshot) {
    print_error("Could not load UDT fixture", (Error*)snapshot_result.err);
    free_error((Error*)snapshot_result.err);
    goto cleanup;
  }
  snapshot = snapshot_result.ok;
  original = get_cschema(snapshot, engine);
  if (!check_schema(original)) {
    goto cleanup;
  }
  char columns[] = "value,empty";
  RequestedSchemaSpec spec = { original, columns };
  EngineSchema requested = { &spec, visit_requested_spec };
  ExternResultHandleSharedScan scan_result = scan(snapshot, engine, NULL, &requested);
  if (scan_result.tag != OkHandleSharedScan) {
    print_error("Could not reconstruct UDT schema", (Error*)scan_result.err);
    free_error((Error*)scan_result.err);
    goto cleanup;
  }
  requested_scan = scan_result.ok;
  free_cschema(original);
  original = NULL;
  SharedSchema* logical = scan_logical_schema(requested_scan);
  rebuilt = build_cschema(logical, engine);
  free_schema(logical);
  free_scan(requested_scan);
  requested_scan = NULL;
  free_snapshot(snapshot);
  snapshot = NULL;
  // Native annotations must outlive both the source and reconstructed Rust schemas.
  success = check_schema(rebuilt);

cleanup:
  if (rebuilt) free_cschema(rebuilt);
  if (original) free_cschema(original);
  if (requested_scan) free_scan(requested_scan);
  if (snapshot) free_snapshot(snapshot);
  if (engine) free_engine(engine);
  return success;
}

int main(int argc, char** argv)
{
  if (argc == 2 && strcmp(argv[1], "--embedded-nul") == 0) {
    KernelStringSlice invalid = { "a\0b", 3 };
    free(copy_udt_annotation_string(invalid));
    return EXIT_SUCCESS;
  }
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <table-path>|--embedded-nul\n", argv[0]);
    return EXIT_FAILURE;
  }
  if (!check_round_trip(argv[1])) {
    fprintf(stderr, "UDT annotation round trip failed\n");
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
