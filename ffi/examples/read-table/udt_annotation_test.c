#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "schema.h"
#include "kernel_schema_visitor.h"

static bool slices_equal(KernelStringSlice left, KernelStringSlice right)
{
  return left.len == right.len && (!left.len || memcmp(left.ptr, right.ptr, left.len) == 0);
}

static bool check_schema(CSchema* schema)
{
  const FfiNullableStringMapEntry expected[] = {
    { { "class", 5 }, { .tag = SomeKernelStringSlice, .some = { "example.Value", 13 } } },
    { { "pyClass", 7 }, { .tag = NoneKernelStringSlice } },
    { { "", 0 }, { .tag = SomeKernelStringSlice, .some = { "", 0 } } },
    { { "a\0b", 3 }, { .tag = SomeKernelStringSlice, .some = { "x\0y", 3 } } },
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
    if (field->annotation.len != expected_len) {
      return false;
    }
    for (size_t j = 0; j < expected_len; j++) {
      bool found = false;
      for (size_t k = 0; k < field->annotation.len; k++) {
        const FfiNullableStringMapEntry* actual = &field->annotation.ptr[k];
        if (slices_equal(actual->key, expected[j].key)) {
          found = actual->value.tag == expected[j].value.tag &&
                  (actual->value.tag == NoneKernelStringSlice ||
                   slices_equal(actual->value.some, expected[j].value.some));
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
  // Retained annotation bytes must outlive both the source and reconstructed Rust schemas.
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
  const KernelStringSlice inputs[] = { { "", 0 }, { "plain", 5 }, { "a\0b", 3 } };
  for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); i++) {
    KernelStringSlice copy = copy_annotation_slice(inputs[i]);
    bool matches = slices_equal(copy, inputs[i]);
    free((void*)copy.ptr);
    if (!matches) {
      fprintf(stderr, "UDT annotation byte copy mismatch\n");
      return EXIT_FAILURE;
    }
  }
  if (argc > 1 && !check_round_trip(argv[1])) {
    fprintf(stderr, "UDT annotation round trip failed\n");
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
