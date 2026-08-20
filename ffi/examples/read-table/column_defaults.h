#pragma once

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

/**
 * Printing of a table's column defaults (the `allowColumnDefaults` writer feature), driven by the
 * `-d` flag.
 *
 * Include this after `schema.h`, whose `CSchema` supplies the top-level field names (that header
 * has no include guard, so it cannot be included twice).
 *
 * Two kernel entry points are exercised, both of which a real connector needs before writing to a
 * table that declares defaults:
 *  - `transaction_visit_top_level_column_defaults`, which calls back once per top-level default
 *  - `schema_field_column_default`, which reports one named field's default by value
 *
 * A connector would then materialize each omitted column (using `parsed_value` for a `Literal`
 * default, or evaluating `raw_sql` itself for a `NonLiteral` one) and call
 * `transaction_ack_column_defaults` before requesting a write context. This example only prints.
 */

static const char* column_default_kind_name(enum CColumnDefaultKind kind)
{
  return kind == CColumnDefaultKindLiteral ? "Literal" : "NonLiteral";
}

// Print one `name: raw_sql [Kind] parsed_value` line. `parsed_value` is omitted for a NonLiteral
// default, where kernel has nothing parsed to report.
static void print_column_default_line(
  const char* indent,
  const char* name,
  const char* raw_sql,
  enum CColumnDefaultKind kind,
  const char* parsed_value)
{
  printf("%s%s: %s [%s]", indent, name, raw_sql, column_default_kind_name(kind));
  if (parsed_value) {
    printf(" %s", parsed_value);
  }
  printf("\n");
}

// Kernel calls this once per top-level column default. Every slice is only valid for the duration
// of the call, so anything we keep would have to be copied -- we only print.
static void visit_column_default(
  void* engine_context,
  KernelStringSlice name,
  KernelStringSlice raw_sql,
  enum CColumnDefaultKind kind,
  KernelStringSlice parsed_value)
{
  (void)engine_context;
  char* name_str = allocate_string(name);
  char* raw_sql_str = allocate_string(raw_sql);
  char* parsed_str = parsed_value.len > 0 ? allocate_string(parsed_value) : NULL;
  print_column_default_line("    ", name_str, raw_sql_str, kind, parsed_str);
  free(name_str);
  free(raw_sql_str);
  free(parsed_str);
}

// Ask kernel for each top-level column's default by name, so the output also covers the columns
// the visitor skipped (those declaring no default).
static void print_per_field_column_defaults(
  CSchema* cschema,
  SharedSnapshot* snapshot,
  SharedExternEngine* engine)
{
  SharedSchema* schema = logical_schema(snapshot);
  SchemaItemList* top_level = &cschema->builder->lists[cschema->list_id];
  for (uint32_t i = 0; i < top_level->len; i++) {
    char* field_name = top_level->list[i].name;
    KernelStringSlice field_path = { field_name, strlen(field_name) };
    ExternResultOptionalValueKernelColumnDefault res =
      schema_field_column_default(schema, field_path, allocate_string, engine);
    if (res.tag != OkOptionalValueKernelColumnDefault) {
      print_error("Failed to get a field's column default.", (Error*)res.err);
      free_error((Error*)res.err);
      continue;
    }
    if (res.ok.tag == NoneKernelColumnDefault) {
      printf("    %s: (no default)\n", field_name);
      continue;
    }
    struct KernelColumnDefault column_default = res.ok.some;
    print_column_default_line(
      "    ",
      field_name,
      column_default.raw_sql,
      column_default.kind,
      column_default.parsed_value);
    // Both strings were allocated by our own allocate_string, so we own them.
    free(column_default.raw_sql);
    free(column_default.parsed_value);
  }
  free_schema(schema);
}

// Print everything kernel knows about this table's column defaults. Returns false if kernel could
// not report them, having already printed the error.
static bool print_column_defaults(
  KernelStringSlice table_path,
  CSchema* cschema,
  SharedSnapshot* snapshot,
  SharedExternEngine* engine)
{
  printf("Column defaults:\n");
  printf("  declared: %s\n", snapshot_has_column_defaults(snapshot) ? "yes" : "no");

  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Could not start a transaction to read column defaults.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    return false;
  }
  ExclusiveTransaction* txn = txn_res.ok;

  printf("  to materialize before writing:\n");
  ExternResultusize visit_res =
    transaction_visit_top_level_column_defaults(txn, engine, NULL, visit_column_default);
  if (visit_res.tag != Okusize) {
    print_error("Failed to visit column defaults.", (Error*)visit_res.err);
    free_error((Error*)visit_res.err);
    free_transaction(txn);
    return false;
  }
  printf("    (%" PRIuPTR " total)\n", visit_res.ok);

  // Re-read them one field at a time, which also covers the columns with no default.
  printf("  by field:\n");
  print_per_field_column_defaults(cschema, snapshot, engine);

  free_transaction(txn);
  printf("\n");
  return true;
}
