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
 * One kernel entry point is exercised, which a real connector needs before writing to a table that
 * declares defaults:
 *  - `transaction_visit_top_level_column_defaults`, which calls back once per top-level default
 *
 * The kernel visits defaults in an unspecified order, so this example collects them and sorts by
 * name before printing (a connector that does not care about order can print straight from the
 * callback). Each literal default arrives as an owned `SharedExpression*` handle; a non-literal
 * (e.g. `current_timestamp()`) arrives as NULL. A real connector would read the typed value by
 * passing the handle to `visit_expression` with an `EngineExpressionVisitor` (or fall back to
 * `raw_sql`), then call `transaction_ack_column_defaults` before requesting a write context. This
 * example only prints the raw SQL and whether the default parsed to a literal, and frees each
 * handle.
 */

// Print one `name: raw_sql [literal|non-literal]` line.
static void print_column_default_line(
  const char* indent,
  const char* name,
  const char* raw_sql,
  bool is_literal)
{
  printf("%s%s: %s [%s]\n", indent, name, raw_sql, is_literal ? "literal" : "non-literal");
}

// One column default collected from the visitor, with every slice copied into owned strings.
struct collected_column_default {
  char* name;
  char* raw_sql;
  bool is_literal;  // whether the kernel handed back a parsed-literal expression handle
};

// Growable list of collected defaults, handed to the visitor as its engine context.
struct column_default_list {
  struct collected_column_default* items;
  size_t len;
  size_t cap;
};

// Kernel calls this once per top-level column default. The `name`/`raw_sql` slices are only valid
// for the duration of the call, so copy anything we keep. `default_expression`, when non-NULL, is
// an owned handle we must free.
static void collect_column_default(
  void* engine_context,
  KernelStringSlice name,
  KernelStringSlice raw_sql,
  SharedExpression* default_expression)
{
  struct column_default_list* list = engine_context;
  if (list->len == list->cap) {
    list->cap = list->cap ? list->cap * 2 : 4;
    list->items = realloc(list->items, list->cap * sizeof(struct collected_column_default));
  }
  struct collected_column_default* item = &list->items[list->len++];
  item->name = allocate_string(name);
  item->raw_sql = allocate_string(raw_sql);
  item->is_literal = default_expression != NULL;
  // This example does not read the typed value; release the owned handle. A connector wanting the
  // value would pass it to visit_expression before freeing.
  if (default_expression) {
    free_kernel_expression(default_expression);
  }
}

static int compare_column_default_by_name(const void* a, const void* b)
{
  const struct collected_column_default* x = a;
  const struct collected_column_default* y = b;
  return strcmp(x->name, y->name);
}

static void free_column_default_list(struct column_default_list* list)
{
  for (size_t i = 0; i < list->len; i++) {
    free(list->items[i].name);
    free(list->items[i].raw_sql);
  }
  free(list->items);
}

// Print everything kernel knows about this table's column defaults. Returns false if kernel could
// not report them, having already printed the error.
static bool print_column_defaults(
  KernelStringSlice table_path,
  SharedExternEngine* engine)
{
  printf("Column defaults:\n");

  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Could not start a transaction to read column defaults.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    return false;
  }
  ExclusiveTransaction* txn = txn_res.ok;

  printf("  to materialize before writing:\n");
  struct column_default_list list = { NULL, 0, 0 };
  ExternResultusize visit_res =
    transaction_visit_top_level_column_defaults(txn, engine, &list, collect_column_default);
  if (visit_res.tag != Okusize) {
    print_error("Failed to visit column defaults.", (Error*)visit_res.err);
    free_error((Error*)visit_res.err);
    free_column_default_list(&list);
    free_transaction(txn);
    return false;
  }

  // Kernel visits in an unspecified order; sort by name so the printed output is deterministic.
  qsort(
    list.items, list.len, sizeof(struct collected_column_default), compare_column_default_by_name);
  for (size_t i = 0; i < list.len; i++) {
    print_column_default_line(
      "    ", list.items[i].name, list.items[i].raw_sql, list.items[i].is_literal);
  }
  printf("    (%" PRIuPTR " total)\n", visit_res.ok);

  free_column_default_list(&list);
  free_transaction(txn);
  printf("\n");
  return true;
}
