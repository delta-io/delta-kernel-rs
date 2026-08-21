#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "column_defaults.h"
#include "kernel_utils.h"

// One column default collected from the visitor, with every slice copied into owned strings.
struct collected_column_default {
  char* name;
  char* raw_sql;
};

// Growable list of collected defaults, handed to the visitor as its engine context.
struct column_default_list {
  struct collected_column_default* items;
  size_t len;
  size_t cap;
};

// Kernel calls this once per top-level column default. The `name`/`raw_sql` slices are only valid
// for the duration of the call, so copy anything we keep.
static void collect_column_default(
  void* engine_context,
  KernelStringSlice name,
  KernelStringSlice raw_sql)
{
  struct column_default_list* list = engine_context;
  if (list->len == list->cap) {
    list->cap = list->cap ? list->cap * 2 : 4;
    list->items = realloc(list->items, list->cap * sizeof(struct collected_column_default));
  }
  struct collected_column_default* item = &list->items[list->len++];
  item->name = allocate_string(name);
  item->raw_sql = allocate_string(raw_sql);
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

bool print_column_defaults_of(ExclusiveTransaction* txn, SharedExternEngine* engine)
{
  printf("Column defaults:\n");
  printf("  to materialize before writing:\n");

  struct column_default_list list = { NULL, 0, 0 };
  ExternResultusize visit_res =
    transaction_visit_top_level_column_defaults(txn, engine, &list, collect_column_default);
  if (visit_res.tag != Okusize) {
    print_error("Failed to visit column defaults.", (Error*)visit_res.err);
    free_error((Error*)visit_res.err);
    free_column_default_list(&list);
    return false;
  }

  // Kernel visits in an unspecified order; sort by name so the printed output is deterministic.
  qsort(
    list.items, list.len, sizeof(struct collected_column_default), compare_column_default_by_name);
  for (size_t i = 0; i < list.len; i++) {
    printf("    %s: %s\n", list.items[i].name, list.items[i].raw_sql);
  }
  printf("    (%" PRIuPTR " total)\n", visit_res.ok);

  free_column_default_list(&list);
  printf("\n");
  return true;
}

bool print_column_defaults(KernelStringSlice table_path, SharedExternEngine* engine)
{
  ExternResultHandleExclusiveTransaction txn_res = transaction(table_path, engine);
  if (txn_res.tag != OkHandleExclusiveTransaction) {
    print_error("Could not start a transaction to read column defaults.", (Error*)txn_res.err);
    free_error((Error*)txn_res.err);
    return false;
  }
  ExclusiveTransaction* txn = txn_res.ok;

  bool ok = print_column_defaults_of(txn, engine);
  free_transaction(txn);
  return ok;
}
