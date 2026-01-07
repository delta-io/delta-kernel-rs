#include <sys/time.h>

/*
 * This header defines a visitor that allows kernel to learn about our schema. A
 * `KernelSchemaVisitor` in kernel parlance.
 */

// This function looks at tahe type field in the schema to figure out which visitor to call. It's a
// bit gross as the schema code is string based, a real implementation would have a more robust way
// to represent a schema.
uintptr_t visit_schema_item(SchemaItem* item, KernelSchemaVisitorState *state, CSchema *cschema) {
  print_diag("Visiting schema item %s (%s)\n", item->name, item->type);
  KernelStringSlice name = { item->name, strlen(item->name) };
  ExternResultusize visit_res;
  if (strcmp(item->type, "string") == 0) {
    visit_res = visit_field_string(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "integer") == 0) {
    visit_res = visit_field_integer(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "short") == 0) {
    visit_res = visit_field_short(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "byte") == 0) {
    visit_res = visit_field_byte(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "long") == 0) {
    visit_res = visit_field_long(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "float") == 0) {
    visit_res = visit_field_float(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "double") == 0) {
    visit_res = visit_field_double(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "boolean") == 0) {
    visit_res = visit_field_boolean(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "binary") == 0) {
    visit_res = visit_field_binary(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "date") == 0) {
    visit_res = visit_field_date(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "timestamp") == 0) {
    visit_res = visit_field_timestamp(state, name, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "timestamp_ntz") == 0) {
    visit_res = visit_field_timestamp_ntz(state, name, item->is_nullable, allocate_error);
  } else if (strncmp(item->type, "decimal", 7) == 0) {
    unsigned int precision;
    int scale;
    sscanf(item->type, "decimal(%u)(%d)", &precision, &scale);
    visit_res = visit_field_decimal(state, name, precision, scale, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "array") == 0) {
    SchemaItemList child_list = cschema->builder->lists[item->children];
    // an array should always have 1 child
    if (child_list.len != 1) {
      printf("[ERROR] Invalid array child list");
      return 0;
    }
    uintptr_t child_visit_id = visit_schema_item(&child_list.list[0], state, cschema);
    if (child_visit_id == 0) {
      // previous visit will have printed the issue
      return 0;
    }
    visit_res = visit_field_array(state, name, child_visit_id, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "map") == 0) {
    SchemaItemList child_list = cschema->builder->lists[item->children];
    // an map should always have 2 children
    if (child_list.len != 2) {
      printf("[ERROR] Invalid map child list");
      return 0;
    }
    uintptr_t key_visit_id = visit_schema_item(&child_list.list[0], state, cschema);
    if (key_visit_id == 0) {
      // previous visit will have printed the issue
      return 0;
    }
    uintptr_t val_visit_id = visit_schema_item(&child_list.list[1], state, cschema);
    if (val_visit_id == 0) {
      // previous visit will have printed the issue
      return 0;
    }
    visit_res = visit_field_map(state, name, key_visit_id, val_visit_id, item->is_nullable, allocate_error);
  } else if (strcmp(item->type, "struct") == 0) {
    SchemaItemList child_list = cschema->builder->lists[item->children];
    uintptr_t child_visit_ids[child_list.len];
    for (uint32_t i = 0; i < child_list.len; i++) {
      // visit all the children
      SchemaItem *item = &child_list.list[i];
      uintptr_t child_id = visit_schema_item(item, state, cschema);
      if (child_id == 0) {
          // previous visit will have printed the issue
          return 0;
      } 
      child_visit_ids[i] = child_id;
    }
    visit_res = visit_field_struct(
      state,
      name,
      child_visit_ids,
      child_list.len,
      item->is_nullable,
      allocate_error);
  } else {
    printf("[ERROR] Can't visit unknown type: %s\n", item->type);
    return 0;
  }

  if (visit_res.tag != Okusize) {
    print_error("Could not visit field", (Error*)visit_res.err);
    return 0;
  }
  return visit_res.ok;
}

typedef struct {
  CSchema* cschema;
  char* requested_cols;
} RequestedSchemaSpec;

// This is the function kernel will call asking it to visit the schema in requested_spec
uintptr_t visit_requested_spec(void* requested_spec, KernelSchemaVisitorState *state) {
  RequestedSchemaSpec *spec = (RequestedSchemaSpec*)requested_spec;
  print_diag("Asked to visit: %s\n", spec->requested_cols);

  // figure out how many columns we are requesting. will be number of commas + 1
  int col_count = 1;
  char* s = spec->requested_cols;
  while (*s) {
    if (*s == ',') {
      col_count++;
    }
    s++;
  }

  uintptr_t cols[col_count];
  int col_index = 0;

  CSchema* cschema = spec->cschema;
  SchemaItemList* top_level_list = &cschema->builder->lists[cschema->list_id];

  char* col = strtok(spec->requested_cols, ",");

  while (col != NULL) {
    print_diag("Visiting requested col: %s\n", col);
    char found_col = 0;
    for (uint32_t i = 0; i < top_level_list->len; i++) {
      SchemaItem* item = &top_level_list->list[i];
      if (strcmp(item->name, col) == 0) {
        found_col = 1;
        uintptr_t col_id = visit_schema_item(item, state, cschema);
        if (col_id == 0) {
          // error will have been printed above
          return 0;
        }
        cols[col_index++] = col_id;
      }
      if (found_col) {
        break;
      }
    }
    if (!found_col) {
      printf("[ERROR] No such column in table: %s\n", col);
      return 0;
    }
    col = strtok(NULL, ",");
  }

  KernelStringSlice name = { "s", 1 }; // name doesn't matter
  ExternResultusize visit_res = visit_field_struct(
    state,
    name,
    cols,
    col_index,
    false,
    allocate_error);

  if (visit_res.tag != Okusize) {
    print_error("Could not visit top_level schema", (Error*)visit_res.err);
    return 0;
  }
  return visit_res.ok;
}
