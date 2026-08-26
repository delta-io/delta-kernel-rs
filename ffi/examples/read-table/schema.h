#include "delta_kernel_ffi.h"
#include "read_table.h"
#include "kernel_utils.h"
#include <limits.h>
#include <stdint.h>

/**
 * This module defines a very simple model of a schema, used only to be able to print the schema of
 * a table. It consists of a "SchemaBuilder" which is our user data that gets passed into each
 * visit_x call. This simply keeps track of all the lists we are asked to allocate.
 *
 * Each list is a "SchemaItemList", which tracks its length an an array of "SchemaItem"s.
 *
 * Each "SchemaItem" has a name and a type, which are just strings. It can also have a list which is
 * its children. This is initially always UINTPTR_MAX, but when visiting a struct, map, or array, we
 * point this at the list id specified in the callback, which allows us to traverse the schema when
 * printing it.
 */

#ifdef VERBOSE
#define _NTH_ARG(_1, _2, _3, _4, _5, N, ...) N
#define NUMARGS(...) _NTH_ARG(__VA_ARGS__, 5, 4, 3, 2, 1)
#define CHILD_FMT "Asked to visit %s named %s belonging to list %li. %s are in %li.\n"
#define NO_CHILD_FMT "Asked to visit %s named %s belonging to list %li.\n"
#define PRINT_CHILD_VISIT(...) printf(CHILD_FMT, __VA_ARGS__)
#define PRINT_NO_CHILD_VISIT(...) printf(NO_CHILD_FMT, __VA_ARGS__)
#else
#define PRINT_CHILD_VISIT(...)
#define PRINT_NO_CHILD_VISIT(...)
#endif

typedef struct SchemaItemList SchemaItemList;

typedef struct
{
  char* name;
  char* type;
  bool is_nullable;
  uintptr_t children;
  char* column_mapping_id;
} SchemaItem;

typedef struct SchemaItemList
{
  uint32_t len;
  SchemaItem* list;
} SchemaItemList;

typedef struct
{
  int list_count;
  SchemaItemList* lists;
  SharedExternEngine* engine;
} SchemaBuilder;

typedef struct
{
  uintptr_t list_id;
  SchemaBuilder* builder;
} CSchema;

// lists are preallocated to have exactly enough space, so we just fill in the next open slot and
// increment our length
SchemaItem* add_to_list(SchemaItemList* list, char* name, char* type, bool is_nullable)
{
  int idx = list->len;
  list->list[idx].name = name;
  list->list[idx].type = type;
  list->list[idx].is_nullable = is_nullable;
  list->list[idx].column_mapping_id = NULL;
  list->len++;
  return &list->list[idx];
}

bool field_type_needs_free(char* type)
{
  return !strncmp(type, "decimal", 7) ||
         !strncmp(type, "geometry", 8) ||
         !strncmp(type, "geography", 9);
}

// print out all items in a list, recursing into any children they may have
void print_list(SchemaBuilder* builder, uintptr_t list_id, int indent, int parents_on_last)
{
  SchemaItemList* list = &builder->lists[list_id];
  for (uint32_t i = 0; i < list->len; i++) {
    bool is_last = i == list->len - 1;
    for (int j = 0; j < indent; j++) {
      if ((indent - parents_on_last) <= j) {
        // don't print a dangling | on any parents that are on their last item
        printf("   ");
      } else {
        printf("│  ");
      }
    }
    SchemaItem* item = &list->list[i];
    char* prefix = is_last ? "└" : "├";
    printf("%s─ %s: %s", prefix, item->name, item->type);
    if (item->column_mapping_id) {
      printf(" (column mapping id: %s)", item->column_mapping_id);
    }
    if (strcmp(item->type, "array") == 0) {
      SchemaItemList child_list = builder->lists[item->children];
      if (child_list.len != 1) {
        printf(" (invalid array child list)\n");
      } else {
        printf(" (can contain null: %s)\n", child_list.list[0].is_nullable ? "true" : "false");
      }
    } else if (strcmp(item->type, "map") == 0) {
      SchemaItemList child_list = builder->lists[item->children];
      if (child_list.len != 2) {
        printf(" (invalid map child list)\n");
      } else {
        printf(" (can contain null: %s)\n", child_list.list[1].is_nullable ? "true" : "false");
      }
    } else {
      printf("\n");
    }
    if (list->list[i].children != UINTPTR_MAX) {
      print_list(builder, list->list[i].children, indent + 1, parents_on_last + is_last);
    }
  }
}

// Read the column-mapping id back out of a field's typed metadata, across the FFI. Returns a
// freshly-allocated string (owned by the caller) when the field carries `delta.columnMapping.id`,
// or NULL when it does not. This exercises the typed metadata path end-to-end: the value crosses
// as a `MetadataNumber`, and we assert that kind before rendering it.
char* read_column_mapping_id(const CMetadataMap* metadata, SharedExternEngine* engine)
{
  char* key_str = "delta.columnMapping.id";
  KernelStringSlice key = { key_str, strlen(key_str) };
  CMetadataValueKind kind;
  ExternResultNullableCvoid res = get_from_metadata_map(metadata, key, &kind, allocate_string, engine);
  if (res.tag != OkNullableCvoid) {
    free_error((Error*)res.err);
    return NULL;
  }
  char* value = res.ok;
  if (value && kind != MetadataNumber) {
    // The kernel types column-mapping ids as numbers; a different kind means the FFI lost the type.
    printf("Unexpected kind %d for delta.columnMapping.id\n", (int)kind);
    free(value);
    return NULL;
  }
  return value;
}

// declare all our visitor methods
uintptr_t make_field_list(void* data, uintptr_t reserve)
{
  SchemaBuilder* builder = data;
  int id = builder->list_count;
#ifdef VERBOSE
  printf("Making a list of lenth %li with id %i\n", reserve, id);
#endif
  builder->list_count++;
  builder->lists = realloc(builder->lists, sizeof(SchemaItemList) * builder->list_count);
  SchemaItem* list = calloc(reserve, sizeof(SchemaItem));
  for (uintptr_t i = 0; i < reserve; i++) {
    list[i].children = UINTPTR_MAX;
  }
  builder->lists[id].len = 0;
  builder->lists[id].list = list;
  return id;
}

void visit_struct(
  void* data,
  uintptr_t sibling_list_id,
  struct KernelStringSlice name,
  bool is_nullable,
  const CMetadataMap * metadata,
  uintptr_t child_list_id)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  PRINT_CHILD_VISIT("struct", name_ptr, sibling_list_id, "Children", child_list_id);
  SchemaItem* struct_item = add_to_list(&builder->lists[sibling_list_id], name_ptr, "struct", is_nullable);
  struct_item->children = child_list_id;
  struct_item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

void visit_array(
  void* data,
  uintptr_t sibling_list_id,
  struct KernelStringSlice name,
  bool is_nullable,
  const CMetadataMap * metadata,
  uintptr_t child_list_id)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  PRINT_CHILD_VISIT("array", name_ptr, sibling_list_id, "Types", child_list_id);
  SchemaItem* array_item = add_to_list(&builder->lists[sibling_list_id], name_ptr, "array", is_nullable);
  array_item->children = child_list_id;
  array_item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

void visit_map(
  void* data,
  uintptr_t sibling_list_id,
  struct KernelStringSlice name,
  bool is_nullable,
  const CMetadataMap * metadata,
  uintptr_t child_list_id)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  PRINT_CHILD_VISIT("map", name_ptr, sibling_list_id, "Types", child_list_id);
  SchemaItem* map_item = add_to_list(&builder->lists[sibling_list_id], name_ptr, "map", is_nullable);
  map_item->children = child_list_id;
  map_item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

void visit_decimal(
  void* data,
  uintptr_t sibling_list_id,
  struct KernelStringSlice name,
  bool is_nullable,
  const CMetadataMap * metadata,
  uint8_t precision,
  uint8_t scale)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  char* type = malloc(19 * sizeof(char));
  snprintf(type, 19, "decimal(%u)(%d)", precision, scale);
  PRINT_NO_CHILD_VISIT(type, name_ptr, sibling_list_id);
  SchemaItem* item = add_to_list(&builder->lists[sibling_list_id], name_ptr, type, is_nullable);
  item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

void visit_simple_type(
  void* data,
  uintptr_t sibling_list_id,
  struct KernelStringSlice name,
  bool is_nullable,
  const CMetadataMap * metadata,
  char* type)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  PRINT_NO_CHILD_VISIT(type, name_ptr, sibling_list_id);
  SchemaItem* item = add_to_list(&builder->lists[sibling_list_id], name_ptr, type, is_nullable);
  item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

#define DEFINE_VISIT_SIMPLE_TYPE(typename)                                                                                                  \
  void visit_##typename(void* data, uintptr_t sibling_list_id, struct KernelStringSlice name, bool is_nullable, const CMetadataMap * metadata)\
  {                                                                                                                                         \
    visit_simple_type(data, sibling_list_id, name, is_nullable, metadata, #typename);                                                       \
  }

DEFINE_VISIT_SIMPLE_TYPE(string)
DEFINE_VISIT_SIMPLE_TYPE(integer)
DEFINE_VISIT_SIMPLE_TYPE(short)
DEFINE_VISIT_SIMPLE_TYPE(byte)
DEFINE_VISIT_SIMPLE_TYPE(long)
DEFINE_VISIT_SIMPLE_TYPE(float)
DEFINE_VISIT_SIMPLE_TYPE(double)
DEFINE_VISIT_SIMPLE_TYPE(boolean)
DEFINE_VISIT_SIMPLE_TYPE(binary)
DEFINE_VISIT_SIMPLE_TYPE(date)
DEFINE_VISIT_SIMPLE_TYPE(timestamp)
DEFINE_VISIT_SIMPLE_TYPE(timestamp_ntz)
DEFINE_VISIT_SIMPLE_TYPE(void)
DEFINE_VISIT_SIMPLE_TYPE(variant)

void visit_interval_year_month(void* data,
                               uintptr_t sibling_list_id,
                               struct KernelStringSlice name,
                               bool is_nullable,
                               const CMetadataMap * metadata)
{
  visit_simple_type(data, sibling_list_id, name, is_nullable, metadata, "interval year to month");
}

void visit_interval_day_time(void* data,
                             uintptr_t sibling_list_id,
                             struct KernelStringSlice name,
                             bool is_nullable,
                             const CMetadataMap * metadata)
{
  visit_simple_type(data, sibling_list_id, name, is_nullable, metadata, "interval day to second");
}

void visit_geometry(void* data,
                    uintptr_t sibling_list_id,
                    struct KernelStringSlice name,
                    bool is_nullable,
                    const CMetadataMap * metadata,
                    struct KernelStringSlice crs)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  size_t type_size = strlen("geometry()") + crs.len + 1;
  char* type = malloc(type_size * sizeof(char));
  snprintf(type, type_size, "geometry(%.*s)", (int)crs.len, crs.ptr);
  PRINT_NO_CHILD_VISIT(type, name_ptr, sibling_list_id);
  SchemaItem* item = add_to_list(&builder->lists[sibling_list_id], name_ptr, type, is_nullable);
  item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

void visit_geography(void* data,
                     uintptr_t sibling_list_id,
                     struct KernelStringSlice name,
                     bool is_nullable,
                     const CMetadataMap * metadata,
                     struct KernelStringSlice crs,
                     struct KernelStringSlice algorithm)
{
  SchemaBuilder* builder = data;
  char* name_ptr = allocate_string(name);
  size_t type_size = strlen("geography(, )") + crs.len + algorithm.len + 1;
  char* type = malloc(type_size * sizeof(char));
  snprintf(
    type,
    type_size,
    "geography(%.*s, %.*s)",
    (int)crs.len,
    crs.ptr,
    (int)algorithm.len,
    algorithm.ptr);
  PRINT_NO_CHILD_VISIT(type, name_ptr, sibling_list_id);
  SchemaItem* item = add_to_list(&builder->lists[sibling_list_id], name_ptr, type, is_nullable);
  item->column_mapping_id = read_column_mapping_id(metadata, builder->engine);
}

// free all the data in the builder and the builder itself
void free_builder(SchemaBuilder* builder)
{
  for (int i = 0; i < builder->list_count; i++) {
    SchemaItemList* list = (builder->lists) + i;
    for (uint32_t j = 0; j < list->len; j++) {
      SchemaItem* item = list->list + j;
      free(item->name);
      free(item->column_mapping_id); // NULL when the field carried no column-mapping id; free(NULL) is a no-op
      // don't free item->type, those are static strings
      if (field_type_needs_free(item->type)) {
        // except decimal and geo types, we malloc'd those :)
        free(item->type);
      }
    }
    free(list->list); // free all the items in this list (we alloc'd them together)
  }
  free(builder->lists);
  free(builder);
}

// Free the schema and any associated builder data
void free_cschema(CSchema *schema) {
  free_builder(schema->builder);
  free(schema);
}

// Get the schema of the snapshot
CSchema* get_cschema(SharedSnapshot* snapshot, SharedExternEngine* engine)
{
  print_diag("Building schema\n");
  SchemaBuilder* builder = malloc(sizeof(SchemaBuilder));
  builder->list_count = 0;
  builder->lists = NULL;
  builder->engine = engine;
  EngineSchemaVisitor visitor = {
    .data = builder,
    .make_field_list = make_field_list,
    .visit_struct = visit_struct,
    .visit_array = visit_array,
    .visit_map = visit_map,
    .visit_decimal = visit_decimal,
    .visit_string = visit_string,
    .visit_long = visit_long,
    .visit_integer = visit_integer,
    .visit_short = visit_short,
    .visit_byte = visit_byte,
    .visit_float = visit_float,
    .visit_double = visit_double,
    .visit_boolean = visit_boolean,
    .visit_binary = visit_binary,
    .visit_date = visit_date,
    .visit_timestamp = visit_timestamp,
    .visit_timestamp_ntz = visit_timestamp_ntz,
    .visit_geometry = visit_geometry,
    .visit_geography = visit_geography,
    .visit_interval_year_month = visit_interval_year_month,
    .visit_interval_day_time = visit_interval_day_time,
    .visit_void = visit_void,
    .visit_variant = visit_variant,
  };
  SharedSchema* schema = logical_schema(snapshot);
  uintptr_t schema_list_id = visit_schema(schema, &visitor);
#ifdef VERBOSE
  printf("Schema returned in list %" PRIxPTR "\n", schema_list_id);
#endif
  print_diag("Done building schema\n");
  CSchema* cschema = malloc(sizeof(CSchema));
  cschema->list_id = schema_list_id;
  cschema->builder = builder;
  free_schema(schema);
  return cschema;
}

// Print out a schema
void print_cschema(CSchema *schema) {
  printf("Schema:\n");
  print_list(schema->builder, schema->list_id, 0, 0);
  printf("\n");
}
