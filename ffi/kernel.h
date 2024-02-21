#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum KernelError {
  UnknownError,
  FFIError,
  ArrowError,
  GenericError,
  ParquetError,
  ObjectStoreError,
  FileNotFoundError,
  MissingColumnError,
  UnexpectedColumnTypeError,
  MissingDataError,
  MissingVersionError,
  DeletionVectorError,
  InvalidUrlError,
  MalformedJsonError,
  MissingMetadataError,
} KernelError;

typedef struct ExternTableClientHandle ExternTableClientHandle;

typedef struct KernelExpressionVisitorState KernelExpressionVisitorState;

typedef struct KernelScanFileIterator KernelScanFileIterator;

typedef struct SnapshotHandle SnapshotHandle;

/**
 * Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
 * the engine functions. The engine retains ownership of the iterator.
 */
typedef struct EngineIterator {
  void *data;
  /**
   * A function that should advance the iterator and return the next time from the data
   * If the iterator is complete, it should return null. It should be safe to
   * call `get_next()` multiple times if it is null.
   */
  const void *(*get_next)(void *data);
} EngineIterator;

/**
 * An error that can be returned to the engine. Engines can define additional struct fields on
 * their side, by e.g. embedding this struct as the first member of a larger struct.
 */
typedef struct EngineError {
  enum KernelError etype;
} EngineError;

typedef enum ExternResult______ExternTableClientHandle_Tag {
  Ok______ExternTableClientHandle,
  Err______ExternTableClientHandle,
} ExternResult______ExternTableClientHandle_Tag;

typedef struct ExternResult______ExternTableClientHandle {
  ExternResult______ExternTableClientHandle_Tag tag;
  union {
    struct {
      const struct ExternTableClientHandle *ok;
    };
    struct {
      struct EngineError *err;
    };
  };
} ExternResult______ExternTableClientHandle;

typedef struct EngineError *(*AllocateErrorFn)(enum KernelError etype,
                                               const char *msg_ptr,
                                               uintptr_t msg_len);

typedef enum ExternResult______SnapshotHandle_Tag {
  Ok______SnapshotHandle,
  Err______SnapshotHandle,
} ExternResult______SnapshotHandle_Tag;

typedef struct ExternResult______SnapshotHandle {
  ExternResult______SnapshotHandle_Tag tag;
  union {
    struct {
      const struct SnapshotHandle *ok;
    };
    struct {
      struct EngineError *err;
    };
  };
} ExternResult______SnapshotHandle;

typedef struct EngineSchemaVisitor {
  void *data;
  uintptr_t (*make_field_list)(void *data, uintptr_t reserve);
  void (*visit_struct)(void *data,
                       uintptr_t sibling_list_id,
                       const char *name,
                       uintptr_t child_list_id);
  void (*visit_string)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_integer)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_long)(void *data, uintptr_t sibling_list_id, const char *name);
} EngineSchemaVisitor;

typedef struct EnginePredicate {
  void *predicate;
  uintptr_t (*visitor)(void *predicate, struct KernelExpressionVisitorState *state);
} EnginePredicate;

/**
 * test function to print for items. this assumes each item is an `int`
 */
void iterate(struct EngineIterator *it);

/**
 * # Safety
 *
 * Caller is responsible to pass a valid path pointer.
 */
struct ExternResult______ExternTableClientHandle get_default_client(const char *path,
                                                                    AllocateErrorFn allocate_error);

/**
 * # Safety
 *
 * Caller is responsible to pass a valid handle.
 */
void drop_table_client(const struct ExternTableClientHandle *table_client);

/**
 * Get the latest snapshot from the specified table
 *
 * # Safety
 *
 * Caller is responsible to pass valid handles and path pointer.
 */
struct ExternResult______SnapshotHandle snapshot(const char *path,
                                                 const struct ExternTableClientHandle *table_client,
                                                 AllocateErrorFn allocate_error);

/**
 * # Safety
 *
 * Caller is responsible to pass a valid handle.
 */
void drop_snapshot(const struct SnapshotHandle *snapshot);

/**
 * Get the version of the specified snapshot
 *
 * # Safety
 *
 * Caller is responsible to pass a valid handle.
 */
uint64_t version(const struct SnapshotHandle *snapshot);

/**
 * # Safety
 *
 * Caller is responsible to pass a valid handle.
 */
uintptr_t visit_schema(const struct SnapshotHandle *snapshot, struct EngineSchemaVisitor *visitor);

uintptr_t visit_expression_and(struct KernelExpressionVisitorState *state,
                               struct EngineIterator *children);

uintptr_t visit_expression_lt(struct KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_le(struct KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_gt(struct KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_ge(struct KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_eq(struct KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_column(struct KernelExpressionVisitorState *state, const char *name);

uintptr_t visit_expression_literal_string(struct KernelExpressionVisitorState *state,
                                          const char *value);

uintptr_t visit_expression_literal_long(struct KernelExpressionVisitorState *state, int64_t value);

/**
 * Get a FileList for all the files that need to be read from the table.
 * # Safety
 *
 * Caller is responsible to pass a valid snapshot pointer.
 */
struct KernelScanFileIterator *kernel_scan_files_init(const struct SnapshotHandle *snapshot,
                                                      const struct ExternTableClientHandle *table_client,
                                                      struct EnginePredicate *predicate);

void kernel_scan_files_next(struct KernelScanFileIterator *files,
                            void *engine_context,
                            void (*engine_visitor)(void *engine_context,
                                                   const char *ptr,
                                                   uintptr_t len));

/**
 * # Safety
 *
 * Caller is responsible to (at most once) pass a valid pointer returned by a call to
 * [kernel_scan_files_init].
 */
void kernel_scan_files_free(struct KernelScanFileIterator *files);
