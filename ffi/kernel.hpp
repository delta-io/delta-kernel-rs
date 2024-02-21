#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class KernelError {
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
};

struct ExternTableClientHandle;

struct KernelExpressionVisitorState;

struct KernelScanFileIterator;

struct SnapshotHandle;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
struct EngineIterator {
  void *data;
  /// A function that should advance the iterator and return the next time from the data
  /// If the iterator is complete, it should return null. It should be safe to
  /// call `get_next()` multiple times if it is null.
  const void *(*get_next)(void *data);
};

/// An error that can be returned to the engine. Engines can define additional struct fields on
/// their side, by e.g. embedding this struct as the first member of a larger struct.
struct EngineError {
  KernelError etype;
};

template<typename T>
struct ExternResult {
  enum class Tag {
    Ok,
    Err,
  };

  struct Ok_Body {
    T _0;
  };

  struct Err_Body {
    EngineError *_0;
  };

  Tag tag;
  union {
    Ok_Body ok;
    Err_Body err;
  };
};

using AllocateErrorFn = EngineError*(*)(KernelError etype, const char *msg_ptr, uintptr_t msg_len);

struct EngineSchemaVisitor {
  void *data;
  uintptr_t (*make_field_list)(void *data, uintptr_t reserve);
  void (*visit_struct)(void *data,
                       uintptr_t sibling_list_id,
                       const char *name,
                       uintptr_t child_list_id);
  void (*visit_string)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_integer)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_long)(void *data, uintptr_t sibling_list_id, const char *name);
};

struct EnginePredicate {
  void *predicate;
  uintptr_t (*visitor)(void *predicate, KernelExpressionVisitorState *state);
};

extern "C" {

/// test function to print for items. this assumes each item is an `int`
void iterate(EngineIterator *it);

/// # Safety
///
/// Caller is responsible to pass a valid path pointer.
ExternResult<const ExternTableClientHandle*> get_default_client(const char *path,
                                                                AllocateErrorFn allocate_error);

/// # Safety
///
/// Caller is responsible to pass a valid handle.
void drop_table_client(const ExternTableClientHandle *table_client);

/// Get the latest snapshot from the specified table
///
/// # Safety
///
/// Caller is responsible to pass valid handles and path pointer.
ExternResult<const SnapshotHandle*> snapshot(const char *path,
                                             const ExternTableClientHandle *table_client,
                                             AllocateErrorFn allocate_error);

/// # Safety
///
/// Caller is responsible to pass a valid handle.
void drop_snapshot(const SnapshotHandle *snapshot);

/// Get the version of the specified snapshot
///
/// # Safety
///
/// Caller is responsible to pass a valid handle.
uint64_t version(const SnapshotHandle *snapshot);

/// # Safety
///
/// Caller is responsible to pass a valid handle.
uintptr_t visit_schema(const SnapshotHandle *snapshot, EngineSchemaVisitor *visitor);

uintptr_t visit_expression_and(KernelExpressionVisitorState *state, EngineIterator *children);

uintptr_t visit_expression_lt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_le(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_gt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_ge(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_eq(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_column(KernelExpressionVisitorState *state, const char *name);

uintptr_t visit_expression_literal_string(KernelExpressionVisitorState *state, const char *value);

uintptr_t visit_expression_literal_long(KernelExpressionVisitorState *state, int64_t value);

/// Get a FileList for all the files that need to be read from the table.
/// # Safety
///
/// Caller is responsible to pass a valid snapshot pointer.
KernelScanFileIterator *kernel_scan_files_init(const SnapshotHandle *snapshot,
                                               const ExternTableClientHandle *table_client,
                                               EnginePredicate *predicate);

void kernel_scan_files_next(KernelScanFileIterator *files,
                            void *engine_context,
                            void (*engine_visitor)(void *engine_context,
                                                   const char *ptr,
                                                   uintptr_t len));

/// # Safety
///
/// Caller is responsible to (at most once) pass a valid pointer returned by a call to
/// [kernel_scan_files_init].
void kernel_scan_files_free(KernelScanFileIterator *files);

} // extern "C"
