#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class KernelError {
  UnknownError,
  FFIError,
  ArrowError,
  EngineDataTypeError,
  ExtractError,
  GenericError,
  IOErrorError,
  ParquetError,
#if defined(DEFINE_DEFAULT_CLIENT)
  ObjectStoreError,
#endif
#if defined(DEFINE_DEFAULT_CLIENT)
  ObjectStorePathError,
#endif
  FileNotFoundError,
  MissingColumnError,
  UnexpectedColumnTypeError,
  MissingDataError,
  MissingVersionError,
  DeletionVectorError,
  InvalidUrlError,
  MalformedJsonError,
  MissingMetadataError,
  MissingProtocolError,
  MissingMetadataAndProtocolError,
  ParseError,
  JoinFailureError,
  Utf8Error,
  ParseIntError,
};

struct ExternEngineInterfaceHandle;

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

/// An error that can be returned to the engine. Engines that wish to associate additional
/// information can define and use any type that is [pointer
/// interconvertible](https://en.cppreference.com/w/cpp/language/static_cast#pointer-interconvertible)
/// with this one -- e.g. by subclassing this struct or by embedding this struct as the first member
/// of a [standard layout](https://en.cppreference.com/w/cpp/language/data_members#Standard-layout)
/// class.
struct EngineError {
  KernelError etype;
};

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible to free it.
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

/// A non-owned slice of a UTF8 string, intended for arg-passing between kernel and engine. The
/// slice is only valid until the function it was passed into returns, and should not be copied.
///
/// # Safety
///
/// Intentionally not Copy, Clone, Send, nor Sync.
///
/// Whoever instantiates the struct must ensure it does not outlive the data it points to. The
/// compiler cannot help us here, because raw pointers don't have lifetimes. To reduce the risk of
/// accidental misuse, it is recommended to only instantiate this struct as a function arg, by
/// converting a `&str` value `Into<KernelStringSlice>`, so the borrowed reference protects the
/// function call (callee must not retain any references to the slice after the call returns):
///
/// ```
/// fn wants_slice(slice: KernelStringSlice) { ... }
/// let msg = String::from(...);
/// wants_slice(msg.as_ref().into());
/// ```
struct KernelStringSlice {
  const char *ptr;
  uintptr_t len;
};

using AllocateErrorFn = EngineError*(*)(KernelError etype, KernelStringSlice msg);

struct EngineSchemaVisitor {
  void *data;
  uintptr_t (*make_field_list)(void *data, uintptr_t reserve);
  void (*visit_struct)(void *data,
                       uintptr_t sibling_list_id,
                       KernelStringSlice name,
                       uintptr_t child_list_id);
  void (*visit_string)(void *data, uintptr_t sibling_list_id, KernelStringSlice name);
  void (*visit_integer)(void *data, uintptr_t sibling_list_id, KernelStringSlice name);
  void (*visit_long)(void *data, uintptr_t sibling_list_id, KernelStringSlice name);
};

struct EnginePredicate {
  void *predicate;
  uintptr_t (*visitor)(void *predicate, KernelExpressionVisitorState *state);
};

extern "C" {

/// test function to print for items. this assumes each item is an `int`
void iterate(EngineIterator *it);

#if defined(DEFINE_DEFAULT_CLIENT)
/// # Safety
///
/// Caller is responsible to pass a valid path pointer.
ExternResult<const ExternEngineInterfaceHandle*> get_default_client(KernelStringSlice path,
                                                                    AllocateErrorFn allocate_error);
#endif

/// # Safety
///
/// Caller is responsible to pass a valid handle.
void drop_table_client(const ExternEngineInterfaceHandle *table_client);

/// Get the latest snapshot from the specified table
///
/// # Safety
///
/// Caller is responsible to pass valid handles and path pointer.
ExternResult<const SnapshotHandle*> snapshot(KernelStringSlice path,
                                             const ExternEngineInterfaceHandle *table_client);

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

/// # Safety
/// The string slice must be valid
ExternResult<uintptr_t> visit_expression_column(KernelExpressionVisitorState *state,
                                                KernelStringSlice name,
                                                AllocateErrorFn allocate_error);

/// # Safety
/// The string slice must be valid
ExternResult<uintptr_t> visit_expression_literal_string(KernelExpressionVisitorState *state,
                                                        KernelStringSlice value,
                                                        AllocateErrorFn allocate_error);

uintptr_t visit_expression_literal_long(KernelExpressionVisitorState *state, int64_t value);

/// Get a FileList for all the files that need to be read from the table.
/// # Safety
///
/// Caller is responsible to pass a valid snapshot pointer.
ExternResult<KernelScanFileIterator*> kernel_scan_files_init(const SnapshotHandle *snapshot,
                                                             const ExternEngineInterfaceHandle *table_client,
                                                             EnginePredicate *predicate);

/// # Safety
///
/// The iterator must be valid (returned by [kernel_scan_files_init]) and not yet freed by
/// [kernel_scan_files_free]. The visitor function pointer must be non-null.
ExternResult<bool> kernel_scan_files_next(KernelScanFileIterator *files,
                                          void *engine_context,
                                          void (*engine_visitor)(void *engine_context,
                                                                 KernelStringSlice file_name));

/// # Safety
///
/// Caller is responsible to (at most once) pass a valid pointer returned by a call to
/// [kernel_scan_files_init].
void kernel_scan_files_free(KernelScanFileIterator *files);

} // extern "C"
