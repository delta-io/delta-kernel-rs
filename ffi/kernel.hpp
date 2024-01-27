#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

// ======================================================================================
// Missing forward declarations from deltakernel crate, added manually via cbindgen.toml
// ======================================================================================
struct Snapshot;
struct TokioBackgroundExecutor;
template<typename E> struct DefaultTableClient;
// ======================================================================================


struct KernelExpressionVisitorState;

struct KernelScanFileIterator;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
struct EngineIterator {
  void *data;
  /// A function that should advance the iterator and return the next time from the data
  /// If the iterator is complete, it should return null. It should be safe to
  /// call `get_next()` multiple times if it is null.
  const void *(*get_next)(void *data);
};

using KernelDefaultTableClient = DefaultTableClient<TokioBackgroundExecutor>;

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

const KernelDefaultTableClient *get_default_client(const char *path);

/// Get the latest snapshot from the specified table
const Snapshot *snapshot(const char *path, const KernelDefaultTableClient *table_client);

/// Get the version of the specified snapshot
uint64_t version(const Snapshot *snapshot);

uintptr_t visit_schema(const Snapshot *snapshot, EngineSchemaVisitor *visitor);

uintptr_t visit_expression_and(KernelExpressionVisitorState *state, EngineIterator *children);

uintptr_t visit_expression_lt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_le(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_gt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_ge(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_eq(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_column(KernelExpressionVisitorState *state, const char *name);

uintptr_t visit_expression_literal_string(KernelExpressionVisitorState *state, const char *value);

uintptr_t visit_expression_literal_long(KernelExpressionVisitorState *state, int64_t value);

/// Get a FileList for all the files that need to be read from the table. NB: This _consumes_ the
/// snapshot, it is no longer valid after making this call (TODO: We should probably fix this?)
///
/// # Safety
///
/// Caller is responsible to pass a valid snapshot pointer.
KernelScanFileIterator *kernel_scan_files_init(const Snapshot *snapshot,
                                               const KernelDefaultTableClient *table_client,
                                               EnginePredicate *predicate);

void kernel_scan_files_next(KernelScanFileIterator *files,
                            void *engine_context,
                            void (*engine_visitor)(void *engine_context,
                                                   const char *ptr,
                                                   uintptr_t len));

void kernel_scan_files_free(KernelScanFileIterator *files);

} // extern "C"
