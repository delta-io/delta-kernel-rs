// Exercises the generated C ABI against a local Delta table; no data files are read.
#define DEFINE_DEFAULT_ENGINE_BASE
#define DEFINE_DEFAULT_ENGINE_RUSTLS
#include "delta_kernel_ffi.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void *kernel_error(void *context, const struct KernelErrorInfo *info) {
  (void)context;
  fprintf(stderr, "kernel error %u: %.*s\n", (unsigned)info->code,
          (int)info->message.len, info->message.ptr);
  return (void *)(uintptr_t)(info->code + 1);
}

static void *delta_error(void *context, const struct DeltaErrorInfo *info) {
  (void)context;
  fprintf(stderr, "delta error: %.*s\n", (int)info->message.len, info->message.ptr);
  return (void *)(uintptr_t)1;
}

struct Load {
  HandleExclusiveSnapshotBuilder builder;
  HandleSharedSnapshot snapshot;
  bool reject;
};

static void *load_snapshot(void *context) {
  struct Load *load = context;
  struct ExternResultHandleSharedSnapshot result = snapshot_builder_build(load->builder);
  if (load->reject) {
    assert(result.tag == ErrHandleSharedSnapshot);
    assert((uintptr_t)result.err == MemoryBudgetError + 1);
    return NULL;
  }
  assert(result.tag == OkHandleSharedSnapshot);
  load->snapshot = result.ok;
  return NULL;
}

static void *observe_encoded(struct KernelBytesSlice bytes) {
  assert(bytes.len > 0);
  return NULL;
}

static void release_batch(NullableCvoid context, HandleSharedScanMetadata batch) {
  (void)context;
  free_scan_metadata(batch);
}

struct Consume {
  HandleSharedScanMetadataIterator iter;
  bool reject;
  size_t batches;
};

static void *consume_metadata(void *context) {
  struct Consume *consume = context;
  for (;;) {
    struct ExternResultbool result = scan_metadata_next(consume->iter, NULL, release_batch);
    if (result.tag == Errbool) {
      assert(consume->reject);
      assert((uintptr_t)result.err == MemoryBudgetError + 1);
      break;
    }
    if (!result.ok) {
      assert(!consume->reject);
      break;
    }
    ++consume->batches;
  }
  free_scan_metadata_iter(consume->iter);
  return NULL;
}

static void *release_plan_bytes(void *context) {
  struct KernelOwnedBytes *bytes = context;
  assert(bytes->len > 0 && bytes->ptr[0] == 0x12);
  free_kernel_bytes(*bytes);
  return NULL;
}

static size_t global_lifecycle(struct KernelStringSlice path, HandleSharedExternEngine engine,
                             bool enforce, size_t limit, size_t initial_peak, bool reject) {
  struct ExternResultHandleExclusiveSnapshotBuilder builder = get_snapshot_builder(path, engine);
  assert(builder.tag == OkHandleExclusiveSnapshotBuilder);
  struct Load load = {builder.ok, NULL, reject};
  struct KernelOwnedBytes plan_bytes = {0};
  pthread_t thread;
  assert(pthread_create(&thread, NULL, load_snapshot, &load) == 0);
  assert(pthread_join(thread, NULL) == 0);
  if (!load.reject) {
    struct ExternResultHandleSharedScan scan = scan_builder_build(scan_builder(load.snapshot), engine);
    assert(scan.tag == OkHandleSharedScan);
    struct ExternResultOptionalValueKernelOwnedBytes plan =
        scan_declarative_metadata_plan(scan.ok, engine);
    assert(plan.tag == OkOptionalValueKernelOwnedBytes);
    assert(plan.ok.tag == SomeKernelOwnedBytes);
    plan_bytes = plan.ok.some;
    struct ExternResultHandleSharedScanMetadataIterator iter = scan_metadata_iter_init(engine, scan.ok);
    assert(iter.tag == OkHandleSharedScanMetadataIterator);
    free_scan(scan.ok);
    struct Consume consume = {iter.ok, false, 0};
    assert(pthread_create(&thread, NULL, consume_metadata, &consume) == 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(consume.batches > 0);

    struct KernelStringSlice info = {"global-ffi", 10};
    struct ExternResultHandleExclusiveUpdateTableTransactionBuilder txn_builder =
        new_update_table_txn_builder(load.snapshot, KernelUpdateTableOperationAlterTable,
                                     info, false, engine);
    assert(txn_builder.tag == OkHandleExclusiveUpdateTableTransactionBuilder);
    struct FfiStringMapEntry entry = {{"poc.global", 10}, {"true", 4}};
    FfiStringMap properties = {&entry, 1};
    txn_builder = update_table_txn_builder_with_table_properties_added(txn_builder.ok, &properties, engine);
    assert(txn_builder.tag == OkHandleExclusiveUpdateTableTransactionBuilder);
    struct ExternResultHandleExclusiveUpdateTableTransaction txn =
        update_table_txn_builder_build(txn_builder.ok, engine);
    assert(txn.tag == OkHandleExclusiveUpdateTableTransaction);
    struct ExternResultHandleSharedWriteState state = update_table_txn_write_state(txn.ok, engine);
    assert(state.tag == OkHandleSharedWriteState);
    struct ExternResultNullableCvoid encoded = write_state_encode(state.ok, observe_encoded, engine);
    assert(encoded.tag == OkNullableCvoid);
    free_write_state(state.ok);
    free_snapshot(load.snapshot);
    struct ExternResultHandleExclusiveCommittedTransaction committed = update_table_txn_commit(txn.ok, engine);
    assert(committed.tag == OkHandleExclusiveCommittedTransaction);
    free_committed_transaction(committed.ok);
  }
  free_engine(engine);
  struct FfiMemoryUsage usage;
  assert(kernel_memory_usage(&usage));
  size_t before_release = usage.reserved;
  if (!load.reject) {
    assert(kernel_memory_usage(&usage));
    assert(usage.reserved >= plan_bytes.len && plan_bytes.len > 0);
    assert(pthread_create(&thread, NULL, release_plan_bytes, &plan_bytes) == 0);
    assert(pthread_join(thread, NULL) == 0);
  }
  assert(kernel_memory_usage(&usage));
  if (enforce) {
    assert(usage.limit.tag == Someusize && usage.limit.some == limit);
    assert(usage.peak <= (initial_peak > limit ? initial_peak : limit));
  } else {
    assert(usage.limit.tag == Noneusize && usage.peak > 0);
  }
  if (!load.reject) assert(before_release - usage.reserved >= plan_bytes.len);
  assert(!load.reject || usage.rejected > 0);
  if (usage.reserved > 0) {
    struct OptionalValueusize too_low = {Someusize, {.some = usage.reserved - 1}};
    assert(!kernel_memory_set_limit(too_low));
  }
  struct OptionalValueusize unlimited = {Noneusize, {.some = 0}};
  assert(kernel_memory_set_limit(unlimited));
  struct FfiMemoryUsage cleared;
  assert(kernel_memory_usage(&cleared));
  assert(cleared.limit.tag == Noneusize && cleared.reserved == usage.reserved);
  printf("C FFI enforce=%d cap=%zu peak=%zu retained_static=%zu rejected=%zu\n",
         enforce, limit, usage.peak, usage.reserved, usage.rejected);
  return usage.reserved;
}

int main(int argc, char **argv) {
  assert(argc == 2 || argc == 3);
  bool enforce = argc == 3;
  size_t limit = enforce ? (size_t)strtoull(argv[2], NULL, 10) : 0;
  struct OptionalValueusize unlimited = {Noneusize, {.some = 0}};
  assert(kernel_memory_set_limit(unlimited));
  struct FfiMemoryUsage initial;
  assert(kernel_memory_usage(&initial));
  bool reject = enforce && limit == 0;
  if (reject) limit = initial.reserved;
  if (enforce) {
    struct OptionalValueusize cap = {Someusize, {.some = limit}};
    assert(kernel_memory_set_limit(cap));
  }
  assert(!kernel_memory_usage(NULL));
  struct KernelStringSlice path = {argv[1], strlen(argv[1])};
  struct ErrorAllocators errors = {NULL, kernel_error, delta_error};
  size_t retained = 0;
  for (int pass = 0; pass < (reject ? 1 : 2); ++pass) {
    if (enforce) {
      struct OptionalValueusize cap = {Someusize, {.some = limit}};
      assert(kernel_memory_set_limit(cap));
    }
    struct ExternResultHandleSharedExternEngine engine = get_default_engine(path, &errors);
    assert(engine.tag == OkHandleSharedExternEngine);
    size_t current = global_lifecycle(path, engine.ok, enforce, limit, initial.peak, reject);
    if (pass > 0 || reject) assert(current == (reject ? initial.reserved : retained));
    retained = current;
  }
  return 0;
}
