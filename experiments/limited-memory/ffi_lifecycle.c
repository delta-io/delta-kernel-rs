// Exercises the generated C ABI against a local Delta table; no data files are read.
#define DEFINE_DEFAULT_ENGINE_BASE
#define DEFINE_DEFAULT_ENGINE_RUSTLS
#define DEFINE_LIMITED_MEMORY
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

static void global_lifecycle(struct KernelStringSlice path, HandleSharedExternEngine engine,
                             size_t limit) {
  struct ExternResultHandleExclusiveSnapshotBuilder builder = get_snapshot_builder(path, engine);
  assert(builder.tag == OkHandleExclusiveSnapshotBuilder);
  struct Load load = {builder.ok, NULL, limit == 0};
  struct KernelOwnedBytes plan_bytes = {0};
  pthread_t thread;
  assert(pthread_create(&thread, NULL, load_snapshot, &load) == 0);
  assert(pthread_join(thread, NULL) == 0);
  if (limit > 0) {
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
  if (limit > 0) {
    assert(memory_budget_global_usage(&usage));
    assert(usage.reserved >= plan_bytes.len && plan_bytes.len > 0);
    assert(pthread_create(&thread, NULL, release_plan_bytes, &plan_bytes) == 0);
    assert(pthread_join(thread, NULL) == 0);
  }
  assert(memory_budget_global_usage(&usage));
  assert(usage.limit == limit && usage.peak <= limit && usage.reserved == 0);
  assert(limit > 0 || usage.rejected > 0);
  printf("C FFI global cap=%zu peak=%zu live=%zu rejected=%zu\n",
         limit, usage.peak, usage.reserved, usage.rejected);
}

int main(int argc, char **argv) {
  assert(argc == 2 || argc == 3);
  size_t limit = argc == 3 ? (size_t)strtoull(argv[2], NULL, 10) : 0;
  if (argc == 3) {
    assert(kernel_memory_budget_install_global(limit));
    assert(!kernel_memory_budget_install_global(limit));
    assert(!memory_budget_global_usage(NULL));
  }
  struct KernelStringSlice path = {argv[1], strlen(argv[1])};
  struct ErrorAllocators errors = {NULL, kernel_error, delta_error};
  struct ExternResultHandleSharedExternEngine engine = get_default_engine(path, &errors);
  assert(engine.tag == OkHandleSharedExternEngine);
  if (argc == 3) {
    global_lifecycle(path, engine.ok, limit);
    return 0;
  }
  for (int reject = 0; reject != 2; ++reject) {
    HandleSharedMemoryBudget budget = kernel_memory_budget_new(128 * 1024 * 1024);
    HandleSharedMemoryBudget observer = clone_memory_budget(budget);
    struct ExternResultHandleExclusiveSnapshotBuilder builder = get_snapshot_builder(path, engine.ok);
    assert(builder.tag == OkHandleExclusiveSnapshotBuilder);
    struct Load load = {snapshot_builder_with_memory_budget(builder.ok, budget), NULL, false};
    free_memory_budget(budget);
    pthread_t thread;
    assert(pthread_create(&thread, NULL, load_snapshot, &load) == 0);
    assert(pthread_join(thread, NULL) == 0);
    struct FfiMemoryUsage usage;
    assert(memory_budget_usage(observer, &usage));
    assert(usage.peak > 0 && usage.peak <= usage.limit);
    HandleExclusiveScanBuilder scan_options = scan_builder(load.snapshot);
    HandleSharedMemoryBudget scan_budget = kernel_memory_budget_new(0);
    if (reject) {
      scan_options = scan_builder_with_memory_budget(scan_options, scan_budget);
    }
    struct ExternResultHandleSharedScan scan = scan_builder_build(scan_options, engine.ok);
    assert(scan.tag == OkHandleSharedScan);
    struct ExternResultHandleSharedScanMetadataIterator iter = scan_metadata_iter_init(engine.ok, scan.ok);
    assert(iter.tag == OkHandleSharedScanMetadataIterator);
    free_scan(scan.ok);
    free_snapshot(load.snapshot);
    struct Consume consume = {iter.ok, reject, 0};
    assert(pthread_create(&thread, NULL, consume_metadata, &consume) == 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(reject || consume.batches > 0);
    assert(memory_budget_usage(observer, &usage));
    assert(usage.reserved == 0 && usage.peak <= usage.limit);
    printf("C FFI scan reject=%d batches=%zu peak=%zu live=%zu\n",
           reject, consume.batches, (size_t)usage.peak, (size_t)usage.reserved);
    assert(memory_budget_usage(scan_budget, &usage));
    assert(!reject || usage.rejected > 0);
    free_memory_budget(scan_budget);
    free_memory_budget(observer);
  }
  HandleSharedMemoryBudget zero = kernel_memory_budget_new(0);
  struct ExternResultHandleExclusiveSnapshotBuilder builder = get_snapshot_builder(path, engine.ok);
  assert(builder.tag == OkHandleExclusiveSnapshotBuilder);
  struct ExternResultHandleSharedSnapshot failure = snapshot_builder_build(
      snapshot_builder_with_memory_budget(builder.ok, zero));
  assert(failure.tag == ErrHandleSharedSnapshot);
  assert((uintptr_t)failure.err == MemoryBudgetError + 1);
  struct FfiMemoryUsage usage;
  assert(memory_budget_usage(zero, &usage));
  assert(usage.reserved == 0 && usage.rejected > 0);
  free_memory_budget(zero);
  free_engine(engine.ok);
  puts("C FFI lifetime, pthread propagation, and budget failures passed");
}
