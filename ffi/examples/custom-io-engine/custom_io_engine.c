// Minimal custom I/O engine smoke test.
//
// Fills in stub vtables for storage, JSON, and parquet, then calls
// test_drive_custom_io_engine to verify make_custom_io_engine accepts the bundle.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "delta_kernel_ffi.h"
#include "kernel_utils.h"

static void noop_free(void* engine_state) { (void)engine_state; }

static void stub_list_from(void* engine_state, KernelStringSlice path, CustomFileIterResult* out) {
  (void)engine_state;
  (void)path;
  out->iter_state = NULL;
  out->error = 1;
}

static void stub_read_files(
    void* engine_state,
    const FileSliceFFI* files,
    uintptr_t files_len,
    CustomBytesIterResult* out) {
  (void)engine_state;
  (void)files;
  (void)files_len;
  out->iter_state = NULL;
  out->error = 1;
}

static void stub_copy_atomic(
    void* engine_state,
    KernelStringSlice src,
    KernelStringSlice dst,
    uint32_t* out_error) {
  (void)engine_state;
  (void)src;
  (void)dst;
  *out_error = 0;
}

static void stub_put(
    void* engine_state,
    KernelStringSlice path,
    const uint8_t* data,
    uintptr_t data_len,
    bool overwrite,
    uint32_t* out_error) {
  (void)engine_state;
  (void)path;
  (void)data;
  (void)data_len;
  (void)overwrite;
  *out_error = 0;
}

static void stub_head(void* engine_state, KernelStringSlice path, CustomFileMetaResult* out) {
  (void)engine_state;
  (void)path;
  out->error = 2;  // file not found
}

static void stub_list_next(void* engine_state, void* iter, CustomFileMetaResult* out) {
  (void)engine_state;
  (void)iter;
  out->done = true;
}

static void stub_bytes_next(void* engine_state, void* iter, CustomBytesResult* out) {
  (void)engine_state;
  (void)iter;
  out->done = true;
}

static void stub_iter_free(void* engine_state, void* iter) {
  (void)engine_state;
  (void)iter;
}

static void stub_read_json(
    void* engine_state,
    const FileMeta* files,
    uintptr_t files_len,
    HandleSharedSchema physical_schema,
    HandleSharedPredicate* predicate,
    CustomReadResult* out) {
  (void)engine_state;
  (void)files;
  (void)files_len;
  free_schema(physical_schema);
  if (predicate != NULL) {
    free_kernel_predicate(*predicate);
  }
  out->iter_state = NULL;
  out->error = 1;
}

static void stub_json_iter_next(void* engine_state, void* iter, CustomEngineDataResult* out) {
  (void)engine_state;
  (void)iter;
  out->done = true;
}

static void stub_write_json(
    void* engine_state,
    KernelStringSlice path,
    ArrowFFIData* batches,
    uintptr_t batches_len,
    bool overwrite,
    uint32_t* out_error) {
  (void)engine_state;
  (void)path;
  (void)batches;
  (void)batches_len;
  (void)overwrite;
  *out_error = 0;
}

static void stub_materialize(
    void* engine_state,
    const VisitRowsRequest* request,
    VisitRowsResult* out) {
  (void)engine_state;
  (void)request;
  out->error = 1;
}

static void stub_free_columns(void* engine_state, const ColumnBuffers* cols, uintptr_t len) {
  (void)engine_state;
  (void)cols;
  (void)len;
}

static void stub_free_batch(void* engine_state, void* batch) {
  (void)engine_state;
  (void)batch;
}

static void stub_read_parquet(
    void* engine_state,
    const FileMeta* files,
    uintptr_t files_len,
    HandleSharedSchema physical_schema,
    HandleSharedPredicate* predicate,
    CustomReadResult* out) {
  (void)engine_state;
  (void)files;
  (void)files_len;
  free_schema(physical_schema);
  if (predicate != NULL) {
    free_kernel_predicate(*predicate);
  }
  out->iter_state = NULL;
  out->error = 1;
}

static void stub_parquet_iter_next(void* engine_state, void* iter, CustomArrowDataResult* out) {
  (void)engine_state;
  (void)iter;
  out->done = true;
}

static void stub_write_parquet(
    void* engine_state,
    KernelStringSlice path,
    ArrowFFIData* batches,
    uintptr_t batches_len,
    uint32_t* out_error) {
  (void)engine_state;
  (void)path;
  (void)batches;
  (void)batches_len;
  *out_error = 0;
}

static uint32_t stub_footer(
    void* engine_state,
    const FileMeta* file,
    HandleSharedSchema* out_schema) {
  (void)engine_state;
  (void)file;
  (void)out_schema;
  return 1;
}

int main(void) {
  CustomStorageCallbacks storage = {
      .engine_state = NULL,
      .list_from = stub_list_from,
      .read_files = stub_read_files,
      .copy_atomic = stub_copy_atomic,
      .put = stub_put,
      .head = stub_head,
      .list_iter_next = stub_list_next,
      .list_iter_free = stub_iter_free,
      .bytes_iter_next = stub_bytes_next,
      .bytes_iter_free = stub_iter_free,
      .free_engine_state = noop_free,
  };

  CustomJsonCallbacks json = {
      .engine_state = NULL,
      .read_json_files = stub_read_json,
      .iter_next = stub_json_iter_next,
      .iter_free = stub_iter_free,
      .write_json_file = stub_write_json,
      .engine_data_callbacks =
          {
              .materialize_columns = stub_materialize,
              .free_columns = stub_free_columns,
              .free_batch = stub_free_batch,
          },
      .free_engine_state = noop_free,
  };

  CustomParquetCallbacks parquet = {
      .engine_state = NULL,
      .read_parquet_files = stub_read_parquet,
      .iter_next = stub_parquet_iter_next,
      .iter_free = stub_iter_free,
      .write_parquet_file = stub_write_parquet,
      .read_parquet_footer = stub_footer,
      .free_engine_state = noop_free,
  };

  CustomIOCallbacks bundle = {
      .storage = &storage,
      .json = &json,
      .parquet = &parquet,
  };

  uint32_t rc = test_drive_custom_io_engine(bundle, allocate_error);
  if (rc != 0) {
    fprintf(stderr, "test_drive_custom_io_engine failed: %u\n", rc);
    return EXIT_FAILURE;
  }
  printf("custom_io_engine: factory smoke test passed\n");
  return EXIT_SUCCESS;
}
