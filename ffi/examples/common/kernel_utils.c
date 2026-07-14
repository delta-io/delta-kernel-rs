#include <delta_kernel_ffi.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include "kernel_utils.h"

// some diagnostic functions
void print_diag(char* fmt, ...)
{
#ifdef VERBOSE
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
#else
  (void)(fmt);
#endif
}

// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err)
{
  printf("[ERROR] %s\n", msg);
  printf("  Kernel Code: %i\n", err->etype.etype);
  printf("  Kernel Msg: %s\n", err->msg);
}

// free an error
void free_error(Error* error)
{
  free(error->msg);
  free(error);
}

// kernel will call this to allocate our errors. This can be used to create an "engine native" type
// error
EngineError* allocate_error(KernelError etype, const KernelStringSlice msg)
{
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

#ifdef WIN32 // windows doesn't have strndup
char *strndup(const char *s, size_t n) {
  size_t len = strnlen(s, n);
  char *p = malloc(len + 1);
  if (p) {
    memcpy(p, s, len);
    p[len] = '\0';
  }
  return p;
}
#endif

// utility to turn a slice into a char*
void* allocate_string(const KernelStringSlice slice)
{
  return strndup(slice.ptr, slice.len);
}

// utility function to convert key/val into slices and set them on a builder
// returns false on failure
bool set_builder_opt(EngineBuilder* engine_builder, char* key, char* val)
{
  KernelStringSlice key_slice = { key, strlen(key) };
  KernelStringSlice val_slice = { val, strlen(val) };
  ExternResultbool res = set_builder_option(engine_builder, key_slice, val_slice);
  if (res.tag != Okbool) {
    print_error("Failed to set builder option.", (Error*)res.err);
    free_error((Error*)res.err);
    return false;
  }
  return true;
}

// utility to print out a metric id as a uuid
void print_metric_id(const char* name, MetricId id) {
  const uint8_t* b = id.bytes;
  printf("  %s: "
         "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x\n",
         name,
         b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
         b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

// utility to print out a table type
void print_table_type(TableType tt) {
  printf("  table_type:");
  switch (tt) {
  case TableTypePathBased: printf(" PathBased,\n"); break;
  case TableTypeCatalogManaged: printf(" CatalogManaged,\n"); break;
  }
}

#define PM_START(name) printf("\nMetric " #name " {\n")
#define PM_END printf("}\n\n");
#define PM_ID(s, f) print_metric_id(#f, (s).f)
#define PM_U64(s, f)   printf("  " #f ": %" PRIu64 ",\n", (uint64_t)(s).f)
#define PM_BOOL(s, f)  printf("  " #f ": %s,\n", (s).f ? "true" : "false")
#define PM_SLICE(s, f) (s.f.len>0?printf("  " #f ": %.*s,\n", (int)(s).f.len, (s).f.ptr):printf("  " #f ": \"\",\n"))

// print out metrics as json(ish) objects
void print_metric(MetricEvent event) {
  switch (event.tag) {
  case MetricEventLogSegmentLoadSuccess:
    PM_START(LogSegmentLoadSuccess);
    LogSegmentLoadSuccess lsls = event.log_segment_load_success;
    PM_ID(lsls, operation_id);
    PM_SLICE(lsls, correlation_id);
    print_table_type(lsls.table_type);
    PM_U64(lsls, duration_ns);
    PM_U64(lsls, num_commit_files);
    PM_U64(lsls, num_checkpoint_files);
    PM_U64(lsls, num_compaction_files);
    PM_BOOL(lsls, has_latest_crc_file);
    PM_END;
    return;

  case MetricEventLogSegmentLoadFailure:
    PM_START(LogSegmentLoadFailure);
    LogSegmentLoadFailure lslf = event.log_segment_load_failure;
    PM_ID(lslf, operation_id);
    PM_SLICE(lslf, correlation_id);
    print_table_type(lslf.table_type);
    PM_END;
    return;

  case MetricEventProtocolMetadataLoadSuccess:
    PM_START(ProtocolMetadataLoadSuccess);
    ProtocolMetadataLoadSuccess pmls = event.protocol_metadata_load_success;
    PM_ID(pmls, operation_id);
    PM_SLICE(pmls, correlation_id);
    print_table_type(pmls.table_type);
    PM_U64(pmls, duration_ns);
    PM_END;
    return;

  case MetricEventProtocolMetadataLoadFailure:
    PM_START(ProtocolMetadataLoadFailure);
    ProtocolMetadataLoadFailure pmlf = event.protocol_metadata_load_failure;
    PM_ID(pmlf, operation_id);
    PM_SLICE(pmlf, correlation_id);
    print_table_type(pmlf.table_type);
    PM_END;
    return;

  case MetricEventSnapshotBuildSuccess:
    PM_START(SnapshotBuildSuccess);
    SnapshotBuildSuccess sbs = event.snapshot_build_success;
    PM_ID(sbs, operation_id);
    PM_SLICE(sbs, correlation_id);
    print_table_type(sbs.table_type);
    PM_U64(sbs, version);
    PM_U64(sbs, duration_ns);
    PM_END;
    return;

  case MetricEventSnapshotBuildFailure:
    PM_START(SnapshotBuildFailure);
    SnapshotBuildFailure slf = event.snapshot_build_failure;
    PM_ID(slf, operation_id);
    PM_SLICE(slf, correlation_id);
    print_table_type(slf.table_type);
    PM_END;
    return;

  case MetricEventTransactionCommitSuccess:
    PM_START(TransactionCommitSuccess);
    TransactionCommitSuccess tcs = event.transaction_commit_success;
    PM_ID(tcs, operation_id);
    PM_SLICE(tcs, correlation_id);
    print_table_type(tcs.table_type);
    PM_U64(tcs, commit_version);
    PM_U64(tcs, num_add_files);
    PM_U64(tcs, num_remove_files);
    PM_U64(tcs, num_dv_updates);
    PM_U64(tcs, add_files_bytes);
    PM_U64(tcs, remove_files_bytes);
    PM_BOOL(tcs, is_blind_append);
    PM_BOOL(tcs, data_change);
    printf("operation: %.*s,\n", (int)tcs.operation.len, tcs.operation.ptr);
    PM_U64(tcs, prepare_duration_ns);
    PM_U64(tcs, committer_duration_ns);
    PM_U64(tcs, total_duration_ns);
    PM_END;
    return;

  case MetricEventTransactionCommitFailure:
    PM_START(TransactionCommitFailure);
    TransactionCommitFailure tcf = event.transaction_commit_failure;
    PM_ID(tcf, operation_id);
    PM_SLICE(tcf, correlation_id);
    print_table_type(tcf.table_type);
    printf("  reason:");
    switch (tcf.reason) {
    case CommitFailureReasonConflict: printf(" Conflict,\n"); break;
    case CommitFailureReasonRetryableIo: printf(" Retryable IO,\n"); break;
    case CommitFailureReasonError: printf(" Error,\n"); break;
    }
    PM_END;
    return;

  case MetricEventDomainMetadataLoadSuccess:
    PM_START(DomainMetadataLoadSuccess);
    DomainMetadataLoadSuccess dmls = event.domain_metadata_load_success;
    PM_BOOL(dmls, from_cache);
    PM_U64(dmls, num_domains_returned);
    PM_U64(dmls, duration_ns);
    PM_END;
    return;

  case MetricEventDomainMetadataLoadFailure:
    PM_START(DomainMetadataLoadFailure);
    PM_END;
    return;

  case MetricEventSetTransactionLoadSuccess:
    PM_START(SetTransactionLoadSuccess);
    SetTransactionLoadSuccess stls = event.set_transaction_load_success;
    PM_BOOL(stls, from_cache);
    PM_BOOL(stls, found);
    PM_U64(stls, duration_ns);
    PM_END;
    return;

  case MetricEventSetTransactionLoadFailure:
    PM_START(SetTransactionLoadFailure);
    PM_END;
    return;

  case MetricEventCrcReadSuccess:
    PM_START(CrcReadSuccess);
    CrcReadSuccess crs = event.crc_read_success;
    PM_U64(crs, bytes_read);
    PM_U64(crs, duration_ns);
    PM_END;
    return;

  case MetricEventCrcReadFailure:
    PM_START(CrcReadFailure);
    PM_END;
    return;

  case MetricEventJsonReadCompleted:
    PM_START(MetricEventJsonReadCompleted);
    JsonReadCompleted jrc = event.json_read_completed;
    PM_U64(jrc, num_files);
    PM_U64(jrc, bytes_read);
    PM_END;
    return;

  case MetricEventParquetReadCompleted:
    PM_START(MetricEventParquetReadCompleted);
    ParquetReadCompleted prc = event.parquet_read_completed;
    PM_U64(prc, num_files);
    PM_U64(prc, bytes_read);
    PM_END;
    return;

  case MetricEventScanMetadataCompleted:
    PM_START(ScanMetadataCompleted);
    ScanMetadataCompleted smc = event.scan_metadata_completed;
    PM_ID(smc, operation_id);
    PM_SLICE(smc, correlation_id);
    print_table_type(smc.table_type);
    printf("  scan_type:");
    switch (smc.scan_type) {
    case ScanTypeSequentialPhase: printf(" Sequential Phase,\n"); break;
    case ScanTypeParallelPhase: printf(" Parallel Phase,\n"); break;
    case ScanTypeFull: printf(" Full,\n"); break;
    }
    PM_U64(smc, duration_ns);
    PM_U64(smc, num_add_files_seen);
    PM_U64(smc, num_active_add_files);
    PM_U64(smc, active_add_files_bytes);
    PM_U64(smc, num_remove_files_seen);
    PM_U64(smc, num_non_file_actions);
    PM_U64(smc, num_predicate_filtered);
    PM_U64(smc, peak_hash_set_size);
    PM_U64(smc, dedup_visitor_time_ns);
    PM_U64(smc, predicate_eval_time_ns);
    PM_END;
    return;

  case MetricEventStorageListCompleted:
    PM_START(MetricEventStorageListCompleted);
    StorageListCompleted slc = event.storage_list_completed;
    PM_U64(slc, duration_ns);
    PM_U64(slc, num_files);
    PM_END;
    return;

  case MetricEventStorageReadCompleted:
    PM_START(MetricEventStorageReadCompleted);
    StorageReadCompleted src = event.storage_read_completed;
    PM_U64(src, duration_ns);
    PM_U64(src, num_files);
    PM_U64(src, bytes_read);
    PM_END;
    return;

  case MetricEventStorageCopyCompleted:
    PM_START(MetricEventStorageCopyCompleted);
    StorageCopyCompleted scc = event.storage_copy_completed;
    PM_U64(scc, duration_ns);
    PM_END;
    return;

  default: printf("\n\n WARNING: Got an unknown metric \n\n"); return;
  }
}
