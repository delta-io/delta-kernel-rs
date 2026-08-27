#pragma once

#include <stdbool.h>

#include <delta_kernel_ffi.h>

/**
 * Printing of a table's column defaults (the `allowColumnDefaults` writer feature), shared by the
 * read-table and write-table examples.
 *
 * The kernel visits defaults in an unspecified order, so these collect them and sort by name before
 * printing (a connector that does not care about order can print straight from the callback). The
 * kernel does not evaluate the SQL: each default arrives as its `CURRENT_DEFAULT` text, and a real
 * connector evaluates that with its own SQL engine, recovering the target type by joining the
 * column name against the table schema.
 *
 * Two entry points, because which transaction is used matters:
 *  - `print_column_defaults` opens a throwaway transaction, for callers that only want to report
 *    what a table declares (the read-table example).
 *  - `print_column_defaults_of` borrows a transaction the caller already owns. A writer MUST use
 *    this one: `transaction_ack_column_defaults` records the acknowledgement on a specific
 *    transaction, so acking a throwaway would leave the real transaction still gated.
 *
 * Both return false if kernel could not report the defaults, having already printed the error.
 */

// Print the column defaults of a transaction the caller owns. `txn` is borrowed: it stays valid and
// still belongs to the caller, which is what lets a writer ack and then request a write context on
// that same transaction.
bool print_column_defaults_of(ExclusiveTransaction* txn, SharedExternEngine* engine);

// Report what the table at `table_path` declares, without a transaction the caller has to manage.
// Readers only -- see the note on acknowledgement above.
bool print_column_defaults(KernelStringSlice table_path, SharedExternEngine* engine);
