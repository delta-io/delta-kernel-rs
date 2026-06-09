#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <sub-flow> <expected_file>
#
# Seeds a temp dir by copying the appropriate kernel-bundled fixture, scrubs any
# pre-existing checkpoint state (V1 + V2 manifest, sidecars, _last_checkpoint hint)
# so the C binary exercises checkpoint EMISSION rather than READING, runs
# ./checkpoint_table against it, then diffs output against the expected file.
#
# Fixtures:
#   inline           -> kernel/tests/data/app-txn-no-checkpoint/    (non-V2, no checkpoint)
#   v2_no_sidecar    -> kernel/tests/data/v2-parquet-sidecars-struct-stats-only/
#   v2_with_sidecars -> kernel/tests/data/v2-parquet-sidecars-struct-stats-only/

SUB_FLOW="$1"
EXPECTED="$2"
shift 2

# Resolve the kernel-data root relative to this script. The runner lives at
# ffi/tests/checkpoint-table-testing/; the fixtures live at kernel/tests/data/.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd -P)
KERNEL_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd -P)

case "$SUB_FLOW" in
    inline)
        FIXTURE="$KERNEL_ROOT/kernel/tests/data/app-txn-no-checkpoint"
        ;;
    v2_no_sidecar|v2_with_sidecars)
        FIXTURE="$KERNEL_ROOT/kernel/tests/data/v2-parquet-sidecars-struct-stats-only"
        ;;
    *)
        echo "ERROR: unknown sub-flow $SUB_FLOW" >&2
        exit 1
        ;;
esac

if [ ! -d "$FIXTURE" ]; then
    echo "ERROR: fixture not found at $FIXTURE" >&2
    exit 1
fi

TABLE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/checkpoint_table_example.XXXXXX")
# macOS resolves /var -> /private/var when canonicalising paths inside the kernel; match the
# write-table example's pattern.
TABLE_DIR=$(cd "$TABLE_DIR" && pwd -P)
OUT_FILE=$(mktemp)
trap 'rm -rf "$TABLE_DIR" "$OUT_FILE"' EXIT

# Seed by copying the entire fixture.
cp -R "$FIXTURE"/. "$TABLE_DIR/"

# Scrub any pre-existing checkpoint artifacts so the test exercises EMISSION, not READING.
# Cover both V1-shape (`<version>.checkpoint.parquet`) and V2-shape
# (`<version>.checkpoint.<uuid>.parquet`) manifest filenames, plus the sidecar directory and
# the _last_checkpoint hint file. The V1 glob is harmless on V2 fixtures; the V2 glob is
# harmless on V1 fixtures; both are kept for shape-agnostic correctness.
rm -f "$TABLE_DIR"/_delta_log/*.checkpoint.parquet
rm -f "$TABLE_DIR"/_delta_log/*.checkpoint.*.parquet
rm -rf "$TABLE_DIR"/_delta_log/_sidecars/
rm -f "$TABLE_DIR"/_delta_log/_last_checkpoint

# Sanity check: scrub left no checkpoint artifacts behind.
if ls "$TABLE_DIR"/_delta_log/ | grep -q checkpoint; then
    echo "ERROR: scrub failed -- checkpoint artifacts remain in $TABLE_DIR/_delta_log/" >&2
    ls -la "$TABLE_DIR"/_delta_log/ >&2
    exit 1
fi

./checkpoint_table "$TABLE_DIR" "$SUB_FLOW" \
    | sed -E "s|$TABLE_DIR|<TABLE>|g" \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
